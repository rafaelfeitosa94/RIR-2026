"""
Publicador continuo do RIR 2026: coleta -> JSON -> commit no GitHub.

Roda nesta maquina durante o evento. A cada ciclo:

  1. chama a API da Zig e baixa as horas que faltam (api_transacoes_rir);
  2. monta o DataFrame ja renomeado (dataframe_rir);
  3. escreve os JSONs em publico/;
  4. faz commit e push do que mudou.

O app do Lovable le esses JSONs por HTTP (raw.githubusercontent.com). Ele nao
executa Python: o GitHub aqui e so transporte de arquivo.

FORMATO PUBLICADO
-----------------
publico/
  indice.json              registro de offsets - as horas ja fechadas
  resumo.json              KPIs agregados (pequeno, reescrito todo ciclo)
  recentes.json            ultimas N transacoes, para o feed ao vivo
  horas/2026-09-02T13.json detalhe completo de uma hora ja fechada

POR QUE NAO SE PUBLICA A HORA CORRENTE INTEIRA
----------------------------------------------
Cada linha ocupa ~870 bytes. Numa hora de pico de festival (15 mil transacoes)
o arquivo da hora passa de 18 MB; reescrever isso a cada 2 minutos gera ~560
MB de objetos git POR HORA, e o repositorio estoura em um ou dois dias.

Por isso os arquivos grandes sao imutaveis: uma hora so e gravada quando ja
fechou (definitiva), uma unica vez, e nunca mais muda - o app pode cachear
para sempre. O que se move a cada ciclo sao apenas resumo.json e
recentes.json, ambos pequenos e de tamanho limitado independentemente do
volume de vendas.

O indice e o que torna a leitura incremental: lista as horas fechadas e
disponiveis. O app guarda quais ja consumiu e so busca as novas; o que ainda
nao fechou ele acompanha por resumo.json e recentes.json.

LATENCIA
--------
Isto NAO e streaming de verdade. A latencia e a soma de:
  * o intervalo do ciclo (padrao 120s);
  * o tempo de commit/push;
  * o cache do raw.githubusercontent.com, que serve conteudo por ate ~5 min.
Na pratica, algo entre 2 e 8 minutos entre a venda e o dashboard. Para tempo
real de verdade seria preciso um banco com push (Supabase Realtime) ou um
endpoint SSE proprio.

DADOS PESSOAIS
--------------
As transacoes cashless trazem documento_cliente (CPF) e email_cliente. Um
repositorio que o navegador le sem autenticacao e publico na pratica, entao
essas duas colunas sao REMOVIDAS por padrao. --incluir-dados-pessoais mantem,
e so deve ser usado em repositorio privado com token no app.

Uso:
    python stream_rir.py                    # publica em ciclo continuo
    python stream_rir.py --uma-vez          # um ciclo so
    python stream_rir.py --sem-git          # gera os JSONs, nao commita
    python stream_rir.py --intervalo 300    # ciclo de 5 minutos
"""

import argparse
import hashlib
import json
import os
import subprocess
import time
from datetime import datetime

import pandas as pd

from api_transacoes_rir import (
    ARQ_ESTADO,
    CODIGO_EVENTO,
    EVENTO_FIM,
    EVENTO_INICIO,
    PASTA_DADOS,
    carregar_estado,
    coletar,
)
from dataframe_rir import COLUNAS_TRANSACAO, carregar_transacoes

# ---------------------------------------------------------------- arquivos
PASTA_PUBLICO = "publico"
PASTA_HORAS = "horas"
ARQ_INDICE = "indice.json"
ARQ_RESUMO = "resumo.json"
ARQ_RECENTES = "recentes.json"
ARQ_PAINEL = "painel.json"

INTERVALO_PADRAO = 120

# Quantas transacoes o feed ao vivo carrega. Limita o tamanho do unico
# arquivo de detalhe que e reescrito a cada ciclo: 300 linhas ~ 260 KB.
LIMITE_RECENTES = 300

# Transacoes no feed do painel (arquivo pequeno, reescrito a cada ciclo).
LIMITE_FEED = 25

# Colunas que nao vao para um repositorio lido pelo navegador.
COLUNAS_PESSOAIS = [
    COLUNAS_TRANSACAO["documento_cliente"],
    COLUNAS_TRANSACAO["email_cliente"],
]

# Atalhos para os nomes de destino usados nas agregacoes.
COL_ID = COLUNAS_TRANSACAO["transacao_id"]
COL_REALIZACAO = COLUNAS_TRANSACAO["data_hora_realizacao"]
COL_VALOR = COLUNAS_TRANSACAO["valor"]
COL_VALOR_TOTAL = COLUNAS_TRANSACAO["valor_total"]
COL_QUANTIDADE = COLUNAS_TRANSACAO["quantidade"]
COL_OPERACAO = COLUNAS_TRANSACAO["operacao"]
COL_PONTO = COLUNAS_TRANSACAO["nome_ponto"]
COL_PRODUTO = COLUNAS_TRANSACAO["produto"]
COL_CATEGORIA = COLUNAS_TRANSACAO["categoria_produto"]
COL_PAGAMENTO = COLUNAS_TRANSACAO["forma_pagamento"]


# ---------------------------------------------------------------- agregacao
def _sinal(df):
    """+1 para venda, -1 para cancelamento/estorno.

    A API assina o campo 'valor' da transacao (um cancelamento vem -25.0) mas
    NAO assina o 'valor_total' do produto, que continua positivo. Somar o
    valor_total cru infla o faturamento: numa amostra com 4 compras e 4
    cancelamentos, a soma crua deu 152 quando o liquido era 0.
    """
    return df[COL_VALOR].fillna(0).apply(lambda v: -1 if v < 0 else 1)


def montar_resumo(df):
    """KPIs agregados, com cancelamentos abatidos corretamente."""
    if df.empty:
        return {
            "atualizado_em": datetime.now().isoformat(timespec="seconds"),
            "codigo_evento": CODIGO_EVENTO,
            "totais": {"transacoes": 0, "itens": 0, "valor_liquido": 0.0},
            "por_ponto": [], "por_produto": [], "por_categoria": [],
            "por_forma_pagamento": [], "por_hora": [],
        }

    df = df.copy()
    sinal = _sinal(df)
    df["_valor_liquido"] = df[COL_VALOR_TOTAL].fillna(0) * sinal
    df["_qtd_liquida"] = df[COL_QUANTIDADE].fillna(0).astype(float) * sinal
    df["_hora"] = df[COL_REALIZACAO].dt.strftime("%Y-%m-%dT%H")
    df["_dia"] = df[COL_REALIZACAO].dt.strftime("%Y-%m-%d")

    def agrupar(coluna, cronologico=False):
        g = (df.groupby(coluna, dropna=True)
               .agg(valor_liquido=("_valor_liquido", "sum"),
                    itens=("_qtd_liquida", "sum"),
                    transacoes=(COL_ID, "nunique"),
                    linhas=(COL_ID, "count"))
               .reset_index())
        # Series temporais pedem ordem de tempo; o resto e ranking.
        g = (g.sort_values(coluna) if cronologico
             else g.sort_values("valor_liquido", ascending=False))
        return json.loads(g.to_json(orient="records", force_ascii=False))

    def serie_horaria():
        """Serie por hora SEM buracos, do primeiro ao ultimo registro.

        Horas sem venda precisam existir valendo zero: se ficarem de fora, o
        grafico liga o ponto anterior ao seguinte e desenha faturamento numa
        madrugada em que nao houve venda nenhuma.
        """
        g = agrupar("_hora", cronologico=True)
        if not g:
            return g

        indice = pd.date_range(df[COL_REALIZACAO].min().floor("h"),
                               df[COL_REALIZACAO].max().floor("h"), freq="h")
        por_chave = {linha["_hora"]: linha for linha in g}
        return [por_chave.get(chave, {"_hora": chave, "valor_liquido": 0.0,
                                      "itens": 0.0, "transacoes": 0, "linhas": 0})
                for chave in indice.strftime("%Y-%m-%dT%H")]

    vendas = df[sinal > 0]
    cancelamentos = df[sinal < 0]

    return {
        "atualizado_em": datetime.now().isoformat(timespec="seconds"),
        "codigo_evento": CODIGO_EVENTO,
        "totais": {
            "transacoes": int(df[COL_ID].nunique()),
            "transacoes_venda": int(vendas[COL_ID].nunique()),
            "transacoes_cancelamento": int(cancelamentos[COL_ID].nunique()),
            "itens": float(df["_qtd_liquida"].sum()),
            "valor_liquido": round(float(df["_valor_liquido"].sum()), 2),
            "valor_bruto_vendas": round(float(vendas["_valor_liquido"].sum()), 2),
            "valor_cancelado": round(float(-cancelamentos["_valor_liquido"].sum()), 2),
            "ultima_transacao": df[COL_REALIZACAO].max().isoformat(),
        },
        "por_ponto": agrupar(COL_PONTO),
        "por_produto": agrupar(COL_PRODUTO),
        "por_categoria": agrupar(COL_CATEGORIA),
        "por_forma_pagamento": agrupar(COL_PAGAMENTO),
        "por_hora": serie_horaria(),
        "por_dia": agrupar("_dia", cronologico=True),
    }


# -------------------------------------------------------------------- feed
def montar_feed(df, limite=LIMITE_FEED):
    """Ultimas transacoes, uma linha por VENDA (nao por produto).

    Como 23% das vendas tem mais de um item, mostrar so o produto mais caro ao
    lado do valor total faria parecer que aquele item custou a venda inteira -
    por isso vem 'outros_produtos' para o front marcar um "+N".
    """
    if df.empty:
        return []

    df = df.sort_values(COL_REALIZACAO)
    ids_recentes = df[COL_ID].drop_duplicates().tail(limite)
    recentes = df[df[COL_ID].isin(ids_recentes)]

    feed = []
    for _, grupo in recentes.groupby(COL_ID, sort=False):
        com_produto = grupo[grupo[COL_VALOR_TOTAL].notna()]
        # Transacoes sem produto (recarga, ativacao) nao tem item principal.
        principal = (com_produto.loc[com_produto[COL_VALOR_TOTAL].idxmax()]
                     if not com_produto.empty else grupo.iloc[0])

        valor = float(principal[COL_VALOR]) if pd.notna(principal[COL_VALOR]) else 0.0
        qtd = grupo[COL_QUANTIDADE].sum(min_count=1)

        feed.append({
            "id": int(principal[COL_ID]),
            "hora": principal[COL_REALIZACAO].strftime("%H:%M:%S"),
            "ponto": principal[COL_PONTO],
            "produto": principal[COL_PRODUTO],
            "outros_produtos": max(int(grupo[COL_PRODUTO].nunique()) - 1, 0),
            "quantidade": None if pd.isna(qtd) else int(qtd),
            "valor": valor,
            "operacao": principal[COL_OPERACAO],
            "pagamento": principal[COL_PAGAMENTO],
            "cancelamento": bool(valor < 0),
        })

    feed.sort(key=lambda f: f["hora"], reverse=True)
    return feed


def montar_snapshot(df):
    """Pacote que o painel consome: agregados + feed + frescor do dado."""
    if df.empty:
        return {"vazio": True,
                "atualizado_em": datetime.now().isoformat(timespec="seconds"),
                "resumo": montar_resumo(df), "feed": [], "atraso_s": None}

    ultima = df[COL_REALIZACAO].max()
    return {
        "vazio": False,
        "atualizado_em": datetime.now().isoformat(timespec="seconds"),
        "ultima_transacao": ultima.isoformat(),
        "atraso_s": max((pd.Timestamp.now() - ultima).total_seconds(), 0),
        "resumo": montar_resumo(df),
        "feed": montar_feed(df),
    }


# --------------------------------------------------------------- publicacao
def _gravar_se_mudou(caminho, conteudo):
    """Grava so quando o conteudo muda, para nao gerar commit inutil."""
    novo = hashlib.sha256(conteudo.encode("utf-8")).hexdigest()
    if os.path.exists(caminho):
        with open(caminho, encoding="utf-8") as arquivo:
            if hashlib.sha256(arquivo.read().encode("utf-8")).hexdigest() == novo:
                return False
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as arquivo:
        arquivo.write(conteudo)
    return True


def publicar(df, pasta_publico=PASTA_PUBLICO, pasta_dados=PASTA_DADOS,
             incluir_pessoais=False, limite_recentes=LIMITE_RECENTES,
             sem_detalhe=False):
    """Escreve indice, resumo, recentes e as horas ja fechadas.

    Devolve a lista de arquivos que mudaram neste ciclo.
    """
    if not incluir_pessoais:
        df = df.drop(columns=[c for c in COLUNAS_PESSOAIS if c in df.columns])

    definitivas = carregar_estado(os.path.join(pasta_dados, ARQ_ESTADO))
    alterados = []
    entradas = []

    if not df.empty:
        horas = df[COL_REALIZACAO].dt.floor("h")
        for hora, grupo in df.groupby(horas):
            # So publica o detalhe de horas que ja fecharam. A hora corrente
            # ainda recebe lancamentos e reescrever seu arquivo a cada ciclo
            # inflaria o repositorio - ela e acompanhada por resumo/recentes.
            if hora.strftime("%Y-%m-%d %H:%M:%S") not in definitivas:
                continue

            rotulo = hora.strftime("%Y-%m-%dT%H")
            relativo = f"{PASTA_HORAS}/{rotulo}.json"

            if not sem_detalhe:
                conteudo = grupo.to_json(orient="records", date_format="iso",
                                         force_ascii=False)
                if _gravar_se_mudou(os.path.join(pasta_publico, PASTA_HORAS,
                                                 f"{rotulo}.json"), conteudo):
                    alterados.append(relativo)

            entradas.append({
                "hora": rotulo,
                "arquivo": None if sem_detalhe else relativo,
                "linhas": int(len(grupo)),
                "transacoes": int(grupo[COL_ID].nunique()),
                # Toda hora listada aqui ja fechou: o arquivo nunca mais muda
                # e o app pode cachear para sempre.
                "definitiva": True,
            })

    indice = {
        "codigo_evento": CODIGO_EVENTO,
        "atualizado_em": datetime.now().isoformat(timespec="seconds"),
        "periodo": {"inicio": EVENTO_INICIO.isoformat(),
                    "fim": EVENTO_FIM.isoformat()},
        "contem_dados_pessoais": incluir_pessoais,
        "horas_fechadas": entradas,
        "ao_vivo": {"resumo": ARQ_RESUMO, "recentes": ARQ_RECENTES,
                    "limite_recentes": limite_recentes},
    }

    if _gravar_se_mudou(os.path.join(pasta_publico, ARQ_INDICE),
                        json.dumps(indice, indent=2, ensure_ascii=False)):
        alterados.append(ARQ_INDICE)

    if _gravar_se_mudou(os.path.join(pasta_publico, ARQ_RESUMO),
                        json.dumps(montar_resumo(df), indent=2, ensure_ascii=False)):
        alterados.append(ARQ_RESUMO)

    recentes = (df.sort_values(COL_REALIZACAO).tail(limite_recentes)
                if not df.empty else df)
    if _gravar_se_mudou(os.path.join(pasta_publico, ARQ_RECENTES),
                        recentes.to_json(orient="records", date_format="iso",
                                         force_ascii=False)):
        alterados.append(ARQ_RECENTES)

    # Feed pronto para o painel estatico (GitHub Pages), que nao tem servidor
    # para agrupar as vendas por transacao.
    if _gravar_se_mudou(os.path.join(pasta_publico, ARQ_PAINEL),
                        json.dumps({"feed": montar_feed(df)},
                                   indent=2, ensure_ascii=False)):
        alterados.append(ARQ_PAINEL)

    return alterados


# ---------------------------------------------------------------------- git
def _git(*args, cwd="."):
    """Roda um comando git e devolve (ok, saida)."""
    try:
        r = subprocess.run(("git",) + args, cwd=cwd, capture_output=True,
                           text=True, timeout=180)
        return r.returncode == 0, (r.stdout + r.stderr).strip()
    except (OSError, subprocess.SubprocessError) as e:
        return False, f"{type(e).__name__}: {e}"


def repositorio_pronto(cwd="."):
    """Diz se da para commitar/pushar daqui."""
    ok, _ = _git("rev-parse", "--is-inside-work-tree", cwd=cwd)
    if not ok:
        return False, "esta pasta nao e um repositorio git (rode o setup do README)"
    ok, saida = _git("remote", "get-url", "origin", cwd=cwd)
    if not ok:
        return False, "o repositorio nao tem remote 'origin' configurado"
    return True, saida


def publicar_no_git(pasta_publico=PASTA_PUBLICO, cwd="."):
    """Commita e empurra o que mudou em publico/. Nunca levanta excecao."""
    pronto, detalhe = repositorio_pronto(cwd)
    if not pronto:
        print(f"  git: {detalhe} - arquivos gravados localmente, sem push")
        return False

    ok, saida = _git("add", "--", pasta_publico, cwd=cwd)
    if not ok:
        print(f"  git add falhou: {saida}")
        return False

    ok, saida = _git("diff", "--cached", "--quiet", "--", pasta_publico, cwd=cwd)
    if ok:
        print("  git: nada a commitar")
        return True

    marca = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ok, saida = _git("commit", "-m", f"dados RIR26 {marca}", cwd=cwd)
    if not ok:
        print(f"  git commit falhou: {saida}")
        return False

    ok, saida = _git("push", cwd=cwd)
    if not ok:
        # Push e a parte que depende de rede e credencial: falhar aqui nao
        # pode derrubar o ciclo, o proximo push leva os commits acumulados.
        print(f"  git push falhou (sera reenviado no proximo ciclo): {saida}")
        return False

    print(f"  git: publicado ({marca})")
    return True


# -------------------------------------------------------------------- ciclo
def ciclo(args):
    """Um ciclo completo: coleta, monta, publica, commita."""
    marca = datetime.now().strftime("%H:%M:%S")
    print(f"\n[{marca}] coletando...")

    novas_t, novas_a, tokens = coletar(args.pasta,
                                       renovar_sempre=not args.reusar_token)
    print(f"  +{novas_t} transacoes ({tokens} tokens)")

    df = carregar_transacoes(args.pasta)
    alterados = publicar(df, args.publico, args.pasta,
                         args.incluir_dados_pessoais,
                         args.recentes, args.sem_detalhe)

    if not alterados:
        print("  nada mudou")
        return

    print(f"  {len(alterados)} arquivo(s) atualizado(s): "
          f"{', '.join(alterados[:4])}{' ...' if len(alterados) > 4 else ''}")

    if not args.sem_git:
        publicar_no_git(args.publico)


def main():
    parser = argparse.ArgumentParser(
        description="Coleta o RIR 2026 e publica os JSONs no repositorio.")
    parser.add_argument("--intervalo", type=int, default=INTERVALO_PADRAO,
                        help=f"segundos entre ciclos (padrao: {INTERVALO_PADRAO})")
    parser.add_argument("--uma-vez", action="store_true",
                        help="roda um ciclo so e sai")
    parser.add_argument("--sem-git", action="store_true",
                        help="grava os JSONs mas nao commita nem faz push")
    parser.add_argument("--incluir-dados-pessoais", action="store_true",
                        help="mantem CPF e e-mail do cliente no JSON "
                             "(so em repositorio privado)")
    parser.add_argument("--recentes", type=int, default=LIMITE_RECENTES,
                        help=f"transacoes no feed ao vivo (padrao: {LIMITE_RECENTES})")
    parser.add_argument("--sem-detalhe", action="store_true",
                        help="publica so os agregados, sem os arquivos por hora")
    parser.add_argument("--reusar-token", action="store_true",
                        help="reaproveita o token entre requisicoes")
    parser.add_argument("--pasta", default=PASTA_DADOS,
                        help=f"pasta dos dados brutos (padrao: {PASTA_DADOS})")
    parser.add_argument("--publico", default=PASTA_PUBLICO,
                        help=f"pasta publicada no repo (padrao: {PASTA_PUBLICO})")
    args = parser.parse_args()

    print("=" * 78)
    print(f"Publicador RIR 2026 | evento {CODIGO_EVENTO}")
    if args.incluir_dados_pessoais:
        print("ATENCAO: CPF e e-mail do cliente SERAO publicados no repositorio.")
    else:
        print("CPF e e-mail do cliente sao removidos antes de publicar.")

    if not args.sem_git:
        pronto, detalhe = repositorio_pronto()
        print(f"git: {detalhe}" if pronto else f"git: {detalhe}")

    if args.uma_vez:
        ciclo(args)
        return

    print(f"ciclo a cada {args.intervalo}s. Ctrl+C para parar.")
    try:
        while True:
            inicio = time.monotonic()
            try:
                ciclo(args)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"  erro no ciclo: {type(e).__name__}: {e}")
            espera = args.intervalo - (time.monotonic() - inicio)
            if espera > 0:
                time.sleep(espera)
    except KeyboardInterrupt:
        print("\nPublicador encerrado.")


if __name__ == "__main__":
    main()
