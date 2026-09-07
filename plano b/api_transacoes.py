"""
Lista de Transacoes - API de Relatorios da Zig (WebService v1.05.003).

Documentacao: "Zig - Integracao via WebService Relatorios 1.09.pdf" (secao 2)

  POST /api/relatorios/ListaTransacoes
    header : token_acesso     (obtido no GeraToken ou na requisicao anterior)
    body   : data_inicio      (datetime, obrigatorio)
             data_fim         (datetime, obrigatorio)
             codigo_ponto     (string, opcional)
             codigo_operador  (string, opcional)

  Retorno:
    { "codigo_retorno": 0,
      "mensagem_erro": "",
      "dados": { "transacoes": [...], "movimentos_ambulantes": [...] } }

Cada transacao/movimento traz uma lista "produtos". O script gera dois CSVs
achatados (uma linha por produto): transacoes e movimentos de ambulantes.

Comportamentos observados no WS de producao que NAO constam na documentacao:

  * O intervalo de cada requisicao nao pode passar de 1 hora. Acima disso o
    WS responde codigo_retorno 1007 ("Nao e possivel realizar uma requisicao
    com o intervalo superior a uma hora."), erro ausente da tabela da secao
    2.3. Por isso o script sempre fatia o periodo pedido em blocos de 1 hora.
  * As datas precisam vir em ISO ("2026-09-02T22:00:00"); o formato brasileiro
    dd/MM/yyyy e recusado com 1001.
  * O token_acesso pode ser reaproveitado em varias chamadas ate a sua
    data_expiracao - nao e preciso gerar um por requisicao.

Obs.: como no GeraToken, a documentacao nao explicita o verbo HTTP nem onde
vao os parametros que nao sao de header. O script tenta variacoes de envio
ate uma responder codigo_retorno = 0 e memoriza a que funcionou (em producao
a que responde e post_json).

Uso:
    python api_transacoes.py --inicio "2026-09-02 20:00:00" --fim "2026-09-02 23:00:00"
    python api_transacoes.py --inicio 2026-09-01 --fim 2026-09-30 --json bruto.json
    python api_transacoes.py --inicio 2026-09-01 --fim 2026-09-02 --dev
"""

import argparse
import csv
import json
import os
from datetime import datetime, timedelta

import requests

from api_teste_token import gerar_token

# ---------------------------------------------------------------- endpoints
URL_PROD = "https://app.netpdv.com:5566/api/relatorios/ListaTransacoes"
URL_DEV = "http://devapp.netpdv.info:5566/api/relatorios/ListaTransacoes"

TOKEN_URL_PROD = "https://app.netpdv.com:5566/api/relatorios/GeraToken"
TOKEN_URL_DEV = "http://devapp.netpdv.info:5566/api/relatorios/GeraToken"

# ------------------------------------------------------------- credenciais
TOKEN_PARCEIRO = "09C7DF1421"
CODIGO_EVENTO = 38049

TIMEOUT = 180

# O WS so aceita datas em ISO; o formato "legivel" e usado apenas nos prints.
FORMATO_API = "%Y-%m-%dT%H:%M:%S"
FORMATO_DATA = "%Y-%m-%d %H:%M:%S"

# Limite de intervalo por requisicao imposto pelo WS (erro 1007).
JANELA_MAXIMA = timedelta(hours=1)

CSV_TRANSACOES = "transacoes_api.csv"
CSV_AMBULANTES = "movimentos_ambulantes_api.csv"

# Tabela de erros da documentacao (secao 2.3) + 1007, observado em producao
ERROS = {
    1001: "Falha na obtencao dos parametros (parametro obrigatorio faltando).",
    1003: "Falha ao processar a requisicao (erro interno - contatar a Zig).",
    1004: "Token de acesso invalido.",
    1005: "Token de acesso expirado.",
    1007: "Intervalo superior a uma hora (nao documentado).",
}

# Colunas de saida: campos do registro + campos do produto (secao 2.2)
CAMPOS_TRANSACAO = [
    "transacao_id", "transacao_original_id", "codigo_evento", "nome_evento",
    "data_hora_realizacao", "data_hora_cadastro", "data_hora_confirmacao",
    "sessao", "operacao", "tipo_ponto", "codigo_ponto", "nome_ponto",
    "latitude", "longitude", "operador", "terminal", "valor", "taxa_ativacao",
    "status", "cod_forma_pagamento", "forma_pagamento", "chip",
    "chip_informado_manual", "documento_cliente", "email_cliente",
]

CAMPOS_AMBULANTE = [
    "movimento_ambulante_id", "codigo_evento", "nome_evento",
    "data_hora_realizacao", "data_hora_cadastro", "data_hora_confirmacao",
    "sessao", "operacao", "codigo_ponto", "nome_ponto", "operador",
    "terminal", "valor", "status", "nome_ambulante", "documento_ambulante",
]

CAMPOS_PRODUTO = [
    "categoria_produto", "cod_produto", "cod_produto_parceiro", "produto",
    "fabricante", "quantidade", "valor_unitario", "valor_total",
]


# --------------------------------------------------------------- requisicao
def _requisicao(url, token_acesso, filtros, estrategia, verificar_ssl=True):
    """Monta e dispara uma variacao da chamada de ListaTransacoes."""
    headers = {
        "token_acesso": token_acesso,
        "Accept": "application/json",
    }
    kwargs = {"headers": headers, "timeout": TIMEOUT, "verify": verificar_ssl}

    if estrategia == "post_json":
        headers["Content-Type"] = "application/json"
        kwargs["json"] = filtros
        return requests.post(url, **kwargs)

    if estrategia == "post_form":
        kwargs["data"] = filtros
        return requests.post(url, **kwargs)

    if estrategia == "post_query":
        kwargs["params"] = filtros
        return requests.post(url, **kwargs)

    if estrategia == "post_header":
        # filtros tambem no header, caso o WS espere tudo la
        for chave, valor in filtros.items():
            headers[chave] = str(valor)
        headers["Content-Type"] = "application/json"
        kwargs["json"] = filtros
        return requests.post(url, **kwargs)

    if estrategia == "get_query":
        kwargs["params"] = filtros
        return requests.get(url, **kwargs)

    raise ValueError(f"Estrategia desconhecida: {estrategia}")


def listar_transacoes(token_acesso, data_inicio, data_fim, codigo_ponto=None,
                      codigo_operador=None, url=URL_PROD, verificar_ssl=True,
                      estrategia_preferida=None, verbose=True):
    """Consulta a lista de transacoes de um intervalo.

    Retorna (dados, token_novo, estrategia) em caso de sucesso, onde 'dados'
    e o objeto com transacoes/movimentos_ambulantes. Em caso de falha,
    retorna (None, None, None).

    O token_acesso e rotativo: cada chamada expira o anterior. Se o WS
    devolver um token novo, ele deve ser usado na proxima requisicao.
    """
    filtros = {
        "data_inicio": data_inicio.strftime(FORMATO_API),
        "data_fim": data_fim.strftime(FORMATO_API),
    }
    if codigo_ponto:
        filtros["codigo_ponto"] = codigo_ponto
    if codigo_operador:
        filtros["codigo_operador"] = codigo_operador

    estrategias = ["post_json", "post_form", "post_query", "post_header", "get_query"]
    if estrategia_preferida in estrategias:
        # Ja sabemos qual funciona - tenta ela primeiro e mantem as outras
        # como fallback caso o WS mude de comportamento.
        estrategias.remove(estrategia_preferida)
        estrategias.insert(0, estrategia_preferida)

    for estrategia in estrategias:
        try:
            resp = _requisicao(url, token_acesso, filtros, estrategia, verificar_ssl)
        except requests.exceptions.SSLError as e:
            if verbose:
                print(f"  [{estrategia}] erro de SSL: {e}")
            continue
        except requests.exceptions.RequestException as e:
            if verbose:
                print(f"  [{estrategia}] falha de conexao: {type(e).__name__}: {e}")
            continue

        try:
            corpo = resp.json()
        except ValueError:
            if verbose:
                trecho = resp.text[:200].replace("\n", " ")
                print(f"  [{estrategia}] HTTP {resp.status_code} - resposta nao-JSON: {trecho}")
            continue

        codigo = corpo.get("codigo_retorno")

        if codigo == 0:
            dados = corpo.get("dados") or {}
            # Alguns WS devolvem o proximo token junto do payload.
            token_novo = (dados.get("token_acesso")
                          or corpo.get("token_acesso")
                          or resp.headers.get("token_acesso"))
            if verbose:
                n_tr = len(dados.get("transacoes") or [])
                n_amb = len(dados.get("movimentos_ambulantes") or [])
                print(f"  [{estrategia}] HTTP {resp.status_code} - SUCESSO | "
                      f"{n_tr} transacoes, {n_amb} movimentos de ambulante")
            return dados, token_novo, estrategia

        if verbose:
            msg = corpo.get("mensagem_erro") or ERROS.get(codigo, "")
            print(f"  [{estrategia}] HTTP {resp.status_code} - "
                  f"codigo_retorno={codigo} | {msg}")

        # Erro de token nao muda com a estrategia de envio - o chamador
        # precisa gerar um token novo antes de tentar de novo.
        if codigo in (1004, 1005):
            return None, None, None

    return None, None, None


# -------------------------------------------------------------- achatamento
def _achatar(registros, campos_pai):
    """Explode a lista 'produtos' de cada registro em uma linha por produto.

    Registros sem produtos viram uma unica linha com os campos de produto
    vazios, para nao se perderem na exportacao.
    """
    linhas = []
    for registro in registros:
        base = {campo: registro.get(campo) for campo in campos_pai}
        produtos = registro.get("produtos") or []

        if not produtos:
            linha = dict(base)
            linha.update({campo: None for campo in CAMPOS_PRODUTO})
            linhas.append(linha)
            continue

        for produto in produtos:
            linha = dict(base)
            linha.update({campo: produto.get(campo) for campo in CAMPOS_PRODUTO})
            linhas.append(linha)

    return linhas


def gravar_csv(registros, campos_pai, caminho):
    """Achata os registros e grava em CSV (utf-8-sig para abrir no Excel)."""
    linhas = _achatar(registros, campos_pai)
    colunas = campos_pai + CAMPOS_PRODUTO
    with open(caminho, "w", newline="", encoding="utf-8-sig") as arquivo:
        writer = csv.DictWriter(arquivo, fieldnames=colunas)
        writer.writeheader()
        writer.writerows(linhas)
    print(f"{caminho}: {len(linhas)} linhas x {len(colunas)} colunas")


# ------------------------------------------------------------------ coleta
def fatiar(data_inicio, data_fim, janela=JANELA_MAXIMA):
    """Divide o intervalo em blocos de no maximo 1 hora (limite do WS).

    As fatias sao [inicio, fim] e compartilham o instante da borda, entao a
    coleta faz deduplicacao por id no final.
    """
    passo = min(janela, JANELA_MAXIMA)

    fatias = []
    inicio = data_inicio
    while inicio < data_fim:
        fim = min(inicio + passo, data_fim)
        fatias.append((inicio, fim))
        inicio = fim
    return fatias


def deduplicar(registros, campo_id):
    """Remove repeticoes causadas pela borda compartilhada entre fatias."""
    vistos, unicos = set(), []
    for registro in registros:
        chave = registro.get(campo_id)
        if chave is not None and chave in vistos:
            continue
        if chave is not None:
            vistos.add(chave)
        unicos.append(registro)
    return unicos


def novo_token(token_url, verificar_ssl, verbose=False):
    dados = gerar_token(TOKEN_PARCEIRO, CODIGO_EVENTO, url=token_url,
                        verificar_ssl=verificar_ssl, verbose=verbose)
    return (dados or {}).get("token_acesso")


def coletar(data_inicio, data_fim, codigo_ponto=None, codigo_operador=None,
            url=URL_PROD, token_url=TOKEN_URL_PROD, verificar_ssl=True):
    """Percorre o intervalo em fatias de 1 hora e devolve transacoes e ambulantes."""
    token = novo_token(token_url, verificar_ssl, verbose=True)
    if not token:
        print("Nao foi possivel gerar o token de acesso. Abortando.")
        return [], []

    transacoes, ambulantes = [], []
    estrategia_ok = None
    fatias = fatiar(data_inicio, data_fim)

    for i, (inicio, fim) in enumerate(fatias, 1):
        print(f"\n[{i}/{len(fatias)}] {inicio.strftime(FORMATO_DATA)} "
              f"-> {fim.strftime(FORMATO_DATA)}")

        dados, _, estrategia = listar_transacoes(
            token, inicio, fim, codigo_ponto, codigo_operador, url,
            verificar_ssl, estrategia_ok)

        if dados is None:
            # O token vale ate a data_expiracao, mas pode ser recusado
            # (1004/1005): gera outro e repete a fatia uma vez.
            print("  token recusado - gerando um novo e repetindo a fatia")
            token = novo_token(token_url, verificar_ssl)
            if not token:
                print("  falha ao renovar o token. Abortando.")
                break
            dados, _, estrategia = listar_transacoes(
                token, inicio, fim, codigo_ponto, codigo_operador, url,
                verificar_ssl, estrategia_ok)
            if dados is None:
                print("  fatia ignorada apos a segunda tentativa.")
                continue

        estrategia_ok = estrategia or estrategia_ok
        transacoes.extend(dados.get("transacoes") or [])
        ambulantes.extend(dados.get("movimentos_ambulantes") or [])

    return (deduplicar(transacoes, "transacao_id"),
            deduplicar(ambulantes, "movimento_ambulante_id"))


# --------------------------------------------------------------------- cli
def _data(texto):
    """Aceita 'YYYY-MM-DD' ou 'YYYY-MM-DD HH:MM:SS'."""
    for formato in (FORMATO_DATA, "%Y-%m-%d"):
        try:
            return datetime.strptime(texto, formato)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Data invalida: {texto} (use YYYY-MM-DD ou 'YYYY-MM-DD HH:MM:SS')")


def main():
    parser = argparse.ArgumentParser(
        description="Baixa a lista de transacoes da API de relatorios da Zig.")
    parser.add_argument("--inicio", type=_data, required=True,
                        help="data_inicio (YYYY-MM-DD ou 'YYYY-MM-DD HH:MM:SS')")
    parser.add_argument("--fim", type=_data, required=True,
                        help="data_fim (YYYY-MM-DD ou 'YYYY-MM-DD HH:MM:SS')")
    parser.add_argument("--ponto", default=None, help="codigo_ponto (opcional)")
    parser.add_argument("--operador", default=None, help="codigo_operador (opcional)")
    parser.add_argument("--dev", action="store_true",
                        help="usar o ambiente de desenvolvimento")
    parser.add_argument("--json", dest="salvar_json", default=None,
                        help="tambem grava o retorno bruto neste arquivo .json")
    parser.add_argument("--saida", default=".",
                        help="pasta de saida dos CSVs (padrao: pasta atual)")
    args = parser.parse_args()

    if args.fim <= args.inicio:
        parser.error("--fim precisa ser posterior a --inicio")

    url = URL_DEV if args.dev else URL_PROD
    token_url = TOKEN_URL_DEV if args.dev else TOKEN_URL_PROD
    fatias = len(fatiar(args.inicio, args.fim))

    print("=" * 78)
    print(f"ListaTransacoes | {'DESENVOLVIMENTO' if args.dev else 'PRODUCAO'}: {url}")
    print(f"evento {CODIGO_EVENTO} | "
          f"{args.inicio.strftime(FORMATO_DATA)} -> {args.fim.strftime(FORMATO_DATA)}")
    print(f"{fatias} requisicao(oes) de ate 1 hora (limite do WS)")
    print("=" * 78)

    transacoes, ambulantes = coletar(
        args.inicio, args.fim, args.ponto, args.operador,
        url=url, token_url=token_url)

    print("\n" + "-" * 78)
    print(f"TOTAL: {len(transacoes)} transacoes, "
          f"{len(ambulantes)} movimentos de ambulante")

    if not transacoes and not ambulantes:
        print("Nada retornado - nenhum CSV gerado.")
        return

    if args.salvar_json:
        with open(args.salvar_json, "w", encoding="utf-8") as arquivo:
            json.dump({"transacoes": transacoes,
                       "movimentos_ambulantes": ambulantes},
                      arquivo, indent=2, ensure_ascii=False)
        print(f"{args.salvar_json}: retorno bruto gravado")

    if transacoes:
        gravar_csv(transacoes, CAMPOS_TRANSACAO,
                   os.path.join(args.saida, CSV_TRANSACOES))
    if ambulantes:
        gravar_csv(ambulantes, CAMPOS_AMBULANTE,
                   os.path.join(args.saida, CSV_AMBULANTES))


if __name__ == "__main__":
    main()
