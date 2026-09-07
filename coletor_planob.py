"""
Plano B: coleta as transacoes do RIR 2026 pelo BackOffice da netpdv, para quando
a API de relatorios cai (tem caido ~20:30-02:00 durante o evento).

Fonte: EXPORT CSV do BackOffice (o relatorio na tela pagina em ~419 paginas via
GetListaTransacoesPage; o ProcessReport so devolve a 1a pagina). O caminho que
traz TUDO e o export, em 3 passos, na mesma sessao logada:
  1. POST /Relatorio/ProcessReport  - monta o relatorio/filtro na sessao
     (report=lista_transacao, id=38049, field-tempo-integral=1, field-periodo).
  2. POST /Relatorio/ExportTransacao - dispara a geracao do CSV; responde
     {data:{FilePathName:"...\\EmProcessamento\\<guid>.csv", FileName:"..."}}.
  3. GET  /Relatorio/DownloadFile?filePathName=&fileName=&contentType= - baixa.
     A geracao e assincrona (~4 min); poll ate o CSV vir com linhas de dados.

O CSV vem em cp1252, separador ';', com ~13 linhas de cabecalho do relatorio
antes da linha de colunas ("Transacao;Data Realizacao;..."). Mesmo conteudo da
API, entao reaproveitamos montar_fatos/montar_resumo/montar_feed do stream_rir
e gravamos os JSONs em publico/planob/. O painel (index.html) tem o botao
Principal/Plano B que so troca a base de publico/ para publico/planob/.

Credenciais do BackOffice: fora do codigo (env NETPDV_LOGIN/NETPDV_SENHA ou
credenciais.json {"netpdv_login":"...","netpdv_senha":"..."}). Nunca commitar.

Uso:
    python coletor_planob.py                # loga, exporta o CSV e grava os JSONs
    python coletor_planob.py --csv arq.csv  # parseia um CSV salvo (teste offline)
"""

import argparse
import html as _html
import json
import os
import re
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from dataframe_rir import COLUNAS_TRANSACAO
import stream_rir as S

# ---------------------------------------------------------------- endpoints
BASE = "https://www.netpdv.com/backoffice"
URL_RELATORIO = f"{BASE}/Relatorio/ProcessReport"
URL_EXPORT = f"{BASE}/Relatorio/ExportTransacao"     # dispara a geracao do CSV
URL_DOWNLOAD = f"{BASE}/Relatorio/DownloadFile"      # baixa o CSV pronto

CODIGO_EVENTO = S.CODIGO_EVENTO
PASTA_PLANOB = os.path.join(S.PASTA_PUBLICO, "planob")
TIMEOUT = 300            # o ProcessReport leva ~4 min para responder
POLL_TIMEOUT = 1500      # espera total pelo CSV ficar pronto (EmProcessamento).
                         # 25 min: a geracao no BackOffice varia muito (ja veio
                         # instantanea e ja passou de 10 min sob carga) e cresce
                         # com o evento; folga evita perder o ciclo por pouco.
POLL_INTERVALO = 20      # segundos entre tentativas de DownloadFile

# De-para do cabecalho -> nome interno (destino) usado no DataFrame. So as
# colunas que o painel usa; o resto e ignorado. O CSV usa "Transacao" (sem ID)
# e o HTML usava "Transacao ID" - as duas entram.
COL = COLUNAS_TRANSACAO
DE_PARA_COLUNAS = {
    "transacao": COL["transacao_id"],
    "transacao id": COL["transacao_id"],
    "data realizacao": COL["data_hora_realizacao"],
    "operacao": COL["operacao"],
    "codigo do ponto": COL["codigo_ponto"],
    "nome ponto": COL["nome_ponto"],
    "operador": COL["operador"],
    "categoria produto": COL["categoria_produto"],
    "produto": COL["produto"],
    "forma de pagamento": COL["forma_pagamento"],
    "quantidade": COL["quantidade"],
    "valor": COL["valor_total"],   # "Valor" da linha = total do item (= valor_total)
    "status": COL["status"],
}


# ------------------------------------------------------------------ parser
def _texto(celula_html):
    return _html.unescape(re.sub(r"<[^>]+>", "", celula_html)).strip()


def _norm(cabecalho):
    """Normaliza cabecalho: sem acento, minusculo, espacos colapsados."""
    t = _texto(cabecalho).lower()
    acentos = str.maketrans("áàâãéêíóôõúç", "aaaaeeiooouc")
    return re.sub(r"\s+", " ", t.translate(acentos)).strip()


def parse_relatorio(doc):
    """HTML do ProcessReport -> DataFrame com as colunas de destino do painel.

    O relatorio vem em varias <table> com o mesmo layout; percorremos todas,
    casando cada <td> ao <th> da propria tabela pelo nome da coluna.
    """
    linhas = []
    for tabela in re.findall(r"<table.*?</table>", doc, re.S | re.I):
        # Cabecalho vem do <thead> (nao de todos os <th> da tabela: algumas
        # celulas do corpo tambem sao <th> e inflariam a contagem).
        cab_bloco = re.search(r"<thead.*?</thead>", tabela, re.S | re.I)
        if not cab_bloco:
            continue
        # se o thead tiver mais de uma linha, a ultima e a de colunas reais
        cab_trs = re.findall(r"<tr[^>]*>(.*?)</tr>", cab_bloco.group(0), re.S | re.I)
        fonte_cab = cab_trs[-1] if cab_trs else cab_bloco.group(0)
        cabecalhos = [_norm(t) for t in
                      re.findall(r"<th[^>]*>(.*?)</th>", fonte_cab, re.S | re.I)]
        # so a tabela principal de transacoes interessa
        if "transacao id" not in cabecalhos:
            continue
        corpo = re.search(r"<tbody.*?</tbody>", tabela, re.S | re.I)
        if not corpo:
            continue
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", corpo.group(0), re.S | re.I):
            tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S | re.I)
            if len(tds) != len(cabecalhos):
                continue
            reg = {}
            for cab, td in zip(cabecalhos, tds):
                destino = DE_PARA_COLUNAS.get(cab)
                if destino:
                    reg[destino] = _texto(td)
            if reg.get(COL["transacao_id"]):
                linhas.append(reg)

    return _finalizar(pd.DataFrame(linhas))


def _finalizar(df):
    """Converte tipos, assina cancelamentos e completa colunas ausentes."""
    if df.empty:
        return df

    # O relatorio do BackOffice inclui LINHAS DE RESUMO da comanda (Produto
    # vazio ou "-----"), com o total da transacao, ALEM das linhas por produto.
    # A API so devolve os produtos; somar as duas dobra o faturamento. Ficamos
    # so com as linhas de produto real, como a API.
    prod = df[COL["produto"]].fillna("").astype(str).str.strip()
    resumo = prod.eq("") | prod.str.fullmatch(r"[-–—\s]+")
    if resumo.any():
        print(f"  linhas de resumo descartadas (sem produto): {int(resumo.sum())}")
    df = df[~resumo].copy()
    if df.empty:
        return df

    # Data: CSV traz string BR ("dd/mm/aaaa HH:MM:SS"); o XLSX pode trazer
    # datetime nativo. Tenta o formato BR e, se tudo virar NaT (caso XLSX),
    # cai para a interpretacao geral.
    dh = df[COL["data_hora_realizacao"]]
    conv = pd.to_datetime(dh, format="%d/%m/%Y %H:%M:%S", errors="coerce")
    if conv.isna().all() and dh.notna().any():
        conv = pd.to_datetime(dh, errors="coerce")
    df[COL["data_hora_realizacao"]] = conv

    def num_br(serie):
        # XLSX ja traz numero nativo; CSV traz string BR "1.234,56" -> 1234.56.
        if pd.api.types.is_numeric_dtype(serie):
            return pd.to_numeric(serie, errors="coerce")
        return pd.to_numeric(
            serie.astype(str).str.replace(".", "", regex=False)
                             .str.replace(",", ".", regex=False),
            errors="coerce")

    valor_total = num_br(df[COL["valor_total"]]).fillna(0.0)
    op = df[COL["operacao"]].fillna("").str.lower()
    # Cancelamento/estorno/devolucao -> valor negativo, como na API. O painel
    # usa o sinal de COL_VALOR e o modulo de COL_VALOR_TOTAL.
    cancel = op.str.contains("cancel|estorno|devolu", regex=True)
    df[COL["valor_total"]] = valor_total.abs()
    df[COL["valor"]] = valor_total.abs() * cancel.map({True: -1, False: 1})

    df[COL["quantidade"]] = (pd.to_numeric(df[COL["quantidade"]], errors="coerce")
                             .astype("Int64"))

    # Colunas que o painel espera existir mas o relatorio nao traz.
    for k in ("documento_cliente", "email_cliente"):
        df[COL[k]] = None

    return df


def parse_csv(raw):
    """CSV do export (bytes cp1252, separador ';') -> DataFrame de destino.

    O arquivo tem ~13 linhas de cabecalho do relatorio; a linha de colunas e a
    que comeca com 'Transacao;'. Da linha seguinte em diante sao os dados.
    """
    import csv as _csv
    texto = raw.decode("cp1252", errors="replace") if isinstance(raw, bytes) else raw
    linhas = texto.splitlines()

    # acha a linha de cabecalho das colunas
    i_cab = next((i for i, l in enumerate(linhas)
                  if _norm(l.split(";", 1)[0]) == "transacao"), None)
    if i_cab is None:
        return pd.DataFrame()

    colunas = [_norm(c) for c in linhas[i_cab].split(";")]
    leitor = _csv.reader(linhas[i_cab + 1:], delimiter=";")
    registros = []
    for campos in leitor:
        if len(campos) < len(colunas):
            continue
        reg = {}
        for cab, val in zip(colunas, campos):
            destino = DE_PARA_COLUNAS.get(cab)
            if destino:
                reg[destino] = val.strip()
        if reg.get(COL["transacao_id"]):
            registros.append(reg)

    return _finalizar(pd.DataFrame(registros))


def parse_xlsx(raw):
    """XLSX do export -> DataFrame de destino. Mesmo layout do CSV: ~13 linhas
    de cabecalho do relatorio, a linha de colunas comeca em 'Transacao', e 45
    colunas por linha (celulas separadas). As celulas vem com tipo NATIVO
    (numero/datetime/texto); _finalizar trata os dois casos (nativo x string BR).
    """
    import io
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(raw), data_only=True)
    ws = wb.active
    linhas = list(ws.iter_rows(min_row=1, max_row=ws.max_row, values_only=True))

    i_cab = next((i for i, row in enumerate(linhas)
                  if row and row[0] is not None
                  and _norm(str(row[0])) == "transacao"), None)
    if i_cab is None:
        return pd.DataFrame()

    colunas = [_norm(str(c)) if c is not None else "" for c in linhas[i_cab]]
    registros = []
    for row in linhas[i_cab + 1:]:
        reg = {}
        for cab, val in zip(colunas, row):
            destino = DE_PARA_COLUNAS.get(cab)
            if destino and val is not None:
                reg[destino] = val
        if reg.get(COL["transacao_id"]) not in (None, ""):
            registros.append(reg)

    df = pd.DataFrame(registros)
    if df.empty:
        return df
    # transacao_id vira str (pode vir int/float do xlsx) para casar com o CSV,
    # a semente e o cache nas comparacoes por transacao.
    df[COL["transacao_id"]] = (df[COL["transacao_id"]].astype(str)
                               .str.replace(r"\.0$", "", regex=True))
    return _finalizar(df)


def _parse_export(raw):
    """Roteia o download pelo formato real: XLSX (assinatura zip PK) ou CSV."""
    return parse_xlsx(raw) if _eh_xlsx(raw) else parse_csv(raw)


# --------------------------------------------------------------- publicacao
def gravar_planob(df, pasta=PASTA_PLANOB):
    """Gera os JSONs do Plano B no mesmo formato do principal."""
    os.makedirs(pasta, exist_ok=True)
    fatos = S.montar_fatos(df)
    resumo = S.montar_resumo(df)
    feed = S.montar_feed(df)

    def grava(nome, obj, compacto=False):
        sep = (",", ":") if compacto else None
        with open(os.path.join(pasta, nome), "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False,
                      indent=None if compacto else 2, separators=sep)

    grava("fatos.json", fatos, compacto=True)
    grava("resumo.json", resumo)
    grava("painel.json", {"feed": feed})
    grava("estado.json", {"atualizado_em": datetime.now().isoformat(timespec="seconds"),
                          "fonte": "BackOffice netpdv (Plano B)",
                          "transacoes": int(df[COL["transacao_id"]].nunique())})
    print(f"{pasta}: {len(df)} linhas | "
          f"{df[COL['transacao_id']].nunique()} transacoes | "
          f"R$ {resumo['totais']['valor_liquido']:,.2f}")


# ------------------------------------------------------------------- login
def _credenciais():
    login = os.environ.get("NETPDV_LOGIN")
    senha = os.environ.get("NETPDV_SENHA")
    if not login or not senha:
        caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "credenciais.json")
        try:
            with open(caminho, encoding="utf-8") as f:
                d = json.load(f)
            login = login or d.get("netpdv_login")
            senha = senha or d.get("netpdv_senha")
        except FileNotFoundError:
            pass
    if not login or not senha:
        raise SystemExit("Faltam credenciais do BackOffice: defina NETPDV_LOGIN/"
                         "NETPDV_SENHA ou netpdv_login/netpdv_senha em credenciais.json.")
    return login, senha


URL_LOGIN = f"{BASE}/Authentication/Login"


def login(sessao):
    """Autentica no BackOffice (ASP.NET anti-forgery).

    Fluxo: GET na pagina de login para pegar o cookie e o campo oculto
    __RequestVerificationToken; POST com vchLoginUsuario/vchSenha + o token.
    O cookie de sessao fica na propria sessao para o ProcessReport seguinte.
    """
    login_usuario, senha = _credenciais()

    r = sessao.get(URL_LOGIN, timeout=60)
    r.raise_for_status()
    m = re.search(
        r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
    if not m:
        raise SystemExit("Nao achei o __RequestVerificationToken na pagina de login.")

    r = sessao.post(URL_LOGIN, timeout=60, allow_redirects=True,
                    headers={"Referer": URL_LOGIN,
                             "Origin": "https://www.netpdv.com"},
                    data={"__RequestVerificationToken": m.group(1),
                          "vchLoginUsuario": login_usuario,
                          "vchSenha": senha})
    r.raise_for_status()

    # Sucesso sai da tela de login; se ainda houver campo de senha, falhou.
    if 'type="password"' in r.text and "vchSenha" in r.text:
        raise SystemExit("Login recusado - verifique NETPDV_LOGIN/NETPDV_SENHA.")


def _post_resiliente(sessao, url, *, tentativas=4, espera=15, **kwargs):
    """POST que repete em erro transitorio (5xx / conexao / timeout).

    O BackOffice retorna 500 de forma intermitente, sobretudo a noite (~20:30-
    02:00). Um blip nao deve derrubar o ciclo inteiro. Erros 4xx (request ruim)
    e o esgotamento das tentativas propagam - coletar_online captura e publica
    o cache/semente.
    """
    ultimo = None
    nome = url.rsplit("/", 1)[-1]
    for tentativa in range(1, tentativas + 1):
        try:
            r = sessao.post(url, **kwargs)
            if 500 <= r.status_code < 600:
                raise requests.HTTPError(f"{r.status_code} {r.reason}", response=r)
            r.raise_for_status()
            return r
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
            ultimo = e
            status = getattr(getattr(e, "response", None), "status_code", None)
            if isinstance(e, requests.HTTPError) and status and status < 500:
                raise                              # 4xx nao adianta repetir
            if tentativa < tentativas:
                print(f"  {nome}: {type(e).__name__} ({status or 'rede'}) - "
                      f"tentativa {tentativa}/{tentativas}, repetindo em {espera}s")
                time.sleep(espera)
    raise ultimo


def buscar_relatorio(sessao, inicio, fim):
    """POST no ProcessReport com o filtro Tempo integral e devolve o HTML.

    Recebe o intervalo (inicio, fim) para filtrar a coleta pelo field-periodo.

    IMPORTANTE: field-tempo-integral=0. Com =1 ("Tempo integral" da tela) o
    relatorio varre TODO o historico de vendas ignorando o periodo - e o que
    causava o 500 (peso) e fazia o export sempre comecar em 02/09 e bater no
    teto de 30k pegando as transacoes mais antigas. Desligado, o relatorio
    respeita [inicio, fim], entao a fatia recente fica leve e rapida.
    """
    periodo = (f"{inicio.strftime('%d/%m/%Y %H:%M')} - "
               f"{fim.strftime('%d/%m/%Y %H:%M')}")
    print(f"  periodo: {periodo}")
    dados = [
        ("id", str(CODIGO_EVENTO)),
        ("report", "lista_transacao"),
        ("fields[]", f"field-periodo={periodo}"),
        ("fields[]", "field-clienteData="),
        ("fields[]", "field-formatacao=1"),
        ("fields[]", "field-pontos="),
        ("fields[]", "field-tempo-integral=0"),
        ("fields[]", "field-operador="),
        ("fields[]", "field-terminal="),
        ("fields[]", "field-forma-pagamento="),
        ("fields[]", "field-cartao="),
        ("fields[]", "field-baladeiro-documento="),
        ("fields[]", "field-operacoes="),
        ("fields[]", "field-pedidos="),
        ("fields[]", "field-tipo-relatorio-transacao=1"),
    ]
    r = _post_resiliente(sessao, URL_RELATORIO, data=dados, timeout=TIMEOUT,
                         headers={"X-Requested-With": "XMLHttpRequest",
                                  "Referer": BASE,
                                  "Origin": "https://www.netpdv.com"})
    return r.text


def _eh_xlsx(raw):
    """O export as vezes vem em XLSX (preferencia da conta no BackOffice) em vez
    de CSV. XLSX e um zip, comeca com a assinatura PK\\x03\\x04."""
    return bool(raw) and raw[:4] == b"PK\x03\x04"


def _xlsx_tem_linhas(raw):
    """True se o XLSX ja tem pelo menos UMA linha de dados abaixo do cabecalho.

    O BackOffice devolve na hora um XLSX vazio (so cabecalho) enquanto ainda
    gera o relatorio; sem esta checagem o polling agarraria esse placeholder.
    Sai no 1o dado encontrado, entao e barato mesmo no arquivo cheio.
    """
    import io
    import openpyxl
    try:
        ws = openpyxl.load_workbook(io.BytesIO(raw), data_only=True).active
        achou_cab = False
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, values_only=True):
            c0 = row[0] if row else None
            if achou_cab:
                if c0 not in (None, ""):
                    return True
            elif c0 is not None and _norm(str(c0)) == "transacao":
                achou_cab = True
        return False
    except Exception:
        return False


def _tem_dados(raw):
    """True quando o download ja e o arquivo PRONTO E COM DADOS (nao o
    placeholder vazio que o BackOffice serve enquanto gera). Vale p/ CSV e XLSX."""
    if not raw or len(raw) < 900:            # so o cabecalho (~824 bytes)
        return False
    if _eh_xlsx(raw):
        return _xlsx_tem_linhas(raw)
    return re.search(rb"(?m)^22\d{7};", raw) is not None


def exportar_csv(sessao):
    """Dispara ExportTransacao e faz polling do DownloadFile ate o CSV ficar
    pronto (sai da pasta EmProcessamento). Devolve os bytes do CSV."""
    r = _post_resiliente(sessao, URL_EXPORT, timeout=TIMEOUT,
                         headers={"X-Requested-With": "XMLHttpRequest",
                                  "Referer": BASE})
    data = r.json().get("data") or {}
    caminho = data.get("FilePathName")
    nome = data.get("FileName", "Lista de Transacoes.csv")
    if not caminho:
        # nao-fatal: coletar_online captura e publica o cache/semente
        raise RuntimeError(f"ExportTransacao nao devolveu FilePathName: {r.text[:200]}")
    print(f"  export disparado: {nome}")

    inicio = time.monotonic()
    limite = inicio + POLL_TIMEOUT
    tentativa = 0
    while time.monotonic() < limite:
        tentativa += 1
        d = sessao.get(URL_DOWNLOAD, timeout=TIMEOUT,
                       params={"filePathName": caminho, "fileName": nome,
                               "contentType": ""},
                       headers={"Referer": BASE})
        decorrido = int(time.monotonic() - inicio)
        if d.status_code == 200 and _tem_dados(d.content):
            print(f"  CSV pronto na tentativa {tentativa} em {decorrido}s "
                  f"({len(d.content):,} bytes)")
            return d.content
        print(f"  tentativa {tentativa} ({decorrido}s): ainda em processamento...")
        time.sleep(POLL_INTERVALO)

    # RuntimeError (nao SystemExit) para ser NAO-fatal: coletar_online captura,
    # mantem o cache/semente e publica o que ja tem, em vez de derrubar o ciclo.
    raise RuntimeError(f"CSV nao ficou pronto em {POLL_TIMEOUT}s "
                       f"({tentativa} tentativas).")


# O export do BackOffice corta em 30.000 transacoes (mantendo as MAIS ANTIGAS).
# Como o evento inteiro ja passa disso, uma exportacao unica perde os dias
# recentes - justo quando o Plano B e necessario. Coletamos entao em pedacos.
CAP_EXPORT = 30000
MAX_CHUNKS = 25          # trava de seguranca (>750k transacoes)

# Cache local (persiste no cache do Actions, gitignored): guarda a uniao das
# transacoes ja coletadas para que cada ciclo exporte so a FATIA RECENTE em vez
# do evento inteiro - o export do BackOffice fica lento sob carga e cresce com
# o evento, entao pedir tudo a cada ciclo e caro. Ver coletar_online.
PASTA_CACHE = "dados_planob"
ARQ_CACHE = "transacoes.pkl"
# Semente versionada no repositorio: um export completo do BackOffice ja
# parseado (sem PII) para o 1o ciclo NAO precisar refazer a coleta inteira do
# zero. So e usada quando nao ha cache; a partir dai o cache assume.
ARQ_SEED = "seed_planob.csv.gz"
# Quanto reexportar para tras do ultimo horario ja em cache, a cada ciclo -
# cobre transacoes que chegaram fora de ordem / a virada do caixa e cicatriza
# um rabo incompleto do ciclo anterior. A sobreposicao e deduplicada.
SOBREPOSICAO = timedelta(hours=3)


def _coletar_intervalo(sessao, inicio, fim):
    """Exporta [inicio, fim] contornando o teto de 30k, devolve o DataFrame.

    Exporta a partir de `inicio`; se o pedaco veio no teto (truncado), continua
    a partir da ultima transacao vista e junta tudo, ate `fim`. Dedup por
    TRANSACAO inteira (nao por linha) - uma transacao nunca se divide entre
    pedacos porque todas as suas linhas tem o mesmo horario.
    """
    col_id = COL["transacao_id"]
    col_dh = COL["data_hora_realizacao"]
    partes = []
    vistos = set()
    for chunk in range(1, MAX_CHUNKS + 1):
        buscar_relatorio(sessao, inicio, fim)
        raw = exportar_csv(sessao)
        df = _parse_export(raw)          # le CSV ou XLSX conforme o formato real
        n = int(df[col_id].nunique()) if not df.empty else 0
        print(f"  pedaco {chunk} (desde {inicio:%d/%m %H:%M}) "
              f"[{'xlsx' if _eh_xlsx(raw) else 'csv'}]: "
              f"{len(df)} linhas, {n} transacoes")

        if df.empty:
            break

        novos = df[~df[col_id].isin(vistos)]
        if not novos.empty:
            partes.append(novos)
            vistos.update(df[col_id].unique())

        # Abaixo do teto: chegou ate o fim do periodo - terminou.
        if n < CAP_EXPORT:
            break

        # Truncou no teto: continua a partir da ultima transacao deste pedaco.
        ultimo = df[col_dh].max()
        if pd.isna(ultimo):
            break
        novo_inicio = (ultimo - timedelta(minutes=1)).to_pydatetime()
        if novo_inicio <= inicio or novos.empty:
            # Nao avancou (ou nada novo veio): evita laco infinito. Se isso
            # ocorre no 1o corte, o filtro de periodo pode nao estar sendo
            # respeitado - o log acima ajuda a diagnosticar.
            print("  aviso: coleta nao avancou; parando para nao repetir")
            break
        inicio = novo_inicio

    if not partes:
        return pd.DataFrame()
    df = pd.concat(partes, ignore_index=True)
    print(f"  intervalo: {len(df)} linhas | "
          f"{df[col_id].nunique()} transacoes em {len(partes)} pedaco(s)")
    return df


def carregar_cache(pasta):
    """Uniao das transacoes ja coletadas (DataFrame). Vazio se nao houver/falhar."""
    caminho = os.path.join(pasta, ARQ_CACHE)
    try:
        return pd.read_pickle(caminho)
    except Exception:                 # ausente, corrompido ou pandas incompativel
        return pd.DataFrame()


def salvar_cache(pasta, df):
    os.makedirs(pasta, exist_ok=True)
    df.to_pickle(os.path.join(pasta, ARQ_CACHE))


def carregar_seed():
    """Semente do repositorio (export completo ja parseado) -> DataFrame.

    Vazio se nao houver. Recoere os tipos que o CSV nao preserva; NAO passa por
    _finalizar (os sinais de cancelamento ja estao gravados no arquivo).
    """
    caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)), ARQ_SEED)
    if not os.path.exists(caminho):
        return pd.DataFrame()
    df = pd.read_csv(caminho)
    df[COL["transacao_id"]] = df[COL["transacao_id"]].astype(str)
    df[COL["data_hora_realizacao"]] = pd.to_datetime(
        df[COL["data_hora_realizacao"]], errors="coerce")
    df[COL["quantidade"]] = (pd.to_numeric(df[COL["quantidade"]], errors="coerce")
                             .astype("Int64"))
    return df


def coletar_online(pasta=PASTA_CACHE):
    """Coleta incremental: reaproveita o cache e exporta so a fatia recente.

    1o ciclo (cache vazio): coleta o evento inteiro (lento, uma vez).
    Ciclos seguintes: exporta [ultimo_horario - SOBREPOSICAO, agora], funde no
    cache substituindo as transacoes reexportadas pela versao fresca e grava.
    Falha na fatia recente NAO derruba o ciclo: publica o que ja ha em cache
    (fica desatualizado, nunca com furo).
    """
    col_id = COL["transacao_id"]
    col_dh = COL["data_hora_realizacao"]

    cache = carregar_cache(pasta)
    if cache.empty:
        cache = carregar_seed()          # 1a vez: parte da semente do repo
        if not cache.empty:
            print(f"sem cache: usando a semente do repositorio "
                  f"({len(cache)} linhas / {cache[col_id].nunique()} transacoes)")

    fim = datetime.now() + timedelta(minutes=5)
    if cache.empty:
        desde = S.EVENTO_INICIO
        print("sem cache nem semente: coleta completa (pode demorar nesta 1a vez)")
    else:
        cmax = cache[col_dh].max()
        desde = (S.EVENTO_INICIO if pd.isna(cmax)
                 else (cmax - SOBREPOSICAO).to_pydatetime())
        print(f"cache: {len(cache)} linhas / {cache[col_id].nunique()} transacoes "
              f"ate {cmax}; coletando desde {desde:%d/%m %H:%M}")

    sessao = requests.Session()
    sessao.headers["User-Agent"] = "Mozilla/5.0 (RIR26 PlanoB)"
    login(sessao)
    try:
        novos = _coletar_intervalo(sessao, desde, fim)
    except Exception as e:
        print(f"  fatia recente falhou ({type(e).__name__}: {e}) - "
              f"publicando o cache atual")
        novos = pd.DataFrame()

    if novos.empty:
        return cache            # nada novo: publica o cache (vazio => nada)

    # Funde: substitui no cache as transacoes que o intervalo re-trouxe pela
    # versao fresca e mantem o resto.
    if cache.empty:
        df = novos
    else:
        ids_novos = set(novos[col_id].unique())
        base = cache[~cache[col_id].isin(ids_novos)]
        df = pd.concat([base, novos], ignore_index=True)

    salvar_cache(pasta, df)
    print(f"  total em cache: {len(df)} linhas | {df[col_id].nunique()} transacoes")
    return df


def main():
    parser = argparse.ArgumentParser(description="Plano B: coleta via BackOffice netpdv.")
    parser.add_argument("--csv", help="parseia um CSV salvo (teste offline, sem login)")
    parser.add_argument("--html", help="parseia um HTML do ProcessReport (teste offline)")
    parser.add_argument("--pasta", default=PASTA_PLANOB, help="pasta de saida dos JSONs")
    args = parser.parse_args()

    if args.csv:
        df = parse_csv(open(args.csv, "rb").read())
    elif args.html:
        with open(args.html, encoding="utf-8") as f:
            df = parse_relatorio(f.read())
    else:
        df = coletar_online()

    if df.empty:
        print("Nada retornado - nenhum JSON gerado.")
        return
    gravar_planob(df, args.pasta)


if __name__ == "__main__":
    main()
