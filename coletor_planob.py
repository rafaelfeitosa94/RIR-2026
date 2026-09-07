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
POLL_TIMEOUT = 600       # espera total pelo CSV ficar pronto (EmProcessamento)
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
    prod = df[COL["produto"]].fillna("").str.strip()
    resumo = prod.eq("") | prod.str.fullmatch(r"[-–—\s]+")
    if resumo.any():
        print(f"  linhas de resumo descartadas (sem produto): {int(resumo.sum())}")
    df = df[~resumo].copy()
    if df.empty:
        return df

    df[COL["data_hora_realizacao"]] = pd.to_datetime(
        df[COL["data_hora_realizacao"]], format="%d/%m/%Y %H:%M:%S", errors="coerce")

    def num_br(serie):
        # "1.234,56" -> 1234.56 ; "" -> NaN
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


def buscar_relatorio(sessao):
    """POST no ProcessReport com o filtro Tempo integral e devolve o HTML.

    Periodo explicito desde 02/09/2026 00:00 ate amanha, e field-tempo-integral=1
    (o mesmo "Tempo integral" da tela) para o relatorio trazer o evento inteiro.
    """
    fim = datetime.now() + timedelta(days=1)
    periodo = (f"{S.EVENTO_INICIO.strftime('%d/%m/%Y %H:%M')} - "
               f"{fim.strftime('%d/%m/%Y %H:%M')}")
    print(f"  periodo: {periodo}")
    dados = [
        ("id", str(CODIGO_EVENTO)),
        ("report", "lista_transacao"),
        ("fields[]", f"field-periodo={periodo}"),
        ("fields[]", "field-clienteData="),
        ("fields[]", "field-formatacao=1"),
        ("fields[]", "field-pontos="),
        ("fields[]", "field-tempo-integral=1"),
        ("fields[]", "field-operador="),
        ("fields[]", "field-terminal="),
        ("fields[]", "field-forma-pagamento="),
        ("fields[]", "field-cartao="),
        ("fields[]", "field-baladeiro-documento="),
        ("fields[]", "field-operacoes="),
        ("fields[]", "field-pedidos="),
        ("fields[]", "field-tipo-relatorio-transacao=1"),
    ]
    r = sessao.post(URL_RELATORIO, data=dados, timeout=TIMEOUT,
                    headers={"X-Requested-With": "XMLHttpRequest",
                             "Referer": BASE, "Origin": "https://www.netpdv.com"})
    r.raise_for_status()
    return r.text


def _tem_dados(raw):
    """True se o CSV baixado ja tem pelo menos uma linha de transacao."""
    if not raw or len(raw) < 900:            # so o cabecalho (~824 bytes)
        return False
    return re.search(rb"(?m)^22\d{7};", raw) is not None


def exportar_csv(sessao):
    """Dispara ExportTransacao e faz polling do DownloadFile ate o CSV ficar
    pronto (sai da pasta EmProcessamento). Devolve os bytes do CSV."""
    r = sessao.post(URL_EXPORT, timeout=TIMEOUT,
                    headers={"X-Requested-With": "XMLHttpRequest", "Referer": BASE})
    r.raise_for_status()
    data = r.json().get("data") or {}
    caminho = data.get("FilePathName")
    nome = data.get("FileName", "Lista de Transacoes.csv")
    if not caminho:
        raise SystemExit(f"ExportTransacao nao devolveu FilePathName: {r.text[:200]}")
    print(f"  export disparado: {nome}")

    limite = time.monotonic() + POLL_TIMEOUT
    tentativa = 0
    while time.monotonic() < limite:
        tentativa += 1
        d = sessao.get(URL_DOWNLOAD, timeout=TIMEOUT,
                       params={"filePathName": caminho, "fileName": nome,
                               "contentType": ""},
                       headers={"Referer": BASE})
        if d.status_code == 200 and _tem_dados(d.content):
            print(f"  CSV pronto na tentativa {tentativa} ({len(d.content):,} bytes)")
            return d.content
        print(f"  tentativa {tentativa}: ainda em processamento...")
        time.sleep(POLL_INTERVALO)

    raise SystemExit("CSV nao ficou pronto dentro do tempo limite.")


def coletar_online():
    sessao = requests.Session()
    sessao.headers["User-Agent"] = "Mozilla/5.0 (RIR26 PlanoB)"
    login(sessao)
    buscar_relatorio(sessao)          # monta o relatorio/filtro na sessao
    raw = exportar_csv(sessao)        # dispara o export e espera ficar pronto
    df = parse_csv(raw)
    print(f"  linhas parseadas: {len(df)} | "
          f"transacoes: {df[COL['transacao_id']].nunique() if not df.empty else 0}")
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
