"""
Plano B: coleta as transacoes do RIR 2026 pelo BackOffice da netpdv, para quando
a API de relatorios cai (tem caido ~20:30-02:00 durante o evento).

Fonte: POST https://www.netpdv.com/backoffice/Relatorio/ProcessReport
  form-urlencoded, report=lista_transacao, id=38049, field-tempo-integral=1,
  field-periodo=<dd/mm/aaaa HH:MM - dd/mm/aaaa HH:MM>, e demais filtros vazios.
  Header X-Requested-With: XMLHttpRequest. Resposta = HTML com a tabela (45
  colunas), quebrada em varias <table> - juntamos todas as linhas.

O HTML traz o MESMO conteudo da API, entao depois de parsear reaproveitamos
montar_fatos / montar_resumo / montar_feed do stream_rir e gravamos os JSONs
em publico/planob/. O painel (index.html) tem o botao Principal/Plano B que
so troca a base de publico/ para publico/planob/.

Credenciais do BackOffice: fora do codigo (env NETPDV_LOGIN/NETPDV_SENHA ou
credenciais.json {"netpdv_login":"...","netpdv_senha":"..."}). Nunca commitar.

Uso:
    python coletor_planob.py                 # loga, coleta e grava os JSONs
    python coletor_planob.py --html arq.html # parseia um HTML salvo (teste offline)
    python coletor_planob.py --uma-vez       # (padrao ja e uma passada)
"""

import argparse
import html as _html
import json
import os
import re
from datetime import datetime, timedelta

import pandas as pd
import requests

from dataframe_rir import COLUNAS_TRANSACAO
import stream_rir as S

# ---------------------------------------------------------------- endpoints
BASE = "https://www.netpdv.com/backoffice"
URL_RELATORIO = f"{BASE}/Relatorio/ProcessReport"

CODIGO_EVENTO = S.CODIGO_EVENTO
PASTA_PLANOB = os.path.join(S.PASTA_PUBLICO, "planob")
TIMEOUT = 300  # o relatorio leva ~4 min para responder

# De-para do cabecalho HTML -> nome interno (destino) usado no DataFrame.
# So mapeamos as colunas que o painel usa; o resto do relatorio e ignorado.
COL = COLUNAS_TRANSACAO
DE_PARA_COLUNAS = {
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

    df = pd.DataFrame(linhas)
    if df.empty:
        return df

    # ---- conversoes ----
    df[COL["data_hora_realizacao"]] = pd.to_datetime(
        df[COL["data_hora_realizacao"]], format="%d/%m/%Y %H:%M:%S", errors="coerce")

    def num_br(serie):
        # "1.234,56" -> 1234.56 ; "" -> NaN
        return pd.to_numeric(
            serie.str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
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

    # Colunas que o painel espera existir mas o relatorio nao traz (documento e
    # e-mail do cliente sao removidos na publicacao mesmo; aqui ficam vazios).
    for k in ("documento_cliente", "email_cliente"):
        df[COL[k]] = None

    return df


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


def login(sessao):
    """Autentica no BackOffice e deixa a sessao pronta para o ProcessReport.

    IMPLEMENTACAO PENDENTE: o HAR foi capturado com o usuario ja logado, entao
    o request de login nao esta nele. Assim que capturarmos o POST de login
    (URL + nomes dos campos), este metodo faz o GET inicial (cookies/tokens) e
    o POST das credenciais. Ate la, so o modo --html (offline) funciona.
    """
    raise NotImplementedError(
        "Login do BackOffice ainda nao mapeado - capturar o POST de login. "
        "Enquanto isso use: python coletor_planob.py --html <arquivo.html>")


def buscar_relatorio(sessao):
    """POST no ProcessReport com o filtro Tempo integral e devolve o HTML."""
    fim = datetime.now() + timedelta(days=1)
    periodo = (f"{S.EVENTO_INICIO.strftime('%d/%m/%Y %H:%M')} - "
               f"{fim.strftime('%d/%m/%Y %H:%M')}")
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


def coletar_online():
    sessao = requests.Session()
    sessao.headers["User-Agent"] = "Mozilla/5.0 (RIR26 PlanoB)"
    login(sessao)
    return parse_relatorio(buscar_relatorio(sessao))


def main():
    parser = argparse.ArgumentParser(description="Plano B: coleta via BackOffice netpdv.")
    parser.add_argument("--html", help="parseia um HTML salvo (teste offline, sem login)")
    parser.add_argument("--pasta", default=PASTA_PLANOB, help="pasta de saida dos JSONs")
    args = parser.parse_args()

    if args.html:
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
