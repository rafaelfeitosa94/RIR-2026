"""
DataFrame das transacoes do RIR 2026, pronto para alimentar outro software.

Le os .jsonl acumulados por api_transacoes_rir.py e devolve um DataFrame
pandas ja tratado:

  * uma linha por produto (a lista "produtos" de cada transacao e achatada,
    repetindo os campos da transacao);
  * transacoes sem produtos (recarga, ativacao, cancelamento) viram uma linha
    unica com os campos de produto vazios, para nao sumirem;
  * deduplicado por transacao_id - a coleta rebaixa a hora corrente, entao o
    mesmo id pode aparecer mais de uma vez no .jsonl (a ocorrencia mais
    recente vence);
  * colunas renomeadas para os nomes de destino e na ordem definida em
    COLUNAS_TRANSACAO;
  * tipos convertidos (datas para datetime, valores para float, quantidade
    para inteiro). Passe converter_tipos=False para receber tudo como veio.

Uso como biblioteca:

    from dataframe_rir import carregar_transacoes

    df = carregar_transacoes()
    # -> DataFrame com as colunas "Transação id", "Nome do Evento", ...

Uso pela linha de comando (inspecao rapida):

    python dataframe_rir.py
    python dataframe_rir.py --info
"""

import argparse
import os

import pandas as pd

from api_transacoes_rir import (
    CAMPOS_AMBULANTE,
    CAMPOS_PRODUTO,
    CAMPOS_TRANSACAO,
    JSONL_AMBULANTES,
    JSONL_TRANSACOES,
    PASTA_DADOS,
    ler_jsonl,
)

# ----------------------------------------------------------------- colunas
# De-para campo da API -> nome da coluna no software de destino.
# A ordem deste dict e a ordem das colunas do DataFrame.
COLUNAS_TRANSACAO = {
    "transacao_id": "Transação id",
    "transacao_original_id": "Transação id Final",
    "codigo_evento": "Codigo do Evento",
    "nome_evento": "Nome do Evento",
    "data_hora_realizacao": "Data Hora Realização",
    "data_hora_cadastro": "Data Hora do Cadastro",
    "data_hora_confirmacao": "Data Hora da Confirmação",
    "sessao": "Sessão",
    "operacao": "Operação",
    "tipo_ponto": "Tipo de Ponto",
    "codigo_ponto": "Código do Ponto",
    "nome_ponto": "Nome do Ponto",
    "latitude": "Latitude",
    "longitude": "Longitude",
    "operador": "Operador",
    "terminal": "Terminal",
    "valor": "Valor",
    "taxa_ativacao": "Taxa Ativação",
    "status": "Status da Transação",
    "cod_forma_pagamento": "Código Forma de Pagamento",
    "forma_pagamento": "Forma de Pagamento",
    "chip": "Chip",
    "chip_informado_manual": "Chip Informado Manualmente",
    "documento_cliente": "Doc. Cliente",
    "email_cliente": "E-mail do Cliente",
    "categoria_produto": "Categoria Produto",
    "cod_produto": "Codigo do Produto",
    "cod_produto_parceiro": "Codigo do Parceiro",
    "produto": "Produto",
    "fabricante": "Fabricante",
    "quantidade": "Quantidade",
    "valor_unitario": "Valor Unitário",
    "valor_total": "Valor Total",
}

# Movimentos de ambulante: reaproveita os nomes acima para os campos que os
# dois retornos tem em comum e nomeia os tres campos exclusivos seguindo o
# mesmo padrao.
COLUNAS_AMBULANTE = {
    "movimento_ambulante_id": "Movimento Ambulante id",
    **{campo: COLUNAS_TRANSACAO[campo]
       for campo in CAMPOS_AMBULANTE if campo in COLUNAS_TRANSACAO},
    "nome_ambulante": "Nome do Ambulante",
    "documento_ambulante": "Doc. Ambulante",
    **{campo: COLUNAS_TRANSACAO[campo] for campo in CAMPOS_PRODUTO},
}

# Conversao de tipos por campo da API (aplicada antes do rename).
CAMPOS_DATA = [
    "data_hora_realizacao", "data_hora_cadastro", "data_hora_confirmacao",
]
CAMPOS_DECIMAIS = [
    "valor", "taxa_ativacao", "valor_unitario", "valor_total",
]
CAMPOS_INTEIROS = ["quantidade"]


# -------------------------------------------------------------- achatamento
def achatar(registros, campos_pai):
    """Explode a lista 'produtos' de cada registro em uma linha por produto.

    Registros sem produtos viram uma unica linha com os campos de produto
    vazios, para nao se perderem.
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


def _converter_tipos(df):
    """Datas para datetime, valores para float, quantidade para inteiro."""
    for campo in CAMPOS_DATA:
        if campo in df.columns:
            # format="ISO8601" e obrigatorio aqui: o WS varia a precisao dos
            # segundos entre os registros ("...:39.107", "...:55.81",
            # "...:25:39"). Sem isso o pandas infere o formato pela primeira
            # linha e transforma as demais em NaT silenciosamente.
            df[campo] = pd.to_datetime(df[campo], format="ISO8601", errors="coerce")

    for campo in CAMPOS_DECIMAIS:
        if campo in df.columns:
            df[campo] = pd.to_numeric(df[campo], errors="coerce")

    for campo in CAMPOS_INTEIROS:
        if campo in df.columns:
            # Int64 (com I maiusculo) aceita nulo, que e o caso das linhas
            # sem produto.
            df[campo] = pd.to_numeric(df[campo], errors="coerce").astype("Int64")

    return df


def _montar(registros, campos_pai, mapa_colunas, converter_tipos):
    """Achata, converte tipos e renomeia para as colunas de destino."""
    linhas = achatar(registros, campos_pai)
    colunas_api = list(mapa_colunas)

    if not linhas:
        # DataFrame vazio, mas com as colunas certas: o software de destino
        # nao precisa tratar o caso "sem dados" de forma diferente.
        return pd.DataFrame(columns=[mapa_colunas[c] for c in colunas_api])

    df = pd.DataFrame(linhas)
    df = df.reindex(columns=colunas_api)  # ordem e colunas faltantes

    if converter_tipos:
        df = _converter_tipos(df)

    return df.rename(columns=mapa_colunas)


# ------------------------------------------------------------------ leitura
def carregar_transacoes(pasta=PASTA_DADOS, converter_tipos=True):
    """DataFrame das transacoes, uma linha por produto, colunas renomeadas."""
    registros = ler_jsonl(os.path.join(pasta, JSONL_TRANSACOES), "transacao_id")
    return _montar(registros, CAMPOS_TRANSACAO, COLUNAS_TRANSACAO, converter_tipos)


def carregar_movimentos_ambulantes(pasta=PASTA_DADOS, converter_tipos=True):
    """DataFrame dos movimentos de ambulante, uma linha por produto."""
    registros = ler_jsonl(os.path.join(pasta, JSONL_AMBULANTES),
                          "movimento_ambulante_id")
    return _montar(registros, CAMPOS_AMBULANTE, COLUNAS_AMBULANTE, converter_tipos)


def main():
    parser = argparse.ArgumentParser(
        description="Monta o DataFrame das transacoes do RIR 2026 ja coletadas.")
    parser.add_argument("--pasta", default=PASTA_DADOS,
                        help=f"pasta com os .jsonl da coleta (padrao: {PASTA_DADOS})")
    parser.add_argument("--info", action="store_true",
                        help="mostra dtypes e uso de memoria em vez das linhas")
    parser.add_argument("--ambulantes", action="store_true",
                        help="usa os movimentos de ambulante no lugar das transacoes")
    args = parser.parse_args()

    df = (carregar_movimentos_ambulantes(args.pasta) if args.ambulantes
          else carregar_transacoes(args.pasta))

    print(f"{len(df)} linhas x {len(df.columns)} colunas\n")

    if args.info:
        df.info()
        return

    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(df.head(10))


if __name__ == "__main__":
    main()
