"""
Exporta para .xlsx as transacoes do RIR 2026 ja coletadas.

Le o DataFrame montado por dataframe_rir.py (colunas ja renomeadas, tipos
convertidos) e grava uma planilha formatada: cabecalho congelado, autofiltro,
larguras ajustadas e formato de data/moeda nas colunas certas.

ATUALIZA ANTES DE EXPORTAR. Chamar a API e o padrao: sem isso a planilha
sairia com o que estava em disco desde a ultima coleta - que pode ser de
horas atras. Use --sem-coletar para pular a API quando quiser so reprocessar
o que ja foi baixado.

Uso:
    python exportar_xlsx.py                   # coleta e exporta tudo, ao vivo
    python exportar_xlsx.py --saida "W:/RIR 2026/transacoes.xlsx"
    python exportar_xlsx.py --de 2026-09-05 --ate 2026-09-07
    python exportar_xlsx.py --resumo          # adiciona abas de agregados
    python exportar_xlsx.py --sem-coletar     # nao chama a API
"""

import argparse
import os
from datetime import datetime

import pandas as pd
from openpyxl.utils import get_column_letter

from dataframe_rir import (
    COLUNAS_TRANSACAO,
    carregar_movimentos_ambulantes,
    carregar_transacoes,
)

# O Excel para em 1.048.576 linhas, contando o cabecalho. Acima disso a
# exportacao e quebrada em varias abas.
MAX_LINHAS_ABA = 1_048_575

ARQ_PADRAO = "rir2026_transacoes.xlsx"

ABA_TRANSACOES = "Transações"
ABA_AMBULANTES = "Movimentos Ambulantes"

# Colunas que ganham formato especifico na planilha.
COLUNAS_MOEDA = [
    COLUNAS_TRANSACAO["valor"],
    COLUNAS_TRANSACAO["taxa_ativacao"],
    COLUNAS_TRANSACAO["valor_unitario"],
    COLUNAS_TRANSACAO["valor_total"],
]
COLUNAS_DATA = [
    COLUNAS_TRANSACAO["data_hora_realizacao"],
    COLUNAS_TRANSACAO["data_hora_cadastro"],
    COLUNAS_TRANSACAO["data_hora_confirmacao"],
]

FORMATO_MOEDA = 'R$ #,##0.00'
FORMATO_DATA = "dd/mm/yyyy hh:mm:ss"

COL_REALIZACAO = COLUNAS_TRANSACAO["data_hora_realizacao"]


def _preparar(df):
    """Deixa o DataFrame digerivel pelo openpyxl.

    O dtype Int64 usa pd.NA para nulo, que o openpyxl nao sabe escrever
    (levanta ValueError). Converte para object com None no lugar.
    """
    df = df.copy()
    for coluna in df.columns:
        if str(df[coluna].dtype) in ("Int64", "Float64", "boolean"):
            df[coluna] = df[coluna].astype(object).where(df[coluna].notna(), None)
    return df


def _formatar_aba(planilha, df):
    """Congela o cabecalho, liga o autofiltro e ajusta larguras e formatos."""
    planilha.freeze_panes = "A2"
    planilha.auto_filter.ref = planilha.dimensions

    for i, coluna in enumerate(df.columns, start=1):
        letra = get_column_letter(i)

        # Largura pelo maior conteudo da coluna, com teto para nao estourar.
        amostra = df[coluna].head(500).astype(str)
        maior = amostra.str.len().max() if len(amostra) else 0
        largura = max(len(str(coluna)), int(maior or 0)) + 2
        planilha.column_dimensions[letra].width = min(largura, 40)

        if coluna in COLUNAS_MOEDA:
            formato = FORMATO_MOEDA
        elif coluna in COLUNAS_DATA:
            formato = FORMATO_DATA
        else:
            continue

        # Linha 1 e o cabecalho; o formato vale so para os dados.
        for celula in planilha[letra][1:]:
            celula.number_format = formato


def _escrever(writer, df, nome_aba):
    """Grava o DataFrame, quebrando em varias abas se passar do limite."""
    df = _preparar(df)

    if len(df) <= MAX_LINHAS_ABA:
        blocos = [(nome_aba, df)]
    else:
        blocos = []
        for n, inicio in enumerate(range(0, len(df), MAX_LINHAS_ABA), start=1):
            # O Excel limita o nome da aba a 31 caracteres.
            rotulo = f"{nome_aba} ({n})"[:31]
            blocos.append((rotulo, df.iloc[inicio:inicio + MAX_LINHAS_ABA]))
        print(f"  {len(df)} linhas passam do limite do Excel: "
              f"quebrado em {len(blocos)} abas")

    for rotulo, bloco in blocos:
        bloco.to_excel(writer, sheet_name=rotulo, index=False)
        _formatar_aba(writer.sheets[rotulo], bloco)
        print(f"  aba {rotulo!r}: {len(bloco)} linhas x {len(bloco.columns)} colunas")


def _abas_resumo(writer, df):
    """Agregados por ponto, produto, categoria, forma de pagamento e hora.

    Usa o sinal de 'Valor' para abater cancelamentos: a API assina o valor da
    transacao (-25.0 num cancelamento) mas NAO assina o valor_total do
    produto, que continua positivo. Somar cru infla o faturamento.
    """
    col_valor = COLUNAS_TRANSACAO["valor"]
    col_total = COLUNAS_TRANSACAO["valor_total"]
    col_qtd = COLUNAS_TRANSACAO["quantidade"]
    col_id = COLUNAS_TRANSACAO["transacao_id"]

    base = df.copy()
    sinal = base[col_valor].fillna(0).apply(lambda v: -1 if v < 0 else 1)
    base["Valor Líquido"] = base[col_total].fillna(0) * sinal
    base["Itens Líquidos"] = base[col_qtd].fillna(0).astype(float) * sinal
    base["Hora"] = base[COL_REALIZACAO].dt.strftime("%Y-%m-%d %H:00")

    agrupamentos = {
        "Por Ponto": COLUNAS_TRANSACAO["nome_ponto"],
        "Por Produto": COLUNAS_TRANSACAO["produto"],
        "Por Categoria": COLUNAS_TRANSACAO["categoria_produto"],
        "Por Forma Pagamento": COLUNAS_TRANSACAO["forma_pagamento"],
        "Por Hora": "Hora",
    }

    for aba, coluna in agrupamentos.items():
        resumo = (base.groupby(coluna, dropna=True)
                      .agg(**{"Valor Líquido": ("Valor Líquido", "sum"),
                              "Itens": ("Itens Líquidos", "sum"),
                              "Transações": (col_id, "nunique")})
                      .reset_index())
        # Por hora a ordem cronologica diz mais; nas demais, o ranking.
        resumo = (resumo.sort_values(coluna) if aba == "Por Hora"
                  else resumo.sort_values("Valor Líquido", ascending=False))

        resumo.to_excel(writer, sheet_name=aba, index=False)
        _formatar_aba(writer.sheets[aba], resumo)
        for celula in writer.sheets[aba]["B"][1:]:
            celula.number_format = FORMATO_MOEDA
        print(f"  aba {aba!r}: {len(resumo)} linhas")


def exportar(caminho=ARQ_PADRAO, pasta=None, de=None, ate=None,
             com_resumo=False, com_ambulantes=True, coletar_antes=True):
    """Monta o .xlsx. Devolve o caminho gravado, ou None se nao havia dados.

    Por padrao chama a API antes de exportar: sem isso a planilha sai com o
    que estava em disco desde a ultima coleta, que pode ser de horas atras.
    """
    kwargs = {"pasta": pasta} if pasta else {}

    if coletar_antes:
        from api_transacoes_rir import PASTA_DADOS, coletar
        print("Atualizando da API antes de exportar...")
        coletar(pasta or PASTA_DADOS)
        print()

    df = carregar_transacoes(**kwargs)

    if df.empty:
        print("Nenhuma transacao coletada ainda - nada a exportar.")
        return None

    if de is not None:
        df = df[df[COL_REALIZACAO] >= de]
    if ate is not None:
        df = df[df[COL_REALIZACAO] < ate]

    if df.empty:
        print("Nenhuma transacao no periodo pedido - nada a exportar.")
        return None

    df = df.sort_values(COL_REALIZACAO)

    destino = os.path.dirname(os.path.abspath(caminho))
    os.makedirs(destino, exist_ok=True)

    with pd.ExcelWriter(caminho, engine="openpyxl",
                        datetime_format=FORMATO_DATA) as writer:
        _escrever(writer, df, ABA_TRANSACOES)

        if com_ambulantes:
            ambulantes = carregar_movimentos_ambulantes(**kwargs)
            if not ambulantes.empty:
                _escrever(writer, ambulantes, ABA_AMBULANTES)

        if com_resumo:
            _abas_resumo(writer, df)

    ultima = df[COL_REALIZACAO].max()
    atraso = pd.Timestamp.now() - ultima
    print(f"\n{caminho}  ({os.path.getsize(caminho) / 1024:.0f} KB)")
    print(f"ultima transacao: {ultima:%d/%m %H:%M:%S} "
          f"({max(atraso.total_seconds(), 0):.0f}s atras)")
    return caminho


def _data(texto):
    """Aceita 'YYYY-MM-DD' ou 'YYYY-MM-DD HH:MM:SS'."""
    for formato in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(texto, formato)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Data invalida: {texto} (use YYYY-MM-DD ou 'YYYY-MM-DD HH:MM:SS')")


def main():
    parser = argparse.ArgumentParser(
        description="Exporta para .xlsx as transacoes do RIR 2026 ja coletadas.")
    parser.add_argument("--saida", default=ARQ_PADRAO,
                        help=f"arquivo .xlsx de destino (padrao: {ARQ_PADRAO})")
    parser.add_argument("--de", type=_data, default=None,
                        help="exporta a partir desta data/hora de realizacao")
    parser.add_argument("--ate", type=_data, default=None,
                        help="exporta ate esta data/hora (exclusiva)")
    parser.add_argument("--resumo", action="store_true",
                        help="adiciona abas de agregados (ponto, produto, hora...)")
    parser.add_argument("--sem-ambulantes", action="store_true",
                        help="nao inclui a aba de movimentos de ambulante")
    parser.add_argument("--sem-coletar", action="store_true",
                        help="nao chama a API; exporta so o que ja esta em disco "
                             "(mais rapido, porem desatualizado)")
    parser.add_argument("--pasta", default=None,
                        help="pasta com os .jsonl da coleta")
    args = parser.parse_args()

    exportar(caminho=args.saida, pasta=args.pasta, de=args.de, ate=args.ate,
             com_resumo=args.resumo, com_ambulantes=not args.sem_ambulantes,
             coletar_antes=not args.sem_coletar)


if __name__ == "__main__":
    main()
