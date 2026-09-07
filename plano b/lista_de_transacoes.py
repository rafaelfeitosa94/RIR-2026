import os
import glob

import pandas as pd

# Pasta de origem dos arquivos exportados manualmente
PASTA = r"W:\ROCK IN RIO - UPDATE MANUAL"

# As 13 primeiras linhas do export sao o cabecalho do relatorio
# (filtros, datas, etc). O cabecalho real da tabela esta na linha 14.
LINHAS_CABECALHO = 13


def listar_arquivos(pasta=PASTA):
    """Lista os .xlsx da pasta (inclusive subpastas), ignorando temporarios do Excel."""
    arquivos = glob.glob(os.path.join(pasta, "**", "*.xlsx"), recursive=True)
    return sorted(a for a in arquivos if not os.path.basename(a).startswith("~$"))


def carregar(pasta=PASTA):
    """Le todos os xlsx da pasta e devolve um unico DataFrame unificado."""
    arquivos = listar_arquivos(pasta)
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo .xlsx encontrado em {pasta}")

    dfs = []
    for caminho in arquivos:
        df = pd.read_excel(caminho, skiprows=LINHAS_CABECALHO)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.dropna(how="all")  # descarta linhas totalmente vazias
        df["arquivo_origem"] = os.path.basename(caminho)
        print(f"{os.path.basename(caminho)}: {len(df)} linhas, {df.shape[1] - 1} colunas")
        dfs.append(df)

    # Avisa se algum arquivo veio com colunas diferentes dos demais
    base = list(dfs[0].columns)
    for caminho, df in zip(arquivos, dfs):
        if list(df.columns) != base:
            faltando = set(base) - set(df.columns)
            sobrando = set(df.columns) - set(base)
            print(f"AVISO: colunas divergentes em {os.path.basename(caminho)} "
                  f"| faltando: {sorted(faltando)} | extras: {sorted(sobrando)}")

    return pd.concat(dfs, ignore_index=True)


if __name__ == "__main__":
    df = carregar()
    print(f"\nTotal unificado: {len(df)} linhas x {df.shape[1]} colunas")
    print(df.head())

df.to_csv("transacoes_unificadas.csv", index=False)