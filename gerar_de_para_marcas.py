"""Gera de_para_produtos.py a partir do PROD;MARCA exportado da operacao.

SO COMIDA entra. As bebidas vem no arquivo marcadas como "Espetto+2" /
"Espetto+1" (sao vendidas por mais de uma marca) e por decisao da operacao
seguem a marca do PONTO DE VENDA, nao a do produto - por isso ficam de fora.

Uso:
    python gerar_de_para_marcas.py "C:/.../Lista de Transacoes (5).csv"
"""

import sys

ARQUIVO_PADRAO = r"C:\Users\rfeitosa\Downloads\Lista de Transações (5).csv"
SAIDA = "de_para_produtos.py"

# Marcas proprias de comida. Qualquer outro rotulo (Espetto+2, Espetto+1,
# vazio) e bebida compartilhada e nao entra.
MARCAS_COMIDA = ("Espetto", "Mané", "Sirene")

CABECALHO = '''"""
De-para de PRODUTO -> MARCA, exportado da operacao (PROD;MARCA).

Por que existe: durante o evento algumas vendas saem por terminais da marca
errada (problema operacional de PDV). Como o produto identifica a marca sem
ambiguidade - um MANE BURGUER e do Mane mesmo que passe no terminal do
Espetto - o ranking e a tabela de meta usam esta tabela em vez do ponto.

BEBIDAS NAO ENTRAM AQUI de proposito: sao vendidas por todas as marcas, entao
seguem a marca do ponto de venda, como pedido pela operacao. No arquivo de
origem elas aparecem como "Espetto+2"/"Espetto+1", e sao descartadas.

Produto que nao estiver nesta tabela tambem cai na marca do ponto de venda.

Gerado por gerar_de_para_marcas.py; para atualizar, regenere em vez de editar
a mao.
"""

'''


def ler(caminho):
    """[(produto, marca)] apenas das comidas. O arquivo vem em cp1252."""
    pares = []
    with open(caminho, encoding="cp1252") as arquivo:
        for linha in arquivo.read().splitlines()[1:]:      # pula o cabecalho
            if not linha.strip():
                continue
            produto, _, marca = linha.partition(";")
            produto, marca = produto.strip(), marca.strip()
            if produto and marca in MARCAS_COMIDA:
                pares.append((produto, marca))
    return pares


def gerar(caminho=ARQUIVO_PADRAO):
    pares = sorted(ler(caminho))
    largura = max(len(p) for p, _ in pares) + 2
    linhas = "".join(f"    {repr(p) + ':':<{largura + 1}} {marca!r},\n"
                     for p, marca in pares)
    corpo = (f"# {len(pares)} produtos de comida com marca definida pelo item.\n"
             f"DE_PARA_PRODUTO_MARCA = {{\n{linhas}}}\n")
    with open(SAIDA, "w", encoding="utf-8") as saida:
        saida.write(CABECALHO + corpo)
    return pares


if __name__ == "__main__":
    caminho = sys.argv[1] if len(sys.argv) > 1 else ARQUIVO_PADRAO
    pares = gerar(caminho)
    import collections
    print(f"{SAIDA}: {len(pares)} produtos de comida")
    for marca, n in collections.Counter(m for _, m in pares).most_common():
        print(f"   {marca:8} {n}")
