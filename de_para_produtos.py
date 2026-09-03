"""
De-para de PRODUTO -> MARCA, da planilha "DE-PARA MARCAS.xlsx" da operacao.

Por que existe: durante o evento algumas vendas saem por terminais da marca
errada (problema operacional de PDV). Como o produto identifica a marca sem
ambiguidade - um MANE BURGUER e do Mane mesmo que passe no terminal do
Espetto - o ranking e a tabela de meta usam esta tabela em vez do ponto.

BEBIDAS NAO ENTRAM AQUI de proposito: sao vendidas por todas as marcas, entao
seguem a marca do ponto de venda, como pedido pela operacao.

Produto que nao estiver nesta tabela tambem cai na marca do ponto de venda.

Gerado a partir da planilha; para atualizar, regenere em vez de editar a mao.
"""

# 49 produtos com marca definida pelo item.
DE_PARA_PRODUTO_MARCA = {
    '(C)1CARN+BT+RB': 'Espetto',
    '(C)2.ESP.CARNE +BT': 'Espetto',
    '(C)BATATA CRINKLE': 'Sirene',
    '(C)BATATA MANE': 'Mané',
    '(C)ESPETO DE CARNE': 'Espetto',
    '(C)SAND.CARNE+BT': 'Espetto',
    '1CARN+1FRAN+BT': 'Espetto',
    '1CARN+BT+RB': 'Espetto',
    '1CARN+BT+RB+CP': 'Espetto',
    '1FRANG+BT+RB+CP': 'Espetto',
    '2.ESP.CARNE +BT': 'Espetto',
    '2.ESP.FRANG+BT': 'Espetto',
    'AMERIC.FISH+BT': 'Sirene',
    'AMERICAN FISH': 'Sirene',
    'BATATA CRINKLE': 'Sirene',
    'BATATA CRINKLE SIRENE': 'Sirene',
    'BATATA MANE': 'Mané',
    'BATATA PORCO': 'Mané',
    'CB 1 ESP CARN BTRB SEM COPO': 'Espetto',
    'CB 1 ESP CARNBTRB COPO': 'Espetto',
    'CB 1 ESP CARNE + BT': 'Espetto',
    'CB 1 ESP CARNE+1FRANGO+BT': 'Espetto',
    'CB 1 ESP FRAN+BT+RB COPO': 'Espetto',
    'CB 1 ESP FRANGO + BT': 'Espetto',
    'CB 1 SAND CARNE + BT': 'Espetto',
    'CB 1 SAND FRANGO + BT': 'Espetto',
    'CB 2 ESPS CARNE + BT': 'Espetto',
    'CB 2 ESPS FRANGO + BT': 'Espetto',
    'CB FISH&CHIPS + RED BULL COPO': 'Sirene',
    'CB MANE BURGUER + BATATA': 'Mané',
    'CB PIPOCA DE PORCO + 2 CHOP': 'Mané',
    'CB PIPOCA+2CHOP': 'Mané',
    'CBAMERICAN FISH + BATATA': 'Sirene',
    'COMBO SALAD FISH + BATATA': 'Sirene',
    'ESP.CARNE+BT': 'Espetto',
    'ESP.FRANGO+BT': 'Espetto',
    'ESPETO DE CARNE': 'Espetto',
    'ESPETO FRANGO': 'Espetto',
    'FISH CHIPS': 'Sirene',
    'FISH&CHIP+RB+CP': 'Sirene',
    'FRITAS': 'Espetto',
    'MANE BURGUER': 'Mané',
    'MANE BURGUER+BT': 'Mané',
    'PIPOCA PORCO': 'Mané',
    'SALAD FISH': 'Sirene',
    'SALAD.FISH+BT': 'Sirene',
    'SAND CHURRAS': 'Espetto',
    'SAND.CARNE+BT': 'Espetto',
    'SAND.FRANGO+BT': 'Espetto',
}
