"""
De-para de PRODUTO -> MARCA, da planilha "DE-PARA MARCAS.xlsx" da operacao.

Por que existe: durante o evento algumas vendas saem por terminais da marca
errada (problema operacional de PDV). Como o produto identifica a marca sem
ambiguidade - um MANE BURGUER e do Mane mesmo que passe no terminal do
Espetto - o ranking usa esta tabela em vez do ponto de venda.

BEBIDAS NAO ENTRAM AQUI de proposito: sao vendidas por todas as marcas, entao
seguem a marca do ponto de venda, como pedido pela operacao.

Produto que nao estiver nesta tabela tambem cai na marca do ponto de venda.
Isso vale para as bebidas (correto) e para qualquer produto novo que a
planilha ainda nao cubra (a corrigir - ver AUSENTES no README).

Gerado a partir da planilha; para atualizar, regenere em vez de editar a mao.
"""

# 33 produtos com marca definida pelo item (todos Comida).
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
    'BATATA MANE': 'Mané',
    'BATATA PORCO': 'Mané',
    'CB PIPOCA+2CHOP': 'Mané',
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
