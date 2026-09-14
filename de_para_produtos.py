"""
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

# 36 produtos de comida com marca definida pelo item.
DE_PARA_PRODUTO_MARCA = {
    'AMERICAN FISH':                 'Sirene',
    'BATATA CRINKLE SIRENE':         'Sirene',
    'BATATA MANE':                   'Mané',
    'BATATA PORCO':                  'Mané',
    'CB 1 ESP CARN BTRB SEM COPO':   'Espetto',
    'CB 1 ESP CARNBTRB COPO':        'Espetto',
    'CB 1 ESP CARNE + BT':           'Espetto',
    'CB 1 ESP CARNE+1FRANGO+BT':     'Espetto',
    'CB 1 ESP FRAN+BT+RB COPO':      'Espetto',
    'CB 1 ESP FRANBTRB SEM COPO':    'Mané',
    'CB 1 ESP FRANGO + BT':          'Espetto',
    'CB 1 SAND CARNE + BT':          'Espetto',
    'CB 1 SAND FRANGO + BT':         'Espetto',
    'CB 2 ESPS CARNE + BT':          'Espetto',
    'CB 2 ESPS FRANGO + BT':         'Espetto',
    'CB BT PORQUINHO + RB COPO':     'Mané',
    'CB BT PORQUINHO + RED BULL':    'Mané',
    'CB FISH&CHIPS + RED BULL':      'Sirene',
    'CB FISH&CHIPS + RED BULL COPO': 'Sirene',
    'CB MANE BURGUER + BATATA':      'Mané',
    'CB PIPOCA DE PORCO + 2 CHOP':   'Mané',
    'CB PIPOCA PORCO + 2 CHOP CP':   'Mané',
    'CBAMERICAN FISH + BATATA':      'Sirene',
    'COMBO SALAD FISH + BATATA':     'Sirene',
    'ESP.CARNE+BT':                  'Espetto',
    'ESP.FRANGO+BT':                 'Espetto',
    'ESPETO DE CARNE':               'Espetto',
    'ESPETO FRANGO':                 'Espetto',
    'FISH CHIPS':                    'Sirene',
    'FRITAS':                        'Espetto',
    'MANE BURGUER':                  'Mané',
    'PIPOCA PORCO':                  'Mané',
    'SALAD FISH':                    'Sirene',
    'SAND CARNE':                    'Espetto',
    'SAND CHURRAS':                  'Espetto',
    'SANDU FRANGO':                  'Espetto',
}
