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

LEITURA CONTINUA (estilo readStream)
------------------------------------
LeitorStream consome as transacoes como um log: guarda um offset (os ids ja
entregues) e a cada ciclo chama a API, le o que chegou e devolve SO os
registros novos, em micro-lotes.

    from dataframe_rir import LeitorStream

    for lote in LeitorStream().stream(intervalo=60):
        if lote.empty:
            continue
        print(len(lote), "transacoes novas")

O offset fica em dados_rir2026/offset_stream.json, entao reiniciar o processo
nao reprocessa o que ja foi entregue.

Uso pela linha de comando (inspecao rapida):

    python dataframe_rir.py
    python dataframe_rir.py --info
    python dataframe_rir.py --stream --intervalo 60
"""

import argparse
import json
import os
import time

import pandas as pd

from api_transacoes_rir import (
    CAMPOS_AMBULANTE,
    CAMPOS_PRODUTO,
    CAMPOS_TRANSACAO,
    JSONL_AMBULANTES,
    JSONL_TRANSACOES,
    PASTA_DADOS,
    coletar,
    ler_jsonl,
)

# Offset do leitor continuo: ids de transacao ja entregues.
ARQ_OFFSET = "offset_stream.json"

# Intervalo padrao entre ciclos, em segundos. O WS entrega no maximo 1 hora
# por requisicao e a hora corrente e re-baixada a cada ciclo, entao nao
# adianta ciclar muito mais rapido do que a granularidade dos dados.
INTERVALO_PADRAO = 60

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


# ------------------------------------------------------------------ stream
class LeitorStream:
    """Consumidor incremental das transacoes, no estilo readStream.

    Mantem um offset - o conjunto de transacao_id ja entregues - e a cada
    ciclo devolve apenas os registros que ainda nao passaram por aqui.

    O offset e por transacao, e nao por hora, de proposito: a coleta re-baixa
    a hora corrente varias vezes para pegar lancamentos atrasados, entao os
    mesmos registros reaparecem no .jsonl. Filtrar por id garante que cada
    transacao seja entregue uma unica vez, mesmo com o processo reiniciando.

    Cancelamentos nao violam isso: a Zig cria uma transacao NOVA apontando
    para a original em transacao_original_id, entao ela vem como um registro
    inedito e e entregue normalmente.
    """

    def __init__(self, pasta=PASTA_DADOS, persistir_offset=True):
        self.pasta = pasta
        self.persistir_offset = persistir_offset
        self.arq_offset = os.path.join(pasta, ARQ_OFFSET)
        self.entregues = self._carregar_offset()

    # ------------------------------------------------------------- offset
    def _carregar_offset(self):
        if not self.persistir_offset or not os.path.exists(self.arq_offset):
            return set()
        try:
            with open(self.arq_offset, encoding="utf-8") as arquivo:
                return set(json.load(arquivo).get("transacao_ids", []))
        except (ValueError, OSError):
            # Offset corrompido nao pode derrubar a coleta: recomeca do zero,
            # o pior caso e reentregar registros que o consumidor ja viu.
            print("  aviso: offset ilegivel, recomecando do zero")
            return set()

    def _salvar_offset(self):
        if not self.persistir_offset:
            return
        os.makedirs(self.pasta, exist_ok=True)
        with open(self.arq_offset, "w", encoding="utf-8") as arquivo:
            json.dump({"atualizado_em": pd.Timestamp.now().isoformat(),
                       "total": len(self.entregues),
                       "transacao_ids": sorted(self.entregues)},
                      arquivo)

    def reiniciar(self):
        """Zera o offset: o proximo lote traz tudo de novo."""
        self.entregues = set()
        self._salvar_offset()

    # -------------------------------------------------------------- lote
    def proximo_lote(self, coletar_antes=True, **kwargs_coleta):
        """Chama a API (opcional), le o acumulado e devolve so o que e novo.

        Devolve um DataFrame ja renomeado; vazio quando nada novo chegou.
        """
        if coletar_antes:
            coletar(self.pasta, **kwargs_coleta)

        df = carregar_transacoes(self.pasta)
        if df.empty:
            return df

        coluna_id = COLUNAS_TRANSACAO["transacao_id"]
        novos = df[~df[coluna_id].isin(self.entregues)]

        if not novos.empty:
            self.entregues.update(novos[coluna_id].tolist())
            self._salvar_offset()

        return novos

    def stream(self, intervalo=INTERVALO_PADRAO, coletar_antes=True,
               **kwargs_coleta):
        """Generator infinito: a cada 'intervalo' segundos, emite um lote.

        Emite um DataFrame vazio quando nada novo chegou, para o consumidor
        poder distinguir "sem novidade" de "processo travado".
        """
        while True:
            inicio = time.monotonic()
            try:
                yield self.proximo_lote(coletar_antes, **kwargs_coleta)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                # Um ciclo que falha (rede, WS fora do ar) nao pode matar o
                # stream: avisa e tenta de novo no proximo intervalo.
                print(f"  erro no ciclo: {type(e).__name__}: {e}")

            espera = intervalo - (time.monotonic() - inicio)
            if espera > 0:
                time.sleep(espera)


def main():
    parser = argparse.ArgumentParser(
        description="Monta o DataFrame das transacoes do RIR 2026 ja coletadas.")
    parser.add_argument("--pasta", default=PASTA_DADOS,
                        help=f"pasta com os .jsonl da coleta (padrao: {PASTA_DADOS})")
    parser.add_argument("--info", action="store_true",
                        help="mostra dtypes e uso de memoria em vez das linhas")
    parser.add_argument("--ambulantes", action="store_true",
                        help="usa os movimentos de ambulante no lugar das transacoes")
    parser.add_argument("--stream", action="store_true",
                        help="leitura continua: coleta e imprime so o que chega de novo")
    parser.add_argument("--intervalo", type=int, default=INTERVALO_PADRAO,
                        help=f"segundos entre ciclos do --stream (padrao: {INTERVALO_PADRAO})")
    parser.add_argument("--sem-coletar", action="store_true",
                        help="nao chama a API; usa so o que ja esta em disco")
    args = parser.parse_args()

    # Sem coletar, o DataFrame sai com o que foi baixado na ultima execucao -
    # que pode ser de horas atras. Atualizar e o padrao.
    if not args.sem_coletar and not args.stream:
        from api_transacoes_rir import coletar
        print("Atualizando da API...")
        coletar(args.pasta)
        print()

    if args.stream:
        leitor = LeitorStream(args.pasta)
        print(f"Lendo em tempo real a cada {args.intervalo}s "
              f"({len(leitor.entregues)} transacoes ja entregues). Ctrl+C para parar.")
        try:
            for lote in leitor.stream(intervalo=args.intervalo):
                marca = pd.Timestamp.now().strftime("%H:%M:%S")
                if lote.empty:
                    print(f"[{marca}] sem novidade")
                else:
                    print(f"[{marca}] +{len(lote)} linhas novas")
                    with pd.option_context("display.max_columns", None,
                                           "display.width", 200):
                        print(lote[["Transação id", "Nome do Ponto",
                                    "Produto", "Valor Total"]].to_string(index=False))
        except KeyboardInterrupt:
            print("\nStream encerrado.")
        return

    df = (carregar_movimentos_ambulantes(args.pasta) if args.ambulantes
          else carregar_transacoes(args.pasta))

    print(f"{len(df)} linhas x {len(df.columns)} colunas")
    if not df.empty:
        ultima = df[COLUNAS_TRANSACAO["data_hora_realizacao"]].max()
        atraso = (pd.Timestamp.now() - ultima).total_seconds()
        print(f"ultima transacao: {ultima:%d/%m %H:%M:%S} ({max(atraso, 0):.0f}s atras)")
    print()

    if args.info:
        df.info()
        return

    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(df.head(10))


if __name__ == "__main__":
    main()
