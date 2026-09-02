"""
Coleta das transacoes do evento 38049 (RIR 2026) - 02/09/2026 a 14/09/2026.

Arquivo autossuficiente: depende apenas de api_teste_token.py (GeraToken) e
da lib requests. Fala direto com o WebService de relatorios da Zig
(documentacao "Zig - Integracao via WebService Relatorios 1.09", secao 2).

  POST /api/relatorios/ListaTransacoes
    header : token_acesso
    body   : data_inicio, data_fim (obrigatorios)
             codigo_ponto, codigo_operador (opcionais)

Comportamentos do WS observados em producao que NAO constam na documentacao:

  * O intervalo de cada requisicao nao pode passar de 1 hora. Acima disso
    responde codigo_retorno 1007 ("Nao e possivel realizar uma requisicao com
    o intervalo superior a uma hora"), erro ausente da tabela da secao 2.3.
  * As datas precisam vir em ISO ("2026-09-02T22:00:00"); dd/MM/yyyy e
    recusado com 1001.
  * O WS NAO devolve um token novo no corpo da resposta, e o token continua
    valido ate a data_expiracao (~24h). Ver GerenciadorToken abaixo.

TOKEN
-----
Por padrao o token e renovado a cada requisicao, como manda a documentacao
("a cada chamada, sera enviado um novo token e o anterior expirado"). Como o
WS nao devolve o proximo token na resposta, renovar significa chamar
GeraToken antes de cada ListaTransacoes - ou seja, o dobro de requisicoes
(624 no evento inteiro em vez de 312).

Na pratica o token e reaproveitavel ate expirar; --reusar-token desliga a
renovacao e corta o numero de requisicoes pela metade.

COLETA INCREMENTAL
------------------
O periodo do evento e maior do que uma execucao cobre enquanto ele ainda
acontece, entao a coleta pode ser repetida a vontade:

  * horas ainda no futuro nao sao requisitadas;
  * cada hora concluida e baixada uma vez e marcada no arquivo de estado;
  * a hora corrente e sempre re-baixada, porque ainda recebe lancamentos
    atrasados. So vira definitiva depois de ATRASO_SEGURANCA;
  * o retorno bruto e acumulado em .jsonl, uma linha por registro.

Interromper com Ctrl+C nao perde o que ja foi baixado.

Este modulo apenas COLETA e ARMAZENA. Para consumir os dados ja tratados
(achatados por produto, colunas renomeadas, tipos convertidos), use
dataframe_rir.py, que le estes .jsonl e devolve um DataFrame pandas.

Uso:
    python api_transacoes_rir.py                  # coleta o que ha ate agora
    python api_transacoes_rir.py --reusar-token   # metade das requisicoes
    python api_transacoes_rir.py --refazer        # ignora o estado, rebaixa tudo
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta

import requests

from api_teste_token import gerar_token

# ---------------------------------------------------------------- endpoints
URL_PROD = "https://app.netpdv.com:5566/api/relatorios/ListaTransacoes"
URL_DEV = "http://devapp.netpdv.info:5566/api/relatorios/ListaTransacoes"

TOKEN_URL_PROD = "https://app.netpdv.com:5566/api/relatorios/GeraToken"
TOKEN_URL_DEV = "http://devapp.netpdv.info:5566/api/relatorios/GeraToken"

# ------------------------------------------------------------- credenciais
TOKEN_PARCEIRO = "09C7DF1421"
CODIGO_EVENTO = 38049

TIMEOUT = 180

# O WS so aceita datas em ISO; o formato "legivel" e usado apenas nos prints.
FORMATO_API = "%Y-%m-%dT%H:%M:%S"
FORMATO_DATA = "%Y-%m-%d %H:%M:%S"

# Limite de intervalo por requisicao imposto pelo WS (erro 1007).
JANELA_MAXIMA = timedelta(hours=1)

# ------------------------------------------------------- periodo do evento
# 14/09 inclusive: o fim exclusivo e a virada para 15/09.
EVENTO_INICIO = datetime(2026, 9, 2, 0, 0, 0)
EVENTO_FIM = datetime(2026, 9, 15, 0, 0, 0)

# Uma hora so e considerada fechada depois desta folga, para dar tempo de as
# transacoes atrasadas (data_hora_cadastro > data_hora_realizacao) chegarem.
ATRASO_SEGURANCA = timedelta(minutes=30)

# Renovar o token a cada requisicao dobra as chamadas, entao vale insistir
# um pouco quando o GeraToken falha por instabilidade.
TENTATIVAS_TOKEN = 3
ESPERA_TOKEN = 2  # segundos, multiplicado pelo numero da tentativa

# ---------------------------------------------------------------- arquivos
PASTA_DADOS = "dados_rir2026"
JSONL_TRANSACOES = "transacoes.jsonl"
JSONL_AMBULANTES = "movimentos_ambulantes.jsonl"
ARQ_ESTADO = "estado_coleta.json"

# Tabela de erros da documentacao (secao 2.3) + 1007, observado em producao
ERROS = {
    1001: "Falha na obtencao dos parametros (parametro obrigatorio faltando).",
    1003: "Falha ao processar a requisicao (erro interno - contatar a Zig).",
    1004: "Token de acesso invalido.",
    1005: "Token de acesso expirado.",
    1007: "Intervalo superior a uma hora (nao documentado).",
}

# Colunas de saida: campos do registro + campos do produto (secao 2.2)
CAMPOS_TRANSACAO = [
    "transacao_id", "transacao_original_id", "codigo_evento", "nome_evento",
    "data_hora_realizacao", "data_hora_cadastro", "data_hora_confirmacao",
    "sessao", "operacao", "tipo_ponto", "codigo_ponto", "nome_ponto",
    "latitude", "longitude", "operador", "terminal", "valor", "taxa_ativacao",
    "status", "cod_forma_pagamento", "forma_pagamento", "chip",
    "chip_informado_manual", "documento_cliente", "email_cliente",
]

CAMPOS_AMBULANTE = [
    "movimento_ambulante_id", "codigo_evento", "nome_evento",
    "data_hora_realizacao", "data_hora_cadastro", "data_hora_confirmacao",
    "sessao", "operacao", "codigo_ponto", "nome_ponto", "operador",
    "terminal", "valor", "status", "nome_ambulante", "documento_ambulante",
]

CAMPOS_PRODUTO = [
    "categoria_produto", "cod_produto", "cod_produto_parceiro", "produto",
    "fabricante", "quantidade", "valor_unitario", "valor_total",
]


# ------------------------------------------------------------------- token
class GerenciadorToken:
    """Fornece o token_acesso de cada requisicao.

    Com renovar_sempre=True (padrao) chama GeraToken antes de cada
    ListaTransacoes, seguindo a documentacao: "a cada chamada, sera enviado
    um novo token e o anterior expirado".

    A doc tambem diz que o token pode vir "pela requisicao anterior". Se o WS
    passar a devolver o proximo token na resposta, adotar() o aproveita e a
    chamada extra ao GeraToken e dispensada - e o que torna a renovacao
    barata. Hoje, em producao, isso nao acontece.

    Com renovar_sempre=False o mesmo token e reaproveitado ate expirar ou ate
    ser recusado (1004/1005), metade das requisicoes.
    """

    def __init__(self, token_url=TOKEN_URL_PROD, verificar_ssl=True,
                 renovar_sempre=True):
        self.token_url = token_url
        self.verificar_ssl = verificar_ssl
        self.renovar_sempre = renovar_sempre
        self.geradas = 0
        self.data_expiracao = None
        self._token = None
        self._veio_da_resposta = False

    def proximo(self, verbose=False):
        """Token a usar na proxima requisicao (None se o GeraToken falhar)."""
        # Um token recem-recebido da resposta anterior ja e "novo": usa ele e
        # poupa a ida ao GeraToken.
        if self._token is None or (self.renovar_sempre and not self._veio_da_resposta):
            self._renovar(verbose)
        self._veio_da_resposta = False
        return self._token

    def adotar(self, token):
        """Aproveita o token que o WS devolveu junto da resposta, se houver."""
        if token:
            self._token = token
            self._veio_da_resposta = True

    def invalidar(self):
        """Descarta o token atual: a proxima chamada a proximo() gera outro."""
        self._token = None
        self._veio_da_resposta = False

    def _renovar(self, verbose=False):
        for tentativa in range(1, TENTATIVAS_TOKEN + 1):
            dados = gerar_token(TOKEN_PARCEIRO, CODIGO_EVENTO, url=self.token_url,
                                verificar_ssl=self.verificar_ssl, verbose=verbose)
            if dados and dados.get("token_acesso"):
                self._token = dados["token_acesso"]
                self.data_expiracao = dados.get("data_expiracao")
                self.geradas += 1
                return
            if tentativa < TENTATIVAS_TOKEN:
                espera = ESPERA_TOKEN * tentativa
                print(f"  falha ao gerar token (tentativa {tentativa}/{TENTATIVAS_TOKEN})"
                      f" - nova tentativa em {espera}s")
                time.sleep(espera)

        self._token = None


# --------------------------------------------------------------- requisicao
def _requisicao(url, token_acesso, filtros, estrategia, verificar_ssl=True):
    """Monta e dispara uma variacao da chamada de ListaTransacoes."""
    headers = {
        "token_acesso": token_acesso,
        "Accept": "application/json",
    }
    kwargs = {"headers": headers, "timeout": TIMEOUT, "verify": verificar_ssl}

    if estrategia == "post_json":
        headers["Content-Type"] = "application/json"
        kwargs["json"] = filtros
        return requests.post(url, **kwargs)

    if estrategia == "post_form":
        kwargs["data"] = filtros
        return requests.post(url, **kwargs)

    if estrategia == "post_query":
        kwargs["params"] = filtros
        return requests.post(url, **kwargs)

    if estrategia == "post_header":
        # filtros tambem no header, caso o WS espere tudo la
        for chave, valor in filtros.items():
            headers[chave] = str(valor)
        headers["Content-Type"] = "application/json"
        kwargs["json"] = filtros
        return requests.post(url, **kwargs)

    if estrategia == "get_query":
        kwargs["params"] = filtros
        return requests.get(url, **kwargs)

    raise ValueError(f"Estrategia desconhecida: {estrategia}")


def listar_transacoes(token_acesso, data_inicio, data_fim, codigo_ponto=None,
                      codigo_operador=None, url=URL_PROD, verificar_ssl=True,
                      estrategia_preferida=None, verbose=True):
    """Consulta a lista de transacoes de um intervalo de ate 1 hora.

    Retorna (dados, token_novo, estrategia) em caso de sucesso, onde 'dados'
    e o objeto com transacoes/movimentos_ambulantes e 'token_novo' e o token
    que o WS eventualmente devolva para a proxima chamada (hoje, None).
    Em caso de falha, retorna (None, None, None).

    A documentacao nao explicita o verbo HTTP nem onde vao os parametros que
    nao sao de header, entao o script tenta variacoes ate uma responder
    codigo_retorno = 0 (em producao, post_json).
    """
    filtros = {
        "data_inicio": data_inicio.strftime(FORMATO_API),
        "data_fim": data_fim.strftime(FORMATO_API),
    }
    if codigo_ponto:
        filtros["codigo_ponto"] = codigo_ponto
    if codigo_operador:
        filtros["codigo_operador"] = codigo_operador

    estrategias = ["post_json", "post_form", "post_query", "post_header", "get_query"]
    if estrategia_preferida in estrategias:
        # Ja sabemos qual funciona - tenta ela primeiro e mantem as outras
        # como fallback caso o WS mude de comportamento.
        estrategias.remove(estrategia_preferida)
        estrategias.insert(0, estrategia_preferida)

    for estrategia in estrategias:
        try:
            resp = _requisicao(url, token_acesso, filtros, estrategia, verificar_ssl)
        except requests.exceptions.SSLError as e:
            if verbose:
                print(f"  [{estrategia}] erro de SSL: {e}")
            continue
        except requests.exceptions.RequestException as e:
            if verbose:
                print(f"  [{estrategia}] falha de conexao: {type(e).__name__}: {e}")
            continue

        try:
            corpo = resp.json()
        except ValueError:
            if verbose:
                trecho = resp.text[:200].replace("\n", " ")
                print(f"  [{estrategia}] HTTP {resp.status_code} - resposta nao-JSON: {trecho}")
            continue

        codigo = corpo.get("codigo_retorno")

        if codigo == 0:
            dados = corpo.get("dados") or {}
            # Caso o WS passe a devolver o proximo token junto do payload.
            token_novo = (dados.get("token_acesso")
                          or corpo.get("token_acesso")
                          or resp.headers.get("token_acesso"))
            if verbose:
                n_tr = len(dados.get("transacoes") or [])
                n_amb = len(dados.get("movimentos_ambulantes") or [])
                print(f"  [{estrategia}] HTTP {resp.status_code} - SUCESSO | "
                      f"{n_tr} transacoes, {n_amb} movimentos de ambulante")
            return dados, token_novo, estrategia

        if verbose:
            msg = corpo.get("mensagem_erro") or ERROS.get(codigo, "")
            print(f"  [{estrategia}] HTTP {resp.status_code} - "
                  f"codigo_retorno={codigo} | {msg}")

        # Erro de token nao muda com a estrategia de envio - o chamador
        # precisa gerar um token novo antes de tentar de novo.
        if codigo in (1004, 1005):
            return None, None, None

    return None, None, None


# ---------------------------------------------------------------- fatiamento
def fatiar(data_inicio, data_fim, janela=JANELA_MAXIMA):
    """Divide o intervalo em blocos de no maximo 1 hora (limite do WS).

    As fatias sao [inicio, fim] e compartilham o instante da borda, entao a
    leitura do .jsonl deduplica por id no final.
    """
    passo = min(janela, JANELA_MAXIMA)

    fatias = []
    inicio = data_inicio
    while inicio < data_fim:
        fim = min(inicio + passo, data_fim)
        fatias.append((inicio, fim))
        inicio = fim
    return fatias


# ------------------------------------------------------------------ estado
def carregar_estado(caminho):
    """Le o conjunto de horas ja baixadas em definitivo."""
    if not os.path.exists(caminho):
        return set()
    with open(caminho, encoding="utf-8") as arquivo:
        return set(json.load(arquivo).get("horas_concluidas", []))


def salvar_estado(caminho, horas_concluidas):
    with open(caminho, "w", encoding="utf-8") as arquivo:
        json.dump({"codigo_evento": CODIGO_EVENTO,
                   "atualizado_em": datetime.now().strftime(FORMATO_DATA),
                   "horas_concluidas": sorted(horas_concluidas)},
                  arquivo, indent=2)


# ------------------------------------------------------------------- jsonl
def acrescentar_jsonl(caminho, registros, hora):
    """Acrescenta os registros da hora, carimbando de qual fatia vieram."""
    if not registros:
        return
    with open(caminho, "a", encoding="utf-8") as arquivo:
        for registro in registros:
            registro = dict(registro)
            registro["_fatia"] = hora
            arquivo.write(json.dumps(registro, ensure_ascii=False) + "\n")


def ler_jsonl(caminho, campo_id):
    """Le o .jsonl deduplicando por id (a ocorrencia mais recente vence)."""
    if not os.path.exists(caminho):
        return []

    unicos, sem_id = {}, []
    with open(caminho, encoding="utf-8") as arquivo:
        for linha in arquivo:
            linha = linha.strip()
            if not linha:
                continue
            registro = json.loads(linha)
            chave = registro.get(campo_id)
            if chave is None:
                sem_id.append(registro)
            else:
                unicos[chave] = registro

    return list(unicos.values()) + sem_id


# ------------------------------------------------------------------ coleta
def horas_a_baixar(inicio, fim, concluidas, agora):
    """Fatias de 1h que faltam: nada no futuro, nada ja concluido.

    Devolve tuplas (inicio, fim, definitiva), onde 'definitiva' indica que a
    hora ja fechou ha tempo suficiente para nao receber mais lancamentos.
    """
    pendentes = []
    for fatia_inicio, fatia_fim in fatiar(inicio, fim):
        if fatia_inicio >= agora:
            break  # hora ainda nao comecou
        chave = fatia_inicio.strftime(FORMATO_DATA)
        if chave in concluidas:
            continue
        pendentes.append((fatia_inicio, fatia_fim,
                          fatia_fim + ATRASO_SEGURANCA <= agora))
    return pendentes


def coletar(pasta, refazer=False, renovar_sempre=True, url=URL_PROD,
            token_url=TOKEN_URL_PROD):
    """Baixa as horas pendentes do evento, gravando conforme avanca."""
    os.makedirs(pasta, exist_ok=True)
    arq_estado = os.path.join(pasta, ARQ_ESTADO)
    arq_transacoes = os.path.join(pasta, JSONL_TRANSACOES)
    arq_ambulantes = os.path.join(pasta, JSONL_AMBULANTES)

    concluidas = set() if refazer else carregar_estado(arq_estado)
    agora = datetime.now()
    pendentes = horas_a_baixar(EVENTO_INICIO, EVENTO_FIM, concluidas, agora)

    total_horas = len(fatiar(EVENTO_INICIO, EVENTO_FIM))
    print(f"Evento {CODIGO_EVENTO} | "
          f"{EVENTO_INICIO.strftime(FORMATO_DATA)} -> {EVENTO_FIM.strftime(FORMATO_DATA)}")
    print(f"{total_horas} horas no periodo | {len(concluidas)} ja concluidas | "
          f"{len(pendentes)} a baixar agora")
    print(f"agora: {agora.strftime(FORMATO_DATA)} "
          f"(horas futuras nao sao requisitadas)")
    print("token: " + ("renovado a cada requisicao"
                       if renovar_sempre else "reaproveitado ate expirar"))

    if not pendentes:
        print("Nada pendente.")
        return 0, 0, 0

    tokens = GerenciadorToken(token_url, renovar_sempre=renovar_sempre)
    estrategia_ok = None
    n_transacoes = n_ambulantes = 0

    try:
        for i, (inicio, fim, definitiva) in enumerate(pendentes, 1):
            marca = "" if definitiva else "  (parcial - sera rebaixada)"
            print(f"\n[{i}/{len(pendentes)}] {inicio.strftime(FORMATO_DATA)} "
                  f"-> {fim.strftime(FORMATO_DATA)}{marca}")

            token = tokens.proximo()
            if not token:
                print("  nao foi possivel gerar o token. Interrompendo.")
                break

            dados, token_novo, estrategia = listar_transacoes(
                token, inicio, fim, url=url, estrategia_preferida=estrategia_ok)

            if dados is None:
                # Token recusado (1004/1005) ou falha de rede: descarta o
                # token atual e repete a hora uma vez.
                print("  requisicao recusada - renovando o token e repetindo a hora")
                tokens.invalidar()
                token = tokens.proximo()
                if not token:
                    print("  falha ao renovar o token. Interrompendo.")
                    break
                dados, token_novo, estrategia = listar_transacoes(
                    token, inicio, fim, url=url, estrategia_preferida=estrategia_ok)
                if dados is None:
                    print("  hora ignorada apos a segunda tentativa "
                          "(sera tentada de novo na proxima execucao).")
                    continue

            tokens.adotar(token_novo)
            estrategia_ok = estrategia or estrategia_ok
            transacoes = dados.get("transacoes") or []
            ambulantes = dados.get("movimentos_ambulantes") or []

            chave = inicio.strftime(FORMATO_DATA)
            acrescentar_jsonl(arq_transacoes, transacoes, chave)
            acrescentar_jsonl(arq_ambulantes, ambulantes, chave)
            n_transacoes += len(transacoes)
            n_ambulantes += len(ambulantes)

            # Hora ainda "quente" nao entra no estado: precisa ser rebaixada
            # na proxima execucao para pegar lancamentos atrasados.
            if definitiva:
                concluidas.add(chave)
                salvar_estado(arq_estado, concluidas)

    except KeyboardInterrupt:
        print("\n\nInterrompido. O que ja foi baixado esta gravado - "
              "basta rodar de novo para continuar.")

    salvar_estado(arq_estado, concluidas)
    return n_transacoes, n_ambulantes, tokens.geradas


def main():
    parser = argparse.ArgumentParser(
        description="Coleta incremental das transacoes do RIR 2026 (02/09 a 14/09).")
    parser.add_argument("--reusar-token", action="store_true",
                        help="reaproveita o mesmo token ate expirar "
                             "(metade das requisicoes; o padrao e renovar a cada uma)")
    parser.add_argument("--refazer", action="store_true",
                        help="ignora o estado e rebaixa todas as horas ja passadas")
    parser.add_argument("--dev", action="store_true",
                        help="usar o ambiente de desenvolvimento")
    parser.add_argument("--pasta", default=PASTA_DADOS,
                        help=f"pasta dos dados brutos e do estado (padrao: {PASTA_DADOS})")
    args = parser.parse_args()

    print("=" * 78)

    novas_t, novas_a, tokens_gerados = coletar(
        args.pasta,
        refazer=args.refazer,
        renovar_sempre=not args.reusar_token,
        url=URL_DEV if args.dev else URL_PROD,
        token_url=TOKEN_URL_DEV if args.dev else TOKEN_URL_PROD)

    print("\n" + "-" * 78)
    print(f"Baixados nesta execucao: {novas_t} transacoes, "
          f"{novas_a} movimentos de ambulante")
    print(f"Tokens gerados: {tokens_gerados}")
    print("\nPara consumir os dados como DataFrame, use dataframe_rir.py")


if __name__ == "__main__":
    main()
