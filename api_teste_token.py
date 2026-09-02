"""
Teste de geracao de token - API de Relatorios da Zig (WebService v1.05.003).

Documentacao: "Zig - Integracao via WebService Relatorios 1.09.pdf"

  POST /api/relatorios/GeraToken
    header : token_parceiro  (codigo do parceiro integrador)
    body   : codigo_evento   (int)

  Retorno:
    { "codigo_retorno": 0,
      "mensagem_erro": "",
      "dados": { "token_acesso": "...", "data_expiracao": "..." } }

Obs.: a documentacao nao explicita o verbo HTTP nem onde vai o codigo_evento,
apenas que os parametros com asterisco (*) vao no header. Por isso o script
tenta algumas variacoes de envio ate uma responder codigo_retorno = 0.
"""

import json

import requests

# ---------------------------------------------------------------- endpoints
URL_PROD = "https://app.netpdv.com:5566/api/relatorios/GeraToken"
URL_DEV = "http://devapp.netpdv.info:5566/api/relatorios/GeraToken"

# ------------------------------------------------------------- credenciais
# Ficam fora do codigo (credenciais.json / variaveis de ambiente), porque
# este repositorio e publicado para o app do Lovable ler os JSONs.
from credenciais import CODIGO_EVENTO, TOKEN_PARCEIRO

PARCEIROS = [TOKEN_PARCEIRO]

TIMEOUT = 30

# Tabela de erros da documentacao (secao 1.3)
ERROS = {
    1001: "Falha na obtencao dos parametros (parametro obrigatorio faltando).",
    1003: "Falha ao processar a requisicao (erro interno - contatar a Zig).",
    2001: "Parceiro nao cadastrado.",
    2002: "Parceiro encontra-se inativo no sistema.",
    2003: "O parceiro nao possui acesso ao evento informado.",
}


def _requisicao(url, token_parceiro, codigo_evento, estrategia, verificar_ssl=True):
    """Monta e dispara uma variacao da chamada de GeraToken."""
    headers = {
        "token_parceiro": token_parceiro,
        "Accept": "application/json",
    }
    kwargs = {"headers": headers, "timeout": TIMEOUT, "verify": verificar_ssl}

    if estrategia == "post_json":
        headers["Content-Type"] = "application/json"
        kwargs["json"] = {"codigo_evento": codigo_evento}
        return requests.post(url, **kwargs)

    if estrategia == "post_form":
        kwargs["data"] = {"codigo_evento": codigo_evento}
        return requests.post(url, **kwargs)

    if estrategia == "post_query":
        kwargs["params"] = {"codigo_evento": codigo_evento}
        return requests.post(url, **kwargs)

    if estrategia == "post_header":
        # codigo_evento tambem no header, caso o WS espere tudo la
        headers["codigo_evento"] = str(codigo_evento)
        headers["Content-Type"] = "application/json"
        kwargs["json"] = {"codigo_evento": codigo_evento}
        return requests.post(url, **kwargs)

    if estrategia == "get_query":
        kwargs["params"] = {"codigo_evento": codigo_evento}
        return requests.get(url, **kwargs)

    raise ValueError(f"Estrategia desconhecida: {estrategia}")


def gerar_token(token_parceiro, codigo_evento=CODIGO_EVENTO, url=URL_PROD,
                verificar_ssl=True, verbose=True):
    """Tenta gerar o token_acesso para um parceiro.

    Retorna o dict de 'dados' em caso de sucesso, ou None.
    """
    estrategias = ["post_json", "post_form", "post_query", "post_header", "get_query"]

    for estrategia in estrategias:
        try:
            resp = _requisicao(url, token_parceiro, codigo_evento, estrategia,
                               verificar_ssl)
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
            if verbose:
                print(f"  [{estrategia}] HTTP {resp.status_code} - SUCESSO")
                print(f"    token_acesso  : {dados.get('token_acesso')}")
                print(f"    data_expiracao: {dados.get('data_expiracao')}")
            return dados

        if verbose:
            msg = corpo.get("mensagem_erro") or ERROS.get(codigo, "")
            print(f"  [{estrategia}] HTTP {resp.status_code} - "
                  f"codigo_retorno={codigo} | {msg}")

        # Erro de negocio (parceiro invalido/sem acesso) nao muda com a
        # estrategia de envio - nao adianta insistir nas outras.
        if codigo in (2001, 2002, 2003):
            return None

    return None


def main():
    for url, ambiente, verificar_ssl in [(URL_PROD, "PRODUCAO", True),
                                         (URL_DEV, "DESENVOLVIMENTO", True)]:
        print("=" * 78)
        print(f"AMBIENTE {ambiente}: {url}")
        print(f"codigo_evento: {CODIGO_EVENTO}")
        print("=" * 78)

        sucessos = {}
        for parceiro in PARCEIROS:
            print(f"\nParceiro: {parceiro}")
            dados = gerar_token(parceiro, url=url, verificar_ssl=verificar_ssl)
            if dados:
                sucessos[parceiro] = dados

        print("\n" + "-" * 78)
        if sucessos:
            print(f"RESUMO {ambiente}: {len(sucessos)} de {len(PARCEIROS)} parceiros com token.")
            print(json.dumps(sucessos, indent=2, ensure_ascii=False))
            return sucessos
        print(f"RESUMO {ambiente}: nenhum token obtido.\n")

    return {}


if __name__ == "__main__":
    main()
