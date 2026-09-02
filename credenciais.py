"""
Credenciais da API da Zig, carregadas de fora do codigo.

Este repositorio e publicado para o app do Lovable ler os JSONs, entao
qualquer segredo commitado vaza. A ordem de busca e:

  1. variaveis de ambiente ZIG_TOKEN_PARCEIRO e ZIG_CODIGO_EVENTO;
  2. o arquivo credenciais.json ao lado deste modulo (ignorado pelo git).

Formato do credenciais.json:

    {"token_parceiro": "XXXXXXXX", "codigo_evento": 12345}

Modulo separado de proposito: api_teste_token.py e api_transacoes_rir.py
importam um do outro, e por aqui os dois pegam a credencial sem criar um
ciclo de importacao.
"""

import json
import os

ARQ_CREDENCIAIS = "credenciais.json"


def carregar():
    """Devolve (token_parceiro, codigo_evento). Encerra com erro claro se faltar."""
    token = os.environ.get("ZIG_TOKEN_PARCEIRO")
    evento = os.environ.get("ZIG_CODIGO_EVENTO")

    if not token or not evento:
        caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               ARQ_CREDENCIAIS)
        try:
            with open(caminho, encoding="utf-8") as arquivo:
                dados = json.load(arquivo)
            token = token or dados.get("token_parceiro")
            evento = evento or dados.get("codigo_evento")
        except FileNotFoundError:
            raise SystemExit(
                f"Credenciais nao encontradas. Crie {ARQ_CREDENCIAIS} com\n"
                '  {"token_parceiro": "...", "codigo_evento": 12345}\n'
                "ou defina ZIG_TOKEN_PARCEIRO e ZIG_CODIGO_EVENTO no ambiente.")
        except ValueError as e:
            raise SystemExit(f"{ARQ_CREDENCIAIS} nao e um JSON valido: {e}")

    if not token or not evento:
        raise SystemExit("token_parceiro ou codigo_evento ausente nas credenciais.")

    return token, int(evento)


TOKEN_PARCEIRO, CODIGO_EVENTO = carregar()
