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

# Codigo do evento e fixo do RIR26; serve de fallback quando as credenciais da
# Zig nao estao presentes (ex.: o coletor do Plano B importa stream_rir so pelas
# funcoes de agregacao e NAO usa a API da Zig).
CODIGO_EVENTO_PADRAO = 38049


def carregar():
    """(token_parceiro, codigo_evento). NAO trava no import.

    O token pode vir vazio quando so as funcoes de agregacao sao usadas (Plano
    B). Quem realmente chama a API da Zig valida o token na hora e reporta a
    falta - ver exigir_token().
    """
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
        except (FileNotFoundError, ValueError):
            pass

    return (token or ""), int(evento) if evento else CODIGO_EVENTO_PADRAO


def exigir_token():
    """Garante que ha token da Zig; usar antes de chamar a API. Erro claro se falta."""
    if not TOKEN_PARCEIRO:
        raise SystemExit(
            f"Credenciais da Zig ausentes. Crie {ARQ_CREDENCIAIS} com\n"
            '  {"token_parceiro": "...", "codigo_evento": 12345}\n'
            "ou defina ZIG_TOKEN_PARCEIRO e ZIG_CODIGO_EVENTO no ambiente.")
    return TOKEN_PARCEIRO


TOKEN_PARCEIRO, CODIGO_EVENTO = carregar()
