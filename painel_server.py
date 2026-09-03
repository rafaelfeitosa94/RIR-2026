"""
Painel de vendas ao vivo do RIR 2026.

Mesmo desenho do painel do Espetto: http.server da stdlib (sem FastAPI), um
snapshot agregado empurrado por SSE e o front em ApexCharts. Porta 8531, para
nao brigar com o painel do Espetto (8530).

  GET /                 painel.html
  GET /api/snapshot     snapshot atual em JSON
  GET /api/stream       SSE - empurra o snapshot a cada atualizacao

Uma thread de fundo chama a API da Zig a cada INTERVALO segundos, remonta o
snapshot e acorda os clientes conectados. O navegador nao consulta a Zig
diretamente (nao teria como: o WS exige header de token e nao manda CORS).

TOKEN
-----
Aqui o token e REAPROVEITADO por padrao, ao contrario do coletor. Um painel
que cicla a cada 20s renovando o token faria ~6 chamadas de GeraToken por
minuto - mais de 100 mil ao longo do evento, sem ganho nenhum. Use
--renovar-token para seguir o comportamento do coletor.

Uso:
    python painel_server.py
    python painel_server.py --intervalo 30 --porta 8531
    python painel_server.py --sem-coletar     # so le o que ja esta em disco
"""

import argparse
import contextlib
import io
import json
import os
import sys
import threading
import time
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# O painel pode ser iniciado de qualquer lugar (launch.json, atalho, tarefa
# agendada). Fixar o diretorio no do proprio script faz painel.html,
# credenciais.json e dados_rir2026/ serem achados de todo jeito.
_AQUI = os.path.dirname(os.path.abspath(__file__))
os.chdir(_AQUI)
if _AQUI not in sys.path:
    sys.path.insert(0, _AQUI)

import pandas as pd

from api_transacoes_rir import PASTA_DADOS, coletar
from dataframe_rir import COLUNAS_TRANSACAO, carregar_transacoes
from stream_rir import montar_snapshot as _montar_snapshot

PORTA_PADRAO = 8531
INTERVALO_PADRAO = 20
ARQ_PAINEL = "index.html"

# Quantas transacoes o feed ao vivo mostra.
LIMITE_FEED = 25

COL_REALIZACAO = COLUNAS_TRANSACAO["data_hora_realizacao"]
COL_ID = COLUNAS_TRANSACAO["transacao_id"]
COL_PONTO = COLUNAS_TRANSACAO["nome_ponto"]
COL_PRODUTO = COLUNAS_TRANSACAO["produto"]
COL_VALOR = COLUNAS_TRANSACAO["valor"]
COL_TOTAL = COLUNAS_TRANSACAO["valor_total"]
COL_QTD = COLUNAS_TRANSACAO["quantidade"]
COL_OPERACAO = COLUNAS_TRANSACAO["operacao"]
COL_PAGAMENTO = COLUNAS_TRANSACAO["forma_pagamento"]


# ---------------------------------------------------------------- snapshot
class Estado:
    """Guarda o snapshot corrente e avisa os clientes SSE quando ele muda."""

    def __init__(self):
        self.snapshot = {"versao": 0, "vazio": True}
        self.versao = 0
        self._condicao = threading.Condition()

    def publicar(self, snapshot):
        with self._condicao:
            self.versao += 1
            snapshot["versao"] = self.versao
            self.snapshot = snapshot
            self._condicao.notify_all()

    def esperar(self, versao_vista, timeout):
        """Bloqueia ate haver versao nova (ou o timeout, para keepalive)."""
        with self._condicao:
            if self.versao != versao_vista:
                return self.snapshot
            self._condicao.wait(timeout)
            return self.snapshot if self.versao != versao_vista else None


ESTADO = Estado()

# O botao "Atualizar agora" seta este evento: o laco acorda antes da hora em
# vez de esperar o intervalo cheio. Usar o mesmo laco (e nao disparar uma
# coleta paralela) evita duas chamadas simultaneas a API da Zig.
ACORDAR = threading.Event()


def montar_snapshot(pasta=PASTA_DADOS):
    """Le o acumulado em disco e monta o snapshot que o painel consome.

    A montagem em si mora em stream_rir.py, para o painel local (SSE) e o
    painel estatico do GitHub Pages verem exatamente os mesmos numeros.
    """
    return _montar_snapshot(carregar_transacoes(pasta))


def laco_coleta(intervalo, pasta, coletar_antes, renovar_token):
    """Thread de fundo: atualiza da API e republica o snapshot."""
    while True:
        inicio = time.monotonic()
        try:
            if coletar_antes:
                # A coleta e falante demais para um laco de 20s: engole a
                # saida e reporta so o essencial.
                with contextlib.redirect_stdout(io.StringIO()):
                    coletar(pasta, renovar_sempre=renovar_token)

            snapshot = montar_snapshot(pasta)
            ESTADO.publicar(snapshot)

            atraso = snapshot.get("atraso_s")
            marca = datetime.now().strftime("%H:%M:%S")
            if snapshot["vazio"]:
                print(f"[{marca}] sem transacoes ainda")
            else:
                t = snapshot["resumo"]["totais"]
                print(f"[{marca}] {t['transacoes']} transacoes | "
                      f"R$ {t['valor_liquido']:,.2f} | dado de {atraso:.0f}s atras")
        except Exception as e:
            print(f"[erro no ciclo] {type(e).__name__}: {e}")

        espera = intervalo - (time.monotonic() - inicio)
        if espera > 0:
            ACORDAR.wait(timeout=espera)
        ACORDAR.clear()


# ------------------------------------------------------------------ http
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass  # o laco de coleta ja imprime o que interessa

    def _cabecalho(self, status, tipo, tamanho=None, extra=None):
        self.send_response(status)
        self.send_header("Content-Type", tipo)
        if tamanho is not None:
            self.send_header("Content-Length", str(tamanho))
        for chave, valor in (extra or {}).items():
            self.send_header(chave, valor)
        self.end_headers()

    def do_GET(self):
        rota = self.path.split("?")[0]

        if rota in ("/", "/index.html", "/painel.html"):
            return self._painel()
        if rota == "/api/snapshot":
            return self._json(ESTADO.snapshot)
        if rota == "/api/stream":
            return self._sse()
        if self._asset(rota):
            return

        self._cabecalho(404, "text/plain; charset=utf-8", 9)
        self.wfile.write(b"nao achei")

    # Serve assets estáticos da pasta (logo, ícones). No GitHub Pages isso é
    # automático; aqui o servidor precisa fazer à mão. Restrito a extensões
    # conhecidas e a nomes simples, para não virar leitura arbitrária de disco.
    _TIPOS = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
              ".svg": "image/svg+xml", ".ico": "image/x-icon",
              ".webp": "image/webp", ".gif": "image/gif"}

    def _asset(self, rota):
        nome = rota.lstrip("/")
        ext = os.path.splitext(nome)[1].lower()
        # Sem barras nem "..": só arquivos da própria pasta.
        if ext not in self._TIPOS or "/" in nome or "\\" in nome or ".." in nome:
            return False
        try:
            with open(nome, "rb") as arquivo:
                corpo = arquivo.read()
        except OSError:
            return False
        self._cabecalho(200, self._TIPOS[ext], len(corpo),
                        {"Cache-Control": "no-store"})
        self.wfile.write(corpo)
        return True

    def do_POST(self):
        if self.path.split("?")[0] == "/api/atualizar":
            ACORDAR.set()
            return self._json({"ok": True, "versao": ESTADO.versao})

        self._cabecalho(404, "text/plain; charset=utf-8", 9)
        self.wfile.write(b"nao achei")

    def _painel(self):
        try:
            with open(ARQ_PAINEL, "rb") as arquivo:
                corpo = arquivo.read()
        except FileNotFoundError:
            corpo = f"{ARQ_PAINEL} nao encontrado".encode("utf-8")
            self._cabecalho(500, "text/plain; charset=utf-8", len(corpo))
            self.wfile.write(corpo)
            return

        self._cabecalho(200, "text/html; charset=utf-8", len(corpo),
                        {"Cache-Control": "no-store"})
        self.wfile.write(corpo)

    def _json(self, dados):
        corpo = json.dumps(dados, ensure_ascii=False).encode("utf-8")
        self._cabecalho(200, "application/json; charset=utf-8", len(corpo),
                        {"Cache-Control": "no-store"})
        self.wfile.write(corpo)

    def _sse(self):
        self._cabecalho(200, "text/event-stream; charset=utf-8", None,
                        {"Cache-Control": "no-store", "Connection": "keep-alive"})
        vista = -1
        try:
            while True:
                snapshot = ESTADO.esperar(vista, timeout=20)
                if snapshot is None:
                    # Nada novo: um comentario SSE segura a conexao viva
                    # atravessando proxies e o timeout do navegador.
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue

                vista = snapshot["versao"]
                corpo = json.dumps(snapshot, ensure_ascii=False)
                self.wfile.write(f"data: {corpo}\n\n".encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass  # o navegador fechou a aba; nada a fazer


def main():
    parser = argparse.ArgumentParser(
        description="Painel de vendas ao vivo do RIR 2026.")
    parser.add_argument("--porta", type=int, default=PORTA_PADRAO,
                        help=f"porta HTTP (padrao: {PORTA_PADRAO})")
    parser.add_argument("--intervalo", type=int, default=INTERVALO_PADRAO,
                        help=f"segundos entre coletas (padrao: {INTERVALO_PADRAO})")
    parser.add_argument("--sem-coletar", action="store_true",
                        help="nao chama a API; so reprocessa o que esta em disco")
    parser.add_argument("--renovar-token", action="store_true",
                        help="gera um token por requisicao (padrao: reaproveita)")
    parser.add_argument("--sem-navegador", action="store_true",
                        help="nao abre o navegador sozinho")
    parser.add_argument("--pasta", default=PASTA_DADOS,
                        help=f"pasta dos dados brutos (padrao: {PASTA_DADOS})")
    args = parser.parse_args()

    endereco = f"http://localhost:{args.porta}"
    print("=" * 70)
    print(f"Painel RIR 2026  ->  {endereco}")
    print(f"coleta a cada {args.intervalo}s | "
          f"token {'renovado' if args.renovar_token else 'reaproveitado'}")
    print("=" * 70)

    threading.Thread(target=laco_coleta, daemon=True,
                     args=(args.intervalo, args.pasta, not args.sem_coletar,
                           args.renovar_token)).start()

    servidor = ThreadingHTTPServer(("", args.porta), Handler)
    servidor.daemon_threads = True

    if not args.sem_navegador:
        threading.Timer(1.0, lambda: webbrowser.open(endereco)).start()

    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        print("\nPainel encerrado.")
        servidor.shutdown()


if __name__ == "__main__":
    main()
