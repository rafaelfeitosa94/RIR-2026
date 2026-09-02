# RIR 2026 — coleta da API da Zig e publicação para o Lovable

Coletor das transações do evento **38049 (RIR26 GRUPO IMPETTUS)**, de 02/09 a
14/09/2026, e publicação dos dados em JSON para o dashboard feito no Lovable.

## Como as peças se encaixam

```
API da Zig  ──►  api_transacoes_rir.py  ──►  dados_rir2026/*.jsonl   (local, não vai pro git)
                                              │
                        dataframe_rir.py  ◄───┘   DataFrame pandas, colunas renomeadas
                                              │
                          stream_rir.py    ◄───┘   escreve publico/*.json + commit/push
                                              │
                                     GitHub  ◄─┘
                                              │
                              app do Lovable  ◄─┘   fetch dos JSONs
```

O app do Lovable roda TypeScript no navegador: **ele não executa Python**. O
GitHub aqui é só transporte de arquivo — o Python roda nesta máquina.

| Arquivo | Papel |
|---|---|
| `credenciais.py` | Lê o código do parceiro de fora do código-fonte |
| `api_teste_token.py` | `GeraToken` |
| `api_transacoes_rir.py` | Coleta incremental, hora a hora, e grava `.jsonl` |
| `dataframe_rir.py` | DataFrame renomeado + `LeitorStream` (leitura contínua) |
| `stream_rir.py` | Publica os JSONs e commita no repositório |
| `exportar_xlsx.py` | Coleta e exporta o DataFrame para uma planilha `.xlsx` formatada |
| `painel_server.py` + `index.html` | Painel de vendas ao vivo (SSE + ApexCharts), porta 8531 |

## Setup

### 1. Credenciais

Já existe um `credenciais.json` nesta máquina (ignorado pelo git). Em outra
máquina, crie:

```json
{"token_parceiro": "SEU_CODIGO", "codigo_evento": 38049}
```

Ou use as variáveis `ZIG_TOKEN_PARCEIRO` e `ZIG_CODIGO_EVENTO`.

### 2. Conectar ao GitHub

O repositório local já foi inicializado (`git init`). Falta identidade e
remote — **isto você precisa rodar**, porque envolve autenticar na sua conta:

```bash
git config user.name "Rafael Feitosa"
```

```bash
git config user.email "bi@grupoimpettus.com.br"
```

```bash
git remote add origin https://github.com/SEU_USUARIO/SEU_REPO.git
```

```bash
git add . && git commit -m "coletor RIR26" && git push -u origin main
```

Se o repositório remoto já tiver arquivos, rode antes
`git pull --rebase origin main`.

### 3. Rodar durante o evento

```bash
python stream_rir.py
```

Fica em ciclo (padrão 120s): coleta → publica → commita → push. `Ctrl+C` para.
Para deixar rodando sozinho, agende no Agendador de Tarefas do Windows com
gatilho "ao iniciar o computador".

Opções úteis: `--uma-vez`, `--sem-git`, `--intervalo 300`, `--sem-detalhe`.

### Painel ao vivo

```bash
python painel_server.py
```

Ou duplo-clique em `iniciar_painel.bat`. Sobe em <http://localhost:8531> e abre
o navegador. Uma thread de fundo coleta da API a cada 20s e empurra o snapshot
por SSE (`/api/stream`) — mesma arquitetura do painel do Espetto, em porta
própria para os dois rodarem juntos.

Diferente do coletor, aqui o **token é reaproveitado** por padrão: um painel que
cicla a cada 20s renovando o token faria ~6 chamadas de `GeraToken` por minuto,
mais de 100 mil ao longo do evento, sem ganho nenhum. `--renovar-token` volta ao
comportamento do coletor.

### Publicar o painel no GitHub Pages

O mesmo `index.html` funciona de duas formas, e ele detecta sozinho em qual está:

| Modo | Como chega o dado | Selo | Latência |
|---|---|---|---|
| Servidor local (`painel_server.py`) | SSE, push a cada coleta | "ao vivo" | segundos |
| GitHub Pages | busca `publico/*.json` a cada 60s | "publicado" | 2–8 min |

No Pages não há Python: a página lê os JSONs que o `stream_rir.py` commita. Para
ligar, com o repositório já no GitHub:

1. **Settings → Pages → Source:** `Deploy from a branch`, branch `main`, pasta `/ (root)`.
2. Deixe o `stream_rir.py` rodando nesta máquina — é ele que alimenta o site.
3. O link fica **`https://SEU_USUARIO.github.io/SEU_REPO/`**.

> **O site é público.** GitHub Pages em repositório privado exige plano pago
> (Pro/Team). Num repositório público, qualquer pessoa com o link vê o
> faturamento do evento. CPF e e-mail já são removidos, mas receita por ponto e
> por produto é informação comercial — confirme com a operação antes de publicar,
> ou hospede num lugar com autenticação.

### Exportar uma planilha

```bash
python exportar_xlsx.py --resumo
```

Coleta da API **antes** de exportar, então a planilha sai com os dados do
momento (defasagem de segundos). `--sem-coletar` pula a API e usa só o que já
está em disco — mais rápido, porém desatualizado. O mesmo vale para
`python dataframe_rir.py`.

## O que é publicado

```
publico/
  indice.json     lista das horas já fechadas
  resumo.json     KPIs agregados (pequeno, atualiza todo ciclo)
  recentes.json   últimas 300 transações (feed ao vivo)
  horas/2026-09-02T13.json   detalhe completo de uma hora fechada
```

**Arquivos de hora são imutáveis.** Só são escritos depois que a hora fecha, uma
única vez. Isso é proposital: cada linha ocupa ~870 bytes, e numa hora de pico
(15 mil transações) o arquivo passa de 18 MB — reescrever isso a cada 2 minutos
geraria ~560 MB de objetos git **por hora** e estouraria o repositório em um ou
dois dias. O que se move a cada ciclo são só `resumo.json` e `recentes.json`,
ambos de tamanho limitado independente do volume de vendas.

Rode `git gc` de vez em quando durante o evento para compactar o histórico.

## Consumindo no Lovable

```ts
const BASE = "https://raw.githubusercontent.com/SEU_USUARIO/SEU_REPO/main/publico";

// KPIs — chame a cada 60s
export async function buscarResumo() {
  const r = await fetch(`${BASE}/resumo.json?t=${Date.now()}`);
  return r.json();
}

// Feed ao vivo
export async function buscarRecentes() {
  const r = await fetch(`${BASE}/recentes.json?t=${Date.now()}`);
  return r.json();
}

// Histórico completo, incremental: busque só as horas que ainda não tem.
// Horas fechadas nunca mudam, então podem ser cacheadas para sempre.
export async function buscarHorasNovas(jaTenho: Set<string>) {
  const indice = await (await fetch(`${BASE}/indice.json?t=${Date.now()}`)).json();
  const novas = indice.horas_fechadas.filter((h: any) => !jaTenho.has(h.hora));
  return Promise.all(
    novas.map(async (h: any) => ({
      hora: h.hora,
      linhas: await (await fetch(`${BASE}/${h.arquivo}`)).json(),
    }))
  );
}
```

O `?t=${Date.now()}` é necessário: o `raw.githubusercontent.com` serve conteúdo
em cache por até ~5 minutos.

## Latência real

Isto **não é streaming**. Entre a venda e o dashboard há:

- o intervalo do ciclo (120s);
- o tempo de commit e push;
- o cache do `raw.githubusercontent.com` (até ~5 min).

Na prática, **2 a 8 minutos**. Para tempo real de verdade seria preciso um
banco com push (Supabase Realtime) ou um endpoint SSE próprio.

## Dados pessoais

As transações cashless trazem `documento_cliente` (CPF) e `email_cliente`. Um
repositório que o navegador lê sem autenticação é público na prática, então
**essas duas colunas são removidas antes de publicar**. A flag
`--incluir-dados-pessoais` mantém, e só deve ser usada em repositório privado
com token no app.

## Armadilhas conhecidas

- **Intervalo de 1 hora.** O WS recusa requisições com intervalo maior
  (`codigo_retorno 1007`, ausente da documentação). O coletor já fatia.
- **Datas em ISO.** `dd/MM/yyyy` é recusado com `1001`.
- **`pd.to_datetime` precisa de `format="ISO8601"`.** O WS varia a precisão dos
  segundos entre registros; sem isso o pandas transforma parte em `NaT` calado.
- **`Valor Total` não tem sinal.** O campo `Valor` da transação vem negativo em
  cancelamentos, mas o `Valor Total` do produto continua positivo. Somar
  `Valor Total` cru infla o faturamento — o `resumo.json` já abate os
  cancelamentos, mas se for agregar por conta própria, use o sinal de `Valor`.
