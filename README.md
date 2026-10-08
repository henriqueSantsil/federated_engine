# Federated Engine

**Virtualize dados locais e em rede usando uma única interface SQL.**

O Federated Engine é um mecanismo de virtualização de dados escrito em Rust. Ele registra arquivos CSV e Parquet, além de views publicadas por outros nós Federated Engine, em um catálogo organizado por workspaces e permite consultar essas fontes por meio de uma interface SQL. O Apache Arrow fornece a representação colunar em memória; o Rayon paraleliza operações selecionadas sobre lotes de dados; e um endpoint opcional compatível com o PostgreSQL Wire Protocol conecta ferramentas que reconhecem o protocolo PostgreSQL.

**Idiomas: Português (Brasil) | [English](README.en.md)**

> **Compatibilidade:** o suporte a SQL e ao protocolo PostgreSQL é intencionalmente parcial; o projeto não oferece compatibilidade completa com PostgreSQL. Consulte as limitações e as observações de segurança de rede antes de usar dados importantes ou expor o serviço a redes não confiáveis.

## Visão geral

| Recurso | O que oferece |
| --- | --- |
| Virtualização de dados | Registre arquivos CSV/Parquet locais e consulte-os por nomes lógicos de tabelas. |
| Interface SQL | Execute consultas em um prompt de comandos ou use a interface guiada no terminal. |
| Execução com Arrow | Leia dados em record batches do Apache Arrow para processamento colunar. |
| Operações paralelas | Use Rayon em operações selecionadas sobre lotes e filtros em memória. |
| Views P2P | Publique uma view por HTTP e consulte-a a partir de outro nó. |
| Filter Pushdown | Envie predicados compatíveis para uma view remota do Federated Engine. |
| Conectividade com BI | Disponibilize um endpoint compatível com o protocolo PostgreSQL para clientes locais. |
| Exportação | Grave resultados de consultas em CSV ou Parquet. |

## Arquitetura

```text
 ┌───────────────────┐          SQL / PostgreSQL Wire         ┌──────────────────────┐
 │ DBeaver / BI tool │ ──────────────────────────────────────> │ Federated Engine     │
 └───────────────────┘                                         │ catálogo + parser SQL│
                                                               └──────────┬───────────┘
                                                                          │
                                                           Arrow record batches
                                                                          │
                     ┌────────────────────────────────────────────────────┴───────────┐
                     │                                                                │
              ┌──────▼───────┐                                                ┌───────▼────────┐
              │ CSV /        │                                                │ Nó remoto do   │
              │ Parquet local│                                                │ Federated Engine│
              └──────────────┘                                                │ view publicada │
                                                                              └───────┬────────┘
                                                                                      │
                                                                    Consulta HTTP + filtro opcional
```

### Virtualização e execução de dados

`CREATE EXTERNAL TABLE` registra uma fonte no workspace ativo; ele não importa o arquivo para um banco de dados. O esquema de arquivos CSV é inferido a partir do conteúdo, enquanto o esquema Parquet é lido dos metadados do próprio arquivo. Depois, as consultas SQL usam o nome lógico registrado.

Durante a consulta, o mecanismo lê as fontes em record batches do Arrow, aplica as operações SQL compatíveis e pode exportar o resultado. Operações selecionadas sobre os lotes usam Rayon para processamento paralelo. O Federated Engine não é um mecanismo de armazenamento: os dados permanecem nos arquivos de origem ou são obtidos em um cache local nos casos compatíveis de fontes remotas.

### Arquitetura P2P: views publicadas, descoberta e Filter Pushdown

Cada nó HTTP pode disponibilizar views publicadas na porta `8080`:

- `PUBLISH VIEW` marca uma view para compartilhamento.
- `SERVE ON 8080;` inicia o nó HTTP.
- `GET /catalog` lista as views publicadas e seus workspaces.
- `GET /query?view=<view>&workspace=<workspace>` retorna os resultados da view em CSV.
- `GET /query?view=<view>&workspace=<workspace>&schema_only=true` retorna o esquema inferido da view.

Outro nó pode registrar essa URL `/query` como uma tabela externa. Para predicados compatíveis, o mecanismo solicitante envia um parâmetro `filter` ao nó de origem, para que o filtro seja aplicado próximo dos dados e não seja necessário transferir todas as linhas primeiro. Os filtros remotos compatíveis incluem comparações, `LIKE`, `IN` e combinações com `AND`/`OR`, usando valores literais. Expressões não compatíveis ou complexas não são suportadas como uma linguagem SQL remota genérica.

`SHOW NETWORK NODES;` procura nós Federated Engine nas sub-redes IPv4 privadas ativas, na porta `8080`. A descoberta foi projetada para redes locais: ela não é um serviço de diretório nem um mecanismo de descoberta de peers na Internet.

### PostgreSQL Wire Protocol

`SERVE PG ON 5432;` inicia o endpoint compatível com PostgreSQL para clientes como o DBeaver. O endpoint executa consultas `SELECT` compatíveis no workspace ativo e disponibiliza tabelas e colunas do workspace por meio das respostas de metadados implementadas.

Essa camada oferece compatibilidade de protocolo, não um servidor PostgreSQL integrado. Ela não implementa toda a linguagem SQL do PostgreSQL, semântica completa de transações, autenticação, roles, extensões ou todos os catálogos de sistema. O listener atual do PostgreSQL está vinculado a `127.0.0.1` e se destina a clientes na mesma máquina. Não presuma que qualquer driver PostgreSQL ou recurso de BI será compatível.

## Requisitos

- Rust stable e Cargo para compilar a partir do código-fonte.
- Um terminal para usar a interface TUI ou o prompt de comandos.
- Permissão de leitura nos arquivos de origem e de escrita no diretório de trabalho para o catálogo, caches e exportações.
- Docker, caso queira executar a imagem de container.

## Primeiros passos

### Compilar e executar localmente

```bash
git clone https://github.com/henriqueSantsil/federated_engine.git
cd federated_engine
cargo build --release
./target/release/federated_engine
```

No Windows, execute `target\release\federated_engine.exe`.

Ao iniciar, escolha **Raw mode** para digitar comandos ou a interface selecionável para navegar pela TUI organizada por categorias. A TUI inclui submenus para consultas e views, tabelas e arquivos, rede e servidores, além de workspaces.

O catálogo é carregado de e salvo em `.federated_catalog.yaml` no diretório de trabalho atual. Mantenha esse arquivo e os arquivos de origem disponíveis entre as execuções.

### Executar o nó HTTP sem terminal interativo

O executável padrão abre a interface interativa. Para executar em um container ou ambiente sem terminal, use o modo dedicado de servidor HTTP:

```bash
./target/release/federated_engine --serve-http 8080
```

Esse comando inicia o servidor HTTP P2P usando o catálogo presente no diretório de trabalho atual. O processo permanece em primeiro plano para que possa ser supervisionado por um gerenciador de containers. A porta deve estar entre `1` e `65535`.

## Exemplos práticos de SQL

Digite os comandos a seguir no **Raw mode**. As instruções SQL devem terminar com ponto e vírgula.

### Mapear arquivos locais

```sql
CREATE EXTERNAL TABLE sales
LOCATION '/data/sales.csv';

CREATE EXTERNAL TABLE products
LOCATION '/data/products.parquet';

SHOW TABLES;
INFO sales;
```

Use a extensão `.parquet` para arquivos Parquet; os demais caminhos são tratados como CSV. Os esquemas CSV/Parquet são inferidos a partir da fonte. Arquivos CSV devem ter uma linha de cabeçalho com os nomes das colunas.

### Consultar, filtrar, combinar e limitar resultados

```sql
SELECT region, revenue
FROM sales
WHERE revenue >= 1000 AND region = 'South'
ORDER BY revenue DESC
LIMIT 25;

SELECT sales.product_id, products.name
FROM sales
JOIN products ON product_id = id;
```

### Usar uma expressão de tabela comum (CTE)

```sql
WITH large_sales AS (
    SELECT region, revenue
    FROM sales
    WHERE revenue >= 1000
)
SELECT region, revenue
FROM large_sales
ORDER BY revenue DESC;
```

As CTEs são materializadas durante a consulta pelo mecanismo atual. O suporte SQL é um subconjunto prático e não deve ser considerado uma implementação completa do padrão SQL.

### Criar e publicar uma view

```sql
CREATE VIEW high_value_sales AS
SELECT region, revenue
FROM sales
WHERE revenue >= 1000;

PUBLISH VIEW high_value_sales;
SERVE ON 8080;
```

O processo interativo permanece aberto enquanto o servidor HTTP é executado em uma thread em segundo plano. As views publicadas são listadas em `/catalog`; somente elas podem ser acessadas pelo endpoint de consulta P2P.

### Consumir uma view de outro nó

Em um nó peer, registre a view publicada no nó de origem:

```sql
CREATE EXTERNAL TABLE remote_high_value_sales
LOCATION 'http://192.168.1.20:8080/query?view=high_value_sales&workspace=default';

SELECT region, revenue
FROM remote_high_value_sales
WHERE revenue >= 5000;
```

Substitua o endereço, a view e o workspace pelos valores informados pelo nó de origem. Filtros compatíveis são enviados ao nó de origem; `REFRESH TABLE remote_high_value_sales;` pode criar ou atualizar o cache local.

### Exportar resultados de consultas

```sql
COPY (SELECT region, revenue FROM sales WHERE revenue >= 1000)
TO '/data/high_value_sales.csv';

COPY (SELECT region, revenue FROM sales WHERE revenue >= 1000)
TO '/data/high_value_sales.parquet';
```

### Workspaces

```sql
CREATE WORKSPACE analytics;
USE analytics;
SHOW WORKSPACES;
```

Tabelas e views pertencem a um workspace. `USE` altera o workspace ativo para os comandos e consultas seguintes.

### Outros comandos úteis

```sql
SHOW TABLES;
INFO sales;
REFRESH TABLE remote_high_value_sales;
SHOW NETWORK NODES;
SERVE PG ON 5432;
DROP VIEW high_value_sales;
DROP TABLE sales;
HELP;
EXIT;
```

`DROP TABLE` remove o mapeamento do catálogo; não apaga o arquivo de origem.

## Conectar usando o DBeaver

1. Inicie o mecanismo em Raw mode e execute `SERVE PG ON 5432;`.
2. No DBeaver, crie uma conexão usando o driver **PostgreSQL**.
3. Conecte-se ao host `127.0.0.1`, porta `5432`. As tabelas virtuais disponíveis correspondem ao workspace ativo no Federated Engine.
4. Explore os metadados ou execute consultas `SELECT` compatíveis.

O endpoint não implementa autenticação PostgreSQL. O vínculo apenas a loopback é intencional: esse serviço não deve ser considerado seguro para acesso remoto.

## Executar com Docker

Compile a imagem multi-stage:

```bash
docker build -t federated-engine:latest .
```

O comando padrão do container inicia o nó HTTP na porta `8080`:

```bash
docker run --rm \
  --name federated-engine \
  -p 8080:8080 \
  -v federated-engine-data:/data \
  -v "$PWD/data:/data/input:ro" \
  federated-engine:latest
```

A imagem executa como usuário não-root, mantém `.federated_catalog.yaml` e os arquivos de cache em `/data` e deixa o servidor em primeiro plano. Monte os arquivos de origem no container e use os caminhos internos ao registrá-los (por exemplo, `/data/input/sales.csv`).

Para configurar o catálogo interativamente usando o mesmo volume persistente:

```bash
docker run --rm -it \
  --entrypoint /bin/sh \
  -v federated-engine-data:/data \
  -v "$PWD/data:/data/input:ro" \
  federated-engine:latest \
  -c 'exec /usr/local/bin/federated_engine'
```

Depois, registre um arquivo montado. Por exemplo:

```sql
CREATE EXTERNAL TABLE sales LOCATION '/data/input/sales.csv';
EXIT;
```

Após configurar o catálogo, inicie o container normalmente em modo headless. A imagem Docker expõe apenas a porta HTTP P2P. O listener PostgreSQL interativo está vinculado a loopback e não é exposto por essa configuração do container.

## Compilar, testar e publicar

```bash
cargo fmt --check
cargo test
cargo build --release --locked
```

Ao enviar uma tag de versão, como `v0.1.0`, o [workflow de release](.github/workflows/release.yml) é executado. Ele compila binários de release no Ubuntu e no Windows e os anexa a uma GitHub Release.

## Limitações e segurança

- O suporte SQL é deliberadamente limitado. Consulte os exemplos e o comando integrado `HELP`; não espere semântica SQL completa do PostgreSQL.
- O endpoint PostgreSQL Wire Protocol não implementa autenticação nem todos os metadados PostgreSQL. Ele escuta em loopback (`127.0.0.1`).
- O serviço HTTP P2P e a descoberta foram projetados para redes locais confiáveis. Os endpoints HTTP não oferecem autenticação nem TLS.
- Publicar uma view permite que clientes capazes de alcançar o nó a consultem. Publique somente dados que você pretende compartilhar.
- A descoberta percorre sub-redes IPv4 privadas, com limite para a quantidade de hosts. A rede do host e a do container podem afetar as interfaces e os peers acessíveis.
- Use somente URLs externas confiáveis. Os dados remotos são baixados ou consultados conforme a fonte e a consulta selecionadas.
- O mecanismo não é um banco de dados durável nem substitui controles de acesso, backups ou governança de dados em produção.

## Contribuir

Issues e pull requests são bem-vindos. Para alterações de código, inclua testes focados nas mudanças de comportamento e execute `cargo fmt --check` e `cargo test` antes de enviar sua contribuição.

