# Federated Engine

**Languages: [English](README.en.md) | [Português (Brasil)](README.md)**

**Virtualize local and networked data behind one SQL interface.**

Federated Engine is a Rust-based data virtualization engine. It maps CSV and Parquet files, as well as published views on other Federated Engine nodes, into a workspace catalog and lets you query those sources with a SQL interface. Apache Arrow provides the in-memory columnar representation; Rayon parallelizes selected batch operations; an optional PostgreSQL Wire Protocol endpoint makes the catalog accessible to PostgreSQL-aware tools.

> **Compatibility:** SQL and PostgreSQL compatibility are intentionally a subset, not a claim of full PostgreSQL compatibility. Review the limitations and network-security notes before using it with important data or exposing it to an untrusted network.

## At a glance

| Capability | What it does |
| --- | --- |
| Data virtualization | Register local CSV/Parquet files and query them through logical table names. |
| SQL interface | Run queries in a line-oriented prompt or use the guided terminal UI. |
| Arrow execution | Read data into Apache Arrow record batches for columnar processing. |
| Parallel operations | Use Rayon for selected in-memory batch and filter operations. |
| P2P views | Publish a view over HTTP and query it from another node. |
| Filter pushdown | Send supported predicates to a remote Federated Engine view. |
| BI connectivity | Serve a PostgreSQL-compatible wire endpoint for local client tools. |
| Export | Write query results to CSV or Parquet. |

## How it fits together

```text
 ┌───────────────────┐         SQL / PostgreSQL Wire         ┌──────────────────────┐
 │ DBeaver / BI tool │ ─────────────────────────────────────> │ Federated Engine     │
 └───────────────────┘                                        │ catalog + SQL parser │
                                                              └──────────┬───────────┘
                                                                         │
                                                          Arrow record batches
                                                                         │
                    ┌────────────────────────────────────────────────────┴───────────┐
                    │                                                                │
             ┌──────▼───────┐                                                ┌───────▼────────┐
             │ Local CSV / │                                                │ Remote Engine  │
             │ Parquet     │                                                │ published view │
             └─────────────┘                                                └───────┬────────┘
                                                                                   │
                                                                   HTTP query + optional filter
```

### Data virtualization and execution

`CREATE EXTERNAL TABLE` registers a source in the active workspace; it does not copy a local file into a database. CSV schemas are inferred from the file, while Parquet schemas are read from the Parquet metadata. A logical table name is then used by SQL queries.

At query time, the engine reads sources into Arrow record batches, applies supported SQL operations, and can export the result. Selected batch-level operations use Rayon to work in parallel. The engine is not a storage engine: data remains in its source file or is fetched into a local cache for supported remote sources.

### P2P architecture: published views, discovery, and filter pushdown

Each HTTP node can expose published views on port `8080`:

- `PUBLISH VIEW` marks a view for sharing.
- `SERVE ON 8080;` starts the HTTP node.
- `GET /catalog` lists published views and their workspaces.
- `GET /query?view=<view>&workspace=<workspace>` returns view results as CSV.
- `GET /query?view=<view>&workspace=<workspace>&schema_only=true` returns the inferred view schema.

Another node can register that `/query` URL as an external table. For supported predicates, the requesting engine sends a `filter` parameter to the source node so filtering happens close to the data instead of transferring every row first. Supported remote predicates include comparisons, `LIKE`, `IN`, and combinations using `AND`/`OR`, with literal values. Unsupported or complex expressions are not a general-purpose remote SQL language.

`SHOW NETWORK NODES;` scans active private IPv4 subnets for Federated Engine nodes on port `8080`. Discovery is intended for a local network; it is not a directory service or Internet-wide peer discovery mechanism.

### PostgreSQL Wire Protocol

`SERVE PG ON 5432;` starts the PostgreSQL-compatible endpoint for clients such as DBeaver. The endpoint can execute supported `SELECT` queries against the active workspace and exposes workspace tables and columns through the supported metadata responses.

This is a wire-protocol compatibility layer, not an embedded PostgreSQL server. It does not provide PostgreSQL's full SQL surface, transaction semantics, authentication, roles, extensions, or complete system catalogs. The current PostgreSQL listener binds to `127.0.0.1`, so it is intended for local clients on the same machine. Do not assume that arbitrary PostgreSQL drivers or BI features will work.

## Requirements

- Rust stable and Cargo for building from source.
- A terminal for the interactive TUI or raw prompt.
- Read access to source files (and write access to the working directory for the catalog, caches, and exports).
- Docker, if running the container image.

## Get started

### Build and run locally

```bash
git clone https://github.com/henriqueSantsil/federated_engine.git
cd federated_engine
cargo build --release
./target/release/federated_engine
```

On Windows, run `target\release\federated_engine.exe`.

On startup, choose **Raw mode** to type commands, or the selectable interface to navigate the categorized TUI. The TUI includes submenus for queries/views, tables/files, network/servers, and workspaces.

The catalog is loaded from and saved to `.federated_catalog.yaml` in the current working directory. Keep that file and any source files available between runs.

### Run the HTTP node without an interactive terminal

The ordinary executable opens the interactive interface. For a container or other headless deployment, use the dedicated HTTP-server mode:

```bash
./target/release/federated_engine --serve-http 8080
```

This starts the HTTP P2P server with the catalog in the current working directory. The process stays in the foreground so container managers can supervise it. The command-line mode accepts one port from `1` to `65535`.

## Practical SQL examples

Enter these statements in **Raw mode**. SQL statements should end in a semicolon.

### Map local files

```sql
CREATE EXTERNAL TABLE sales
LOCATION '/data/sales.csv';

CREATE EXTERNAL TABLE products
LOCATION '/data/products.parquet';

SHOW TABLES;
INFO sales;
```

Use a `.parquet` extension for Parquet files; other locations are treated as CSV. CSV/Parquet schemas are inferred from the source. For CSV, provide a header row with column names.

### Query, filter, join, and limit

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

### Use a common table expression (CTE)

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

CTEs are materialized for the query by the current execution engine. Query support is a practical subset and should not be treated as complete SQL-standard coverage.

### Create and publish a view

```sql
CREATE VIEW high_value_sales AS
SELECT region, revenue
FROM sales
WHERE revenue >= 1000;

PUBLISH VIEW high_value_sales;
SERVE ON 8080;
```

The interactive process stays open while the HTTP server runs in a background thread. Published views are listed by `/catalog`; only published views are available through the P2P query endpoint.

### Consume a view from another node

On a peer node, register the source node's published view:

```sql
CREATE EXTERNAL TABLE remote_high_value_sales
LOCATION 'http://192.168.1.20:8080/query?view=high_value_sales&workspace=default';

SELECT region, revenue
FROM remote_high_value_sales
WHERE revenue >= 5000;
```

Replace the address, view, and workspace with values reported by the source node. Supported filters are pushed to the source node; `REFRESH TABLE remote_high_value_sales;` can create or update a local cache.

### Export query results

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

Tables and views belong to a workspace. `USE` switches the active workspace for subsequent commands and queries.

### Other useful commands

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

`DROP TABLE` removes the table mapping from the catalog; it does not delete the source file.

## Connect with DBeaver

1. Start the engine in Raw mode and run `SERVE PG ON 5432;`.
2. In DBeaver, create a connection using the **PostgreSQL** driver.
3. Connect to host `127.0.0.1`, port `5432`. The active Federated Engine workspace supplies the visible virtual tables.
4. Use the connection to explore metadata or run supported `SELECT` statements.

The endpoint has no PostgreSQL authentication implementation. Its loopback-only bind is intentional; it is not a secure, remotely accessible database service.

## Run with Docker

Build the multi-stage image:

```bash
docker build -t federated-engine:latest .
```

The default container command is equivalent to starting the HTTP node on port `8080`:

```bash
docker run --rm \
  --name federated-engine \
  -p 8080:8080 \
  -v federated-engine-data:/data \
  -v "$PWD/data:/data/input:ro" \
  federated-engine:latest
```

The image runs as a non-root user, stores `.federated_catalog.yaml` and cache files under `/data`, and keeps the server in the foreground. Mount source files into the container and refer to their in-container paths (for example, `/data/sales.csv`) when registering them.

To configure the catalog interactively using the same persistent volume:

```bash
docker run --rm -it \
  --entrypoint /bin/sh \
  -v federated-engine-data:/data \
  -v "$PWD/data:/data/input:ro" \
  federated-engine:latest \
  -c 'exec /usr/local/bin/federated_engine'
```

Then register a mounted file, for example:

```sql
CREATE EXTERNAL TABLE sales LOCATION '/data/input/sales.csv';
EXIT;
```

After configuration, start the normal headless container command. The Docker image exposes the HTTP P2P port only. The interactive PostgreSQL listener currently binds to loopback and is not exposed by this container setup.

## Build, test, and release

```bash
cargo fmt --check
cargo test
cargo build --release --locked
```

Pushing a version tag such as `v0.1.0` triggers [the release workflow](.github/workflows/release.yml). It builds release binaries on Ubuntu and Windows and attaches them to a GitHub Release.

## Limitations and security

- SQL support is deliberately limited. Use the examples and the built-in `HELP` command as the supported surface; do not expect full PostgreSQL SQL semantics.
- The PostgreSQL Wire Protocol endpoint does not implement authentication or full PostgreSQL metadata. It binds to loopback (`127.0.0.1`).
- The P2P HTTP service and discovery are designed for trusted local networks. HTTP endpoints have no authentication or TLS.
- Publishing a view makes it queryable by clients that can reach the node. Publish only data you intend to share.
- Discovery scans private IPv4 subnets, with a bounded host count; container and host networking can affect which interfaces and peers are reachable.
- Treat external URLs as trusted inputs. Remote data is downloaded or queried according to the selected source and query.
- The engine is not a durable database or a substitute for access control, backups, or production data governance.

## Contributing

Issues and pull requests are welcome. For code changes, include focused tests for behavior changes and run `cargo fmt --check` and `cargo test` before submitting.

Before publishing this repository as open source, add a `LICENSE` file with the project's chosen license and ensure all included dependencies, sample data, and other assets can be redistributed under their applicable terms. This repository currently does not declare a project license.
