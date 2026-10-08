use crate::catalog::{Catalog, ColumnDef, VirtualTable};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::io::Read;

#[derive(Debug, Clone)]
pub enum FilterNode {
    Condition {
        column: String,
        operator: String,
        value: String,
    },
    Like {
        column: String,
        pattern: String,
        negated: bool,
    },
    InList {
        column: String,
        values: Vec<String>,
        negated: bool,
    },
    And(Box<FilterNode>, Box<FilterNode>),
    Or(Box<FilterNode>, Box<FilterNode>),
}

#[derive(Debug, Clone)]
pub struct JoinInfo {
    pub right_table: String,
    pub left_column: String,
    pub right_column: String,
    pub right_filter: Option<FilterNode>,
}

#[derive(Debug, Clone)]
pub struct AggregateNode {
    pub func: String,
    pub column: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WindowNode {
    pub func: String,
    pub column: String,
    pub partition_by: Vec<String>,
    pub alias: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderByNode {
    pub column: String,
    pub asc: bool,
}

#[derive(Debug, Clone)]
pub struct CaseWhenNode {
    pub condition: FilterNode,
    pub then_result: String,
    pub else_result: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExtractNode {
    pub field: String,
    pub column: String,
    pub alias: Option<String>,
}

pub fn parse_command(sql: &str, catalog: &mut Catalog) {
    let sql_trimmed = sql.trim();
    let sql_upper = sql_trimmed.to_uppercase();

    if sql_upper.starts_with("CLEAR") {
        print!("\x1B[2J\x1B[1;1H");
        return;
    }
    if sql_upper.starts_with("HELP") {
        println!("{}", include_str!("../help.txt"));
        return;
    }
    if sql_upper.starts_with("SHOW WORKSPACES") {
        catalog.show_workspaces();
        return;
    }
    if sql_upper.starts_with("SHOW TABLES") {
        catalog.show_tables();
        return;
    }
    if sql_upper.starts_with("SHOW NETWORK NODES") {
        crate::network_discovery::show_network_nodes();
        return;
    }

    if sql_upper.starts_with("INFO") || sql_upper.starts_with("DESCRIBE") {
        let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
        if parts.len() >= 2 {
            let table_name = parts[1].trim_end_matches(';');
            catalog.describe_table(table_name);
        } else {
            println!("\x1B[1;31mErro:\x1B[0m Sintaxe incorreta. Use INFO <tabela>;");
        }
        return;
    }

    if sql_upper.starts_with("REFRESH TABLE") {
        let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
        if parts.len() != 3 {
            println!("\x1B[1;31mErro:\x1B[0m Sintaxe incorreta. Use: REFRESH TABLE <tabela>;");
        } else {
            let table_name = parts[2].trim_end_matches(';').trim_matches('"');
            match refresh_external_table(table_name, catalog) {
                Ok(()) => println!(
                    "\x1B[1;32mSucesso:\x1B[0m Cache da tabela '{}' atualizado.",
                    table_name
                ),
                Err(error) => println!("\x1B[1;31mErro ao atualizar cache:\x1B[0m {}", error),
            }
        }
        return;
    }

    if sql_upper.starts_with("CREATE WORKSPACE") {
        let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
        if parts.len() >= 3 {
            let name = parts[2]
                .trim_end_matches(';')
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            catalog.create_workspace(&name);
        } else {
            println!("\x1B[1;31mErro:\x1B[0m Nome do workspace nao especificado.");
        }
        return;
    }

    if sql_upper.starts_with("USE") {
        let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
        if parts.len() >= 2 {
            let name = parts[1]
                .trim_end_matches(';')
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            catalog.use_workspace(&name);
        } else {
            println!("\x1B[1;31mErro:\x1B[0m Nome do workspace nao especificado.");
        }
        return;
    }

    if sql_upper.starts_with("DROP TABLE") {
        let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
        if parts.len() >= 3 {
            let name = parts[2]
                .trim_end_matches(';')
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            catalog.drop_table(&name);
        } else {
            println!("\x1B[1;31mErro:\x1B[0m Nome da tabela nao especificado.");
        }
        return;
    }

    if sql_upper.starts_with("ALTER TABLE") {
        let clean_sql = sql_trimmed.trim_end_matches(';');
        let parts: Vec<&str> = clean_sql.split_whitespace().collect();
        if parts.len() == 8
            && parts[3].eq_ignore_ascii_case("ALTER")
            && parts[4].eq_ignore_ascii_case("COLUMN")
            && parts[6].eq_ignore_ascii_case("TYPE")
        {
            catalog.alter_column_type(parts[2], parts[5], parts[7]);
        } else {
            println!("\x1B[1;31mErro:\x1B[0m Sintaxe incorreta. Use: ALTER TABLE <tabela> ALTER COLUMN <coluna> TYPE <tipo>;");
        }
        return;
    }

    if sql_upper.starts_with("CREATE VIEW") {
        let clean_sql = sql_trimmed.trim_end_matches(';');
        if let Some(as_pos) = clean_sql.to_uppercase().find(" AS ") {
            let parts: Vec<&str> = clean_sql[..as_pos].split_whitespace().collect();
            if parts.len() >= 3 {
                catalog.add_view(
                    parts[2].trim_matches('"').trim_matches('\'').to_string(),
                    clean_sql[as_pos + 4..].to_string(),
                );
            } else {
                println!("\x1B[1;31mErro:\x1B[0m Sintaxe incorreta. Use: CREATE VIEW <nome> AS SELECT ...;");
            }
        } else {
            println!("\x1B[1;31mErro:\x1B[0m Falta a clausula 'AS' no CREATE VIEW.");
        }
        return;
    }

    if sql_upper.starts_with("DROP VIEW") {
        let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
        if parts.len() >= 3 {
            let name = parts[2]
                .trim_end_matches(';')
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            catalog.drop_view(&name);
        } else {
            println!("\x1B[1;31mErro:\x1B[0m Nome da view nao especificado.");
        }
        return;
    }

    if sql_upper.starts_with("PUBLISH VIEW") {
        let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
        if parts.len() >= 3 {
            let view_name = parts[2]
                .trim_end_matches(';')
                .trim_matches('\'')
                .trim_matches('"');
            catalog.publish_view(view_name);
        } else {
            println!("\x1B[1;31mErro:\x1B[0m Sintaxe incorreta. Use: PUBLISH VIEW <nome>;");
        }
        return;
    }

    let dialect = GenericDialect {};
    match Parser::parse_sql(&dialect, sql) {
        Ok(ast) => {
            for statement in ast {
                match statement {
                    Statement::CreateTable {
                        name,
                        columns: ast_columns,
                        external,
                        location,
                        ..
                    } => {
                        if !external {
                            println!("\x1B[1;31mErro:\x1B[0m Use 'CREATE EXTERNAL TABLE'.");
                            continue;
                        }
                        let table_name = name.to_string();
                        let path_or_url =
                            location.unwrap_or_else(|| "".to_string()).replace("'", "");
                        let table_format = if has_parquet_extension(&path_or_url) {
                            "PARQUET"
                        } else {
                            "CSV"
                        };
                        let mut physical_path = path_or_url.clone();
                        let source_url = if path_or_url.starts_with("http://")
                            || path_or_url.starts_with("https://")
                        {
                            Some(path_or_url.clone())
                        } else {
                            None
                        };

                        let mut remote_columns = None;
                        if let Some(source_url) = &source_url {
                            println!(
                                "📡 Conectando ao servidor remoto: \x1B[1;36m{}\x1B[0m...",
                                path_or_url
                            );
                            let cache_file =
                                format!(".cache_{}.{}", table_name, table_format.to_lowercase());
                            physical_path = cache_file.clone();
                            if is_p2p_query_url(source_url) {
                                match ureq::get(source_url).query("schema_only", "true").call() {
                                    Ok(response) => match response.into_string() {
                                        Ok(data) => {
                                            match serde_json::from_str::<Vec<ColumnDef>>(&data) {
                                                Ok(columns) => {
                                                    remote_columns = Some(columns);
                                                    println!("\x1B[1;32mEsquema remoto obtido.\x1B[0m Os dados nao foram baixados; use REFRESH TABLE para criar/atualizar o cache.");
                                                }
                                                Err(error) => {
                                                    println!("\x1B[1;31mErro:\x1B[0m Esquema remoto invalido: {}", error);
                                                    continue;
                                                }
                                            }
                                        }
                                        Err(error) => {
                                            println!("\x1B[1;31mErro:\x1B[0m Falha ao ler o esquema remoto: {}", error);
                                            continue;
                                        }
                                    },
                                    Err(ureq::Error::Status(400, _)) => {
                                        println!(
                                            "\x1B[1;33mAviso:\x1B[0m O servidor nao conseguiu inferir o esquema da view; baixando o CSV completo para compatibilidade."
                                        );
                                        if let Err(error) =
                                            download_remote_cache(source_url, &cache_file)
                                        {
                                            println!(
                                                "\x1B[1;31mErro de Conexao P2P:\x1B[0m {}",
                                                error
                                            );
                                            continue;
                                        }
                                    }
                                    Err(error) => {
                                        println!("\x1B[1;31mErro de Conexao P2P:\x1B[0m Falha ao obter o esquema remoto: {}", error);
                                        continue;
                                    }
                                }
                            } else {
                                let download_result = if table_format == "PARQUET" {
                                    download_remote_cache_binary(source_url, &cache_file)
                                } else {
                                    download_remote_cache(source_url, &cache_file)
                                };
                                if let Err(error) = download_result {
                                    println!("\x1B[1;31mErro de Conexao P2P:\x1B[0m {}", error);
                                    continue;
                                }
                                println!("\x1B[1;32mDownload concluido.\x1B[0m Tabela convertida em cache local.");
                            }
                        }

                        let mut manual_overrides = HashMap::new();
                        for col_def in ast_columns {
                            manual_overrides
                                .insert(col_def.name.value.clone(), col_def.data_type.to_string());
                        }

                        let mut columns = if let Some(columns) = remote_columns {
                            columns
                        } else {
                            let inferred_schema = if table_format == "PARQUET" {
                                crate::connectors::parquet::infer_schema(&physical_path)
                            } else {
                                crate::connectors::csv::infer_schema(&physical_path)
                            };
                            match inferred_schema {
                                Ok(schema) => schema
                                    .fields()
                                    .iter()
                                    .map(|field| ColumnDef {
                                        name: field.name().clone(),
                                        data_type: field.data_type().to_string(),
                                    })
                                    .collect(),
                                Err(error) => {
                                    println!(
                                        "\x1B[1;33mAviso:\x1B[0m Falha ao inferir esquema na fonte {}: {}",
                                        physical_path, error
                                    );
                                    Vec::new()
                                }
                            }
                        };
                        for column in &mut columns {
                            if let Some(data_type) = manual_overrides.get(&column.name) {
                                column.data_type = data_type.clone();
                            }
                        }

                        if columns.is_empty() {
                            println!("\x1B[1;31mErro:\x1B[0m Nao foi possivel definir as colunas. Tabela nao mapeada.");
                            continue;
                        }

                        let new_table = VirtualTable {
                            name: table_name.clone(),
                            format: table_format.to_string(),
                            physical_path,
                            source_url,
                            columns,
                        };
                        if catalog.add_table(new_table).is_ok() {
                            println!(
                                "Tabela '{}' mapeada com sucesso no workspace '{}'.",
                                table_name, catalog.active_workspace
                            );
                        }
                    }
                    Statement::Query(query) => {
                        let _ = parse_and_execute_query(
                            &query,
                            None,
                            catalog,
                            &catalog.active_workspace,
                            false,
                        );
                    }
                    Statement::Copy { source, target, .. } => {
                        let export_path = match target {
                            sqlparser::ast::CopyTarget::File { filename } => {
                                filename.replace("'", "")
                            }
                            _ => "".to_string(),
                        };
                        if export_path.is_empty() {
                            println!("\x1B[1;31mErro:\x1B[0m Caminho de destino invalido.");
                            continue;
                        }
                        if let sqlparser::ast::CopySource::Query(query) = source {
                            parse_and_execute_query(
                                &query,
                                Some(export_path),
                                catalog,
                                &catalog.active_workspace,
                                false,
                            );
                        }
                    }
                    _ => {
                        println!("\x1B[1;31mErro:\x1B[0m Comando SQL nao suportado.");
                    }
                }
            }
        }
        Err(e) => {
            println!("\x1B[1;31mErro de Sintaxe SQL:\x1B[0m {:?}", e);
        }
    }
}

pub fn execute_query(
    sql: &str,
    catalog: &Catalog,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|error| format!("Erro de sintaxe SQL: {}", error))?;
    if statements.len() != 1 {
        return Err("O protocolo PostgreSQL aceita uma consulta por comando.".to_string());
    }
    let query = match &statements[0] {
        Statement::Query(query) => query,
        _ => return Err("O servidor PostgreSQL suporta consultas SELECT.".to_string()),
    };
    Ok(parse_and_execute_query(
        query,
        None,
        catalog,
        &catalog.active_workspace,
        true,
    ))
}

fn refresh_external_table(table_name: &str, catalog: &mut Catalog) -> Result<(), String> {
    let table = catalog
        .get_table_qualified(table_name, &catalog.active_workspace)
        .cloned()
        .ok_or_else(|| format!("Tabela '{}' nao encontrada.", table_name))?;
    let source_url = table.source_url.as_deref().ok_or_else(|| {
        format!(
            "Tabela '{}' nao possui uma origem HTTP registrada.",
            table_name
        )
    })?;
    let response = ureq::get(source_url)
        .call()
        .map_err(|e| format!("Falha ao baixar a origem '{}': {}", source_url, e))?;
    let mut data = Vec::new();
    response
        .into_reader()
        .read_to_end(&mut data)
        .map_err(|e| format!("Falha ao ler a resposta da origem: {}", e))?;
    let columns = replace_cache_atomically(&table, &data)?;
    let refreshed_table = VirtualTable { columns, ..table };
    let active_workspace = catalog.active_workspace.clone();
    catalog.replace_table_qualified(table_name, &active_workspace, refreshed_table)
}

fn replace_cache_atomically(table: &VirtualTable, data: &[u8]) -> Result<Vec<ColumnDef>, String> {
    if data.is_empty() {
        return Err("A origem retornou um arquivo vazio.".to_string());
    }
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("Falha ao gerar nome temporario: {}", e))?;
    let temporary_path = format!(
        "{}.refresh.{}.{}.tmp",
        table.physical_path,
        std::process::id(),
        duration.as_nanos()
    );
    if let Err(error) = std::fs::write(&temporary_path, data) {
        if let Err(remove_error) = std::fs::remove_file(&temporary_path) {
            if remove_error.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "Falha ao remover cache temporario '{}': {}",
                    temporary_path, remove_error
                );
            }
        }
        return Err(format!("Falha ao gravar o download temporario: {}", error));
    }

    let inferred_schema = match infer_table_schema(&temporary_path, &table.format) {
        Ok(schema) => schema,
        Err(error) => {
            if let Err(remove_error) = std::fs::remove_file(&temporary_path) {
                eprintln!(
                    "Falha ao remover cache temporario '{}': {}",
                    temporary_path, remove_error
                );
            }
            return Err(format!(
                "Nao foi possivel inferir o esquema baixado: {}",
                error
            ));
        }
    };
    let columns = inferred_schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = table
                .columns
                .iter()
                .find(|column| column.name == *field.name())
                .map(|column| column.data_type.clone())
                .unwrap_or_else(|| field.data_type().to_string());
            ColumnDef {
                name: field.name().clone(),
                data_type,
            }
        })
        .collect();

    if let Err(error) = std::fs::rename(&temporary_path, &table.physical_path) {
        if let Err(remove_error) = std::fs::remove_file(&temporary_path) {
            eprintln!(
                "Falha ao remover cache temporario '{}': {}",
                temporary_path, remove_error
            );
        }
        return Err(format!("Falha ao substituir o cache atual: {}", error));
    }

    Ok(columns)
}

fn infer_table_schema(path: &str, format: &str) -> Result<arrow::datatypes::Schema, String> {
    if format.eq_ignore_ascii_case("PARQUET") {
        crate::connectors::parquet::infer_schema(path)
    } else {
        crate::connectors::csv::infer_schema(path)
    }
}

fn has_parquet_extension(location: &str) -> bool {
    let path = location.split(['?', '#']).next().unwrap_or(location);
    path.rsplit('/')
        .next()
        .unwrap_or(path)
        .to_ascii_lowercase()
        .ends_with(".parquet")
}

fn is_p2p_query_url(source_url: &str) -> bool {
    source_url
        .split(['?', '#'])
        .next()
        .unwrap_or_default()
        .trim_end_matches('/')
        .ends_with("/query")
}

fn download_remote_cache(source_url: &str, cache_path: &str) -> Result<(), String> {
    let response = ureq::get(source_url)
        .call()
        .map_err(|e| format!("Nao foi possivel acessar a origem remota: {}", e))?;
    let data = response
        .into_string()
        .map_err(|e| format!("Falha ao ler os dados transmitidos pelo servidor: {}", e))?;
    std::fs::write(cache_path, data).map_err(|e| format!("Falha ao criar o cache local: {}", e))
}

fn download_remote_cache_binary(source_url: &str, cache_path: &str) -> Result<(), String> {
    let response = ureq::get(source_url)
        .call()
        .map_err(|error| format!("Nao foi possivel acessar a origem remota: {}", error))?;
    let mut reader = response.into_reader();
    let mut data = Vec::new();
    reader
        .read_to_end(&mut data)
        .map_err(|error| format!("Falha ao ler dados transmitidos pelo servidor: {}", error))?;
    if data.is_empty() {
        return Err("A origem remota retornou um arquivo vazio.".to_string());
    }
    std::fs::write(cache_path, data)
        .map_err(|error| format!("Falha ao criar o cache local: {}", error))
}

fn parse_expr(
    expr: &sqlparser::ast::Expr,
    catalog: &Catalog,
    current_ws: &str,
) -> Option<FilterNode> {
    match expr {
        sqlparser::ast::Expr::Nested(inner) => parse_expr(inner, catalog, current_ws),
        sqlparser::ast::Expr::BinaryOp { left, op, right } => match op {
            sqlparser::ast::BinaryOperator::And => {
                let l = parse_expr(left, catalog, current_ws)?;
                let r = parse_expr(right, catalog, current_ws)?;
                Some(FilterNode::And(Box::new(l), Box::new(r)))
            }
            sqlparser::ast::BinaryOperator::Or => {
                let l = parse_expr(left, catalog, current_ws)?;
                let r = parse_expr(right, catalog, current_ws)?;
                Some(FilterNode::Or(Box::new(l), Box::new(r)))
            }
            _ => {
                let col = if let sqlparser::ast::Expr::Identifier(ident) = &**left {
                    ident.value.clone()
                } else if let sqlparser::ast::Expr::CompoundIdentifier(idents) = &**left {
                    idents.last().unwrap().value.clone()
                } else {
                    return None;
                };

                let val = match &**right {
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => n.clone(),
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => {
                        s.clone()
                    }
                    _ => return None,
                };

                let operator = match op {
                    sqlparser::ast::BinaryOperator::Eq => "=",
                    sqlparser::ast::BinaryOperator::NotEq => "!=",
                    sqlparser::ast::BinaryOperator::Gt => ">",
                    sqlparser::ast::BinaryOperator::Lt => "<",
                    sqlparser::ast::BinaryOperator::GtEq => ">=",
                    sqlparser::ast::BinaryOperator::LtEq => "<=",
                    _ => return None,
                }
                .to_string();

                Some(FilterNode::Condition {
                    column: col,
                    operator,
                    value: val,
                })
            }
        },
        sqlparser::ast::Expr::Like {
            negated,
            expr,
            pattern,
            ..
        }
        | sqlparser::ast::Expr::ILike {
            negated,
            expr,
            pattern,
            ..
        } => {
            let col = match &**expr {
                sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
                sqlparser::ast::Expr::CompoundIdentifier(idents) => {
                    idents.last().unwrap().value.clone()
                }
                _ => return None,
            };
            let pat = match &**pattern {
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => {
                    s.clone()
                }
                _ => return None,
            };
            Some(FilterNode::Like {
                column: col,
                pattern: pat,
                negated: *negated,
            })
        }
        sqlparser::ast::Expr::InList {
            expr,
            list,
            negated,
        } => {
            let col = match &**expr {
                sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
                sqlparser::ast::Expr::CompoundIdentifier(idents) => {
                    idents.last().unwrap().value.clone()
                }
                _ => return None,
            };
            let mut vals = Vec::new();
            for item in list {
                if let sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) =
                    item
                {
                    vals.push(s.clone());
                } else if let sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) =
                    item
                {
                    vals.push(n.clone());
                }
            }
            Some(FilterNode::InList {
                column: col,
                values: vals,
                negated: *negated,
            })
        }
        sqlparser::ast::Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let col = match &**expr {
                sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
                sqlparser::ast::Expr::CompoundIdentifier(idents) => {
                    idents.last().unwrap().value.clone()
                }
                _ => return None,
            };

            let (sub_t, sub_p, sub_f, sub_j, _, _, _, _, _, _, _, _) =
                extract_query_parts(subquery, catalog, current_ws);

            match crate::physical_plan::execute_subquery_for_list(
                &sub_t, &sub_p, &sub_f, &sub_j, catalog, current_ws,
            ) {
                Ok(values) => Some(FilterNode::InList {
                    column: col,
                    values,
                    negated: *negated,
                }),
                Err(e) => {
                    println!("\x1B[1;31mErro na Subquery:\x1B[0m {}", e);
                    None
                }
            }
        }
        _ => None,
    }
}

fn extract_query_parts(
    query: &sqlparser::ast::Query,
    catalog: &Catalog,
    current_ws: &str,
) -> (
    String,
    Vec<(String, Option<String>)>,
    Option<FilterNode>,
    Option<JoinInfo>,
    Option<usize>,
    Vec<String>,
    Vec<AggregateNode>,
    Option<OrderByNode>,
    Vec<CaseWhenNode>,
    bool,
    Vec<ExtractNode>,
    Vec<WindowNode>,
) {
    let mut table_name = String::new();
    let mut projection = Vec::new();
    let mut filter_node = None;
    let mut join_info = None;
    let mut limit_val = None;
    let mut group_by = Vec::new();
    let mut aggregates = Vec::new();
    let mut order_by_node = None;
    let mut cases = Vec::new();
    let mut has_wildcard = false;
    let mut extracts = Vec::new();
    let mut windows = Vec::new();

    if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
        if let Some(table_with_joins) = select.from.first() {
            if let sqlparser::ast::TableFactor::Table { name, .. } = &table_with_joins.relation {
                table_name = name
                    .0
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<String>>()
                    .join(".");
            }

            if !table_with_joins.joins.is_empty() {
                let join = &table_with_joins.joins[0];
                if let sqlparser::ast::TableFactor::Table { name, .. } = &join.relation {
                    let right_table = name
                        .0
                        .iter()
                        .map(|ident| ident.value.clone())
                        .collect::<Vec<String>>()
                        .join(".");
                    if let sqlparser::ast::JoinOperator::Inner(
                        sqlparser::ast::JoinConstraint::On(sqlparser::ast::Expr::BinaryOp {
                            left,
                            op: _,
                            right,
                        }),
                    ) = &join.join_operator
                    {
                        let left_col = match &**left {
                            sqlparser::ast::Expr::CompoundIdentifier(idents) => {
                                idents.last().unwrap().value.clone()
                            }
                            sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
                            _ => "".to_string(),
                        };
                        let right_col = match &**right {
                            sqlparser::ast::Expr::CompoundIdentifier(idents) => {
                                idents.last().unwrap().value.clone()
                            }
                            sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
                            _ => "".to_string(),
                        };
                        if !left_col.is_empty() && !right_col.is_empty() {
                            join_info = Some(JoinInfo {
                                right_table,
                                left_column: left_col,
                                right_column: right_col,
                                right_filter: None,
                            });
                        }
                    }
                }
            }

            let mut process_expr = |expr: &sqlparser::ast::Expr, alias: Option<String>| match expr {
                sqlparser::ast::Expr::Identifier(ident) => {
                    projection.push((ident.value.clone(), alias));
                }
                sqlparser::ast::Expr::Function(func) => {
                    let func_name = func.name.to_string().to_uppercase();
                    let mut col_name = String::new();
                    if let Some(arg) = func.args.first() {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(
                                sqlparser::ast::Expr::Identifier(ident),
                            ),
                        ) = arg
                        {
                            col_name = ident.value.clone();
                        } else if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(
                                sqlparser::ast::Expr::CompoundIdentifier(idents),
                            ),
                        ) = arg
                        {
                            if let Some(ident) = idents.last() {
                                col_name = ident.value.clone();
                            }
                        } else if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Wildcard,
                        ) = arg
                        {
                            col_name = "*".to_string();
                        }
                    }
                    if func.over.is_some() {
                        let window_spec = match &func.over {
                            Some(sqlparser::ast::WindowType::WindowSpec(spec)) => Some(spec),
                            Some(sqlparser::ast::WindowType::NamedWindow(name)) => select
                                .named_window
                                .iter()
                                .find(|definition| {
                                    definition.0.value.eq_ignore_ascii_case(&name.value)
                                })
                                .map(|definition| &definition.1),
                            None => None,
                        };
                        let mut error = if let Some(sqlparser::ast::WindowType::NamedWindow(name)) =
                            &func.over
                        {
                            window_spec.is_none().then(|| {
                                format!("Janela nomeada '{}' nao foi definida.", name.value)
                            })
                        } else {
                            None
                        };
                        if window_spec
                            .map(|spec| !spec.order_by.is_empty() || spec.window_frame.is_some())
                            .unwrap_or(false)
                        {
                            error = Some(
                                "ORDER BY e frames em funcoes de janela ainda nao sao suportados."
                                    .to_string(),
                            );
                        }
                        let partition_exprs = window_spec.map(|spec| &spec.partition_by);
                        let partition_by = partition_exprs
                            .into_iter()
                            .flatten()
                            .filter_map(|expr| match expr {
                                sqlparser::ast::Expr::Identifier(ident) => {
                                    Some(ident.value.clone())
                                }
                                sqlparser::ast::Expr::CompoundIdentifier(idents) => {
                                    idents.last().map(|ident| ident.value.clone())
                                }
                                _ => None,
                            })
                            .collect::<Vec<_>>();
                        if partition_exprs.map(Vec::len).unwrap_or(0) != partition_by.len() {
                            error = Some(
                                "A funcao de janela possui uma expressao PARTITION BY nao suportada."
                                    .to_string(),
                            );
                        }
                        let output_name = alias
                            .clone()
                            .unwrap_or_else(|| format!("{}({})", func_name, col_name));
                        windows.push(WindowNode {
                            func: func_name,
                            column: col_name,
                            partition_by,
                            alias: Some(output_name.clone()),
                            error,
                        });
                        projection.push((output_name, None));
                    } else {
                        aggregates.push(AggregateNode {
                            func: func_name,
                            column: col_name,
                            alias,
                        });
                    }
                }
                sqlparser::ast::Expr::Case {
                    conditions,
                    results,
                    else_result,
                    ..
                } => {
                    if let (Some(cond_expr), Some(res_expr)) = (conditions.first(), results.first())
                    {
                        if let Some(condition) = parse_expr(cond_expr, catalog, current_ws) {
                            let then_result = match res_expr {
                                sqlparser::ast::Expr::Value(
                                    sqlparser::ast::Value::SingleQuotedString(s),
                                ) => s.clone(),
                                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(
                                    n,
                                    _,
                                )) => n.clone(),
                                _ => "Desconhecido".to_string(),
                            };
                            let mut else_res = "NULL".to_string();
                            if let Some(e) = else_result {
                                match &**e {
                                    sqlparser::ast::Expr::Value(
                                        sqlparser::ast::Value::SingleQuotedString(s),
                                    ) => else_res = s.clone(),
                                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(
                                        n,
                                        _,
                                    )) => else_res = n.clone(),
                                    _ => {}
                                }
                            }
                            cases.push(CaseWhenNode {
                                condition,
                                then_result,
                                else_result: else_res,
                                alias,
                            });
                        }
                    }
                }
                sqlparser::ast::Expr::Extract { field, expr } => {
                    let col_name = match &**expr {
                        sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
                        _ => "".to_string(),
                    };
                    let ext_col_name = alias
                        .clone()
                        .unwrap_or_else(|| format!("EXTRACT_{}", field.to_string().to_uppercase()));

                    extracts.push(ExtractNode {
                        field: field.to_string().to_uppercase(),
                        column: col_name,
                        alias: Some(ext_col_name.clone()),
                    });
                    projection.push((ext_col_name, alias));
                }
                _ => {}
            };

            for item in &select.projection {
                match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                        process_expr(expr, None);
                    }
                    sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                        process_expr(expr, Some(alias.value.clone()));
                    }
                    sqlparser::ast::SelectItem::Wildcard(_) => {
                        has_wildcard = true;
                    }
                    _ => {}
                }
            }

            if let Some(selection) = &select.selection {
                filter_node = parse_expr(selection, catalog, current_ws);
            }

            match &select.group_by {
                sqlparser::ast::GroupByExpr::Expressions(exprs) => {
                    for expr in exprs {
                        if let sqlparser::ast::Expr::Identifier(ident) = expr {
                            group_by.push(ident.value.clone());
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if let Some(order_expr) = query.order_by.first() {
        let asc = order_expr.asc.unwrap_or(true);
        match &order_expr.expr {
            sqlparser::ast::Expr::Identifier(ident) => {
                order_by_node = Some(OrderByNode {
                    column: ident.value.clone(),
                    asc,
                });
            }
            sqlparser::ast::Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                if let Some(arg) = func.args.first() {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(sqlparser::ast::Expr::Identifier(
                            ident,
                        )),
                    ) = arg
                    {
                        let col_name = format!("{}({})", func_name, ident.value);
                        order_by_node = Some(OrderByNode {
                            column: col_name,
                            asc,
                        });
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(expr) = &query.limit {
        if let sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) = expr {
            limit_val = n.parse::<usize>().ok();
        }
    }

    (
        table_name,
        projection,
        filter_node,
        join_info,
        limit_val,
        group_by,
        aggregates,
        order_by_node,
        cases,
        has_wildcard,
        extracts,
        windows,
    )
}

fn parse_and_execute_query(
    query: &sqlparser::ast::Query,
    export_path: Option<String>,
    catalog: &Catalog,
    current_ws: &str,
    return_batches: bool,
) -> Vec<arrow::record_batch::RecordBatch> {
    let mut ctes = crate::physical_plan::CteTables::new();
    materialize_ctes(query, catalog, current_ws, &mut ctes);

    let (
        outer_table,
        outer_proj,
        outer_filter,
        mut outer_join,
        outer_limit,
        outer_group,
        outer_agg,
        outer_order,
        outer_cases,
        outer_wildcard,
        outer_extracts,
        mut outer_windows,
    ) = extract_query_parts(query, catalog, current_ws);

    let mut final_table = outer_table.clone();
    let mut final_proj = outer_proj.clone();
    let mut final_limit = outer_limit;
    let mut final_filter = outer_filter.clone();
    let mut final_group = outer_group.clone();
    let mut final_agg = outer_agg.clone();
    let mut final_order = outer_order.clone();
    let mut final_cases = outer_cases.clone();
    let mut final_wildcard = outer_wildcard;
    let mut final_extracts = outer_extracts.clone();
    let mut final_ws = current_ws.to_string();

    if let Some((view_ws, view_query_str)) = catalog.get_view_qualified(&outer_table, current_ws) {
        let dialect = GenericDialect {};
        if let Ok(ast) = Parser::parse_sql(&dialect, &view_query_str) {
            if let Some(Statement::Query(inner_query)) = ast.first() {
                let (
                    inner_table,
                    inner_proj,
                    inner_filter,
                    inner_join,
                    inner_limit,
                    inner_group,
                    inner_agg,
                    inner_order,
                    inner_cases,
                    inner_wildcard,
                    inner_extracts,
                    inner_windows,
                ) = extract_query_parts(inner_query, catalog, &view_ws);

                final_table = inner_table;
                final_ws = view_ws;
                if outer_proj.is_empty() && outer_agg.is_empty() && outer_extracts.is_empty() {
                    final_proj = inner_proj;
                }
                if final_limit.is_none() {
                    final_limit = inner_limit;
                }
                if outer_join.is_none() {
                    outer_join = inner_join;
                }
                if final_group.is_empty() {
                    final_group = inner_group;
                }
                if final_agg.is_empty() {
                    final_agg = inner_agg;
                }
                if final_order.is_none() {
                    final_order = inner_order;
                }
                if final_cases.is_empty() {
                    final_cases = inner_cases;
                }
                if outer_proj.is_empty() {
                    final_wildcard = inner_wildcard;
                }
                if final_extracts.is_empty() {
                    final_extracts = inner_extracts;
                }
                if outer_windows.is_empty() {
                    outer_windows = inner_windows;
                }

                if let Some(inner_f) = inner_filter {
                    if let Some(outer_f) = final_filter {
                        final_filter = Some(FilterNode::And(Box::new(inner_f), Box::new(outer_f)));
                    } else {
                        final_filter = Some(inner_f);
                    }
                }
            }
        }
    }

    if let Some(join) = &mut outer_join {
        if let Some((view_ws, view_query_str)) =
            catalog.get_view_qualified(&join.right_table, current_ws)
        {
            let dialect = GenericDialect {};
            if let Ok(ast) = Parser::parse_sql(&dialect, &view_query_str) {
                if let Some(Statement::Query(inner_query)) = ast.first() {
                    let (mut inner_table, _, inner_filter, _, _, _, _, _, _, _, _, _) =
                        extract_query_parts(inner_query, catalog, current_ws);
                    if !inner_table.contains('.') {
                        inner_table = format!("{}.{}", view_ws, inner_table);
                    }
                    join.right_table = inner_table;
                    join.right_filter = inner_filter;
                }
            }
        }
    }

    crate::physical_plan::execute_select_with_ctes(
        &final_table,
        final_proj,
        final_filter,
        outer_join,
        final_limit,
        final_group,
        final_agg,
        final_order,
        final_cases,
        final_wildcard,
        final_extracts,
        catalog,
        export_path,
        &final_ws,
        &ctes,
        outer_windows,
        return_batches,
    )
}

fn materialize_ctes(
    query: &sqlparser::ast::Query,
    catalog: &Catalog,
    current_ws: &str,
    ctes: &mut crate::physical_plan::CteTables,
) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            materialize_ctes(&cte.query, catalog, current_ws, ctes);
            let (
                table,
                projection,
                filter,
                join,
                limit,
                group_by,
                aggregates,
                order_by,
                cases,
                wildcard,
                extracts,
                windows,
            ) = extract_query_parts(&cte.query, catalog, current_ws);
            let rows = crate::physical_plan::execute_select_with_ctes(
                &table, projection, filter, join, limit, group_by, aggregates, order_by, cases,
                wildcard, extracts, catalog, None, current_ws, ctes, windows, true,
            );
            ctes.insert(cte.alias.name.value.to_ascii_lowercase(), rows);
        }
    }
}

fn is_safe_remote_filter(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::{BinaryOperator, Expr, Value};

    match expr {
        Expr::Nested(inner) => is_safe_remote_filter(inner),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                is_safe_remote_filter(left) && is_safe_remote_filter(right)
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq => {
                matches!(&**left, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                    && matches!(
                        &**right,
                        Expr::Value(Value::Number(_, _))
                            | Expr::Value(Value::SingleQuotedString(_))
                    )
            }
            _ => false,
        },
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            matches!(&**expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                && matches!(&**pattern, Expr::Value(Value::SingleQuotedString(_)))
        }
        Expr::InList { expr, list, .. } => {
            matches!(&**expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                && list.iter().all(|item| {
                    matches!(
                        item,
                        Expr::Value(Value::Number(_, _))
                            | Expr::Value(Value::SingleQuotedString(_))
                    )
                })
        }
        _ => false,
    }
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub fn execute_remote_view_query(
    view_name: &str,
    workspace: &str,
    filter_sql: Option<&str>,
    export_path: String,
    catalog: &Catalog,
) -> Result<(), String> {
    let from = if workspace.is_empty() {
        quote_sql_identifier(view_name)
    } else {
        format!(
            "{}.{}",
            quote_sql_identifier(workspace),
            quote_sql_identifier(view_name)
        )
    };
    let sql = match filter_sql {
        Some(filter) => format!("SELECT * FROM {} WHERE {}", from, filter),
        None => format!("SELECT * FROM {}", from),
    };
    let statements = Parser::parse_sql(&GenericDialect {}, &sql)
        .map_err(|e| format!("Filtro remoto invalido: {}", e))?;
    if statements.len() != 1 {
        return Err("Consulta remota deve conter uma unica instrucao SQL.".to_string());
    }
    let query = match &statements[0] {
        Statement::Query(query) => query,
        _ => return Err("Consulta remota invalida.".to_string()),
    };

    if filter_sql.is_some() {
        let select = match &*query.body {
            sqlparser::ast::SetExpr::Select(select) => select,
            _ => return Err("Filtro remoto deve ser aplicado a uma consulta SELECT.".to_string()),
        };
        let selection = select
            .selection
            .as_ref()
            .ok_or_else(|| "Filtro remoto nao foi interpretado.".to_string())?;
        if !is_safe_remote_filter(selection) {
            return Err("O filtro remoto usa uma expressao nao suportada.".to_string());
        }
        let (_, _, parsed_filter, _, _, _, _, _, _, _, _, _) =
            extract_query_parts(query, catalog, workspace);
        if parsed_filter.is_none() {
            return Err("Nao foi possivel interpretar o filtro remoto.".to_string());
        }
    }

    let _ = parse_and_execute_query(query, Some(export_path), catalog, workspace, false);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        execute_query, execute_remote_view_query, extract_query_parts, has_parquet_extension,
        materialize_ctes, replace_cache_atomically,
    };
    use crate::catalog::{Catalog, ColumnDef, VirtualTable};
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn detects_parquet_extensions_in_paths_and_urls() {
        assert!(has_parquet_extension("/data/rainfall.PARQUET"));
        assert!(has_parquet_extension(
            "https://example.test/rainfall.parquet?token=abc"
        ));
        assert!(!has_parquet_extension(
            "https://example.test/query?view=rainfall"
        ));
        assert!(!has_parquet_extension("/data/rainfall.csv"));
    }

    #[test]
    fn execute_query_returns_all_rows_without_cli_truncation() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "federated-engine-pg-query-{}-{}.csv",
            std::process::id(),
            nonce
        ));
        let mut csv = String::from("year\n");
        for year in 2000..2025 {
            csv.push_str(&format!("{year}\n"));
        }
        fs::write(&path, csv).unwrap();
        let mut catalog = Catalog::new();
        catalog
            .workspaces
            .get_mut("default")
            .unwrap()
            .tables
            .insert(
                "climate".to_string(),
                VirtualTable {
                    name: "climate".to_string(),
                    format: "CSV".to_string(),
                    physical_path: path.to_string_lossy().into_owned(),
                    source_url: None,
                    columns: vec![ColumnDef {
                        name: "year".to_string(),
                        data_type: "Int64".to_string(),
                    }],
                },
            );

        let batches = execute_query("SELECT year FROM climate", &catalog).unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            25
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn remote_view_query_applies_filter_and_exports_all_matching_rows() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let source_path = std::env::temp_dir().join(format!(
            "federated-engine-source-{}-{}.csv",
            std::process::id(),
            nonce
        ));
        let output_path =
            std::env::temp_dir().join(format!(".temp_net_{}-{}.csv", std::process::id(), nonce));
        let mut csv = String::from("year,place\n");
        for year in 2000..2020 {
            csv.push_str(&format!("{},city{}\n", year, year));
        }
        csv.push_str("2024,Sorriso\n");
        fs::write(&source_path, csv).unwrap();

        let mut catalog = Catalog::new();
        let workspace = catalog.workspaces.get_mut("default").unwrap();
        workspace.tables.insert(
            "climate".to_string(),
            VirtualTable {
                name: "climate".to_string(),
                format: "CSV".to_string(),
                physical_path: source_path.to_string_lossy().into_owned(),
                source_url: None,
                columns: vec![
                    ColumnDef {
                        name: "year".to_string(),
                        data_type: "Int64".to_string(),
                    },
                    ColumnDef {
                        name: "place".to_string(),
                        data_type: "Utf8".to_string(),
                    },
                ],
            },
        );
        workspace.views.insert(
            "climate_view".to_string(),
            "SELECT * FROM climate".to_string(),
        );
        workspace.published_views.push("climate_view".to_string());

        execute_remote_view_query(
            "climate_view",
            "default",
            Some("\"year\" = '2024'"),
            output_path.to_string_lossy().into_owned(),
            &catalog,
        )
        .unwrap();

        let result = fs::read_to_string(&output_path).unwrap();
        let rows = result.lines().collect::<Vec<_>>();
        assert_eq!(rows.len(), 2, "header and matching row should be exported");
        assert!(rows[0].contains("year"));
        assert!(rows[1].contains("Sorriso"));

        fs::remove_file(source_path).unwrap();
        fs::remove_file(output_path).unwrap();
    }

    #[test]
    fn remote_view_query_rejects_multi_statement_filter() {
        let catalog = Catalog::new();
        let result = execute_remote_view_query(
            "climate_view",
            "default",
            Some("1 = 1; DROP TABLE climate"),
            ".temp_net_invalid.csv".to_string(),
            &catalog,
        );
        assert!(result.is_err());
    }

    #[test]
    fn refresh_replaces_cache_only_after_valid_csv_is_inferred() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let cache_path = std::env::temp_dir().join(format!(
            "federated-engine-cache-{}-{}.csv",
            std::process::id(),
            nonce
        ));
        fs::write(&cache_path, "year\n2023\n").unwrap();
        let table = VirtualTable {
            name: "climate".to_string(),
            format: "CSV".to_string(),
            physical_path: cache_path.to_string_lossy().into_owned(),
            source_url: Some("http://127.0.0.1:8080/query?view=climate".to_string()),
            columns: vec![ColumnDef {
                name: "year".to_string(),
                data_type: "Int64".to_string(),
            }],
        };

        let columns = replace_cache_atomically(&table, b"year,place\n2024,Sorriso\n").unwrap();
        assert_eq!(
            fs::read_to_string(&cache_path).unwrap(),
            "year,place\n2024,Sorriso\n"
        );
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["year", "place"]
        );
        assert_eq!(columns[0].data_type, "Int64");

        let previous_cache = fs::read_to_string(&cache_path).unwrap();
        assert!(replace_cache_atomically(&table, b"").is_err());
        assert_eq!(fs::read_to_string(&cache_path).unwrap(), previous_cache);
        fs::remove_file(cache_path).unwrap();
    }

    #[test]
    fn ctes_materialize_cascading_queries_and_join_on_either_side() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let source_path = std::env::temp_dir().join(format!(
            "federated-engine-cte-{}-{}.csv",
            std::process::id(),
            nonce
        ));
        fs::write(&source_path, "year,place\n2023,Other\n2024,Sorriso\n").unwrap();

        let mut catalog = Catalog::new();
        catalog
            .workspaces
            .get_mut("default")
            .unwrap()
            .tables
            .insert(
                "climate".to_string(),
                VirtualTable {
                    name: "climate".to_string(),
                    format: "CSV".to_string(),
                    physical_path: source_path.to_string_lossy().into_owned(),
                    source_url: None,
                    columns: vec![
                        ColumnDef {
                            name: "year".to_string(),
                            data_type: "Int64".to_string(),
                        },
                        ColumnDef {
                            name: "place".to_string(),
                            data_type: "Utf8".to_string(),
                        },
                    ],
                },
            );

        let run_query = |sql: &str| {
            let dialect = GenericDialect {};
            let statements = Parser::parse_sql(&dialect, sql).unwrap();
            let Statement::Query(query) = &statements[0] else {
                panic!("expected query");
            };
            let mut ctes = crate::physical_plan::CteTables::new();
            materialize_ctes(query, &catalog, "default", &mut ctes);
            let (
                table,
                projection,
                filter,
                join,
                limit,
                group_by,
                aggregates,
                order_by,
                cases,
                wildcard,
                extracts,
                windows,
            ) = extract_query_parts(query, &catalog, "default");
            crate::physical_plan::execute_select_with_ctes(
                &table, projection, filter, join, limit, group_by, aggregates, order_by, cases,
                wildcard, extracts, &catalog, None, "default", &ctes, windows, true,
            )
        };

        let cascaded = run_query(
            "WITH base AS (SELECT year, place FROM climate), filtered AS \
             (SELECT year, place FROM base WHERE year = 2024) SELECT * FROM filtered",
        );
        assert_eq!(
            cascaded.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );

        let cte_on_left = run_query(
            "WITH picked AS (SELECT year, place FROM climate WHERE year = 2024) \
             SELECT * FROM picked JOIN climate ON picked.year = climate.year",
        );
        assert_eq!(
            cte_on_left
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );
        assert!(cte_on_left[0]
            .schema()
            .field_with_name("place_climate")
            .is_ok());

        let cte_on_right = run_query(
            "WITH picked AS (SELECT year, place FROM climate WHERE year = 2024) \
             SELECT * FROM climate JOIN picked ON climate.year = picked.year",
        );
        assert_eq!(
            cte_on_right
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );
        assert!(cte_on_right[0]
            .schema()
            .field_with_name("place_picked")
            .is_ok());

        fs::remove_file(source_path).unwrap();
    }

    #[test]
    fn window_sum_broadcasts_partition_total_without_collapsing_rows() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let source_path = std::env::temp_dir().join(format!(
            "federated-engine-window-{}-{}.csv",
            std::process::id(),
            nonce
        ));
        fs::write(
            &source_path,
            "municipality,rain\nSorriso,10\nSorriso,20\nLucas,7\n",
        )
        .unwrap();
        let mut catalog = Catalog::new();
        catalog
            .workspaces
            .get_mut("default")
            .unwrap()
            .tables
            .insert(
                "rainfall".to_string(),
                VirtualTable {
                    name: "rainfall".to_string(),
                    format: "CSV".to_string(),
                    physical_path: source_path.to_string_lossy().into_owned(),
                    source_url: None,
                    columns: vec![
                        ColumnDef {
                            name: "municipality".to_string(),
                            data_type: "Utf8".to_string(),
                        },
                        ColumnDef {
                            name: "rain".to_string(),
                            data_type: "Int64".to_string(),
                        },
                    ],
                },
            );
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(
            &dialect,
            "SELECT municipality, rain, SUM(rain) OVER (PARTITION BY municipality) AS total_rain, \
             AVG(rain) OVER (PARTITION BY municipality) AS avg_rain \
             FROM rainfall",
        )
        .unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        let (
            table,
            projection,
            filter,
            join,
            limit,
            group_by,
            aggregates,
            order_by,
            cases,
            wildcard,
            extracts,
            windows,
        ) = super::extract_query_parts(query, &catalog, "default");
        assert!(aggregates.is_empty());
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].partition_by, vec!["municipality"]);

        let batches = crate::physical_plan::execute_select_with_ctes(
            &table,
            projection,
            filter,
            join,
            limit,
            group_by,
            aggregates,
            order_by,
            cases,
            wildcard,
            extracts,
            &catalog,
            None,
            "default",
            &crate::physical_plan::CteTables::new(),
            windows,
            true,
        );
        let result = &batches[0];
        assert_eq!(result.num_rows(), 3);
        let total_idx = result.schema().index_of("total_rain").unwrap();
        let totals = result
            .column(total_idx)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        let average_idx = result.schema().index_of("avg_rain").unwrap();
        let averages = result
            .column(average_idx)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(totals.value(0), 30.0);
        assert_eq!(totals.value(1), 30.0);
        assert_eq!(totals.value(2), 7.0);
        assert_eq!(averages.value(0), 15.0);
        assert_eq!(averages.value(1), 15.0);
        assert_eq!(averages.value(2), 7.0);
        fs::remove_file(source_path).unwrap();
    }

    #[test]
    fn parallel_filter_and_case_projection_process_multiple_csv_batches() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let source_path = std::env::temp_dir().join(format!(
            "federated-engine-parallel-{}-{}.csv",
            std::process::id(),
            nonce
        ));
        let mut csv = String::from("id\n");
        for id in 0..8192 {
            csv.push_str(&format!("{id}\n"));
        }
        fs::write(&source_path, csv).unwrap();

        let mut catalog = Catalog::new();
        catalog
            .workspaces
            .get_mut("default")
            .unwrap()
            .tables
            .insert(
                "records".to_string(),
                VirtualTable {
                    name: "records".to_string(),
                    format: "CSV".to_string(),
                    physical_path: source_path.to_string_lossy().into_owned(),
                    source_url: None,
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: "Int64".to_string(),
                    }],
                },
            );
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(
            &dialect,
            "SELECT id, CASE WHEN id > 4095 THEN 'high' ELSE 'low' END AS band \
             FROM records WHERE id >= 0",
        )
        .unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        let (
            table,
            projection,
            filter,
            join,
            limit,
            group_by,
            aggregates,
            order_by,
            cases,
            wildcard,
            extracts,
            windows,
        ) = super::extract_query_parts(query, &catalog, "default");
        let batches = crate::physical_plan::execute_select_with_ctes(
            &table,
            projection,
            filter,
            join,
            limit,
            group_by,
            aggregates,
            order_by,
            cases,
            wildcard,
            extracts,
            &catalog,
            None,
            "default",
            &crate::physical_plan::CteTables::new(),
            windows,
            true,
        );
        assert!(
            batches.len() > 1,
            "CSV rows should span multiple RecordBatches"
        );
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            8192
        );

        let band_idx = batches[0].schema().index_of("band").unwrap();
        let first_batch_band = batches[0].column(band_idx);
        let first_values = first_batch_band
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(first_values.value(0), "low");

        let last_batch = batches.last().unwrap();
        let last_band = last_batch
            .column(last_batch.schema().index_of("band").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(last_band.value(last_batch.num_rows() - 1), "high");
        fs::remove_file(source_path).unwrap();
    }
}
