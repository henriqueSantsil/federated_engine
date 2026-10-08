use crate::catalog::{Catalog, ColumnDef};
use dialoguer::{theme::ColorfulTheme, Confirm, Input, MultiSelect, Select};
use std::sync::{Arc, Mutex};

struct SelectableSource {
    name: String,
    label: String,
    columns: Option<Vec<ColumnDef>>,
}

fn choose<T: ToString>(prompt: &str, items: &[T]) -> Result<Option<usize>, String> {
    Select::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(items)
        .default(0)
        .interact_opt()
        .map_err(|error| error.to_string())
}

fn ask_text(prompt: &str, default: Option<&str>) -> Result<String, String> {
    let theme = ColorfulTheme::default();
    let input = Input::<String>::with_theme(&theme).with_prompt(prompt);
    let input = match default {
        Some(value) => input.default(value.to_string()),
        None => input,
    };
    input.interact_text().map_err(|error| error.to_string())
}

fn execute_sql(sql: &str, catalog: &Arc<Mutex<Catalog>>) {
    if sql.to_uppercase().starts_with("SERVE PG ON ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() == 4 {
            match parts[3].trim_end_matches(';').parse::<u16>() {
                Ok(port) if port > 0 => {
                    let catalog = catalog.clone();
                    tokio::spawn(async move {
                        if let Err(error) = crate::pg_server::run(catalog, port).await {
                            eprintln!("\x1B[1;31mErro no servidor PostgreSQL:\x1B[0m {}", error);
                        }
                    });
                }
                _ => println!("\x1B[1;31mErro:\x1B[0m Porta invalida."),
            }
        } else {
            println!("\x1B[1;31mErro:\x1B[0m Sintaxe incorreta. Use: SERVE PG ON <porta>;");
        }
        return;
    }
    if sql.to_uppercase().starts_with("SERVE ON ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() == 3 {
            match parts[2].trim_end_matches(';').parse::<u16>() {
                Ok(port) if port > 0 => crate::server::start_server(catalog.clone(), port),
                _ => println!("\x1B[1;31mErro:\x1B[0m Porta invalida."),
            }
        } else {
            println!("\x1B[1;31mErro:\x1B[0m Sintaxe incorreta. Use: SERVE ON <porta>;");
        }
        return;
    }
    crate::parser::parse_command(sql, &mut catalog.lock().unwrap());
}

pub fn choose_interface_mode() -> Result<super::InterfaceMode, String> {
    let options = [
        "Raw mode (digitar comandos SQL)",
        "Modo selecionavel (setas e Enter)",
    ];
    match choose("Escolha a interface", &options)? {
        Some(0) => Ok(super::InterfaceMode::Raw),
        Some(_) => Ok(super::InterfaceMode::Selectable),
        None => Ok(super::InterfaceMode::Raw),
    }
}

pub fn run_selectable_mode(
    catalog: Arc<Mutex<Catalog>>,
) -> Result<Option<super::InterfaceMode>, String> {
    loop {
        let snapshot = catalog.lock().unwrap().clone();
        let options = [
            "Consultar dados (assistente SELECT)",
            "Listar tabelas e views",
            "Mapear arquivo/URL como tabela externa",
            "Atualizar cache de tabela remota",
            "Criar view (assistente SELECT)",
            "Publicar view na rede",
            "Descobrir nos na rede local",
            "Iniciar servidor P2P",
            "Gerenciar workspaces",
            "Digitar comando SQL livre",
            "Alternar para raw mode",
            "Sair",
        ];
        let workspace = &snapshot.active_workspace;
        let prompt = format!(
            "Federated Engine | workspace: {} | escolha uma acao",
            workspace
        );
        match choose(&prompt, &options)? {
            Some(0) => {
                if let Some(query) = build_select_query(&snapshot)? {
                    execute_sql(&query, &catalog);
                }
            }
            Some(1) => execute_sql("SHOW TABLES;", &catalog),
            Some(2) => {
                if let Some(sql) = map_external_table()? {
                    execute_sql(&sql, &catalog);
                }
            }
            Some(3) => {
                let names = snapshot
                    .workspaces
                    .get(&snapshot.active_workspace)
                    .map(|workspace| {
                        workspace
                            .tables
                            .iter()
                            .filter(|(_, table)| table.source_url.is_some())
                            .map(|(name, _)| name.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                if names.is_empty() {
                    println!("Nao ha tabelas remotas neste workspace.");
                } else if let Some(index) = choose("Qual tabela deseja atualizar?", &names)? {
                    execute_sql(
                        &format!("REFRESH TABLE {};", quote_identifier(&names[index])),
                        &catalog,
                    );
                }
            }
            Some(4) => {
                if let Some(query) = build_select_query(&snapshot)? {
                    let name = validate_identifier(&ask_text("Nome da nova view", None)?)?;
                    execute_sql(&format!("CREATE VIEW {} AS {};", name, query), &catalog);
                }
            }
            Some(5) => {
                let names = snapshot
                    .workspaces
                    .get(&snapshot.active_workspace)
                    .map(|workspace| workspace.views.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                if names.is_empty() {
                    println!("Nao ha views neste workspace.");
                } else if let Some(index) = choose("Qual view deseja publicar?", &names)? {
                    execute_sql(
                        &format!("PUBLISH VIEW {};", quote_identifier(&names[index])),
                        &catalog,
                    );
                }
            }
            Some(6) => execute_sql("SHOW NETWORK NODES;", &catalog),
            Some(7) => {
                let port = ask_text("Porta para servir as views", Some("8080"))?;
                match port.parse::<u16>() {
                    Ok(port) if port > 0 => execute_sql(&format!("SERVE ON {};", port), &catalog),
                    _ => println!("\x1B[1;31mErro:\x1B[0m Informe uma porta entre 1 e 65535."),
                }
            }
            Some(8) => manage_workspaces(&snapshot, &catalog)?,
            Some(9) => {
                let sql = ask_text("Comando SQL (termine com ;)", None)?;
                execute_sql(&sql, &catalog);
            }
            Some(10) | None => return Ok(Some(super::InterfaceMode::Raw)),
            Some(11) => return Ok(None),
            Some(_) => unreachable!(),
        }
    }
}

fn map_external_table() -> Result<Option<String>, String> {
    let name = validate_identifier(&ask_text("Nome da tabela", None)?)?;
    let location = ask_text("Caminho CSV/Parquet ou URL HTTP", None)?;
    Ok(Some(format!(
        "CREATE EXTERNAL TABLE {} LOCATION '{}';",
        name,
        quote_string(&location)
    )))
}

fn manage_workspaces(snapshot: &Catalog, catalog: &Arc<Mutex<Catalog>>) -> Result<(), String> {
    let options = [
        "Listar workspaces",
        "Criar workspace",
        "Usar workspace",
        "Voltar",
    ];
    match choose("Gerenciar workspaces", &options)? {
        Some(0) => execute_sql("SHOW WORKSPACES;", catalog),
        Some(1) => {
            let name = validate_identifier(&ask_text("Nome do workspace", None)?)?;
            execute_sql(&format!("CREATE WORKSPACE {};", name), catalog);
        }
        Some(2) => {
            let names = snapshot.workspaces.keys().cloned().collect::<Vec<_>>();
            if let Some(index) = choose("Workspace ativo", &names)? {
                execute_sql(
                    &format!("USE {};", quote_identifier(&names[index])),
                    catalog,
                );
            }
        }
        Some(3) | None => {}
        Some(_) => unreachable!(),
    }
    Ok(())
}

fn build_select_query(snapshot: &Catalog) -> Result<Option<String>, String> {
    let Some(workspace) = snapshot.workspaces.get(&snapshot.active_workspace) else {
        return Err(format!(
            "Workspace '{}' nao encontrado.",
            snapshot.active_workspace
        ));
    };
    let mut sources = Vec::new();
    for (name, table) in &workspace.tables {
        sources.push(SelectableSource {
            name: name.clone(),
            label: format!("Tabela: {}", name),
            columns: Some(table.columns.clone()),
        });
    }
    for name in workspace.views.keys() {
        sources.push(SelectableSource {
            name: name.clone(),
            label: format!("View: {}", name),
            columns: snapshot.get_view_columns_qualified(name, &snapshot.active_workspace),
        });
    }
    if sources.is_empty() {
        println!("Este workspace ainda nao tem tabelas ou views.");
        return Ok(None);
    }

    let labels = sources
        .iter()
        .map(|source| source.label.clone())
        .collect::<Vec<_>>();
    let Some(source_index) = choose("Qual conjunto de dados deseja consultar?", &labels)? else {
        return Ok(None);
    };
    let source = &sources[source_index];

    let projection = match &source.columns {
        Some(columns) if !columns.is_empty() => {
            let labels = columns
                .iter()
                .map(|column| format!("{} ({})", column.name, column.data_type))
                .collect::<Vec<_>>();
            let defaults = vec![true; labels.len()];
            let selected = MultiSelect::with_theme(&ColorfulTheme::default())
                .with_prompt("Escolha colunas (Space marca, Enter confirma)")
                .items(&labels)
                .defaults(&defaults)
                .interact()
                .map_err(|error| error.to_string())?;
            if selected.len() == columns.len() || selected.is_empty() {
                "*".to_string()
            } else {
                selected
                    .iter()
                    .map(|index| quote_identifier(&columns[*index].name))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        }
        _ => "*".to_string(),
    };

    let mut query = format!(
        "SELECT {} FROM {}",
        projection,
        quote_identifier(&source.name)
    );
    if Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt("Adicionar filtro?")
        .default(false)
        .interact()
        .map_err(|error| error.to_string())?
    {
        if let Some(columns) = &source.columns {
            let column_names = columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            if let Some(column_index) = choose("Coluna do filtro", &column_names)? {
                let operators = ["=", "!=", ">", ">=", "<", "<=", "LIKE", "NOT LIKE"];
                if let Some(operator_index) = choose("Operador", &operators)? {
                    let value = ask_text("Valor (LIKE aceita % como curinga)", None)?;
                    query.push_str(&format!(
                        " WHERE {} {} '{}'",
                        quote_identifier(&column_names[column_index]),
                        operators[operator_index],
                        quote_string(&value)
                    ));
                }
            }
        } else {
            println!("Nao ha esquema disponivel para montar um filtro.");
        }
    }
    let limit = ask_text("Limite de linhas (vazio para padrao)", Some(""))?;
    if !limit.trim().is_empty() {
        match limit.trim().parse::<usize>() {
            Ok(limit) => query.push_str(&format!(" LIMIT {}", limit)),
            Err(_) => return Err("O limite deve ser um numero inteiro nao negativo.".to_string()),
        }
    }
    query.push(';');
    Ok(Some(query))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn validate_identifier(identifier: &str) -> Result<String, String> {
    let mut characters = identifier.chars();
    let valid_start = characters
        .next()
        .map(|character| character.is_ascii_alphabetic() || character == '_')
        .unwrap_or(false);
    if valid_start
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        Ok(identifier.to_string())
    } else {
        Err("Use um identificador comeca por letra/underscore e contem apenas letras, numeros ou underscore.".to_string())
    }
}
