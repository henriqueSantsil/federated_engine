use crate::catalog::Catalog;
use crate::parser;
use rouille::Response;
use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize)]
struct PublishedView {
    workspace: String,
    view: String,
    query: String,
}

#[derive(Deserialize)]
pub(crate) struct RemoteView {
    pub(crate) workspace: String,
    pub(crate) view: String,
}

pub fn start_server(catalog_arc: Arc<Mutex<Catalog>>, port: u16) {
    let addr = format!("0.0.0.0:{}", port);
    println!(
        "\x1B[1;32m[Servidor Node]\x1B[0m Escutando requisicoes P2P em http://{}",
        addr
    );

    thread::spawn(move || {
        rouille::start_server(addr, move |request| {
            let request_url = request.url();
            let path = request_url.split('?').next().unwrap_or_default();
            match path {
                "/catalog" => {
                    let catalog = catalog_arc.lock().unwrap();
                    let mut items = Vec::new();

                    for (ws_name, ws) in &catalog.workspaces {
                        for view_name in &ws.published_views {
                            if let Some(query) = ws.views.get(view_name) {
                                items.push(PublishedView {
                                    workspace: ws_name.clone(),
                                    view: view_name.clone(),
                                    query: query.clone(),
                                });
                            }
                        }
                    }

                    match serde_json::to_string(&items) {
                        Ok(json_payload) => Response::text(json_payload)
                            .with_additional_header("Content-Type", "application/json")
                            .with_additional_header("Access-Control-Allow-Origin", "*"),
                        Err(error) => {
                            eprintln!("Falha ao serializar catalogo de views: {}", error);
                            Response::text("Erro interno ao serializar o catalogo.")
                                .with_status_code(500)
                        }
                    }
                }
                "/query" => {
                    let view_name = request.get_param("view").unwrap_or_default();
                    let ws_name = request.get_param("workspace").unwrap_or_default();
                    let filter_sql = request.get_param("filter");
                    let schema_only = request
                        .get_param("schema_only")
                        .map(|value| value.eq_ignore_ascii_case("true"))
                        .unwrap_or(false);

                    if view_name.is_empty() {
                        return Response::text("Erro: Parametro 'view' ausente.")
                            .with_status_code(400);
                    }

                    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                    let temp_file = format!(
                        ".temp_net_{}_{}.csv",
                        std::process::id(),
                        duration.as_nanos()
                    );

                    let catalog = catalog_arc.lock().unwrap();
                    let view_workspace = if ws_name.is_empty() {
                        catalog.active_workspace.as_str()
                    } else {
                        ws_name.as_str()
                    };
                    let is_published = catalog
                        .workspaces
                        .get(view_workspace)
                        .map(|workspace| {
                            workspace
                                .published_views
                                .iter()
                                .any(|name| name == &view_name)
                        })
                        .unwrap_or(false);
                    if !is_published {
                        return Response::text("View nao publicada ou nao encontrada.")
                            .with_status_code(404);
                    }

                    if schema_only {
                        let columns =
                            match catalog.get_view_columns_qualified(&view_name, view_workspace) {
                                Some(columns) => columns,
                                None => {
                                    return Response::text(
                                        "Nao foi possivel inferir o esquema desta view.",
                                    )
                                    .with_status_code(400)
                                }
                            };
                        return match serde_json::to_string(&columns) {
                            Ok(schema) => Response::text(schema)
                                .with_additional_header("Content-Type", "application/json")
                                .with_additional_header("Access-Control-Allow-Origin", "*"),
                            Err(error) => {
                                eprintln!("Falha ao serializar esquema remoto: {}", error);
                                Response::text("Erro interno ao serializar o esquema.")
                                    .with_status_code(500)
                            }
                        };
                    }

                    let query_result = parser::execute_remote_view_query(
                        &view_name,
                        view_workspace,
                        filter_sql.as_deref(),
                        temp_file.clone(),
                        &catalog,
                    );
                    drop(catalog);

                    if let Err(error) = query_result {
                        let _ = fs::remove_file(&temp_file);
                        return Response::text(error).with_status_code(400);
                    }

                    match fs::read(&temp_file) {
                        Ok(csv_data) => {
                            if let Err(error) = fs::remove_file(&temp_file) {
                                eprintln!(
                                    "Falha ao remover arquivo temporario '{}': {}",
                                    temp_file, error
                                );
                            }
                            Response::from_data("text/csv", csv_data)
                                .with_additional_header("Content-Type", "text/csv")
                                .with_additional_header("Access-Control-Allow-Origin", "*")
                        }
                        Err(error) => {
                            eprintln!("Falha ao ler CSV temporario '{}': {}", temp_file, error);
                            let _ = fs::remove_file(&temp_file);
                            Response::text("Erro interno ao executar a consulta.")
                                .with_status_code(500)
                        }
                    }
                }
                _ => Response::text(
                    "Federated Engine P2P Node. Acesse /catalog para listar as views.",
                )
                .with_additional_header("Access-Control-Allow-Origin", "*"),
            }
        });
    });
}

#[cfg(test)]
mod tests {
    use super::start_server;
    use crate::catalog::{Catalog, ColumnDef, VirtualTable};
    use crate::parser::FilterNode;
    use std::fs;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn p2p_schema_and_filter_pushdown_work_over_http() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let source_path = std::env::temp_dir().join(format!(
            "federated-engine-server-source-{}-{}.csv",
            std::process::id(),
            nonce
        ));
        let output_path =
            std::env::temp_dir().join(format!(".temp_net_{}-{}.csv", std::process::id(), nonce));
        let mut csv = String::from("year,place\n");
        for year in 2000..2100 {
            csv.push_str(&format!("{},city{}\n", year, year));
        }
        fs::write(&source_path, csv).unwrap();

        let mut publisher_catalog = Catalog::new();
        let workspace = publisher_catalog.workspaces.get_mut("default").unwrap();
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

        let port_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = port_listener.local_addr().unwrap().port();
        drop(port_listener);
        start_server(Arc::new(Mutex::new(publisher_catalog)), port);

        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_millis(500))
            .build();
        let base_url = format!("http://127.0.0.1:{}", port);
        let mut server_ready = false;
        for _ in 0..50 {
            if agent.get(&format!("{}/catalog", base_url)).call().is_ok() {
                server_ready = true;
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert!(server_ready, "P2P server did not become ready");

        let schema_response = agent
            .get(&format!("{}/query", base_url))
            .query("view", "climate_view")
            .query("workspace", "default")
            .query("schema_only", "true")
            .call()
            .unwrap()
            .into_string()
            .unwrap();
        let schema: Vec<ColumnDef> = serde_json::from_str(&schema_response).unwrap();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "year");

        let mut remote_catalog = Catalog::new();
        remote_catalog
            .workspaces
            .get_mut("default")
            .unwrap()
            .tables
            .insert(
                "remote_climate".to_string(),
                VirtualTable {
                    name: "remote_climate".to_string(),
                    format: "CSV".to_string(),
                    physical_path: output_path
                        .with_extension("missing")
                        .to_string_lossy()
                        .into_owned(),
                    source_url: Some(format!(
                        "{}/query?view=climate_view&workspace=default",
                        base_url
                    )),
                    columns: schema,
                },
            );

        crate::physical_plan::execute_select(
            "remote_climate",
            Vec::new(),
            Some(FilterNode::Condition {
                column: "year".to_string(),
                operator: ">=".to_string(),
                value: "2000".to_string(),
            }),
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            Vec::new(),
            true,
            Vec::new(),
            &remote_catalog,
            Some(output_path.to_string_lossy().into_owned()),
            "default",
        );

        let filtered_csv = fs::read_to_string(&output_path).unwrap();
        assert_eq!(filtered_csv.lines().count(), 101);
        assert!(filtered_csv.contains("city2099"));

        fs::remove_file(source_path).unwrap();
        fs::remove_file(output_path).unwrap();
    }
}
