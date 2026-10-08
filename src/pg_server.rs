use crate::catalog::Catalog;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::{stream, Sink};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::fmt::Debug;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

struct EngineHandler {
    catalog: Arc<Mutex<Catalog>>,
}

#[async_trait]
impl NoopStartupHandler for EngineHandler {
    async fn post_startup<C>(
        &self,
        _client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for EngineHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let catalog = Arc::clone(&self.catalog);
        let query = query.to_string();
        let batches = tokio::task::spawn_blocking(move || {
            let catalog = catalog.lock().map_err(|error| error.to_string())?;
            if is_metadata_query(&query) {
                mock_pg_catalog(&query, &catalog)
            } else {
                crate::parser::execute_query(&query, &catalog)
            }
        })
        .await
        .map_err(|error| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                format!("Falha ao executar consulta: {}", error),
            )))
        })?
        .map_err(|error| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42601".to_string(),
                error,
            )))
        })?;

        batches_to_response(batches).map(|response| vec![response])
    }
}

fn is_metadata_query(query: &str) -> bool {
    let query = query.to_ascii_lowercase();
    [
        "pg_catalog",
        "pg_type",
        "pg_class",
        "pg_attribute",
        "pg_namespace",
        "pg_tables",
        "information_schema",
    ]
    .iter()
    .any(|keyword| query.contains(keyword))
}

#[derive(Clone)]
struct MetadataColumn {
    name: &'static str,
    data_type: DataType,
}

fn metadata_table(catalog: &Catalog) -> Vec<(String, String, String, Vec<(String, String)>, bool)> {
    let Some(workspace) = catalog.workspaces.get(&catalog.active_workspace) else {
        return Vec::new();
    };
    let mut tables = Vec::new();
    for table in workspace.tables.values() {
        tables.push((
            table.name.clone(),
            catalog.active_workspace.clone(),
            table.format.clone(),
            table
                .columns
                .iter()
                .map(|column| (column.name.clone(), column.data_type.clone()))
                .collect(),
            false,
        ));
    }
    for (view_name, _) in &workspace.views {
        let columns = catalog
            .get_view_columns_qualified(view_name, &catalog.active_workspace)
            .unwrap_or_default()
            .into_iter()
            .map(|column| (column.name, column.data_type))
            .collect();
        tables.push((
            view_name.clone(),
            catalog.active_workspace.clone(),
            "VIEW".to_string(),
            columns,
            true,
        ));
    }
    tables.sort_by(|left, right| left.0.cmp(&right.0));
    tables
}

fn pg_type_oid(data_type: &str) -> i64 {
    match data_type.to_ascii_lowercase().as_str() {
        "int8" | "bigint" => 20,
        "int2" | "smallint" => 21,
        "int4" | "integer" => 23,
        "float4" | "real" => 700,
        "float8" | "double" | "double precision" => 701,
        "bool" | "boolean" => 16,
        _ => 25,
    }
}

fn postgres_type_name(data_type: &str) -> (&'static str, &'static str) {
    match data_type.to_ascii_lowercase().as_str() {
        "int8" | "bigint" | "int64" => ("bigint", "int8"),
        "int2" | "smallint" | "int16" => ("smallint", "int2"),
        "int4" | "integer" | "int32" => ("integer", "int4"),
        "float4" | "real" => ("real", "float4"),
        "float8" | "double" | "double precision" | "float64" => ("double precision", "float8"),
        "bool" | "boolean" => ("boolean", "bool"),
        _ => ("text", "text"),
    }
}

fn metadata_fields(kind: &str) -> Vec<MetadataColumn> {
    let text = DataType::Utf8;
    let int = DataType::Int64;
    match kind {
        "pg_class" => vec![
            MetadataColumn {
                name: "oid",
                data_type: int.clone(),
            },
            MetadataColumn {
                name: "relname",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "relnamespace",
                data_type: int.clone(),
            },
            MetadataColumn {
                name: "relkind",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "reltype",
                data_type: int.clone(),
            },
            MetadataColumn {
                name: "relowner",
                data_type: int.clone(),
            },
        ],
        "pg_namespace" => vec![
            MetadataColumn {
                name: "oid",
                data_type: int.clone(),
            },
            MetadataColumn {
                name: "nspname",
                data_type: text.clone(),
            },
        ],
        "pg_tables" => vec![
            MetadataColumn {
                name: "schemaname",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "tablename",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "tableowner",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "tablespace",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "hasindexes",
                data_type: DataType::Boolean,
            },
            MetadataColumn {
                name: "hasrules",
                data_type: DataType::Boolean,
            },
            MetadataColumn {
                name: "hastriggers",
                data_type: DataType::Boolean,
            },
            MetadataColumn {
                name: "rowsecurity",
                data_type: DataType::Boolean,
            },
        ],
        "pg_attribute" => vec![
            MetadataColumn {
                name: "attrelid",
                data_type: int.clone(),
            },
            MetadataColumn {
                name: "attname",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "atttypid",
                data_type: int.clone(),
            },
            MetadataColumn {
                name: "attnum",
                data_type: DataType::Int64,
            },
            MetadataColumn {
                name: "attnotnull",
                data_type: DataType::Boolean,
            },
        ],
        "pg_type" => vec![
            MetadataColumn {
                name: "oid",
                data_type: int.clone(),
            },
            MetadataColumn {
                name: "typname",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "typlen",
                data_type: int.clone(),
            },
            MetadataColumn {
                name: "typcategory",
                data_type: text.clone(),
            },
        ],
        "information_schema.columns" => vec![
            MetadataColumn {
                name: "table_catalog",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "table_schema",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "table_name",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "column_name",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "ordinal_position",
                data_type: int.clone(),
            },
            MetadataColumn {
                name: "data_type",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "udt_name",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "is_nullable",
                data_type: text.clone(),
            },
        ],
        _ => vec![
            MetadataColumn {
                name: "table_catalog",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "table_schema",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "table_name",
                data_type: text.clone(),
            },
            MetadataColumn {
                name: "table_type",
                data_type: text,
            },
        ],
    }
}

fn metadata_rows(kind: &str, catalog: &Catalog) -> Vec<Vec<Option<String>>> {
    let tables = metadata_table(catalog);
    match kind {
        "pg_class" => tables
            .iter()
            .enumerate()
            .map(|(index, (name, _, _, _, is_view))| {
                vec![
                    Some((16_384 + index as i64).to_string()),
                    Some(name.clone()),
                    Some("2200".to_string()),
                    Some(if *is_view { "v" } else { "r" }.to_string()),
                    Some("0".to_string()),
                    Some("10".to_string()),
                ]
            })
            .collect(),
        "pg_namespace" => vec![
            vec![Some("11".to_string()), Some("pg_catalog".to_string())],
            vec![
                Some("2200".to_string()),
                Some(catalog.active_workspace.clone()),
            ],
            vec![
                Some("2201".to_string()),
                Some("information_schema".to_string()),
            ],
        ],
        "pg_tables" => tables
            .iter()
            .filter(|(_, _, _, _, is_view)| !is_view)
            .map(|(name, schema, _, _, _)| {
                vec![
                    Some(schema.clone()),
                    Some(name.clone()),
                    Some("federated_engine".to_string()),
                    None,
                    Some("false".to_string()),
                    Some("false".to_string()),
                    Some("false".to_string()),
                    Some("false".to_string()),
                ]
            })
            .collect(),
        "pg_attribute" => {
            let mut rows = Vec::new();
            for (table_index, (_, _, _, columns, _)) in tables.iter().enumerate() {
                let relation_oid = 16_384 + table_index as i64;
                for (column_index, (column_name, data_type)) in columns.iter().enumerate() {
                    rows.push(vec![
                        Some(relation_oid.to_string()),
                        Some(column_name.clone()),
                        Some(pg_type_oid(data_type).to_string()),
                        Some((column_index + 1).to_string()),
                        Some("false".to_string()),
                    ]);
                }
            }
            rows
        }
        "pg_type" => vec![
            (16, "bool", 1, "B"),
            (20, "int8", 8, "N"),
            (21, "int2", 2, "N"),
            (23, "int4", 4, "N"),
            (25, "text", -1, "S"),
            (700, "float4", 4, "N"),
            (701, "float8", 8, "N"),
        ]
        .into_iter()
        .map(|(oid, name, length, category)| {
            vec![
                Some(oid.to_string()),
                Some(name.to_string()),
                Some(length.to_string()),
                Some(category.to_string()),
            ]
        })
        .collect(),
        "information_schema.columns" => tables
            .iter()
            .flat_map(|(table_name, schema, _, columns, _)| {
                columns
                    .iter()
                    .enumerate()
                    .map(move |(index, (column, type_name))| {
                        let (data_type, udt_name) = postgres_type_name(type_name);
                        vec![
                            Some("federated_engine".to_string()),
                            Some(schema.clone()),
                            Some(table_name.clone()),
                            Some(column.clone()),
                            Some((index + 1).to_string()),
                            Some(data_type.to_string()),
                            Some(udt_name.to_string()),
                            Some("YES".to_string()),
                        ]
                    })
            })
            .collect(),
        _ => tables
            .iter()
            .map(|(name, schema, _, _, is_view)| {
                vec![
                    Some("federated_engine".to_string()),
                    Some(schema.clone()),
                    Some(name.clone()),
                    Some(if *is_view { "VIEW" } else { "BASE TABLE" }.to_string()),
                ]
            })
            .collect(),
    }
}

fn make_metadata_batch(
    fields: &[MetadataColumn],
    rows: &[Vec<Option<String>>],
) -> Result<RecordBatch, String> {
    let arrow_fields = fields
        .iter()
        .map(|field| Field::new(field.name, field.data_type.clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(arrow_fields));
    let arrays = fields
        .iter()
        .enumerate()
        .map(|(column_index, field)| match field.data_type {
            DataType::Int64 => {
                let values = rows
                    .iter()
                    .map(|row| {
                        row.get(column_index)
                            .and_then(Option::as_deref)
                            .and_then(|value| value.parse::<i64>().ok())
                    })
                    .collect::<Vec<_>>();
                Arc::new(Int64Array::from(values)) as ArrayRef
            }
            DataType::Float64 => {
                let values = rows
                    .iter()
                    .map(|row| {
                        row.get(column_index)
                            .and_then(Option::as_deref)
                            .and_then(|value| value.parse::<f64>().ok())
                    })
                    .collect::<Vec<_>>();
                Arc::new(Float64Array::from(values)) as ArrayRef
            }
            DataType::Boolean => {
                let values = rows
                    .iter()
                    .map(|row| {
                        row.get(column_index)
                            .and_then(Option::as_deref)
                            .and_then(|value| value.parse::<bool>().ok())
                    })
                    .collect::<Vec<_>>();
                Arc::new(arrow::array::BooleanArray::from(values)) as ArrayRef
            }
            _ => {
                let values = rows
                    .iter()
                    .map(|row| row.get(column_index).cloned().flatten())
                    .collect::<Vec<_>>();
                Arc::new(StringArray::from(values)) as ArrayRef
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, arrays)
        .map_err(|error| format!("Falha ao construir mock do catalogo: {}", error))
}

fn requested_projection(query: &str, schema: &Schema) -> Option<Vec<(usize, String)>> {
    let statements = Parser::parse_sql(&GenericDialect {}, query).ok()?;
    let Statement::Query(query) = statements.first()? else {
        return None;
    };
    let SetExpr::Select(select) = &*query.body else {
        return None;
    };
    if select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    }) {
        return None;
    }
    let mut projection = Vec::new();
    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => return None,
        };
        let column = match expr {
            Expr::Identifier(ident) => ident.value.as_str(),
            Expr::CompoundIdentifier(idents) => idents.last()?.value.as_str(),
            Expr::Nested(inner) => match &**inner {
                Expr::Identifier(ident) => ident.value.as_str(),
                Expr::CompoundIdentifier(idents) => idents.last()?.value.as_str(),
                _ => return None,
            },
            _ => return None,
        };
        let index = schema.index_of(column).ok()?;
        projection.push((index, alias.unwrap_or_else(|| column.to_string())));
    }
    Some(projection)
}

fn project_metadata_batch(batch: RecordBatch, query: &str) -> Result<RecordBatch, String> {
    let Some(projection) = requested_projection(query, &batch.schema()) else {
        return Ok(batch);
    };
    let schema = batch.schema();
    let fields = projection
        .iter()
        .map(|(index, name)| {
            let field = schema.field(*index);
            Arc::new(Field::new(
                name,
                field.data_type().clone(),
                field.is_nullable(),
            ))
        })
        .collect::<Vec<_>>();
    let columns = projection
        .iter()
        .map(|(index, _)| batch.column(*index).clone())
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| format!("Falha ao projetar mock do catalogo: {}", error))
}

fn mock_pg_catalog(query: &str, catalog: &Catalog) -> Result<Vec<RecordBatch>, String> {
    let normalized = query.to_ascii_lowercase();
    let kind = if normalized.contains("pg_attribute") {
        "pg_attribute"
    } else if normalized.contains("pg_namespace") {
        "pg_namespace"
    } else if normalized.contains("pg_type") {
        "pg_type"
    } else if normalized.contains("pg_tables") {
        "pg_tables"
    } else if normalized.contains("information_schema") {
        if normalized.contains("information_schema.columns") {
            "information_schema.columns"
        } else {
            "information_schema"
        }
    } else {
        "pg_class"
    };
    let fields = metadata_fields(kind);
    let rows = metadata_rows(kind, catalog);
    let batch = make_metadata_batch(&fields, &rows)?;
    Ok(vec![project_metadata_batch(batch, query)?])
}

struct HandlerFactory {
    handler: Arc<EngineHandler>,
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::clone(&self.handler)
    }
}

fn batches_to_response(batches: Vec<RecordBatch>) -> PgWireResult<Response> {
    let Some(first_batch) = batches.first() else {
        let error = ErrorInfo::new(
            "ERROR".to_string(),
            "XX000".to_string(),
            "A consulta nao retornou esquema Arrow.".to_string(),
        );
        return Ok(Response::Error(Box::new(error)));
    };

    let fields = Arc::new(
        first_batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let pg_type = match field.data_type() {
                    DataType::Int64 => Type::INT8,
                    DataType::Float64 => Type::FLOAT8,
                    DataType::Utf8 => Type::TEXT,
                    _ => Type::TEXT,
                };
                FieldInfo::new(field.name().clone(), None, None, pg_type, FieldFormat::Text)
            })
            .collect::<Vec<_>>(),
    );

    let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            for column_index in 0..batch.num_columns() {
                let array = batch.column(column_index);
                if array.is_null(row) {
                    encoder.encode_field(&Option::<String>::None)?;
                    continue;
                }

                match array.data_type() {
                    DataType::Int64 => {
                        let value = array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("Arrow Int64 type must use Int64Array")
                            .value(row);
                        encoder.encode_field(&Some(value))?;
                    }
                    DataType::Float64 => {
                        let value = array
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .expect("Arrow Float64 type must use Float64Array")
                            .value(row);
                        encoder.encode_field(&Some(value))?;
                    }
                    _ => {
                        let string_array = if array.data_type() == &DataType::Utf8 {
                            array.clone()
                        } else {
                            cast(array, &DataType::Utf8).map_err(|error| {
                                PgWireError::UserError(Box::new(ErrorInfo::new(
                                    "ERROR".to_string(),
                                    "0A000".to_string(),
                                    format!(
                                        "Tipo Arrow '{}' nao pode ser serializado como texto: {}",
                                        array.data_type(),
                                        error
                                    ),
                                )))
                            })?
                        };
                        let value = crate::physical_plan::get_key_as_string(&string_array, row);
                        encoder.encode_field(&Some(value))?;
                    }
                }
            }
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(Response::Query(QueryResponse::new(
        fields,
        stream::iter(rows),
    )))
}

pub async fn run(catalog: Arc<Mutex<Catalog>>, port: u16) -> io::Result<()> {
    let address = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&address).await?;
    println!(
        "\x1B[1;32m[Servidor PostgreSQL]\x1B[0m Escutando em {}",
        address
    );
    let factory = Arc::new(HandlerFactory {
        handler: Arc::new(EngineHandler { catalog }),
    });

    loop {
        let (socket, _) = listener.accept().await?;
        let factory = Arc::clone(&factory);
        tokio::spawn(async move {
            if let Err(error) = process_socket(socket, None, factory).await {
                eprintln!("Conexao PostgreSQL encerrada com erro: {}", error);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{batches_to_response, is_metadata_query, mock_pg_catalog};
    use crate::catalog::{Catalog, ColumnDef, VirtualTable};
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use pgwire::api::results::Response;
    use std::sync::Arc;

    #[test]
    fn metadata_queries_are_intercepted_and_list_active_workspace_objects() {
        let mut catalog = Catalog::new();
        let workspace = catalog.workspaces.get_mut("default").unwrap();
        workspace.tables.insert(
            "rainfall".to_string(),
            VirtualTable {
                name: "rainfall".to_string(),
                format: "CSV".to_string(),
                physical_path: "/missing/rainfall.csv".to_string(),
                source_url: None,
                columns: vec![ColumnDef {
                    name: "municipality".to_string(),
                    data_type: "Utf8".to_string(),
                }],
            },
        );
        workspace.views.insert(
            "rainfall_view".to_string(),
            "SELECT municipality FROM rainfall".to_string(),
        );

        let sql = "SELECT relname FROM pg_catalog.pg_class";
        assert!(is_metadata_query(sql));
        let batches = mock_pg_catalog(sql, &catalog).unwrap();
        assert_eq!(batches[0].schema().fields().len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "relname");
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.len(), 2);
        assert!(names.value(0) == "rainfall" || names.value(1) == "rainfall");
        assert!(names.value(0) == "rainfall_view" || names.value(1) == "rainfall_view");

        let columns =
            mock_pg_catalog("SELECT attname FROM pg_catalog.pg_attribute", &catalog).unwrap();
        assert_eq!(columns[0].num_rows(), 2);
    }

    #[test]
    fn pg_type_returns_basic_postgres_type_oids() {
        let catalog = Catalog::new();
        let batches =
            mock_pg_catalog("SELECT oid, typname FROM pg_catalog.pg_type", &catalog).unwrap();
        assert_eq!(batches[0].num_rows(), 7);
        assert_eq!(batches[0].num_columns(), 2);
        let type_names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(type_names.value(4), "text");
    }

    #[test]
    fn namespace_and_information_schema_expose_active_workspace_metadata() {
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
                    physical_path: "/missing/rainfall.csv".to_string(),
                    source_url: None,
                    columns: vec![ColumnDef {
                        name: "year".to_string(),
                        data_type: "Int64".to_string(),
                    }],
                },
            );

        let namespace_query = "SELECT nspname FROM pg_catalog.pg_namespace";
        assert!(is_metadata_query(namespace_query));
        let namespaces = mock_pg_catalog(namespace_query, &catalog).unwrap();
        let names = namespaces[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!((0..names.len()).any(|index| names.value(index) == "default"));

        let columns_query =
            "SELECT table_name, column_name, data_type FROM information_schema.columns";
        let columns = mock_pg_catalog(columns_query, &catalog).unwrap();
        assert_eq!(columns[0].num_rows(), 1);
        assert_eq!(columns[0].schema().field(2).name(), "data_type");
        let table_names = columns[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let column_names = columns[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let data_types = columns[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(table_names.value(0), "rainfall");
        assert_eq!(column_names.value(0), "year");
        assert_eq!(data_types.value(0), "bigint");
    }

    #[tokio::test]
    async fn encodes_arrow_values_as_postgres_data_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("year", DataType::Int64, false),
            Field::new("rain", DataType::Float64, false),
            Field::new("municipality", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![2024])),
                Arc::new(Float64Array::from(vec![12.5])),
                Arc::new(StringArray::from(vec!["Sorriso"])),
            ],
        )
        .unwrap();

        let Response::Query(mut response) = batches_to_response(vec![batch]).unwrap() else {
            panic!("expected query response");
        };
        assert_eq!(response.row_schema.len(), 3);
        assert_eq!(response.row_schema[0].datatype(), &pgwire::api::Type::INT8);
        assert_eq!(
            response.row_schema[1].datatype(),
            &pgwire::api::Type::FLOAT8
        );
        assert_eq!(response.row_schema[2].datatype(), &pgwire::api::Type::TEXT);
        let row = response.data_rows.next().await.unwrap().unwrap();
        assert_eq!(row.field_count, 3);
        let encoded = String::from_utf8_lossy(&row.data);
        assert!(encoded.contains("2024"));
        assert!(encoded.contains("12.5"));
        assert!(encoded.contains("Sorriso"));
        assert!(response.data_rows.next().await.is_none());
    }
}
