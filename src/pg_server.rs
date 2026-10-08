use crate::catalog::Catalog;
use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::{stream, Sink};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
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
        Ok(vec![execute_query_response(query, &self.catalog).await?])
    }
}

#[async_trait]
impl ExtendedQueryHandler for EngineHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        execute_query_response(&portal.statement.statement, &self.catalog).await
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let fields = describe_query_fields(&statement.statement, &self.catalog).await?;
        let parameter_types = statement
            .parameter_types
            .iter()
            .map(|parameter_type| parameter_type.clone().unwrap_or(Type::UNKNOWN))
            .collect();
        Ok(DescribeStatementResponse::new(parameter_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let fields = describe_query_fields(&portal.statement.statement, &self.catalog).await?;
        Ok(DescribePortalResponse::new(fields))
    }
}

async fn execute_query_response(
    query: &str,
    catalog: &Arc<Mutex<Catalog>>,
) -> PgWireResult<Response> {
    if query.trim().trim_matches(';').trim().is_empty() {
        return Ok(Response::Execution(Tag::new("OK")));
    }

    if let Some(metadata_batches) = metadata_batches_for_raw_query(query, catalog) {
        let batches = metadata_batches.map_err(|error| user_error("XX000", error))?;
        return match batches_to_response(batches) {
            Ok(response) => Ok(response),
            Err(error) => {
                let fallback =
                    empty_metadata_batches().map_err(|error| user_error("XX000", error))?;
                batches_to_response(fallback).map_err(|fallback_error| {
                    user_error(
                        "XX000",
                        format!(
                            "Falha ao codificar resposta de catalogo ({}; fallback: {})",
                            error, fallback_error
                        ),
                    )
                })
            }
        };
    }

    let query = query.to_string();
    let catalog = Arc::clone(catalog);
    let batches = tokio::task::spawn_blocking(move || {
        let catalog = catalog.lock().map_err(|error| error.to_string())?;
        crate::parser::execute_query(&query, &catalog)
    })
    .await
    .map_err(|error| {
        user_error(
            "XX000",
            format!("Falha ao executar consulta no worker: {}", error),
        )
    })?
    .map_err(|error| user_error("42601", error))?;

    batches_to_response(batches)
}

async fn describe_query_fields(
    query: &str,
    catalog: &Arc<Mutex<Catalog>>,
) -> PgWireResult<Vec<FieldInfo>> {
    if query.trim().trim_matches(';').trim().is_empty() {
        return Ok(Vec::new());
    }
    if let Some(metadata_batches) = metadata_batches_for_raw_query(query, catalog) {
        let batches = metadata_batches.map_err(|error| user_error("XX000", error))?;
        return match batches_to_response(batches).map_err(|error| {
            user_error(
                "XX000",
                format!("Falha ao descrever resultado de catalogo: {}", error),
            )
        })? {
            Response::Query(response) => Ok(response.row_schema.as_ref().clone()),
            _ => Ok(empty_metadata_fields()),
        };
    }

    let query = query.to_string();
    let catalog = Arc::clone(catalog);
    let batches = tokio::task::spawn_blocking(move || {
        let catalog = catalog.lock().map_err(|error| error.to_string())?;
        crate::parser::execute_query(&query, &catalog)
    })
    .await
    .map_err(|error| {
        user_error(
            "XX000",
            format!("Falha ao descrever consulta no worker: {}", error),
        )
    })?
    .map_err(|error| user_error("42601", error))?;

    match batches_to_response(batches)
        .map_err(|error| user_error("XX000", format!("Falha ao descrever schema: {}", error)))?
    {
        Response::Query(response) => Ok(response.row_schema.as_ref().clone()),
        _ => Err(user_error(
            "XX000",
            "A consulta nao produziu metadados de resultado.".to_string(),
        )),
    }
}

fn metadata_batches_for_raw_query(
    query: &str,
    catalog: &Arc<Mutex<Catalog>>,
) -> Option<Result<Vec<RecordBatch>, String>> {
    let normalized = query.to_ascii_lowercase();
    if !is_metadata_query(query) {
        return None;
    }

    let catalog = match catalog.lock() {
        Ok(catalog) => catalog,
        Err(error) => {
            return Some(metadata_fallback(&format!(
                "nao foi possivel acessar catalogo: {}",
                error
            )))
        }
    };
    let result = if normalized.contains("pg_namespace") {
        pg_namespace_batches()
    } else if normalized.contains("pg_class") {
        pg_class_batches(&catalog)
    } else if normalized.contains("pg_type") {
        pg_type_batches()
    } else {
        metadata_fallback("consulta de sistema/configuracao sem mock especifico")
    };
    Some(result)
}

fn pg_namespace_batches() -> Result<Vec<RecordBatch>, String> {
    make_metadata_record_batch(
        vec![
            Field::new("nspname", DataType::Utf8, false),
            Field::new("oid", DataType::Int32, false),
        ],
        vec![
            Arc::new(StringArray::from(vec!["public"])),
            Arc::new(Int32Array::from(vec![2200])),
        ],
    )
}

fn pg_class_batches(catalog: &Catalog) -> Result<Vec<RecordBatch>, String> {
    let mut relations = catalog
        .workspaces
        .get(&catalog.active_workspace)
        .map(|workspace| {
            workspace
                .tables
                .keys()
                .map(|name| name.clone())
                .chain(workspace.views.keys().cloned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    relations.sort();
    make_metadata_record_batch(
        vec![
            Field::new("relname", DataType::Utf8, false),
            Field::new("oid", DataType::Int32, false),
            Field::new("relnamespace", DataType::Int32, false),
            Field::new("relkind", DataType::Utf8, false),
        ],
        vec![
            Arc::new(StringArray::from(
                relations.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..relations.len())
                    .map(|index| 10_000 + index as i32)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(vec![2200; relations.len()])),
            Arc::new(StringArray::from(vec!["r"; relations.len()])),
        ],
    )
}

fn pg_type_batches() -> Result<Vec<RecordBatch>, String> {
    make_metadata_record_batch(
        vec![
            Field::new("typname", DataType::Utf8, false),
            Field::new("oid", DataType::Int32, false),
            Field::new("typbasetype", DataType::Int32, false),
            Field::new("typarray", DataType::Int32, false),
        ],
        vec![
            Arc::new(StringArray::from(vec!["text", "int4", "float8"])),
            Arc::new(Int32Array::from(vec![25, 23, 701])),
            Arc::new(Int32Array::from(vec![0, 0, 0])),
            Arc::new(Int32Array::from(vec![1009, 1007, 1022])),
        ],
    )
}

fn make_metadata_record_batch(
    fields: Vec<Field>,
    columns: Vec<ArrayRef>,
) -> Result<Vec<RecordBatch>, String> {
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|error| format!("Falha ao construir mock de catalogo PostgreSQL: {}", error))?;
    Ok(vec![batch])
}

fn metadata_fallback(reason: &str) -> Result<Vec<RecordBatch>, String> {
    eprintln!(
        "Aviso: Consulta de metadados respondida com resultado vazio seguro: {}",
        reason
    );
    empty_metadata_batches()
}

fn user_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        message,
    )))
}

fn empty_metadata_fields() -> Vec<FieldInfo> {
    vec![FieldInfo::new(
        "dummy_col".to_string(),
        None,
        None,
        Type::TEXT,
        FieldFormat::Text,
    )]
}

fn empty_metadata_batches() -> Result<Vec<RecordBatch>, String> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dummy_col",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef],
    )
    .map_err(|error| format!("Falha ao construir resultado vazio de catalogo: {}", error))?;
    Ok(vec![batch])
}

fn is_metadata_query(query: &str) -> bool {
    let query = query.to_ascii_lowercase();
    query.contains("pg_")
        || query.contains("information_schema")
        || ["show", "set", "begin", "commit", "discard"]
            .iter()
            .any(|keyword| {
                query
                    .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                    .any(|token| token == *keyword)
            })
}

struct HandlerFactory {
    handler: Arc<EngineHandler>,
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
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
                    DataType::Int32 => Type::INT4,
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
                    DataType::Int32 => {
                        let value = array
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("Arrow Int32 type must use Int32Array")
                            .value(row);
                        encoder.encode_field(&Some(value))?;
                    }
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
    use super::{batches_to_response, is_metadata_query, metadata_batches_for_raw_query};
    use super::{empty_metadata_batches, execute_query_response};
    use crate::catalog::{Catalog, VirtualTable};
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use pgwire::api::results::Response;
    use pgwire::api::Type;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn complex_or_unparseable_catalog_queries_return_empty_typed_results() {
        let catalog = Arc::new(Mutex::new(Catalog::new()));
        let unsupported = "SELECT c.relname, a.attname FROM pg_catalog.pg_class c \
                           JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid";
        let malformed = "SELECT ( FROM pg_catalog.pg_class";
        let unknown_column = "SELECT no_such_column FROM pg_catalog.pg_class";
        for query in [unsupported, malformed, unknown_column] {
            let Response::Query(mut response) =
                execute_query_response(query, &catalog).await.unwrap()
            else {
                panic!("system query must return a result set");
            };
            assert_eq!(response.row_schema.len(), 4);
            assert_eq!(response.row_schema[0].name(), "relname");
            assert!(response.data_rows.next().await.is_none());
        }

        let batches = empty_metadata_batches().unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[tokio::test]
    async fn empty_or_whitespace_queries_complete_without_physical_execution() {
        let catalog = Arc::new(Mutex::new(Catalog::new()));
        for query in ["", "   \n\t ", ";; ;"] {
            assert!(matches!(
                execute_query_response(query, &catalog).await.unwrap(),
                Response::Execution(_)
            ));
        }
    }

    #[test]
    fn system_catalog_filter_includes_additional_postgres_catalogs() {
        for query in [
            "SELECT name FROM pg_settings",
            "SELECT description FROM pg_shdescription",
            "SELECT description FROM pg_catalog.pg_description",
            "SELECT rolname FROM pg_roles",
        ] {
            assert!(
                is_metadata_query(query),
                "query was not intercepted: {query}"
            );
        }
    }

    #[tokio::test]
    async fn unsupported_system_catalogs_return_empty_result_sets() {
        let catalog = Arc::new(Mutex::new(Catalog::new()));
        for query in [
            "SELECT name FROM pg_settings",
            "SELECT description FROM pg_shdescription",
            "SELECT description FROM pg_catalog.pg_description",
            "SELECT rolname FROM pg_roles",
            "SHOW search_path",
            "SET application_name = 'dbeaver'",
            "BEGIN",
            "COMMIT",
            "DISCARD ALL",
            "SELECT table_name FROM information_schema.tables",
            "SELECT attname FROM pg_catalog.pg_attribute",
        ] {
            let Response::Query(mut response) =
                execute_query_response(query, &catalog).await.unwrap()
            else {
                panic!("system catalog fallback must return a result set");
            };
            assert_eq!(response.row_schema.len(), 1);
            assert_eq!(response.row_schema[0].name(), "dummy_col");
            assert_eq!(response.row_schema[0].datatype(), &Type::TEXT);
            assert!(response.data_rows.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn raw_interceptor_returns_exact_namespace_class_and_type_mocks() {
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
                    columns: vec![],
                },
            );
        catalog.workspaces.get_mut("default").unwrap().views.insert(
            "rainfall_view".to_string(),
            "SELECT * FROM rainfall".to_string(),
        );
        let catalog = Arc::new(Mutex::new(catalog));

        let namespace =
            metadata_batches_for_raw_query("garbage SQL with pg_namespace!!!", &catalog)
                .unwrap()
                .unwrap();
        assert_eq!(
            namespace[0]
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.data_type()))
                .collect::<Vec<_>>(),
            vec![("nspname", &DataType::Utf8), ("oid", &DataType::Int32)]
        );
        assert_eq!(
            namespace[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "public"
        );
        assert_eq!(
            namespace[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            2200
        );

        let classes = metadata_batches_for_raw_query("bad JOIN query pg_class", &catalog)
            .unwrap()
            .unwrap();
        assert_eq!(classes[0].num_columns(), 4);
        let names = classes[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.len(), 2);
        assert!(names.value(0) == "rainfall" || names.value(1) == "rainfall");
        let oids = classes[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(oids.value(0), 10_000);

        let types = metadata_batches_for_raw_query("nonsense pg_type query", &catalog)
            .unwrap()
            .unwrap();
        assert_eq!(types[0].num_rows(), 3);
        assert_eq!(types[0].num_columns(), 4);
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
