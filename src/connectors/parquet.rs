use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

pub fn infer_schema(path: &str) -> Result<Schema, String> {
    let file = File::open(path)
        .map_err(|error| format!("Falha ao abrir arquivo Parquet '{}': {}", path, error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| format!("Falha ao ler metadados Parquet '{}': {}", path, error))?;
    Ok(builder.schema().as_ref().clone())
}
