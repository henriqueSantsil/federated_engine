use arrow::csv::WriterBuilder;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn export_to_csv(batches: &[RecordBatch], path: &str) {
    if let Ok(file) = File::create(path) {
        let mut writer = WriterBuilder::new()
            .with_header(true)
            .with_delimiter(b',')
            .build(file);
        let mut count = 0;
        for batch in batches {
            if writer.write(batch).is_ok() {
                count += batch.num_rows();
            }
        }
        if !path.contains(".temp_net_") {
            println!(
                "\x1B[1;32mSucesso:\x1B[0m {} linhas exportadas para '{}'.",
                count, path
            );
        }
    } else {
        if !path.contains(".temp_net_") {
            println!("\x1B[1;31mErro:\x1B[0m Nao foi possivel criar o arquivo destino.");
        }
    }
}

pub fn export_to_parquet(batches: &[RecordBatch], path: &str) -> Result<usize, String> {
    let first_batch = batches
        .first()
        .ok_or_else(|| "Nao ha esquema para exportar o arquivo Parquet.".to_string())?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("Falha ao gerar nome temporario para Parquet: {}", error))?
        .as_nanos();
    let temporary_path = format!("{}.tmp-{}-{}", path, std::process::id(), nonce);
    let result = (|| {
        let file = File::create(&temporary_path)
            .map_err(|error| format!("Nao foi possivel criar arquivo Parquet: {}", error))?;
        let mut writer = ArrowWriter::try_new(file, first_batch.schema(), None)
            .map_err(|error| format!("Nao foi possivel iniciar o escritor Parquet: {}", error))?;
        let mut row_count = 0;
        for batch in batches {
            writer.write(batch).map_err(|error| {
                format!("Falha ao escrever dados no arquivo Parquet: {}", error)
            })?;
            row_count += batch.num_rows();
        }
        writer
            .close()
            .map_err(|error| format!("Falha ao finalizar o arquivo Parquet: {}", error))?;
        std::fs::rename(&temporary_path, path)
            .map_err(|error| format!("Falha ao publicar arquivo Parquet: {}", error))?;
        Ok(row_count)
    })();

    if result.is_err() {
        if let Err(error) = std::fs::remove_file(&temporary_path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "Falha ao remover arquivo Parquet temporario '{}': {}",
                    temporary_path, error
                );
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::export_to_parquet;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parquet_export_creates_a_readable_parquet_file() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "federated-engine-export-{}-{}.parquet",
            std::process::id(),
            nonce
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "year",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![2023, 2024]))])
                .unwrap();

        assert_eq!(
            export_to_parquet(&[batch], path.to_str().unwrap()).unwrap(),
            2
        );
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(batches[0].schema().field(0).name(), "year");

        std::fs::remove_file(path).unwrap();
    }
}
