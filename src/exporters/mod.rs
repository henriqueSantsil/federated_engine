use arrow::csv::WriterBuilder;
use arrow::record_batch::RecordBatch;
use std::fs::File;

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

pub fn export_to_parquet(batches: &[RecordBatch], path: &str) {
    println!("\x1B[1;33mAviso:\x1B[0m Exportacao nativa Parquet nao habilitada. Salvando como CSV alternativo...");
    export_to_csv(batches, path);
}
