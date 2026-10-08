use arrow::csv::reader::Format;
use arrow::datatypes::Schema;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek};

pub fn infer_schema(path: &str) -> Result<Schema, String> {
    let mut file = File::open(path).map_err(|e| e.to_string())?;

    let mut delim = b',';
    if let Ok(f2) = File::open(path) {
        let mut reader = BufReader::new(f2);
        let mut line = String::new();
        if reader.read_line(&mut line).is_ok() {
            if line.matches(';').count() > line.matches(',').count() {
                delim = b';';
            }
        }
    }

    let format = Format::default().with_header(true).with_delimiter(delim);
    let (schema, _) = format
        .infer_schema(&mut file, Some(100))
        .map_err(|e| e.to_string())?;
    Ok(schema)
}
