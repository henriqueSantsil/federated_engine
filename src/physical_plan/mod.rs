use crate::catalog::Catalog;
use crate::parser::{AggregateNode, CaseWhenNode, ExtractNode, FilterNode, JoinInfo, OrderByNode};
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt32Array};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::{
    and, cast, concat_batches, filter_record_batch, or, sort_to_indices, take, SortOptions,
};
use arrow::csv::reader::{Format, ReaderBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Seek};
use std::sync::Arc;

fn detect_delimiter(path: &str) -> u8 {
    if let Ok(file) = File::open(path) {
        let mut reader = BufReader::new(file);
        let mut first_line = String::new();
        if reader.read_line(&mut first_line).is_ok() {
            let comma_count = first_line.matches(',').count();
            let semi_count = first_line.matches(';').count();
            if semi_count > comma_count {
                return b';';
            }
        }
    }
    b','
}

fn detect_delimiter_from_content(content: &str) -> u8 {
    let first_line = content.lines().next().unwrap_or_default();
    if first_line.matches(';').count() > first_line.matches(',').count() {
        b';'
    } else {
        b','
    }
}

fn is_p2p_query_endpoint(source_url: &str) -> bool {
    source_url
        .split(['?', '#'])
        .next()
        .unwrap_or_default()
        .trim_end_matches('/')
        .ends_with("/query")
}

fn sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn filter_to_sql(filter: &FilterNode) -> String {
    match filter {
        FilterNode::Condition {
            column,
            operator,
            value,
        } => {
            format!(
                "{} {} {}",
                sql_identifier(column),
                operator,
                sql_string(value)
            )
        }
        FilterNode::Like {
            column,
            pattern,
            negated,
        } => format!(
            "{} {}LIKE {}",
            sql_identifier(column),
            if *negated { "NOT " } else { "" },
            sql_string(pattern)
        ),
        FilterNode::InList {
            column,
            values,
            negated,
        } => format!(
            "{} {}IN ({})",
            sql_identifier(column),
            if *negated { "NOT " } else { "" },
            values
                .iter()
                .map(|value| sql_string(value))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        FilterNode::And(left, right) => {
            format!("({}) AND ({})", filter_to_sql(left), filter_to_sql(right))
        }
        FilterNode::Or(left, right) => {
            format!("({}) OR ({})", filter_to_sql(left), filter_to_sql(right))
        }
    }
}

fn read_csv_batches<R: Read + Seek>(
    mut file: R,
    delimiter: u8,
    filter_tree: &Option<FilterNode>,
) -> Result<Vec<RecordBatch>, String> {
    let format = Format::default()
        .with_header(true)
        .with_delimiter(delimiter);
    let (schema, _) = format
        .infer_schema(&mut file, Some(100))
        .map_err(|e| format!("Erro de inferencia: {}", e))?;
    file.rewind()
        .map_err(|e| format!("Erro ao reposicionar CSV: {}", e))?;

    let builder = ReaderBuilder::new(Arc::new(schema))
        .with_header(true)
        .with_delimiter(delimiter);
    let csv_reader = builder
        .build(file)
        .map_err(|e| format!("Erro Arrow: {}", e))?;
    let mut batches = Vec::new();

    for batch_result in csv_reader {
        let mut batch = batch_result.map_err(|e| format!("Erro ao ler CSV: {}", e))?;
        if let Some(root_node) = filter_tree {
            let boolean_mask = evaluate_node(root_node, &batch)
                .map_err(|e| format!("Erro na filtragem: {}", e))?;
            batch = filter_record_batch(&batch, &boolean_mask)
                .map_err(|e| format!("Erro ao aplicar filtro: {}", e))?;
        }
        batches.push(batch);
    }

    Ok(batches)
}

fn get_key_as_string(array: &Arc<dyn Array>, row_idx: usize) -> String {
    if array.is_null(row_idx) {
        return "NULL".to_string();
    }
    match array.data_type() {
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row_idx)
            .to_string(),
        _ => "UNSUPPORTED".to_string(),
    }
}

fn evaluate_node(node: &FilterNode, batch: &RecordBatch) -> Result<BooleanArray, String> {
    match node {
        FilterNode::Condition {
            column,
            operator,
            value,
        } => {
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|_| format!("Coluna '{}' nao encontrada.", column))?;
            let mut array = batch.column(col_idx).clone();

            if matches!(
                array.data_type(),
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
            ) {
                if let Ok(casted) = cast(&array, &DataType::Utf8) {
                    array = casted;
                }
            }

            let mask = match array.data_type() {
                DataType::Int64 => {
                    let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    let val = value
                        .parse::<i64>()
                        .map_err(|_| format!("'{}' nao e inteiro.", value))?;
                    let val_array = Int64Array::from(vec![val; int_array.len()]);
                    match operator.as_str() {
                        "=" => eq(int_array, &val_array).unwrap(),
                        ">" => gt(int_array, &val_array).unwrap(),
                        "<" => lt(int_array, &val_array).unwrap(),
                        ">=" => gt_eq(int_array, &val_array).unwrap(),
                        "<=" => lt_eq(int_array, &val_array).unwrap(),
                        "!=" => neq(int_array, &val_array).unwrap(),
                        _ => return Err(format!("Operador '{}' nao suportado.", operator)),
                    }
                }
                DataType::Float64 => {
                    let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    let val = value
                        .parse::<f64>()
                        .map_err(|_| format!("'{}' nao e decimal.", value))?;
                    let val_array = Float64Array::from(vec![val; float_array.len()]);
                    match operator.as_str() {
                        "=" => eq(float_array, &val_array).unwrap(),
                        ">" => gt(float_array, &val_array).unwrap(),
                        "<" => lt(float_array, &val_array).unwrap(),
                        ">=" => gt_eq(float_array, &val_array).unwrap(),
                        "<=" => lt_eq(float_array, &val_array).unwrap(),
                        "!=" => neq(float_array, &val_array).unwrap(),
                        _ => return Err(format!("Operador '{}' nao suportado.", operator)),
                    }
                }
                DataType::Utf8 => {
                    let str_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    let val_array = StringArray::from(vec![value.as_str(); str_array.len()]);
                    match operator.as_str() {
                        "=" => eq(str_array, &val_array).unwrap(),
                        "!=" => neq(str_array, &val_array).unwrap(),
                        ">" => gt(str_array, &val_array).unwrap(),
                        "<" => lt(str_array, &val_array).unwrap(),
                        ">=" => gt_eq(str_array, &val_array).unwrap(),
                        "<=" => lt_eq(str_array, &val_array).unwrap(),
                        _ => {
                            return Err(format!(
                                "Operador '{}' nao suportado para textos.",
                                operator
                            ))
                        }
                    }
                }
                _ => {
                    return Err(format!(
                        "Tipo de dado nao suportado para a coluna '{}'.",
                        column
                    ))
                }
            };
            Ok(mask)
        }
        FilterNode::InList {
            column,
            values,
            negated,
        } => {
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|_| format!("Coluna '{}' nao encontrada.", column))?;
            let array = batch.column(col_idx);
            let str_array = cast(array, &DataType::Utf8).unwrap_or_else(|_| array.clone());
            let mut bools = Vec::with_capacity(str_array.len());
            for i in 0..str_array.len() {
                if str_array.is_null(i) {
                    bools.push(None);
                } else {
                    let val_str = get_key_as_string(&str_array, i);
                    let mut found = values.contains(&val_str);
                    if *negated {
                        found = !found;
                    }
                    bools.push(Some(found));
                }
            }
            Ok(BooleanArray::from(bools))
        }
        FilterNode::Like {
            column,
            pattern,
            negated,
        } => {
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|_| format!("Coluna '{}' nao encontrada.", column))?;
            let array = batch.column(col_idx);
            let str_array = cast(array, &DataType::Utf8).unwrap_or_else(|_| array.clone());
            let mut bools = Vec::with_capacity(str_array.len());
            let starts_with = pattern.ends_with('%') && !pattern.starts_with('%');
            let ends_with = pattern.starts_with('%') && !pattern.ends_with('%');
            let contains = pattern.starts_with('%') && pattern.ends_with('%');
            let clean_pat = pattern.replace('%', "");

            for i in 0..str_array.len() {
                if str_array.is_null(i) {
                    bools.push(None);
                } else {
                    let val_str = get_key_as_string(&str_array, i);
                    let mut matches = if contains {
                        val_str.contains(&clean_pat)
                    } else if starts_with {
                        val_str.starts_with(&clean_pat)
                    } else if ends_with {
                        val_str.ends_with(&clean_pat)
                    } else {
                        val_str == clean_pat
                    };
                    if *negated {
                        matches = !matches;
                    }
                    bools.push(Some(matches));
                }
            }
            Ok(BooleanArray::from(bools))
        }
        FilterNode::And(left, right) => {
            let left_mask = evaluate_node(left, batch)?;
            let right_mask = evaluate_node(right, batch)?;
            Ok(and(&left_mask, &right_mask).unwrap())
        }
        FilterNode::Or(left, right) => {
            let left_mask = evaluate_node(left, batch)?;
            let right_mask = evaluate_node(right, batch)?;
            Ok(or(&left_mask, &right_mask).unwrap())
        }
    }
}

fn fetch_and_filter_batches(
    table_name: &str,
    filter_tree: &Option<FilterNode>,
    join_info: &Option<JoinInfo>,
    catalog: &Catalog,
    current_ws: &str,
) -> Result<Vec<RecordBatch>, String> {
    let table = catalog
        .get_table_qualified(table_name, current_ws)
        .ok_or_else(|| format!("Tabela '{}' nao existe.", table_name))?;

    let use_remote_source = table
        .source_url
        .as_deref()
        .map(is_p2p_query_endpoint)
        .unwrap_or(false)
        && (filter_tree.is_some() || !std::path::Path::new(&table.physical_path).exists());
    let batches =
        if let Some(source_url) = table.source_url.as_deref().filter(|_| use_remote_source) {
            let mut request = ureq::get(source_url);
            if let Some(filter) = filter_tree {
                request = request.query("filter", &filter_to_sql(filter));
            }
            let response = request
                .call()
                .map_err(|e| format!("Erro ao consultar a origem HTTP remota: {}", e))?;
            let content = response
                .into_string()
                .map_err(|e| format!("Erro ao ler os dados retornados pelo nó remoto: {}", e))?;
            let delimiter = detect_delimiter_from_content(&content);
            read_csv_batches(Cursor::new(content.into_bytes()), delimiter, filter_tree)?
        } else {
            let file = File::open(&table.physical_path)
                .map_err(|e| format!("Erro ao abrir arquivo: {}", e))?;
            read_csv_batches(file, detect_delimiter(&table.physical_path), filter_tree)?
        };

    if let Some(join) = join_info {
        if let Some(right_table_def) = catalog.get_table_qualified(&join.right_table, current_ws) {
            if let Ok(mut right_file) = File::open(&right_table_def.physical_path) {
                let right_delim = detect_delimiter(&right_table_def.physical_path);
                let right_format = Format::default()
                    .with_header(true)
                    .with_delimiter(right_delim);

                if let Ok((right_schema, _)) = right_format.infer_schema(&mut right_file, Some(100))
                {
                    let _ = right_file.rewind();
                    let right_builder = ReaderBuilder::new(Arc::new(right_schema))
                        .with_header(true)
                        .with_delimiter(right_delim);

                    if let Ok(right_reader) = right_builder.build(right_file) {
                        let mut right_batches_raw = Vec::new();
                        for rb in right_reader {
                            if let Ok(mut b) = rb {
                                if let Some(ref r_filter) = join.right_filter {
                                    match evaluate_node(r_filter, &b) {
                                        Ok(boolean_mask) => {
                                            if let Ok(filtered) =
                                                filter_record_batch(&b, &boolean_mask)
                                            {
                                                b = filtered;
                                            }
                                        }
                                        Err(e) => {
                                            return Err(format!(
                                                "Erro na filtragem da View (JOIN): {}",
                                                e
                                            ))
                                        }
                                    }
                                }
                                if b.num_rows() > 0 {
                                    right_batches_raw.push(b);
                                }
                            }
                        }

                        if !right_batches_raw.is_empty() {
                            let right_schema_ref = right_batches_raw[0].schema();
                            if let Ok(right_batch) =
                                concat_batches(&right_schema_ref, &right_batches_raw)
                            {
                                if let Ok(right_col_idx) =
                                    right_batch.schema().index_of(&join.right_column)
                                {
                                    let right_col = right_batch.column(right_col_idx);
                                    let right_str_col = cast(right_col, &DataType::Utf8)
                                        .unwrap_or_else(|_| right_col.clone());
                                    let mut right_map = HashMap::new();

                                    for i in 0..right_batch.num_rows() {
                                        let key = get_key_as_string(&right_str_col, i);
                                        right_map.insert(key, i as u32);
                                    }

                                    let mut joined_batches = Vec::new();
                                    for left_batch in batches {
                                        if let Ok(left_col_idx) =
                                            left_batch.schema().index_of(&join.left_column)
                                        {
                                            let left_col = left_batch.column(left_col_idx);
                                            let left_str_col = cast(left_col, &DataType::Utf8)
                                                .unwrap_or_else(|_| left_col.clone());
                                            let mut left_indices = Vec::new();
                                            let mut right_indices = Vec::new();

                                            for i in 0..left_batch.num_rows() {
                                                let key = get_key_as_string(&left_str_col, i);
                                                if let Some(&r_idx) = right_map.get(&key) {
                                                    left_indices.push(i as u32);
                                                    right_indices.push(r_idx);
                                                }
                                            }

                                            if left_indices.is_empty() {
                                                continue;
                                            }

                                            let left_take = UInt32Array::from(left_indices);
                                            let right_take = UInt32Array::from(right_indices);

                                            let mut new_columns = Vec::new();
                                            let mut new_fields: Vec<Arc<Field>> = Vec::new();

                                            for (i, field) in
                                                left_batch.schema().fields().iter().enumerate()
                                            {
                                                new_fields.push(field.clone());
                                                new_columns.push(
                                                    take(left_batch.column(i), &left_take, None)
                                                        .unwrap(),
                                                );
                                            }

                                            for (i, field) in
                                                right_batch.schema().fields().iter().enumerate()
                                            {
                                                if field.name() == &join.right_column {
                                                    continue;
                                                }
                                                let new_name = if left_batch
                                                    .schema()
                                                    .field_with_name(field.name())
                                                    .is_ok()
                                                {
                                                    format!(
                                                        "{}_{}",
                                                        field.name(),
                                                        join.right_table
                                                            .split('.')
                                                            .last()
                                                            .unwrap_or(&join.right_table)
                                                    )
                                                } else {
                                                    field.name().clone()
                                                };

                                                let new_field = Field::new(
                                                    &new_name,
                                                    field.data_type().clone(),
                                                    field.is_nullable(),
                                                );
                                                new_fields.push(Arc::new(new_field));
                                                new_columns.push(
                                                    take(right_batch.column(i), &right_take, None)
                                                        .unwrap(),
                                                );
                                            }

                                            if let Ok(joined_batch) = RecordBatch::try_new(
                                                Arc::new(Schema::new(new_fields)),
                                                new_columns,
                                            ) {
                                                joined_batches.push(joined_batch);
                                            }
                                        }
                                    }
                                    return Ok(joined_batches);
                                }
                            }
                        }
                    }
                }
            } else {
                return Err(format!(
                    "JOIN ignorado. A tabela base da direita '{}' nao existe.",
                    join.right_table
                ));
            }
        }
    }
    Ok(batches)
}

pub fn execute_subquery_for_list(
    table_name: &str,
    projection: &Vec<(String, Option<String>)>,
    filter_tree: &Option<FilterNode>,
    join_info: &Option<JoinInfo>,
    catalog: &Catalog,
    current_ws: &str,
) -> Result<Vec<String>, String> {
    let batches =
        fetch_and_filter_batches(table_name, filter_tree, join_info, catalog, current_ws)?;
    let mut results = Vec::new();
    if projection.is_empty() {
        return Ok(results);
    }
    let target_col = &projection[0].0;

    for batch in batches {
        if let Ok(col_idx) = batch.schema().index_of(target_col) {
            let array = batch.column(col_idx);
            let str_array = cast(array, &DataType::Utf8).unwrap_or_else(|_| array.clone());
            for i in 0..batch.num_rows() {
                if !str_array.is_null(i) {
                    results.push(get_key_as_string(&str_array, i));
                }
            }
        }
    }

    results.sort();
    results.dedup();
    Ok(results)
}

fn print_custom_table(batches: &[RecordBatch]) {
    if batches.is_empty() {
        println!("(Nenhum dado encontrado)");
        return;
    }
    let schema = batches[0].schema();
    let max_col_width = 30;

    let headers: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            let mut name = f.name().clone();
            if name.len() > max_col_width {
                name.truncate(max_col_width - 3);
                name.push_str("...");
            }
            name
        })
        .collect();

    let mut all_rows = Vec::new();
    for batch in batches {
        let mut string_columns = Vec::new();
        for col_idx in 0..batch.num_columns() {
            let array = batch.column(col_idx);
            let casted = cast(array, &DataType::Utf8)
                .unwrap_or_else(|_| Arc::new(StringArray::from(vec!["?"; batch.num_rows()])));
            string_columns.push(casted);
        }

        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let array = &string_columns[col_idx];
                let mut text = if array.is_null(row_idx) {
                    "NULL".to_string()
                } else {
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row_idx)
                        .to_string()
                };
                if text.len() > max_col_width {
                    text.truncate(max_col_width - 3);
                    text.push_str("...");
                }
                row.push(text);
            }
            all_rows.push(row);
        }
    }

    let mut widths = vec![0; headers.len()];
    for (i, h) in headers.iter().enumerate() {
        widths[i] = h.len();
    }
    for row in &all_rows {
        for (i, cell) in row.iter().enumerate() {
            if cell.len() > widths[i] {
                widths[i] = cell.len();
            }
        }
    }

    let max_line_width = 120;
    let mut chunks = Vec::new();
    let mut current_chunk = Vec::new();
    let mut current_width = 0;

    for (i, w) in widths.iter().enumerate() {
        let col_display_width = w + 3;
        if !current_chunk.is_empty() && current_width + col_display_width > max_line_width {
            chunks.push(current_chunk);
            current_chunk = Vec::new();
            current_width = 0;
        }
        current_chunk.push(i);
        current_width += col_display_width;
    }
    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    println!();
    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        if chunk_idx > 0 {
            println!(
                "\n... continuacao (colunas {} a {}):",
                chunk[0] + 1,
                chunk.last().unwrap() + 1
            );
        }
        for &i in chunk {
            print!("{:<width$} | ", headers[i], width = widths[i]);
        }
        println!();
        for &i in chunk {
            print!("{:-<width$}-+-", "", width = widths[i]);
        }
        println!();
        for row in &all_rows {
            for &i in chunk {
                print!("{:<width$} | ", row[i], width = widths[i]);
            }
            println!();
        }
    }
    println!();
}

pub fn execute_select(
    table_name: &str,
    projection: Vec<(String, Option<String>)>,
    filter_tree: Option<FilterNode>,
    join_info: Option<JoinInfo>,
    limit: Option<usize>,
    group_by: Vec<String>,
    aggregates: Vec<AggregateNode>,
    order_by_info: Option<OrderByNode>,
    cases: Vec<CaseWhenNode>,
    has_wildcard: bool,
    extracts: Vec<ExtractNode>,
    catalog: &Catalog,
    export_path: Option<String>,
    current_ws: &str,
) {
    let mut batches =
        match fetch_and_filter_batches(table_name, &filter_tree, &join_info, catalog, current_ws) {
            Ok(b) => b,
            Err(e) => {
                println!("\x1B[1;31mErro de Execucao:\x1B[0m {}", e);
                return;
            }
        };

    if !extracts.is_empty() {
        let mut new_batches = Vec::new();
        for batch in batches {
            let mut new_fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
            let mut new_columns: Vec<Arc<dyn Array>> = batch.columns().iter().cloned().collect();

            for ext in &extracts {
                if let Ok(col_idx) = batch.schema().index_of(&ext.column) {
                    let array = batch.column(col_idx);
                    let str_array = cast(array, &DataType::Utf8).unwrap_or_else(|_| array.clone());
                    let mut extracted_vals = Vec::with_capacity(batch.num_rows());

                    for i in 0..batch.num_rows() {
                        if str_array.is_null(i) {
                            extracted_vals.push(None);
                        } else {
                            let val_str = get_key_as_string(&str_array, i);
                            let clean_str = val_str.split(' ').next().unwrap_or("");
                            let parts: Vec<&str> =
                                clean_str.split(|c| c == '-' || c == '/').collect();

                            let mut res = None;
                            if parts.len() >= 3 {
                                match ext.field.as_str() {
                                    "YEAR" => res = parts[0].parse::<i64>().ok(),
                                    "MONTH" => res = parts[1].parse::<i64>().ok(),
                                    "DAY" => res = parts[2].parse::<i64>().ok(),
                                    _ => {}
                                }
                            }
                            extracted_vals.push(res);
                        }
                    }

                    let ext_col_name = ext.alias.as_ref().unwrap();
                    new_fields.push(Arc::new(Field::new(ext_col_name, DataType::Int64, true)));
                    new_columns.push(Arc::new(Int64Array::from(extracted_vals)) as Arc<dyn Array>);
                } else {
                    println!(
                        "\x1B[1;31mErro:\x1B[0m Coluna de data '{}' nao encontrada.",
                        ext.column
                    );
                    return;
                }
            }
            if let Ok(new_batch) =
                RecordBatch::try_new(Arc::new(Schema::new(new_fields)), new_columns)
            {
                new_batches.push(new_batch);
            }
        }
        batches = new_batches;
    }

    if !group_by.is_empty() || !aggregates.is_empty() {
        if !batches.is_empty() {
            let current_schema = batches[0].schema();
            if let Ok(single_batch) = concat_batches(&current_schema, &batches) {
                let mut groups: HashMap<String, Vec<u32>> = HashMap::new();
                let num_rows = single_batch.num_rows();

                let mut group_arrays = Vec::new();
                for col_name in &group_by {
                    if let Ok(idx) = current_schema.index_of(col_name) {
                        let array = single_batch.column(idx);
                        let str_array =
                            cast(array, &DataType::Utf8).unwrap_or_else(|_| array.clone());
                        group_arrays.push(str_array);
                    } else {
                        println!(
                            "\x1B[1;31mErro:\x1B[0m Coluna de agrupamento '{}' nao encontrada.",
                            col_name
                        );
                        return;
                    }
                }

                for row_idx in 0..num_rows {
                    let mut key_parts = Vec::new();
                    for arr in &group_arrays {
                        key_parts.push(get_key_as_string(arr, row_idx));
                    }
                    let key = key_parts.join("|");
                    groups
                        .entry(key)
                        .or_insert_with(Vec::new)
                        .push(row_idx as u32);
                }

                let mut first_indices = Vec::new();
                for indices in groups.values() {
                    first_indices.push(indices[0]);
                }
                let take_indices = UInt32Array::from(first_indices);

                let mut new_fields: Vec<Arc<Field>> = Vec::new();
                let mut new_columns = Vec::new();

                for col_name in &group_by {
                    let idx = current_schema.index_of(col_name).unwrap();
                    let original_field = current_schema.field(idx);

                    let mut final_col_name = col_name.clone();
                    for (p_col, p_alias) in &projection {
                        if p_col == col_name {
                            if let Some(a) = p_alias {
                                final_col_name = a.clone();
                            }
                            break;
                        }
                    }

                    new_fields.push(Arc::new(Field::new(
                        &final_col_name,
                        original_field.data_type().clone(),
                        original_field.is_nullable(),
                    )));
                    new_columns.push(take(single_batch.column(idx), &take_indices, None).unwrap());
                }

                for agg in &aggregates {
                    let agg_name = agg
                        .alias
                        .clone()
                        .unwrap_or_else(|| format!("{}({})", agg.func, agg.column));

                    if agg.func == "COUNT" {
                        let mut counts = Vec::new();
                        for indices in groups.values() {
                            counts.push(indices.len() as i64);
                        }
                        new_fields.push(Arc::new(Field::new(&agg_name, DataType::Int64, true)));
                        new_columns.push(Arc::new(Int64Array::from(counts)) as Arc<dyn Array>);
                    } else {
                        if let Ok(col_idx) = current_schema.index_of(&agg.column) {
                            let array = single_batch.column(col_idx);
                            let mut results = Vec::new();
                            for indices in groups.values() {
                                let mut sum = 0.0;
                                let mut max = f64::MIN;
                                let mut min = f64::MAX;
                                let mut count = 0.0;
                                for &row_idx in indices {
                                    if let Ok(v) =
                                        get_key_as_string(array, row_idx as usize).parse::<f64>()
                                    {
                                        sum += v;
                                        if v > max {
                                            max = v;
                                        }
                                        if v < min {
                                            min = v;
                                        }
                                        count += 1.0;
                                    }
                                }
                                let res = match agg.func.as_str() {
                                    "SUM" => sum,
                                    "AVG" => {
                                        if count > 0.0 {
                                            sum / count
                                        } else {
                                            0.0
                                        }
                                    }
                                    "MAX" => {
                                        if count > 0.0 {
                                            max
                                        } else {
                                            0.0
                                        }
                                    }
                                    "MIN" => {
                                        if count > 0.0 {
                                            min
                                        } else {
                                            0.0
                                        }
                                    }
                                    _ => 0.0,
                                };
                                results.push(res);
                            }
                            new_fields.push(Arc::new(Field::new(
                                &agg_name,
                                DataType::Float64,
                                true,
                            )));
                            new_columns
                                .push(Arc::new(Float64Array::from(results)) as Arc<dyn Array>);
                        } else {
                            println!(
                                "\x1B[1;31mErro:\x1B[0m Coluna numerica '{}' nao encontrada.",
                                agg.column
                            );
                            return;
                        }
                    }
                }

                if let Ok(agg_batch) =
                    RecordBatch::try_new(Arc::new(Schema::new(new_fields)), new_columns)
                {
                    batches = vec![agg_batch];
                }
            }
        } else {
            batches = vec![];
        }
    }

    let mut projected_batches = Vec::new();
    if group_by.is_empty() && aggregates.is_empty() {
        for batch in batches {
            if !projection.is_empty() || !cases.is_empty() || has_wildcard {
                let mut new_fields: Vec<Arc<Field>> = Vec::new();
                let mut new_columns = Vec::new();
                let schema = batch.schema();

                if has_wildcard {
                    for i in 0..schema.fields().len() {
                        new_fields.push(Arc::new(schema.field(i).clone()));
                        new_columns.push(batch.column(i).clone());
                    }
                } else {
                    for (col_name, alias) in &projection {
                        if let Ok(idx) = schema.index_of(col_name) {
                            let original_field = schema.field(idx);
                            let final_name = alias.clone().unwrap_or_else(|| col_name.clone());
                            new_fields.push(Arc::new(Field::new(
                                &final_name,
                                original_field.data_type().clone(),
                                original_field.is_nullable(),
                            )));
                            new_columns.push(batch.column(idx).clone());
                        } else {
                            println!(
                                "\x1B[1;31mErro:\x1B[0m Coluna '{}' nao existe no esquema.",
                                col_name
                            );
                            return;
                        }
                    }
                }

                for case in &cases {
                    let mask = match evaluate_node(&case.condition, &batch) {
                        Ok(m) => m,
                        Err(e) => {
                            println!("\x1B[1;31mErro no CASE WHEN:\x1B[0m {}", e);
                            return;
                        }
                    };
                    let mut case_results = Vec::with_capacity(batch.num_rows());
                    for i in 0..batch.num_rows() {
                        if mask.is_null(i) || !mask.value(i) {
                            case_results.push(case.else_result.as_str());
                        } else {
                            case_results.push(case.then_result.as_str());
                        }
                    }
                    let case_col_name = case
                        .alias
                        .clone()
                        .unwrap_or_else(|| "case_result".to_string());
                    new_fields.push(Arc::new(Field::new(&case_col_name, DataType::Utf8, true)));
                    new_columns.push(Arc::new(StringArray::from(case_results)) as Arc<dyn Array>);
                }

                if let Ok(projected) =
                    RecordBatch::try_new(Arc::new(Schema::new(new_fields)), new_columns)
                {
                    projected_batches.push(projected);
                }
            } else {
                projected_batches.push(batch);
            }
        }
    } else {
        projected_batches = batches;
    }
    let mut batches = projected_batches;

    if let Some(order) = order_by_info {
        if !batches.is_empty() {
            let current_schema = batches[0].schema();
            if let Ok(single_batch) = concat_batches(&current_schema, &batches) {
                if let Ok(col_idx) = current_schema.index_of(&order.column) {
                    let sort_opts = SortOptions {
                        descending: !order.asc,
                        nulls_first: false,
                    };
                    if let Ok(indices) =
                        sort_to_indices(single_batch.column(col_idx), Some(sort_opts), None)
                    {
                        let mut sorted_columns = Vec::new();
                        for i in 0..single_batch.num_columns() {
                            sorted_columns
                                .push(take(single_batch.column(i), &indices, None).unwrap());
                        }
                        if let Ok(sorted_batch) =
                            RecordBatch::try_new(current_schema, sorted_columns)
                        {
                            batches = vec![sorted_batch];
                        }
                    } else {
                        println!(
                            "\x1B[1;31mErro:\x1B[0m Falha ao ordenar os dados pela coluna '{}'.",
                            order.column
                        );
                        return;
                    }
                } else {
                    println!("\x1B[1;31mErro:\x1B[0m Coluna de ordenacao '{}' nao encontrada no resultado.", order.column);
                    return;
                }
            }
        }
    }

    let max_rows = limit.unwrap_or(if export_path.is_some() {
        usize::MAX
    } else {
        15
    });
    let mut limited_batches = Vec::new();
    let mut counted = 0;
    for batch in batches {
        if counted >= max_rows {
            break;
        }
        if counted + batch.num_rows() > max_rows {
            limited_batches.push(batch.slice(0, max_rows - counted));
            break;
        }
        counted += batch.num_rows();
        limited_batches.push(batch);
    }
    let final_batches = limited_batches;

    if let Some(path) = export_path {
        let is_silent = path.contains(".temp_net_");
        let lower_path = path.to_lowercase();

        if lower_path.ends_with(".csv") {
            if !is_silent {
                println!("Exportador: Gerando arquivo CSV -> {}", path);
            }
            crate::exporters::export_to_csv(&final_batches, &path);
        } else if lower_path.ends_with(".parquet") {
            if !is_silent {
                println!("Exportador: Gerando arquivo Parquet -> {}", path);
            }
            crate::exporters::export_to_parquet(&final_batches, &path);
        } else {
            if !is_silent {
                println!("\x1B[1;31mErro:\x1B[0m Formato de exportacao nao suportado.");
            }
        }
    } else {
        println!(
            "\x1B[1;32mSucesso:\x1B[0m Processamento concluido. Exibindo (Max {} linhas):",
            max_rows
        );
        print_custom_table(&final_batches);
    }
}

#[cfg(test)]
mod tests {
    use super::filter_to_sql;
    use crate::parser::FilterNode;

    #[test]
    fn renders_filter_values_safely_for_http_query_parameters() {
        let filter = FilterNode::And(
            Box::new(FilterNode::Condition {
                column: "year".to_string(),
                operator: "=".to_string(),
                value: "2024".to_string(),
            }),
            Box::new(FilterNode::Condition {
                column: "city\"name".to_string(),
                operator: "=".to_string(),
                value: "Sorriso' OR 1=1 --".to_string(),
            }),
        );

        assert_eq!(
            filter_to_sql(&filter),
            "(\"year\" = '2024') AND (\"city\"\"name\" = 'Sorriso'' OR 1=1 --')"
        );
    }
}
