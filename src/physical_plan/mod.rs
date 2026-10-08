use crate::catalog::{Catalog, VirtualTable};
use crate::parser::{
    AggregateNode, CaseWhenNode, ExtractNode, FilterNode, JoinInfo, OrderByNode, WindowNode,
};
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt32Array};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::{
    and, cast, concat_batches, filter_record_batch, or, sort_to_indices, take, SortOptions,
};
use arrow::csv::reader::{Format, ReaderBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Seek};
use std::sync::Arc;

pub type CteTables = HashMap<String, Vec<RecordBatch>>;

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

    let schema = Arc::new(schema);
    let builder = ReaderBuilder::new(Arc::clone(&schema))
        .with_header(true)
        .with_delimiter(delimiter);
    let csv_reader = builder
        .build(file)
        .map_err(|e| format!("Erro Arrow: {}", e))?;
    let mut batches = csv_reader
        .map(|batch_result| batch_result.map_err(|e| format!("Erro ao ler CSV: {}", e)))
        .collect::<Result<Vec<_>, _>>()?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(schema));
    }

    batches
        .into_par_iter()
        .map(|batch| {
            if let Some(root_node) = filter_tree {
                let boolean_mask = evaluate_node(root_node, &batch)
                    .map_err(|e| format!("Erro na filtragem: {}", e))?;
                filter_record_batch(&batch, &boolean_mask)
                    .map_err(|e| format!("Erro ao aplicar filtro: {}", e))
            } else {
                Ok(batch)
            }
        })
        .collect()
}

fn read_parquet_batches(
    file: File,
    filter_tree: &Option<FilterNode>,
) -> Result<Vec<RecordBatch>, String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| format!("Erro ao abrir arquivo Parquet: {}", error))?;
    let schema = Arc::clone(builder.schema());
    let reader = builder
        .build()
        .map_err(|error| format!("Erro ao criar leitor Parquet: {}", error))?;
    let mut batches = reader
        .map(|result| result.map_err(|error| format!("Erro ao ler Parquet: {}", error)))
        .collect::<Result<Vec<_>, _>>()?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(schema));
    }

    batches
        .into_par_iter()
        .map(|batch| {
            if let Some(filter) = filter_tree {
                let mask = evaluate_node(filter, &batch)
                    .map_err(|error| format!("Erro na filtragem: {}", error))?;
                filter_record_batch(&batch, &mask)
                    .map_err(|error| format!("Erro ao aplicar filtro: {}", error))
            } else {
                Ok(batch)
            }
        })
        .collect()
}

fn read_table_batches(
    table: &VirtualTable,
    filter_tree: &Option<FilterNode>,
) -> Result<Vec<RecordBatch>, String> {
    let file = File::open(&table.physical_path).map_err(|error| {
        format!(
            "Erro ao abrir arquivo da tabela '{}': {}",
            table.name, error
        )
    })?;
    if table.format.eq_ignore_ascii_case("PARQUET") {
        read_parquet_batches(file, filter_tree)
    } else {
        read_csv_batches(file, detect_delimiter(&table.physical_path), filter_tree)
    }
}

pub(crate) fn get_key_as_string(array: &Arc<dyn Array>, row_idx: usize) -> String {
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

fn apply_filter_to_batches(
    batches: Vec<RecordBatch>,
    filter_tree: &Option<FilterNode>,
) -> Result<Vec<RecordBatch>, String> {
    batches
        .into_par_iter()
        .map(|batch| {
            let batch = if let Some(filter) = filter_tree {
                let mask = evaluate_node(filter, &batch)?;
                filter_record_batch(&batch, &mask)
                    .map_err(|error| format!("Erro ao filtrar CTE em memoria: {}", error))?
            } else {
                batch
            };
            Ok(batch)
        })
        .collect::<Result<Vec<_>, String>>()
}

fn fetch_and_filter_batches(
    table_name: &str,
    filter_tree: &Option<FilterNode>,
    join_info: &Option<JoinInfo>,
    catalog: &Catalog,
    current_ws: &str,
    ctes: &CteTables,
) -> Result<Vec<RecordBatch>, String> {
    let mut batches = if let Some(cte_batches) = ctes.get(&table_name.to_ascii_lowercase()) {
        apply_filter_to_batches(cte_batches.clone(), filter_tree)?
    } else {
        let table = catalog
            .get_table_qualified(table_name, current_ws)
            .ok_or_else(|| format!("Tabela '{}' nao existe.", table_name))?;
        let use_remote_source = table
            .source_url
            .as_deref()
            .map(is_p2p_query_endpoint)
            .unwrap_or(false)
            && (filter_tree.is_some() || !std::path::Path::new(&table.physical_path).exists());
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
            read_csv_batches(
                Cursor::new(content.clone().into_bytes()),
                detect_delimiter_from_content(&content),
                filter_tree,
            )?
        } else {
            read_table_batches(table, filter_tree)?
        }
    };

    if let Some(join) = join_info {
        let right_batches = if let Some(cte_batches) =
            ctes.get(&join.right_table.to_ascii_lowercase())
        {
            apply_filter_to_batches(cte_batches.clone(), &join.right_filter)?
        } else if let Some(right_table) = catalog.get_table_qualified(&join.right_table, current_ws)
        {
            read_table_batches(right_table, &join.right_filter)?
        } else {
            return Err(format!(
                "Tabela ou CTE do lado direito '{}' nao encontrada.",
                join.right_table
            ));
        };
        batches = join_record_batches(batches, right_batches, join)?;
    }
    Ok(batches)
}

fn join_record_batches(
    batches: Vec<RecordBatch>,
    right_batches: Vec<RecordBatch>,
    join: &JoinInfo,
) -> Result<Vec<RecordBatch>, String> {
    if right_batches.is_empty() {
        return Ok(Vec::new());
    }
    let right_schema = right_batches[0].schema();
    let right_batch = concat_batches(&right_schema, &right_batches)
        .map_err(|error| format!("Falha ao consolidar resultados da CTE JOIN: {}", error))?;
    let right_col_idx = right_batch
        .schema()
        .index_of(&join.right_column)
        .map_err(|_| format!("Coluna '{}' nao encontrada na CTE JOIN.", join.right_column))?;
    let right_col = right_batch.column(right_col_idx);
    let right_str_col = cast(right_col, &DataType::Utf8).unwrap_or_else(|_| right_col.clone());
    let mut right_map = HashMap::new();
    for i in 0..right_batch.num_rows() {
        right_map.insert(get_key_as_string(&right_str_col, i), i as u32);
    }

    let mut joined_batches = Vec::new();
    for left_batch in batches {
        let left_col_idx = left_batch
            .schema()
            .index_of(&join.left_column)
            .map_err(|_| {
                format!(
                    "Coluna '{}' nao encontrada na tabela base do JOIN.",
                    join.left_column
                )
            })?;
        let left_col = left_batch.column(left_col_idx);
        let left_str_col = cast(left_col, &DataType::Utf8).unwrap_or_else(|_| left_col.clone());
        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();
        for i in 0..left_batch.num_rows() {
            if let Some(&right_index) = right_map.get(&get_key_as_string(&left_str_col, i)) {
                left_indices.push(i as u32);
                right_indices.push(right_index);
            }
        }
        if left_indices.is_empty() {
            continue;
        }

        let left_take = UInt32Array::from(left_indices);
        let right_take = UInt32Array::from(right_indices);
        let mut fields: Vec<Arc<Field>> = Vec::new();
        let mut columns = Vec::new();
        for (index, field) in left_batch.schema().fields().iter().enumerate() {
            fields.push(field.clone());
            columns.push(
                take(left_batch.column(index), &left_take, None).map_err(|error| {
                    format!("Falha ao projetar tabela esquerda do JOIN: {}", error)
                })?,
            );
        }
        for (index, field) in right_batch.schema().fields().iter().enumerate() {
            if field.name() == &join.right_column {
                continue;
            }
            let name = if left_batch.schema().field_with_name(field.name()).is_ok() {
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
            fields.push(Arc::new(Field::new(
                &name,
                field.data_type().clone(),
                field.is_nullable(),
            )));
            columns.push(
                take(right_batch.column(index), &right_take, None).map_err(|error| {
                    format!("Falha ao projetar tabela direita do JOIN: {}", error)
                })?,
            );
        }
        joined_batches.push(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .map_err(|error| format!("Falha ao montar resultado do JOIN: {}", error))?,
        );
    }
    Ok(joined_batches)
}

pub fn execute_subquery_for_list(
    table_name: &str,
    projection: &Vec<(String, Option<String>)>,
    filter_tree: &Option<FilterNode>,
    join_info: &Option<JoinInfo>,
    catalog: &Catalog,
    current_ws: &str,
) -> Result<Vec<String>, String> {
    let batches = fetch_and_filter_batches(
        table_name,
        filter_tree,
        join_info,
        catalog,
        current_ws,
        &CteTables::new(),
    )?;
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
) -> Vec<RecordBatch> {
    execute_select_with_ctes(
        table_name,
        projection,
        filter_tree,
        join_info,
        limit,
        group_by,
        aggregates,
        order_by_info,
        cases,
        has_wildcard,
        extracts,
        catalog,
        export_path,
        current_ws,
        &CteTables::new(),
        Vec::new(),
        false,
    )
}

fn apply_window_functions(
    batches: Vec<RecordBatch>,
    windows: &[WindowNode],
) -> Result<Vec<RecordBatch>, String> {
    if windows.is_empty() || batches.is_empty() {
        return Ok(batches);
    }

    let schema = batches[0].schema();
    let mut batch = concat_batches(&schema, &batches)
        .map_err(|error| format!("Falha ao consolidar lotes para funcao de janela: {}", error))?;
    for window in windows {
        if let Some(error) = &window.error {
            return Err(error.clone());
        }
        if !matches!(
            window.func.as_str(),
            "SUM" | "AVG" | "COUNT" | "MIN" | "MAX"
        ) {
            return Err(format!("Funcao de janela '{}' nao suportada.", window.func));
        }
        let mut partition_arrays = Vec::with_capacity(window.partition_by.len());
        for partition_column in &window.partition_by {
            let index = batch.schema().index_of(partition_column).map_err(|_| {
                format!(
                    "Coluna de particao '{}' nao encontrada na funcao de janela.",
                    partition_column
                )
            })?;
            partition_arrays.push(batch.column(index).clone());
        }

        let value_array = if window.column == "*" {
            None
        } else {
            let index = batch.schema().index_of(&window.column).map_err(|_| {
                format!(
                    "Coluna '{}' nao encontrada na funcao de janela.",
                    window.column
                )
            })?;
            Some(batch.column(index).clone())
        };

        let mut row_keys = Vec::with_capacity(batch.num_rows());
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
        for row in 0..batch.num_rows() {
            let mut key = String::new();
            for array in &partition_arrays {
                let value = get_key_as_string(array, row);
                key.push_str(&format!("{}:{value};", value.len()));
            }
            groups.entry(key.clone()).or_default().push(row);
            row_keys.push(key);
        }

        let output_name = window
            .alias
            .clone()
            .unwrap_or_else(|| format!("{}({})", window.func, window.column));
        let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
        let mut columns: Vec<Arc<dyn Array>> = batch.columns().iter().cloned().collect();

        if window.func == "COUNT" {
            let mut group_values = HashMap::new();
            for (key, rows) in &groups {
                let count = rows
                    .iter()
                    .filter(|&&row| {
                        value_array
                            .as_ref()
                            .map(|array| !array.is_null(row))
                            .unwrap_or(true)
                    })
                    .count() as i64;
                group_values.insert(key, count);
            }
            let values = row_keys
                .iter()
                .map(|key| group_values[key])
                .collect::<Vec<_>>();
            fields.push(Arc::new(Field::new(&output_name, DataType::Int64, false)));
            columns.push(Arc::new(Int64Array::from(values)) as Arc<dyn Array>);
        } else {
            let array = value_array.as_ref().ok_or_else(|| {
                format!(
                    "A funcao de janela '{}' exige uma coluna numerica.",
                    window.func
                )
            })?;
            let mut group_values = HashMap::new();
            for (key, rows) in &groups {
                let mut sum = 0.0;
                let mut count = 0usize;
                let mut min = f64::INFINITY;
                let mut max = f64::NEG_INFINITY;
                for &row in rows {
                    if array.is_null(row) {
                        continue;
                    }
                    if let Ok(value) = get_key_as_string(array, row).parse::<f64>() {
                        sum += value;
                        min = min.min(value);
                        max = max.max(value);
                        count += 1;
                    }
                }
                let result = match window.func.as_str() {
                    "SUM" => sum,
                    "AVG" => {
                        if count == 0 {
                            0.0
                        } else {
                            sum / count as f64
                        }
                    }
                    "MIN" => {
                        if count == 0 {
                            0.0
                        } else {
                            min
                        }
                    }
                    "MAX" => {
                        if count == 0 {
                            0.0
                        } else {
                            max
                        }
                    }
                    _ => unreachable!(),
                };
                group_values.insert(key, result);
            }
            let values = row_keys
                .iter()
                .map(|key| group_values[key])
                .collect::<Vec<_>>();
            fields.push(Arc::new(Field::new(&output_name, DataType::Float64, false)));
            columns.push(Arc::new(Float64Array::from(values)) as Arc<dyn Array>);
        }
        batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| format!("Falha ao adicionar coluna de janela: {}", error))?;
    }

    Ok(vec![batch])
}

pub fn execute_select_with_ctes(
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
    ctes: &CteTables,
    windows: Vec<WindowNode>,
    materialize: bool,
) -> Vec<RecordBatch> {
    let mut batches = match fetch_and_filter_batches(
        table_name,
        &filter_tree,
        &join_info,
        catalog,
        current_ws,
        ctes,
    ) {
        Ok(b) => b,
        Err(e) => {
            println!("\x1B[1;31mErro de Execucao:\x1B[0m {}", e);
            return Vec::new();
        }
    };

    if !extracts.is_empty() {
        let extracted_result = batches
            .into_par_iter()
            .map(|batch| -> Result<RecordBatch, String> {
                let mut new_fields: Vec<Arc<Field>> =
                    batch.schema().fields().iter().cloned().collect();
                let mut new_columns: Vec<Arc<dyn Array>> =
                    batch.columns().iter().cloned().collect();

                for ext in &extracts {
                    let col_idx = batch
                        .schema()
                        .index_of(&ext.column)
                        .map_err(|_| format!("Coluna de data '{}' nao encontrada.", ext.column))?;
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

                    let ext_col_name = ext.alias.as_deref().ok_or_else(|| {
                        format!("Alias nao definido para EXTRACT de '{}'.", ext.column)
                    })?;
                    new_fields.push(Arc::new(Field::new(ext_col_name, DataType::Int64, true)));
                    new_columns.push(Arc::new(Int64Array::from(extracted_vals)) as Arc<dyn Array>);
                }
                RecordBatch::try_new(Arc::new(Schema::new(new_fields)), new_columns)
                    .map_err(|error| format!("Erro ao adicionar coluna EXTRACT: {}", error))
            })
            .collect::<Result<Vec<_>, _>>();
        batches = match extracted_result {
            Ok(batches) => batches,
            Err(error) => {
                println!("\x1B[1;31mErro no EXTRACT:\x1B[0m {}", error);
                return Vec::new();
            }
        }
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
                        return Vec::new();
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
                            return Vec::new();
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

    match apply_window_functions(batches, &windows) {
        Ok(window_batches) => batches = window_batches,
        Err(error) => {
            println!("\x1B[1;31mErro nas funcoes de janela:\x1B[0m {}", error);
            return Vec::new();
        }
    }

    let projected_batches = if group_by.is_empty() && aggregates.is_empty() {
        let projected_result = batches
            .into_par_iter()
            .map(|batch| -> Result<RecordBatch, String> {
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
                                return Err(format!(
                                    "Coluna '{}' nao existe no esquema.",
                                    col_name
                                ));
                            }
                        }
                    }

                    for case in &cases {
                        let mask = evaluate_node(&case.condition, &batch)
                            .map_err(|error| format!("Erro no CASE WHEN: {}", error))?;
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
                        new_columns
                            .push(Arc::new(StringArray::from(case_results)) as Arc<dyn Array>);
                    }

                    RecordBatch::try_new(Arc::new(Schema::new(new_fields)), new_columns)
                        .map_err(|error| format!("Erro ao projetar RecordBatch: {}", error))
                } else {
                    Ok(batch)
                }
            })
            .collect::<Result<Vec<_>, _>>();
        match projected_result {
            Ok(batches) => batches,
            Err(error) => {
                println!("\x1B[1;31mErro na projecao:\x1B[0m {}", error);
                return Vec::new();
            }
        }
    } else {
        batches
    };
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
                        return Vec::new();
                    }
                } else {
                    println!("\x1B[1;31mErro:\x1B[0m Coluna de ordenacao '{}' nao encontrada no resultado.", order.column);
                    return Vec::new();
                }
            }
        }
    }

    let max_rows = limit.unwrap_or(if materialize || export_path.is_some() {
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

    if materialize {
        return final_batches;
    }

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
    final_batches
}

#[cfg(test)]
mod tests {
    use super::{fetch_and_filter_batches, filter_to_sql, CteTables};
    use crate::catalog::{Catalog, ColumnDef, VirtualTable};
    use crate::parser::FilterNode;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::fs::{self, File};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

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

    #[test]
    fn parquet_schema_and_filter_are_read_into_arrow_batches() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "federated-engine-parquet-{}-{}.parquet",
            std::process::id(),
            nonce
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "year",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![2022, 2023, 2024]))],
        )
        .unwrap();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let inferred = crate::connectors::parquet::infer_schema(path.to_str().unwrap()).unwrap();
        assert_eq!(inferred.field(0).name(), "year");
        assert_eq!(inferred.field(0).data_type(), &DataType::Int64);

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
                    format: "PARQUET".to_string(),
                    physical_path: path.to_string_lossy().into_owned(),
                    source_url: None,
                    columns: vec![ColumnDef {
                        name: "year".to_string(),
                        data_type: "Int64".to_string(),
                    }],
                },
            );

        let result = fetch_and_filter_batches(
            "rainfall",
            &Some(FilterNode::Condition {
                column: "year".to_string(),
                operator: ">=".to_string(),
                value: "2024".to_string(),
            }),
            &None,
            &catalog,
            "default",
            &CteTables::new(),
        )
        .unwrap();
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let years = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(years.value(0), 2024);

        fs::remove_file(path).unwrap();
    }
}
