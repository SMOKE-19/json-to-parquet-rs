use arrow_array::builder::{ListBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::{Float64Type, Int32Type};
use arrow_array::{ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use memchr::memchr;
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

use crate::json_parser::{extract_top_level_array_ranges, find_json_string_end};
use crate::reference::{build_dense_index, load_reference_map, restore_dense_row, DenseRow};

const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

fn emit_timing(stage_kind: &str, input_json_path: &str, output_parquet_path: &str, stats: &HashMap<String, f64>) {
    println!(
        "[json_to_parquet_rs] stage={stage_kind} input_json_path={input_json_path} output_parquet_path={output_parquet_path}"
    );
    let ordered_keys = [
        "rows",
        "rows_total",
        "rows_materialized",
        "extract_file_open_sec",
        "extract_mmap_sec",
        "extract_scan_offsets_sec",
        "extract_materialize_strings_sec",
        "extract_sec",
        "reference_load_sec",
        "parse_scalar_sec",
        "parse_float_sec",
        "parse_il1_sec",
        "parse_il2_sec",
        "dense_fill_sec",
        "restore_sec",
        "arrow_batch_build_sec",
        "parquet_write_sec",
        "total_sec",
    ];
    for key in ordered_keys {
        if let Some(value) = stats.get(key) {
            println!("[json_to_parquet_rs]   {key}={value:.6}");
        }
    }
}

fn emit_stage_banner(stage_kind: &str, input_json_path: &str, output_parquet_path: &str) {
    println!(
        "[json_to_parquet_rs] version={PKG_VERSION} stage={stage_kind} input_json_path={input_json_path} output_parquet_path={output_parquet_path}"
    );
}

fn emit_stage_metric(stage_kind: &str, key: &str, value: f64) {
    println!("[json_to_parquet_rs] stage={stage_kind} {key}={value:.6}");
}

#[derive(Debug, Deserialize)]
struct ConversionConfig {
    lookup: LookupConfig,
    restore: RestoreConfig,
    output: OutputConfig,
}

#[derive(Debug, Deserialize)]
struct PassthroughConversionConfig {
    output: OutputConfig,
}

#[derive(Debug, Deserialize)]
struct LookupConfig {
    key_column: String,
    order_column: String,
    coord_columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RestoreConfig {
    source: RestoreSourceConfig,
    output: RestoreOutputConfig,
}

#[derive(Debug, Deserialize)]
struct RestoreSourceConfig {
    value: String,
    coords: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RestoreOutputConfig {
    value: String,
    coords: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct OutputConfig {
    pass_through: Vec<String>,
    derived: Vec<DerivedColumnConfig>,
}

#[derive(Debug, Deserialize)]
struct DerivedColumnConfig {
    name: String,
    from: String,
    op: String,
}

enum OutputColumnValues {
    Utf8(Vec<String>),
    Int32(Vec<i32>),
    Float64(Vec<f64>),
    Utf8List(Vec<Vec<String>>),
    F64List(Vec<DenseRow>),
    I32List(Vec<Vec<i32>>),
}

struct OutputColumn {
    name: String,
    values: OutputColumnValues,
}

struct RestoreColumns {
    value_rows: Vec<DenseRow>,
    coord_rows: Vec<Vec<Vec<i32>>>,
}

#[derive(Debug)]
struct OpenJsonProfile {
    mmap: Mmap,
    file_open_sec: f64,
    mmap_sec: f64,
}

fn normalize_dtype(dtype: &str) -> &str {
    match dtype {
        "TEXT" | "Utf8" | "String" => "TEXT",
        "DATE" | "Date" => "DATE",
        "TIMESTAMP" | "Datetime" => "TIMESTAMP",
        "TINYINT" | "Int8" => "TINYINT",
        "INTEGER" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64" => {
            "INTEGER"
        }
        "FLOAT" | "Float32" => "FLOAT",
        "DOUBLE" | "Float64" => "DOUBLE",
        "INTEGER[]" | "List(Int8)" | "List(Int16)" | "List(Int32)" | "List(Int64)" => "INTEGER[]",
        "FLOAT[]" | "List(Float32)" => "FLOAT[]",
        "DOUBLE[]" | "List(Float64)" => "DOUBLE[]",
        "TEXT[]" | "List(Utf8)" | "List(String)" => "TEXT[]",
        _ => dtype,
    }
}

fn open_mmap_json(json_path: &str) -> pyo3::PyResult<OpenJsonProfile> {
    let open_started = Instant::now();
    let file = File::open(json_path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to open json file {json_path}: {err}"
        ))
    })?;
    let file_open_sec = open_started.elapsed().as_secs_f64();

    let mmap_started = Instant::now();
    let mmap = unsafe {
        Mmap::map(&file).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to mmap json file {json_path}: {err}"
            ))
        })?
    };
    let mmap_sec = mmap_started.elapsed().as_secs_f64();

    Ok(OpenJsonProfile {
        mmap,
        file_open_sec,
        mmap_sec,
    })
}

fn skip_ws(bytes: &[u8], mut index: usize, end: usize) -> usize {
    while index < end && matches!(bytes[index], b' ' | b'\t' | b'\r' | b'\n') {
        index += 1;
    }
    index
}

fn trim_ascii_ws(bytes: &[u8], start: usize, mut end: usize) -> (usize, usize) {
    let mut start = start;
    while start < end && matches!(bytes[start], b' ' | b'\t' | b'\r' | b'\n') {
        start += 1;
    }
    while end > start && matches!(bytes[end - 1], b' ' | b'\t' | b'\r' | b'\n') {
        end -= 1;
    }
    (start, end)
}

fn parse_utf8_token(bytes: &[u8], start: usize, end: usize) -> pyo3::PyResult<String> {
    let (start, end) = trim_ascii_ws(bytes, start, end);
    if start >= end {
        return Ok(String::new());
    }
    if bytes[start] == b'"' {
        let raw = std::str::from_utf8(&bytes[start..end]).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid utf-8 in JSON string: {err}"))
        })?;
        return serde_json::from_str::<String>(raw).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid JSON string token: {err}"))
        });
    }
    std::str::from_utf8(&bytes[start..end])
        .map(str::to_string)
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid utf-8 in scalar token: {err}"))
        })
}

fn parse_i32_token(bytes: &[u8], start: usize, end: usize, name: &str) -> pyo3::PyResult<i32> {
    let raw = parse_utf8_token(bytes, start, end)?;
    raw.parse::<i32>().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid {name} '{raw}': {err}"))
    })
}

fn parse_f64_token(bytes: &[u8], start: usize, end: usize, name: &str) -> pyo3::PyResult<f64> {
    let raw = parse_utf8_token(bytes, start, end)?;
    raw.parse::<f64>().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid {name} '{raw}': {err}"))
    })
}

fn next_comma(bytes: &[u8], start: usize, end: usize) -> Option<usize> {
    memchr(b',', &bytes[start..end]).map(|offset| start + offset)
}

fn parse_utf8_array_safe(bytes: &[u8], range: (usize, usize)) -> pyo3::PyResult<Vec<String>> {
    let mut values = Vec::new();
    let mut index = skip_ws(bytes, range.0, range.1);
    while index < range.1 {
        if bytes[index] == b',' {
            index = skip_ws(bytes, index + 1, range.1);
            continue;
        }

        if bytes[index] == b'"' {
            let next = find_json_string_end(bytes, index)?;
            values.push(parse_utf8_token(bytes, index, next)?);
            index = skip_ws(bytes, next, range.1);
        } else {
            let token_end = next_comma(bytes, index, range.1).unwrap_or(range.1);
            values.push(parse_utf8_token(bytes, index, token_end)?);
            index = skip_ws(bytes, token_end, range.1);
        }

        if index < range.1 && bytes[index] == b',' {
            index = skip_ws(bytes, index + 1, range.1);
        }
    }
    Ok(values)
}

fn parse_utf8_array_fast(
    bytes: &[u8],
    range: (usize, usize),
    expected_rows: usize,
) -> pyo3::PyResult<Vec<String>> {
    let mut values = Vec::with_capacity(expected_rows);
    let mut index = skip_ws(bytes, range.0, range.1);
    while index < range.1 {
        if bytes[index] == b',' {
            index = skip_ws(bytes, index + 1, range.1);
            continue;
        }
        let token_end = next_comma(bytes, index, range.1).unwrap_or(range.1);
        values.push(parse_utf8_token(bytes, index, token_end)?);
        index = skip_ws(bytes, token_end, range.1);
        if index < range.1 && bytes[index] == b',' {
            index = skip_ws(bytes, index + 1, range.1);
        }
    }
    if values.len() != expected_rows {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "string fast-path row mismatch: expected {expected_rows}, got {}",
            values.len()
        )));
    }
    Ok(values)
}

fn parse_bare_scalar_array<T, F>(
    bytes: &[u8],
    range: (usize, usize),
    expected_rows: Option<usize>,
    mut parse_token: F,
) -> pyo3::PyResult<Vec<T>>
where
    F: FnMut(&[u8], usize, usize) -> pyo3::PyResult<T>,
{
    let capacity = expected_rows.unwrap_or(16);
    let mut values = Vec::with_capacity(capacity);
    let mut index = skip_ws(bytes, range.0, range.1);
    while index < range.1 {
        if bytes[index] == b',' {
            index = skip_ws(bytes, index + 1, range.1);
            continue;
        }
        let token_end = next_comma(bytes, index, range.1).unwrap_or(range.1);
        values.push(parse_token(bytes, index, token_end)?);
        index = skip_ws(bytes, token_end, range.1);
        if index < range.1 && bytes[index] == b',' {
            index = skip_ws(bytes, index + 1, range.1);
        }
    }
    if let Some(expected) = expected_rows {
        if values.len() != expected {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "scalar row mismatch: expected {expected}, got {}",
                values.len()
            )));
        }
    }
    Ok(values)
}

fn extract_passthrough_scalar_outputs_profiled(
    input_json_path: &str,
    columns: &[String],
    schema: &HashMap<String, String>,
    pass_through: &[String],
    sample_rows: Option<usize>,
) -> pyo3::PyResult<(Vec<OutputColumn>, HashMap<String, f64>)> {
    let open = open_mmap_json(input_json_path)?;

    let scan_started = Instant::now();
    let array_ranges = extract_top_level_array_ranges(&open.mmap, columns)?;
    let scan_sec = scan_started.elapsed().as_secs_f64();

    let parse_started = Instant::now();
    let mut outputs = Vec::with_capacity(pass_through.len());
    let mut expected_rows: Option<usize> = None;

    for column_name in pass_through {
        let range = array_ranges.get(column_name).copied().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "top-level array range missing for column {column_name}"
            ))
        })?;
        let dtype = normalized_schema_type(schema, column_name)?;
        let values = match dtype {
            "TEXT" | "TIMESTAMP" | "DATE" => {
                let rows = match expected_rows {
                    Some(expected) => parse_utf8_array_fast(&open.mmap, range, expected)
                        .or_else(|_| parse_utf8_array_safe(&open.mmap, range))?,
                    None => parse_utf8_array_safe(&open.mmap, range)?,
                };
                if expected_rows.is_none() {
                    expected_rows = Some(rows.len());
                }
                let rows = if let Some(limit) = sample_rows {
                    rows.into_iter().take(limit).collect()
                } else {
                    rows
                };
                OutputColumnValues::Utf8(rows)
            }
            "TINYINT" | "INTEGER" => {
                let mut rows = parse_bare_scalar_array(
                    &open.mmap,
                    range,
                    expected_rows,
                    |bytes, start, end| parse_i32_token(bytes, start, end, column_name),
                )?;
                if expected_rows.is_none() {
                    expected_rows = Some(rows.len());
                }
                if let Some(limit) = sample_rows {
                    rows.truncate(limit);
                }
                OutputColumnValues::Int32(rows)
            }
            "FLOAT" | "DOUBLE" => {
                let mut rows = parse_bare_scalar_array(
                    &open.mmap,
                    range,
                    expected_rows,
                    |bytes, start, end| parse_f64_token(bytes, start, end, column_name),
                )?;
                if expected_rows.is_none() {
                    expected_rows = Some(rows.len());
                }
                if let Some(limit) = sample_rows {
                    rows.truncate(limit);
                }
                OutputColumnValues::Float64(rows)
            }
            other if other.starts_with("DECIMAL(") => {
                let mut rows = parse_bare_scalar_array(
                    &open.mmap,
                    range,
                    expected_rows,
                    |bytes, start, end| parse_f64_token(bytes, start, end, column_name),
                )?;
                if expected_rows.is_none() {
                    expected_rows = Some(rows.len());
                }
                if let Some(limit) = sample_rows {
                    rows.truncate(limit);
                }
                OutputColumnValues::Float64(rows)
            }
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "passthrough fast path does not support type {other} for column {column_name}"
                )));
            }
        };

        outputs.push(OutputColumn {
            name: column_name.clone(),
            values,
        });
    }

    let parse_sec = parse_started.elapsed().as_secs_f64();
    let rows_total = expected_rows.unwrap_or(0);

    let mut profile = HashMap::new();
    profile.insert("file_open_sec".to_string(), open.file_open_sec);
    profile.insert("mmap_sec".to_string(), open.mmap_sec);
    profile.insert("scan_offsets_sec".to_string(), scan_sec);
    profile.insert("materialize_strings_sec".to_string(), 0.0);
    profile.insert("rows_total".to_string(), rows_total as f64);
    profile.insert(
        "rows_materialized".to_string(),
        sample_rows.map(|limit| limit.min(rows_total)).unwrap_or(rows_total) as f64,
    );
    profile.insert("typed_parse_sec".to_string(), parse_sec);

    Ok((outputs, profile))
}

fn collect_restore_required_columns(config: &ConversionConfig) -> Vec<String> {
    let mut names = Vec::new();
    let mut push_unique = |name: &str| {
        if !names.iter().any(|existing| existing == name) {
            names.push(name.to_string());
        }
    };

    push_unique(&config.lookup.key_column);
    push_unique(&config.restore.source.value);
    for name in &config.restore.source.coords {
        push_unique(name);
    }
    for name in &config.output.pass_through {
        push_unique(name);
    }
    for derived in &config.output.derived {
        push_unique(&derived.from);
    }

    names
}

fn extract_restore_inputs_profiled(
    input_json_path: &str,
    columns: &[String],
    config: &ConversionConfig,
    sample_rows: Option<usize>,
) -> pyo3::PyResult<(HashMap<String, Vec<String>>, HashMap<String, f64>)> {
    let open = open_mmap_json(input_json_path)?;

    let scan_started = Instant::now();
    let array_ranges = extract_top_level_array_ranges(&open.mmap, columns)?;
    let scan_sec = scan_started.elapsed().as_secs_f64();

    let parse_started = Instant::now();
    let required_columns = collect_restore_required_columns(config);
    let mut extracted = HashMap::with_capacity(required_columns.len());
    let mut total_rows = 0usize;

    for column_name in required_columns {
        let range = array_ranges.get(&column_name).copied().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "top-level array range missing for column {column_name}"
            ))
        })?;
        let mut values = parse_utf8_array_safe(&open.mmap, range)?;
        if total_rows == 0 {
            total_rows = values.len();
        } else if values.len() != total_rows {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "column row mismatch for {column_name}: expected {total_rows}, got {}",
                values.len()
            )));
        }
        if let Some(limit) = sample_rows {
            values.truncate(limit);
        }
        extracted.insert(column_name, values);
    }

    let parse_sec = parse_started.elapsed().as_secs_f64();
    let rows_materialized = sample_rows.map(|limit| limit.min(total_rows)).unwrap_or(total_rows);

    let mut profile = HashMap::new();
    profile.insert("file_open_sec".to_string(), open.file_open_sec);
    profile.insert("mmap_sec".to_string(), open.mmap_sec);
    profile.insert("scan_offsets_sec".to_string(), scan_sec);
    profile.insert("materialize_strings_sec".to_string(), parse_sec);
    profile.insert("rows_total".to_string(), total_rows as f64);
    profile.insert("rows_materialized".to_string(), rows_materialized as f64);
    Ok((extracted, profile))
}

fn parse_conversion_config(config_json: &str) -> pyo3::PyResult<ConversionConfig> {
    let config: ConversionConfig = serde_json::from_str(config_json).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid conversion config: {err}"))
    })?;
    if config.lookup.coord_columns.len() != 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "lookup.coord_columns must contain exactly 2 items, got {}",
            config.lookup.coord_columns.len()
        )));
    }
    if config.restore.source.coords.len() != 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "restore.source.coords must contain exactly 2 items, got {}",
            config.restore.source.coords.len()
        )));
    }
    if config.restore.output.coords.len() != 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "restore.output.coords must contain exactly 2 items, got {}",
            config.restore.output.coords.len()
        )));
    }
    Ok(config)
}

fn parse_passthrough_config(config_json: &str) -> pyo3::PyResult<PassthroughConversionConfig> {
    serde_json::from_str(config_json).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid conversion config: {err}"))
    })
}

fn required_column<'a>(
    extracted: &'a HashMap<String, Vec<String>>,
    name: &str,
) -> pyo3::PyResult<&'a Vec<String>> {
    extracted
        .get(name)
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!("missing {name} column")))
}

fn parse_i32_values(raw_values: &[String], name: &str) -> pyo3::PyResult<Vec<i32>> {
    raw_values
        .iter()
        .map(|value| {
            value.parse::<i32>().map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid {name} '{value}': {err}"))
            })
        })
        .collect()
}

fn parse_f64_values(raw_values: &[String], name: &str) -> pyo3::PyResult<Vec<f64>> {
    raw_values
        .iter()
        .map(|value| {
            value.parse::<f64>().map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid {name} '{value}': {err}"))
            })
        })
        .collect()
}

fn parse_string_list_values(raw_values: &[String], name: &str) -> pyo3::PyResult<Vec<Vec<String>>> {
    raw_values
        .iter()
        .map(|raw_value| {
            let value = serde_json::from_str::<serde_json::Value>(raw_value).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid JSON string array for {name}: {err}"
                ))
            })?;
            let array = value.as_array().ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "expected JSON string array for {name}"
                ))
            })?;
            array
                .iter()
                .map(|item| {
                    item.as_str().map(str::to_string).ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "string array contains non-string value for {name}"
                        ))
                    })
                })
                .collect()
        })
        .collect()
}

fn normalized_schema_type<'a>(
    schema: &'a HashMap<String, String>,
    column_name: &str,
) -> pyo3::PyResult<&'a str> {
    let dtype = schema.get(column_name).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "missing schema type for pass-through column {column_name}"
        ))
    })?;
    Ok(normalize_dtype(dtype))
}

fn parse_scalar_outputs(
    extracted: &HashMap<String, Vec<String>>,
    schema: &HashMap<String, String>,
    config: &ConversionConfig,
) -> pyo3::PyResult<Vec<OutputColumn>> {
    let mut outputs = Vec::with_capacity(config.output.pass_through.len());
    for column_name in &config.output.pass_through {
        let raw_values = required_column(extracted, column_name)?.clone();
        let values = match normalized_schema_type(schema, column_name)? {
            "TEXT" | "TIMESTAMP" | "DATE" => OutputColumnValues::Utf8(raw_values),
            "TINYINT" | "INTEGER" => {
                OutputColumnValues::Int32(parse_i32_values(&raw_values, column_name)?)
            }
            "FLOAT" | "DOUBLE" => {
                OutputColumnValues::Float64(parse_f64_values(&raw_values, column_name)?)
            }
            other if other.starts_with("DECIMAL(") => {
                OutputColumnValues::Float64(parse_f64_values(&raw_values, column_name)?)
            }
            "INTEGER[]" => OutputColumnValues::I32List(
                raw_values
                    .iter()
                    .map(|value| crate::parser::parse_json_i32_array(value))
                    .collect::<pyo3::PyResult<Vec<_>>>()?,
            ),
            "FLOAT[]" | "DOUBLE[]" => OutputColumnValues::F64List(
                raw_values
                    .iter()
                    .map(|value| {
                        crate::parser::parse_json_f64_array(value)
                            .map(|row| row.into_iter().map(Some).collect())
                    })
                    .collect::<pyo3::PyResult<Vec<_>>>()?,
            ),
            "TEXT[]" => OutputColumnValues::Utf8List(parse_string_list_values(&raw_values, column_name)?),
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unsupported pass-through type {other} for column {column_name}"
                )))
            }
        };
        outputs.push(OutputColumn {
            name: column_name.clone(),
            values,
        });
    }
    Ok(outputs)
}

fn validate_passthrough_scalar_schema(
    schema: &HashMap<String, String>,
    pass_through: &[String],
) -> pyo3::PyResult<()> {
    for column_name in pass_through {
        let dtype = normalized_schema_type(schema, column_name)?;
        match dtype {
            "TEXT" | "TIMESTAMP" | "DATE" | "TINYINT" | "INTEGER" | "FLOAT" | "DOUBLE" => {}
            other if other.starts_with("DECIMAL(") => {}
            "INTEGER[]" | "FLOAT[]" | "DOUBLE[]" | "TEXT[]" => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "passthrough fast path does not support list columns: {column_name} has type {dtype}"
                )));
            }
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unsupported passthrough schema type {other} for column {column_name}"
                )));
            }
        }
    }
    Ok(())
}

fn build_derived_outputs(
    extracted: &HashMap<String, Vec<String>>,
    config: &ConversionConfig,
) -> pyo3::PyResult<Vec<OutputColumn>> {
    let mut outputs = Vec::with_capacity(config.output.derived.len());
    for derived in &config.output.derived {
        let source_values = required_column(extracted, &derived.from)?;
        let values = match derived.op.as_str() {
            "prefix4" => source_values
                .iter()
                .map(|value| value.chars().take(4).collect())
                .collect(),
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unsupported derived op {other} for column {}",
                    derived.name
                )))
            }
        };
        outputs.push(OutputColumn {
            name: derived.name.clone(),
            values: OutputColumnValues::Utf8(values),
        });
    }
    Ok(outputs)
}

fn build_restore_outputs(
    extracted: &HashMap<String, Vec<String>>,
    refs: &HashMap<String, (Vec<i32>, Vec<i32>)>,
    config: &ConversionConfig,
) -> pyo3::PyResult<(RestoreColumns, HashMap<String, f64>)> {
    let key_values = required_column(extracted, &config.lookup.key_column)?;
    let value_sparse_json = required_column(extracted, &config.restore.source.value)?;
    let coord_a_sparse_json = required_column(extracted, &config.restore.source.coords[0])?;
    let coord_b_sparse_json = required_column(extracted, &config.restore.source.coords[1])?;
    let nrows = key_values.len();

    let mut value_rows: Vec<DenseRow> = Vec::with_capacity(nrows);
    let mut coord_rows: Vec<Vec<Vec<i32>>> =
        vec![Vec::with_capacity(nrows), Vec::with_capacity(nrows)];
    let mut parse_float_sec = 0.0f64;
    let mut parse_il1_sec = 0.0f64;
    let mut parse_il2_sec = 0.0f64;
    let mut dense_fill_sec = 0.0f64;

    for idx in 0..nrows {
        let group_key = &key_values[idx];
        let (dense_coord_a, dense_coord_b) = refs.get(group_key).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("unknown lookup key '{group_key}'"))
        })?;

        let started = Instant::now();
        let value_sparse = crate::parser::parse_json_f64_array(&value_sparse_json[idx])?;
        parse_float_sec += started.elapsed().as_secs_f64();

        let started = Instant::now();
        let coord_a_sparse = crate::parser::parse_json_i32_array(&coord_a_sparse_json[idx])?;
        parse_il1_sec += started.elapsed().as_secs_f64();

        let started = Instant::now();
        let coord_b_sparse = crate::parser::parse_json_i32_array(&coord_b_sparse_json[idx])?;
        parse_il2_sec += started.elapsed().as_secs_f64();

        let started = Instant::now();
        let dense_index = build_dense_index(dense_coord_a, dense_coord_b)?;
        let dense_value = restore_dense_row(
            &value_sparse,
            &coord_a_sparse,
            &coord_b_sparse,
            &dense_index,
            dense_coord_a.len(),
        );
        dense_fill_sec += started.elapsed().as_secs_f64();

        value_rows.push(dense_value);
        coord_rows[0].push(dense_coord_a.clone());
        coord_rows[1].push(dense_coord_b.clone());
    }

    let mut metrics = HashMap::new();
    metrics.insert("parse_float_sec".to_string(), parse_float_sec);
    metrics.insert("parse_il1_sec".to_string(), parse_il1_sec);
    metrics.insert("parse_il2_sec".to_string(), parse_il2_sec);
    metrics.insert("dense_fill_sec".to_string(), dense_fill_sec);
    Ok((
        RestoreColumns {
            value_rows,
            coord_rows,
        },
        metrics,
    ))
}

fn list_f64_array(rows: &[DenseRow]) -> ArrayRef {
    let mut builder = ListBuilder::new(PrimitiveBuilder::<Float64Type>::with_capacity(
        rows.iter().map(|row| row.len()).sum::<usize>(),
    ));
    for row in rows {
        for value in row {
            match value {
                Some(value) => builder.values().append_value(*value),
                None => builder.values().append_null(),
            }
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn list_i32_array(rows: &[Vec<i32>]) -> ArrayRef {
    let mut builder = ListBuilder::new(PrimitiveBuilder::<Int32Type>::with_capacity(
        rows.iter().map(|row| row.len()).sum::<usize>(),
    ));
    for row in rows {
        for value in row {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn list_utf8_array(rows: &[Vec<String>]) -> ArrayRef {
    let value_capacity = rows.iter().map(|row| row.len()).sum::<usize>();
    let mut builder = ListBuilder::new(StringBuilder::with_capacity(value_capacity, value_capacity * 8));
    for row in rows {
        for value in row {
            builder.values().append_value(value);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn output_field(name: &str, values: &OutputColumnValues) -> Field {
    match values {
        OutputColumnValues::Utf8(_) => Field::new(name, DataType::Utf8, false),
        OutputColumnValues::Int32(_) => Field::new(name, DataType::Int32, false),
        OutputColumnValues::Float64(_) => Field::new(name, DataType::Float64, false),
        OutputColumnValues::Utf8List(_) => Field::new(
            name,
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        OutputColumnValues::F64List(_) => Field::new(
            name,
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            false,
        ),
        OutputColumnValues::I32List(_) => Field::new(
            name,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        ),
    }
}

fn output_array(values: OutputColumnValues) -> ArrayRef {
    match values {
        OutputColumnValues::Utf8(values) => Arc::new(StringArray::from(values)),
        OutputColumnValues::Int32(values) => Arc::new(Int32Array::from(values)),
        OutputColumnValues::Float64(values) => Arc::new(Float64Array::from(values)),
        OutputColumnValues::Utf8List(values) => list_utf8_array(&values),
        OutputColumnValues::F64List(values) => list_f64_array(&values),
        OutputColumnValues::I32List(values) => list_i32_array(&values),
    }
}

fn build_record_batch(outputs: Vec<OutputColumn>) -> pyo3::PyResult<RecordBatch> {
    let fields = outputs
        .iter()
        .map(|column| output_field(&column.name, &column.values))
        .collect::<Vec<_>>();
    let arrays = outputs
        .into_iter()
        .map(|column| output_array(column.values))
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("failed to build arrow RecordBatch: {err}"))
    })
}

fn build_arrow_batch(
    config: &ConversionConfig,
    mut derived_outputs: Vec<OutputColumn>,
    mut scalar_outputs: Vec<OutputColumn>,
    restored: RestoreColumns,
) -> pyo3::PyResult<RecordBatch> {
    let mut outputs = Vec::with_capacity(
        derived_outputs.len() + scalar_outputs.len() + 1 + restored.coord_rows.len(),
    );
    outputs.append(&mut derived_outputs);
    outputs.append(&mut scalar_outputs);
    outputs.push(OutputColumn {
        name: config.restore.output.value.clone(),
        values: OutputColumnValues::F64List(restored.value_rows),
    });
    for (index, name) in config.restore.output.coords.iter().enumerate() {
        outputs.push(OutputColumn {
            name: name.clone(),
            values: OutputColumnValues::I32List(restored.coord_rows[index].clone()),
        });
    }

    build_record_batch(outputs)
}

fn write_parquet_batch(batch: &RecordBatch, output_parquet_path: &str) -> pyo3::PyResult<()> {
    let file = File::create(output_parquet_path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to create output parquet {output_parquet_path}: {err}"
        ))
    })?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to create arrow parquet writer {output_parquet_path}: {err}"
        ))
    })?;
    writer.write(batch).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to write parquet via arrow {output_parquet_path}: {err}"
        ))
    })?;
    writer.close().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to finalize parquet file {output_parquet_path}: {err}"
        ))
    })?;
    Ok(())
}

pub fn convert_json_to_parquet_impl(
    input_json_path: String,
    output_parquet_path: String,
    lookup_path: String,
    columns: Vec<String>,
    schema: HashMap<String, String>,
    config_json: String,
    sample_rows: Option<usize>,
    print_timing: bool,
) -> pyo3::PyResult<HashMap<String, f64>> {
    let total_started = Instant::now();
    let config = parse_conversion_config(&config_json)?;
    if print_timing {
        emit_stage_banner("restore", &input_json_path, &output_parquet_path);
    }

    let extract_started = Instant::now();
    let (extracted, extract_profile) =
        extract_restore_inputs_profiled(&input_json_path, &columns, &config, sample_rows)?;
    let extract_sec = extract_started.elapsed().as_secs_f64();
    if print_timing {
        emit_stage_metric("restore", "extract_sec", extract_sec);
    }

    let refs_started = Instant::now();
    let refs = load_reference_map(
        &lookup_path,
        &config.lookup.key_column,
        &config.lookup.order_column,
        &config.lookup.coord_columns,
    )?;
    let refs_sec = refs_started.elapsed().as_secs_f64();
    if print_timing {
        emit_stage_metric("restore", "reference_load_sec", refs_sec);
    }

    let nrows = required_column(&extracted, &config.lookup.key_column)?.len();

    let parse_scalar_started = Instant::now();
    let scalar_outputs = parse_scalar_outputs(&extracted, &schema, &config)?;
    let derived_outputs = build_derived_outputs(&extracted, &config)?;
    let parse_scalar_sec = parse_scalar_started.elapsed().as_secs_f64();
    if print_timing {
        emit_stage_metric("restore", "parse_scalar_sec", parse_scalar_sec);
    }

    let restore_started = Instant::now();
    let (restored, restore_metrics) = build_restore_outputs(&extracted, &refs, &config)?;
    let restore_sec = restore_started.elapsed().as_secs_f64();
    if print_timing {
        if let Some(value) = restore_metrics.get("parse_float_sec") {
            emit_stage_metric("restore", "parse_float_sec", *value);
        }
        if let Some(value) = restore_metrics.get("parse_il1_sec") {
            emit_stage_metric("restore", "parse_il1_sec", *value);
        }
        if let Some(value) = restore_metrics.get("parse_il2_sec") {
            emit_stage_metric("restore", "parse_il2_sec", *value);
        }
        if let Some(value) = restore_metrics.get("dense_fill_sec") {
            emit_stage_metric("restore", "dense_fill_sec", *value);
        }
        emit_stage_metric("restore", "restore_sec", restore_sec);
    }

    let arrow_started = Instant::now();
    let batch = build_arrow_batch(&config, derived_outputs, scalar_outputs, restored)?;
    let arrow_sec = arrow_started.elapsed().as_secs_f64();
    if print_timing {
        emit_stage_metric("restore", "arrow_batch_build_sec", arrow_sec);
    }

    let write_started = Instant::now();
    write_parquet_batch(&batch, &output_parquet_path)?;
    let parquet_write_sec = write_started.elapsed().as_secs_f64();
    if print_timing {
        emit_stage_metric("restore", "parquet_write_sec", parquet_write_sec);
    }

    let mut stats = HashMap::new();
    stats.insert("rows".to_string(), nrows as f64);
    stats.insert("extract_sec".to_string(), extract_sec);
    stats.insert("reference_load_sec".to_string(), refs_sec);
    stats.insert("parse_scalar_sec".to_string(), parse_scalar_sec);
    stats.insert("restore_sec".to_string(), restore_sec);
    stats.insert("arrow_batch_build_sec".to_string(), arrow_sec);
    stats.insert("parquet_write_sec".to_string(), parquet_write_sec);
    stats.insert(
        "total_sec".to_string(),
        total_started.elapsed().as_secs_f64(),
    );
    if print_timing {
        if let Some(rows) = stats.get("rows") {
            emit_stage_metric("restore", "rows", *rows);
        }
        if let Some(total_sec) = stats.get("total_sec") {
            emit_stage_metric("restore", "total_sec", *total_sec);
        }
    }
    for (key, value) in restore_metrics {
        stats.insert(key, value);
    }
    for (key, value) in extract_profile {
        stats.insert(format!("extract_{key}"), value);
    }
    if print_timing {
        emit_timing(
            "restore",
            &input_json_path,
            &output_parquet_path,
            &stats,
        );
    }
    Ok(stats)
}

pub fn convert_json_to_parquet_passthrough_impl(
    input_json_path: String,
    output_parquet_path: String,
    columns: Vec<String>,
    schema: HashMap<String, String>,
    config_json: String,
    sample_rows: Option<usize>,
    print_timing: bool,
) -> pyo3::PyResult<HashMap<String, f64>> {
    let total_started = Instant::now();
    let config = parse_passthrough_config(&config_json)?;
    validate_passthrough_scalar_schema(&schema, &config.output.pass_through)?;
    if print_timing {
        emit_stage_banner("passthrough", &input_json_path, &output_parquet_path);
    }

    let extract_started = Instant::now();
    let (scalar_outputs, extract_profile) = extract_passthrough_scalar_outputs_profiled(
        &input_json_path,
        &columns,
        &schema,
        &config.output.pass_through,
        sample_rows,
    )?;
    let extract_sec = extract_started.elapsed().as_secs_f64();
    if print_timing {
        emit_stage_metric("passthrough", "extract_sec", extract_sec);
    }

    let parse_scalar_started = Instant::now();
    let scalar_outputs = scalar_outputs;
    let parse_scalar_sec = parse_scalar_started.elapsed().as_secs_f64();
    if print_timing {
        emit_stage_metric("passthrough", "parse_scalar_sec", parse_scalar_sec);
    }

    let arrow_started = Instant::now();
    let batch = build_record_batch(scalar_outputs)?;
    let arrow_sec = arrow_started.elapsed().as_secs_f64();
    if print_timing {
        emit_stage_metric("passthrough", "arrow_batch_build_sec", arrow_sec);
    }

    let write_started = Instant::now();
    write_parquet_batch(&batch, &output_parquet_path)?;
    let parquet_write_sec = write_started.elapsed().as_secs_f64();
    if print_timing {
        emit_stage_metric("passthrough", "parquet_write_sec", parquet_write_sec);
    }

    let nrows = extract_profile
        .get("rows_materialized")
        .copied()
        .unwrap_or(0.0) as usize;

    let mut stats = HashMap::new();
    stats.insert("rows".to_string(), nrows as f64);
    stats.insert("extract_sec".to_string(), extract_sec);
    stats.insert("parse_scalar_sec".to_string(), parse_scalar_sec);
    stats.insert("restore_sec".to_string(), 0.0);
    stats.insert("arrow_batch_build_sec".to_string(), arrow_sec);
    stats.insert("parquet_write_sec".to_string(), parquet_write_sec);
    stats.insert(
        "total_sec".to_string(),
        total_started.elapsed().as_secs_f64(),
    );
    if print_timing {
        if let Some(rows) = stats.get("rows") {
            emit_stage_metric("passthrough", "rows", *rows);
        }
        if let Some(total_sec) = stats.get("total_sec") {
            emit_stage_metric("passthrough", "total_sec", *total_sec);
        }
    }
    for (key, value) in extract_profile {
        stats.insert(format!("extract_{key}"), value);
    }
    if print_timing {
        emit_timing(
            "passthrough",
            &input_json_path,
            &output_parquet_path,
            &stats,
        );
    }
    Ok(stats)
}
