use arrow_array::builder::{ListBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::{Float64Type, Int32Type};
use arrow_array::{ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

use crate::json_parser::extract_json_columns_profiled;
use crate::reference::{build_dense_index, load_reference_map, restore_dense_row, DenseRow};

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

fn parse_scalar_outputs(
    extracted: &HashMap<String, Vec<String>>,
    schema: &HashMap<String, String>,
    config: &ConversionConfig,
) -> pyo3::PyResult<Vec<OutputColumn>> {
    let mut outputs = Vec::with_capacity(config.output.pass_through.len());
    for column_name in &config.output.pass_through {
        let raw_values = required_column(extracted, column_name)?.clone();
        let dtype = schema.get(column_name).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "missing schema type for pass-through column {column_name}"
            ))
        })?;
        let values = match dtype.as_str() {
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
) -> pyo3::PyResult<HashMap<String, f64>> {
    let total_started = Instant::now();
    let config = parse_conversion_config(&config_json)?;

    let extract_started = Instant::now();
    let (extracted, extract_profile) =
        extract_json_columns_profiled(&input_json_path, &columns, &schema, sample_rows)?;
    let extract_sec = extract_started.elapsed().as_secs_f64();

    let refs_started = Instant::now();
    let refs = load_reference_map(
        &lookup_path,
        &config.lookup.key_column,
        &config.lookup.order_column,
        &config.lookup.coord_columns,
    )?;
    let refs_sec = refs_started.elapsed().as_secs_f64();

    let nrows = required_column(&extracted, &config.lookup.key_column)?.len();

    let parse_scalar_started = Instant::now();
    let scalar_outputs = parse_scalar_outputs(&extracted, &schema, &config)?;
    let derived_outputs = build_derived_outputs(&extracted, &config)?;
    let parse_scalar_sec = parse_scalar_started.elapsed().as_secs_f64();

    let restore_started = Instant::now();
    let (restored, restore_metrics) = build_restore_outputs(&extracted, &refs, &config)?;
    let restore_sec = restore_started.elapsed().as_secs_f64();

    let arrow_started = Instant::now();
    let batch = build_arrow_batch(&config, derived_outputs, scalar_outputs, restored)?;
    let arrow_sec = arrow_started.elapsed().as_secs_f64();

    let write_started = Instant::now();
    write_parquet_batch(&batch, &output_parquet_path)?;
    let parquet_write_sec = write_started.elapsed().as_secs_f64();

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
    for (key, value) in restore_metrics {
        stats.insert(key, value);
    }
    for (key, value) in extract_profile {
        stats.insert(format!("extract_{key}"), value);
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
) -> pyo3::PyResult<HashMap<String, f64>> {
    let total_started = Instant::now();
    let config = parse_passthrough_config(&config_json)?;

    let extract_started = Instant::now();
    let (extracted, extract_profile) =
        extract_json_columns_profiled(&input_json_path, &columns, &schema, sample_rows)?;
    let extract_sec = extract_started.elapsed().as_secs_f64();

    let parse_scalar_started = Instant::now();
    let fake_config = ConversionConfig {
        lookup: LookupConfig {
            key_column: String::new(),
            order_column: String::new(),
            coord_columns: Vec::new(),
        },
        restore: RestoreConfig {
            source: RestoreSourceConfig {
                value: String::new(),
                coords: Vec::new(),
            },
            output: RestoreOutputConfig {
                value: String::new(),
                coords: Vec::new(),
            },
        },
        output: config.output,
    };
    let scalar_outputs = parse_scalar_outputs(&extracted, &schema, &fake_config)?;
    let parse_scalar_sec = parse_scalar_started.elapsed().as_secs_f64();

    let arrow_started = Instant::now();
    let batch = build_record_batch(scalar_outputs)?;
    let arrow_sec = arrow_started.elapsed().as_secs_f64();

    let write_started = Instant::now();
    write_parquet_batch(&batch, &output_parquet_path)?;
    let parquet_write_sec = write_started.elapsed().as_secs_f64();

    let nrows = columns
        .first()
        .and_then(|column| extracted.get(column))
        .map(|values| values.len())
        .unwrap_or(0);

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
    for (key, value) in extract_profile {
        stats.insert(format!("extract_{key}"), value);
    }
    Ok(stats)
}
