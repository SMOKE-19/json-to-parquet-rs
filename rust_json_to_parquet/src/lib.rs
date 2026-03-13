use pyo3::prelude::*;
use std::collections::HashMap;

mod converter;
mod json_parser;
mod parser;
mod reference;

#[pyfunction]
#[pyo3(
    text_signature = "(input_json_path, output_parquet_path, lookup_path, columns, schema, config_json, sample_rows=None)"
)]
fn convert_json_to_parquet(
    input_json_path: String,
    output_parquet_path: String,
    lookup_path: String,
    columns: Vec<String>,
    schema: HashMap<String, String>,
    config_json: String,
    sample_rows: Option<usize>,
) -> PyResult<HashMap<String, f64>> {
    converter::convert_json_to_parquet_impl(
        input_json_path,
        output_parquet_path,
        lookup_path,
        columns,
        schema,
        config_json,
        sample_rows,
    )
}

#[pymodule]
fn json_to_parquet_rs(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add(
        "__doc__",
        "Rust module for converting one JSON file into one typed parquet file",
    )?;
    module.add_function(wrap_pyfunction!(convert_json_to_parquet, module)?)?;
    Ok(())
}
