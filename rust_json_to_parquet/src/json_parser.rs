use memmap2::Mmap;
use std::collections::HashMap;
use std::fs::File;
use std::time::Instant;

fn skip_ws(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && matches!(bytes[index], b' ' | b'\t' | b'\r' | b'\n') {
        index += 1;
    }
    index
}

fn parse_json_string(bytes: &[u8], start: usize) -> pyo3::PyResult<(String, usize)> {
    if start >= bytes.len() || bytes[start] != b'"' {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "expected JSON string",
        ));
    }

    let mut index = start + 1;
    let mut escaped = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' if !escaped => escaped = true,
            b'"' if !escaped => {
                let raw = std::str::from_utf8(&bytes[start..=index]).map_err(|err| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "invalid utf-8 in JSON key: {err}"
                    ))
                })?;
                let parsed: String = serde_json::from_str(raw).map_err(|err| {
                    pyo3::exceptions::PyValueError::new_err(format!("invalid JSON string: {err}"))
                })?;
                return Ok((parsed, index + 1));
            }
            _ => escaped = false,
        }
        index += 1;
    }

    Err(pyo3::exceptions::PyValueError::new_err(
        "unterminated JSON string",
    ))
}

fn skip_json_value(bytes: &[u8], start: usize) -> pyo3::PyResult<usize> {
    let index = skip_ws(bytes, start);
    if index >= bytes.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "unexpected end of JSON while skipping value",
        ));
    }

    match bytes[index] {
        b'"' => parse_json_string(bytes, index).map(|(_, next)| next),
        b'[' | b'{' => {
            let open = bytes[index];
            let close = if open == b'[' { b']' } else { b'}' };
            let mut depth = 1usize;
            let mut pos = index + 1;
            let mut in_string = false;
            let mut escaped = false;

            while pos < bytes.len() {
                let byte = bytes[pos];
                if in_string {
                    match byte {
                        b'\\' if !escaped => escaped = true,
                        b'"' if !escaped => in_string = false,
                        _ => escaped = false,
                    }
                } else {
                    match byte {
                        b'"' => in_string = true,
                        value if value == open => depth += 1,
                        value if value == close => {
                            depth -= 1;
                            if depth == 0 {
                                return Ok(pos + 1);
                            }
                        }
                        _ => {}
                    }
                }
                pos += 1;
            }
            Err(pyo3::exceptions::PyValueError::new_err(
                "unterminated JSON container",
            ))
        }
        _ => {
            let mut pos = index;
            while pos < bytes.len()
                && !matches!(
                    bytes[pos],
                    b',' | b']' | b'}' | b' ' | b'\t' | b'\r' | b'\n'
                )
            {
                pos += 1;
            }
            Ok(pos)
        }
    }
}

fn extract_top_level_column_order(bytes: &[u8]) -> pyo3::PyResult<Vec<String>> {
    let mut index = skip_ws(bytes, 0);
    if index >= bytes.len() || bytes[index] != b'{' {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "expected top-level JSON object",
        ));
    }
    index += 1;

    let mut columns = Vec::new();
    loop {
        index = skip_ws(bytes, index);
        if index >= bytes.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "unterminated top-level JSON object",
            ));
        }
        if bytes[index] == b'}' {
            break;
        }

        let (key, next_index) = parse_json_string(bytes, index)?;
        columns.push(key);
        index = skip_ws(bytes, next_index);
        if index >= bytes.len() || bytes[index] != b':' {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "expected ':' after JSON key",
            ));
        }
        index = skip_json_value(bytes, index + 1)?;
        index = skip_ws(bytes, index);

        if index < bytes.len() && bytes[index] == b',' {
            index += 1;
        }
    }

    Ok(columns)
}

fn validate_value_type(column: &str, raw_value: &str, dtype: &str) -> pyo3::PyResult<()> {
    match dtype {
        "TEXT" | "TIMESTAMP" => Ok(()),
        "TINYINT" | "INTEGER" => {
            raw_value.parse::<i32>().map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "Column '{column}' type mismatch: expected {dtype}, got '{raw_value}' ({err})"
                ))
            })?;
            Ok(())
        }
        "FLOAT[]" => {
            let value = crate::parser::parse_json_f64_array(raw_value).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "Column '{column}' type mismatch: expected FLOAT[], got {err}"
                ))
            })?;
            let _ = value;
            Ok(())
        }
        "INTEGER[]" => {
            let _ = crate::parser::parse_json_i32_array(raw_value).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "Column '{column}' type mismatch: expected INTEGER[], got {err}"
                ))
            })?;
            Ok(())
        }
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unsupported schema type '{other}' for column '{column}'"
        ))),
    }
}

fn validate_json_column_order(bytes: &[u8], columns: &[String]) -> pyo3::PyResult<()> {
    let found_columns = extract_top_level_column_order(bytes)?;
    if found_columns != columns {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "JSON column order does not match expected order: expected {:?}, got {:?}",
            columns, found_columns
        )));
    }
    Ok(())
}

fn validate_first_row_types(
    extracted: &HashMap<String, Vec<String>>,
    columns: &[String],
    schema: &HashMap<String, String>,
) -> pyo3::PyResult<()> {
    for column in columns {
        let Some(dtype) = schema.get(column) else {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "schema missing type for column '{column}'"
            )));
        };

        let Some(values) = extracted.get(column) else {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "missing extracted values for column '{column}'"
            )));
        };

        if let Some(raw_value) = values.first() {
            validate_value_type(column, raw_value, dtype)?;
        }
    }
    Ok(())
}

pub fn scan_json_column_offsets(
    bytes: &[u8],
    columns: &[String],
) -> pyo3::PyResult<(HashMap<String, Vec<(usize, usize)>>, usize)> {
    let mut offsets: HashMap<String, Vec<(usize, usize)>> =
        columns.iter().map(|c| (c.clone(), Vec::new())).collect();

    let col_keys: HashMap<Vec<u8>, String> = columns
        .iter()
        .map(|c| (format!("\"{c}\"").into_bytes(), c.clone()))
        .collect();

    let size = bytes.len();
    let mut i = 0usize;
    let mut curr_col: Option<String> = None;
    let mut depth = 0i32;
    let mut expected_rows: Option<usize> = None;

    while i < size {
        if let Some(expected) = expected_rows {
            if columns
                .iter()
                .all(|c| offsets.get(c).map(|v| v.len()).unwrap_or_default() >= expected)
            {
                break;
            }
        }

        if curr_col.is_none() {
            if bytes[i] != b'"' {
                i += 1;
                continue;
            }

            let mut j = i + 1;
            while j < size && bytes[j] != b'"' {
                j += 1;
            }
            if j >= size {
                break;
            }

            let key = &bytes[i..=j];
            let Some(col) = col_keys.get(key).cloned() else {
                i = j + 1;
                continue;
            };
            i = j + 1;

            while i < size && matches!(bytes[i], b' ' | b'\t' | b'\r' | b'\n') {
                i += 1;
            }
            if i >= size || bytes[i] != b':' {
                continue;
            }
            i += 1;
            while i < size && matches!(bytes[i], b' ' | b'\t' | b'\r' | b'\n') {
                i += 1;
            }

            if i < size && bytes[i] == b'[' {
                curr_col = Some(col);
                depth = 1;
                i += 1;
            }
            continue;
        }

        match bytes[i] {
            b'[' => {
                depth += 1;
                i += 1;
            }
            b']' => {
                depth -= 1;
                i += 1;
                if depth == 0 {
                    if expected_rows.is_none()
                        && curr_col.as_ref().map(|c| c == &columns[0]).unwrap_or(false)
                    {
                        expected_rows = curr_col
                            .as_ref()
                            .and_then(|c| offsets.get(c))
                            .map(|v| v.len());
                    }
                    curr_col = None;
                }
            }
            b'"' => {
                let start = i + 1;
                let mut j = start;
                loop {
                    while j < size && bytes[j] != b'"' {
                        j += 1;
                    }
                    if j >= size {
                        break;
                    }
                    if j == 0 || bytes[j - 1] != b'\\' {
                        break;
                    }
                    j += 1;
                }

                let col = curr_col.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("json scan parser state error")
                })?;
                offsets
                    .get_mut(col)
                    .ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "missing offsets for column {col}"
                        ))
                    })?
                    .push((start, j));
                i = j + 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    let row_count =
        expected_rows.unwrap_or_else(|| offsets.get(&columns[0]).map(|v| v.len()).unwrap_or(0));
    Ok((offsets, row_count))
}

pub fn extract_json_columns_profiled(
    json_path: &str,
    columns: &[String],
    schema: &HashMap<String, String>,
    sample_rows: Option<usize>,
) -> pyo3::PyResult<(HashMap<String, Vec<String>>, HashMap<String, f64>)> {
    let open_started = Instant::now();
    let file = File::open(json_path).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "failed to open json file {json_path}: {err}"
        ))
    })?;
    let open_elapsed = open_started.elapsed().as_secs_f64();

    let mmap_started = Instant::now();
    let mmap = unsafe {
        Mmap::map(&file).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "failed to mmap json file {json_path}: {err}"
            ))
        })?
    };
    let mmap_elapsed = mmap_started.elapsed().as_secs_f64();

    validate_json_column_order(&mmap, columns)?;

    let scan_started = Instant::now();
    let (offsets, total_rows) = scan_json_column_offsets(&mmap, columns)?;
    let scan_elapsed = scan_started.elapsed().as_secs_f64();

    let nrows = sample_rows
        .map(|value| value.min(total_rows))
        .unwrap_or(total_rows);

    let materialize_started = Instant::now();
    let mut result: HashMap<String, Vec<String>> = HashMap::new();
    for column in columns {
        let ranges = offsets.get(column).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("offsets missing for column {column}"))
        })?;
        let mut values = Vec::with_capacity(nrows);
        for (start, end) in ranges.iter().take(nrows) {
            let value = String::from_utf8(mmap[*start..*end].to_vec()).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid utf-8 in column {column}: {err}"
                ))
            })?;
            values.push(value);
        }
        result.insert(column.clone(), values);
    }
    let materialize_elapsed = materialize_started.elapsed().as_secs_f64();

    validate_first_row_types(&result, columns, schema)?;

    let mut profile = HashMap::new();
    profile.insert("file_open_sec".to_string(), open_elapsed);
    profile.insert("mmap_sec".to_string(), mmap_elapsed);
    profile.insert("scan_offsets_sec".to_string(), scan_elapsed);
    profile.insert("materialize_strings_sec".to_string(), materialize_elapsed);
    profile.insert("rows_total".to_string(), total_rows as f64);
    profile.insert("rows_materialized".to_string(), nrows as f64);
    Ok((result, profile))
}
