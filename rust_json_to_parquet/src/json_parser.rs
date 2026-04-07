use std::collections::HashMap;

fn skip_ws(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && matches!(bytes[index], b' ' | b'\t' | b'\r' | b'\n') {
        index += 1;
    }
    index
}

pub(crate) fn find_json_string_end(bytes: &[u8], start: usize) -> pyo3::PyResult<usize> {
    if start >= bytes.len() || bytes[start] != b'"' {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "expected JSON string",
        ));
    }

    let mut index = start + 1;
    while index < bytes.len() {
        if bytes[index] == b'"' {
            let mut backslash_count = 0usize;
            let mut cursor = index;
            while cursor > start && bytes[cursor - 1] == b'\\' {
                backslash_count += 1;
                cursor -= 1;
            }
            if backslash_count % 2 == 0 {
                return Ok(index + 1);
            }
        }
        index += 1;
    }

    Err(pyo3::exceptions::PyValueError::new_err(
        "unterminated JSON string",
    ))
}

fn parse_json_string(bytes: &[u8], start: usize) -> pyo3::PyResult<(String, usize)> {
    let end = find_json_string_end(bytes, start)?;
    let raw = std::str::from_utf8(&bytes[start..end]).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid utf-8 in JSON key: {err}"))
    })?;
    let parsed: String = serde_json::from_str(raw).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid JSON string: {err}"))
    })?;
    Ok((parsed, end))
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

pub(crate) fn validate_json_column_order(bytes: &[u8], columns: &[String]) -> pyo3::PyResult<()> {
    let found_columns = extract_top_level_column_order(bytes)?;
    if found_columns != columns {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "JSON column order does not match expected order: expected {:?}, got {:?}",
            columns, found_columns
        )));
    }
    Ok(())
}

fn find_matching_array_end(bytes: &[u8], start: usize) -> pyo3::PyResult<usize> {
    if start >= bytes.len() || bytes[start] != b'[' {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "expected JSON array start",
        ));
    }

    let mut depth = 1usize;
    let mut pos = start + 1;
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
                b'[' => depth += 1,
                b']' => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(pos);
                    }
                }
                _ => {}
            }
        }
        pos += 1;
    }

    Err(pyo3::exceptions::PyValueError::new_err(
        "unterminated JSON array",
    ))
}

pub(crate) fn extract_top_level_array_ranges(
    bytes: &[u8],
    columns: &[String],
) -> pyo3::PyResult<HashMap<String, (usize, usize)>> {
    validate_json_column_order(bytes, columns)?;

    let mut index = skip_ws(bytes, 0);
    if index >= bytes.len() || bytes[index] != b'{' {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "expected top-level JSON object",
        ));
    }
    index += 1;

    let mut ranges = HashMap::with_capacity(columns.len());
    let mut expected_index = 0usize;

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
        let expected_key = columns.get(expected_index).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("unexpected extra JSON column")
        })?;
        if &key != expected_key {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "JSON column order does not match expected order: expected '{}' at position {}, got '{}'",
                expected_key, expected_index, key
            )));
        }

        index = skip_ws(bytes, next_index);
        if index >= bytes.len() || bytes[index] != b':' {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "expected ':' after JSON key '{}'",
                key
            )));
        }
        index = skip_ws(bytes, index + 1);
        if index >= bytes.len() || bytes[index] != b'[' {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "expected top-level array for column '{}'",
                key
            )));
        }

        let array_end = find_matching_array_end(bytes, index)?;
        ranges.insert(key, (index + 1, array_end));
        index = skip_ws(bytes, array_end + 1);
        if index < bytes.len() && bytes[index] == b',' {
            index += 1;
        }
        expected_index += 1;
    }

    Ok(ranges)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_top_level_array_ranges_for_nested_arrays() {
        let payload = br#"{
            "record_id": ["rec_1", "rec_2"],
            "value_sparse": [[10.0, 20.0], [30.0, 40.0]],
            "coord_a_sparse": [[1, 1], [2, 2]],
            "coord_b_sparse": [[1, 2], [1, 2]]
        }"#;
        let columns = vec![
            "record_id".to_string(),
            "value_sparse".to_string(),
            "coord_a_sparse".to_string(),
            "coord_b_sparse".to_string(),
        ];
        let ranges = extract_top_level_array_ranges(payload, &columns).unwrap();

        let record_id = std::str::from_utf8(&payload[ranges["record_id"].0..ranges["record_id"].1]).unwrap();
        let value_sparse =
            std::str::from_utf8(&payload[ranges["value_sparse"].0..ranges["value_sparse"].1]).unwrap();
        assert_eq!(record_id, "\"rec_1\", \"rec_2\"");
        assert_eq!(value_sparse, "[10.0, 20.0], [30.0, 40.0]");
    }
}
