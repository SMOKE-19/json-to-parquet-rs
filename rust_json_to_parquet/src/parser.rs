fn skip_ws(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && matches!(bytes[index], b' ' | b'\t' | b'\r' | b'\n') {
        index += 1;
    }
    index
}

fn trim_ascii_ws(bytes: &[u8], mut start: usize, mut end: usize) -> (usize, usize) {
    while start < end && matches!(bytes[start], b' ' | b'\t' | b'\r' | b'\n') {
        start += 1;
    }
    while end > start && matches!(bytes[end - 1], b' ' | b'\t' | b'\r' | b'\n') {
        end -= 1;
    }
    (start, end)
}

fn find_json_string_end(bytes: &[u8], start: usize) -> pyo3::PyResult<usize> {
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

fn decode_json_string(raw: &str, kind: &str) -> pyo3::PyResult<String> {
    serde_json::from_str::<String>(raw).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "invalid JSON {kind} string wrapper: {err}"
        ))
    })
}

fn normalize_array_input(raw: &str, kind: &str) -> pyo3::PyResult<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "empty JSON {kind} array"
        )));
    }

    if trimmed.starts_with('[') {
        if trimmed.contains("\\\"") {
            return Ok(trimmed.replace("\\\\\"", "\"").replace("\\\"", "\""));
        }
        return Ok(trimmed.to_string());
    }

    if trimmed.starts_with('"') {
        let decoded = decode_json_string(trimmed, kind)?;
        let decoded_trimmed = decoded.trim();
        if decoded_trimmed.starts_with('[') {
            return Ok(decoded_trimmed.to_string());
        }
    }

    let normalized = trimmed.replace("\\\\\"", "\"").replace("\\\"", "\"");
    if normalized.starts_with('[') {
        return Ok(normalized);
    }

    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "invalid JSON {kind} array"
    )))
}

fn parse_i32_token(raw: &str) -> pyo3::PyResult<i32> {
    raw.parse::<i32>().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid integer '{raw}': {err}"))
    })
}

fn parse_f64_token(raw: &str) -> pyo3::PyResult<f64> {
    raw.parse::<f64>().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid float '{raw}': {err}"))
    })
}

fn parse_string_token(bytes: &[u8], start: usize, end: usize) -> pyo3::PyResult<String> {
    let raw = std::str::from_utf8(&bytes[start..end]).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid utf-8 in JSON string token: {err}"))
    })?;
    decode_json_string(raw, "string")
}

pub fn parse_json_i32_array(raw: &str) -> pyo3::PyResult<Vec<i32>> {
    let normalized = normalize_array_input(raw, "int")?;
    let bytes = normalized.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'[' || bytes[bytes.len() - 1] != b']' {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "expected JSON array for int list",
        ));
    }

    let mut result = Vec::new();
    let mut index = skip_ws(bytes, 1);
    let end = bytes.len() - 1;
    while index < end {
        if bytes[index] == b',' {
            index = skip_ws(bytes, index + 1);
            continue;
        }

        let token_end = bytes[index..end]
            .iter()
            .position(|byte| *byte == b',')
            .map(|offset| index + offset)
            .unwrap_or(end);
        let (start, token_end) = trim_ascii_ws(bytes, index, token_end);
        let raw_token = std::str::from_utf8(&bytes[start..token_end]).map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid utf-8 in int list: {err}"))
        })?;
        result.push(parse_i32_token(raw_token)?);
        index = skip_ws(bytes, token_end);
        if index < end && bytes[index] == b',' {
            index = skip_ws(bytes, index + 1);
        }
    }
    Ok(result)
}

pub fn parse_json_f64_array(raw: &str) -> pyo3::PyResult<Vec<f64>> {
    let normalized = normalize_array_input(raw, "float")?;
    let bytes = normalized.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'[' || bytes[bytes.len() - 1] != b']' {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "expected JSON array for float list",
        ));
    }

    let mut result = Vec::new();
    let mut index = skip_ws(bytes, 1);
    let end = bytes.len() - 1;
    while index < end {
        if bytes[index] == b',' {
            index = skip_ws(bytes, index + 1);
            continue;
        }

        if bytes[index] == b'"' {
            let next = find_json_string_end(bytes, index)?;
            let text = parse_string_token(bytes, index, next)?;
            result.push(parse_f64_token(&text)?);
            index = skip_ws(bytes, next);
        } else {
            let token_end = bytes[index..end]
                .iter()
                .position(|byte| *byte == b',')
                .map(|offset| index + offset)
                .unwrap_or(end);
            let (start, token_end) = trim_ascii_ws(bytes, index, token_end);
            let raw_token = std::str::from_utf8(&bytes[start..token_end]).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid utf-8 in float list: {err}"))
            })?;
            result.push(parse_f64_token(raw_token)?);
            index = skip_ws(bytes, token_end);
        }

        if index < end && bytes[index] == b',' {
            index = skip_ws(bytes, index + 1);
        }
    }
    Ok(result)
}
