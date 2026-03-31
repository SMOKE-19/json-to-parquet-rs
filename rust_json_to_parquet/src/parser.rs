use serde_json::Value;

fn normalize_escaped_json(raw: &str) -> String {
    raw.replace("\\\\\"", "\"").replace("\\\"", "\"")
}

fn parse_json_array_like(raw: &str) -> Option<serde_json::Result<Value>> {
    if let Ok(value) = serde_json::from_str::<Value>(raw) {
        return Some(Ok(value));
    }

    let normalized = normalize_escaped_json(raw);
    if normalized != raw {
        return Some(serde_json::from_str::<Value>(&normalized));
    }
    None
}

fn parse_json_value(raw: &str, kind: &str) -> pyo3::PyResult<Value> {
    match parse_json_array_like(raw) {
        Some(Ok(Value::String(text))) => match parse_json_array_like(&text) {
            Some(Ok(value)) => Ok(value),
            Some(Err(inner_err)) => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "invalid JSON {kind} array inside string value: {inner_err}"
            ))),
            None => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "expected JSON {kind} array string but could not normalize input"
            ))),
        },
        Some(Ok(value)) => Ok(value),
        Some(Err(primary_err)) => {
            let normalized = normalize_escaped_json(raw);
            if normalized != raw {
                return serde_json::from_str::<Value>(&normalized).map_err(|normalized_err| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "invalid JSON {kind} array after unescaping quotes: {normalized_err}; original error: {primary_err}"
                    ))
                });
            }
            Err(pyo3::exceptions::PyValueError::new_err(format!(
                "invalid JSON {kind} array: {primary_err}"
            )))
        }
        None => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "invalid JSON {kind} array"
        ))),
    }
}

pub fn parse_json_i32_array(raw: &str) -> pyo3::PyResult<Vec<i32>> {
    let value = parse_json_value(raw, "int")?;
    let array = value.as_array().ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err("expected JSON array for int list")
    })?;

    let mut result = Vec::with_capacity(array.len());
    for item in array {
        let parsed = item.as_i64().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("int array contains non-integer value")
        })?;
        let casted = i32::try_from(parsed).map_err(|_| {
            pyo3::exceptions::PyValueError::new_err(format!("int value out of i32 range: {parsed}"))
        })?;
        result.push(casted);
    }
    Ok(result)
}

pub fn parse_json_f64_array(raw: &str) -> pyo3::PyResult<Vec<f64>> {
    let value = parse_json_value(raw, "float")?;
    let array = value.as_array().ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err("expected JSON array for float list")
    })?;

    let mut result = Vec::with_capacity(array.len());
    for item in array {
        let parsed = match item {
            Value::Number(num) => num.as_f64().ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("float array contains non-f64 number")
            })?,
            Value::String(text) => text.parse::<f64>().map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid float string '{text}': {err}"
                ))
            })?,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "float array contains unsupported value type",
                ))
            }
        };
        result.push(parsed);
    }
    Ok(result)
}
