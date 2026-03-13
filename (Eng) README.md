# fetch_open

`fetch_open` is a general-purpose example that converts one column-oriented JSON file into one typed parquet file.

This folder is structured as a configuration-driven conversion example:

- synthetic sample data included
- example-oriented field names
- YAML-driven column order and type validation
- YAML-driven lookup, restore, and output metadata

## What It Does

- reads one JSON file with top-level arrays
- validates column order against `scheme` in YAML
- validates the first row against YAML types
- loads a lookup parquet using YAML `lookup` metadata
- expands sparse list values into dense list values using YAML `restore` metadata
- assembles pass-through and derived output columns from YAML `output` metadata
- writes one typed parquet file

## Files

- [json_to_parquet.py](/c:/Users/super/OneDrive/문서/project/etl2/fetch_open/json_to_parquet.py)
- [generate_sample_data.py](/c:/Users/super/OneDrive/문서/project/etl2/fetch_open/generate_sample_data.py)
- [rust_json_to_parquet](/c:/Users/super/OneDrive/문서/project/etl2/fetch_open/rust_json_to_parquet)
- [BUILD.md](/c:/Users/super/OneDrive/문서/project/etl2/fetch_open/BUILD.md)

## Example Fields

- `record_id`
- `group_key`
- `metric_id`
- `category_code`
- `value_sparse`
- `coord_a_sparse`
- `coord_b_sparse`
- `event_ts`
- `ingest_ts`

## Run

Generate synthetic sample files:

```powershell
.\.venv\Scripts\python.exe fetch_open\generate_sample_data.py
```

Build and install the Rust extension:

```powershell
.\.venv\Scripts\maturin.exe build --release --interpreter .\.venv\Scripts\python.exe --manifest-path fetch_open\rust_json_to_parquet\Cargo.toml
.\.venv\Scripts\python.exe -m pip install --force-reinstall fetch_open\rust_json_to_parquet\target\wheels\json_to_parquet_rs-*.whl
```

Run the example:

```powershell
.\.venv\Scripts\python.exe fetch_open\json_to_parquet.py
```
