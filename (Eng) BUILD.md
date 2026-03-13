# Build

Build the Rust extension:

```powershell
.\.venv\Scripts\maturin.exe build --release --interpreter .\.venv\Scripts\python.exe --manifest-path fetch_open\rust_json_to_parquet\Cargo.toml
```

Install the wheel:

```powershell
.\.venv\Scripts\python.exe -m pip install --force-reinstall fetch_open\rust_json_to_parquet\target\wheels\json_to_parquet_rs-*.whl
```

Generate synthetic sample files:

```powershell
.\.venv\Scripts\python.exe fetch_open\generate_sample_data.py
```

Run the example:

```powershell
.\.venv\Scripts\python.exe fetch_open\json_to_parquet.py
```
