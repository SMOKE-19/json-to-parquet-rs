# 빌드

Rust extension 빌드:

```powershell
.\.venv\Scripts\maturin.exe build --release --interpreter .\.venv\Scripts\python.exe --manifest-path fetch_open\rust_json_to_parquet\Cargo.toml
```

wheel 설치:

```powershell
.\.venv\Scripts\python.exe -m pip install --force-reinstall fetch_open\rust_json_to_parquet\target\wheels\json_to_parquet_rs-*.whl
```

synthetic 샘플 파일 생성:

```powershell
.\.venv\Scripts\python.exe fetch_open\generate_sample_data.py
```

예제 실행:

```powershell
.\.venv\Scripts\python.exe fetch_open\json_to_parquet.py
```
