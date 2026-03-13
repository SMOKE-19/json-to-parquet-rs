# fetch_open

`fetch_open`은 하나의 column-oriented JSON 파일을 하나의 typed parquet 파일로 변환하는 범용 예제입니다.

이 폴더는 설정 기반 변환 기능을 예제로 보여주기 위한 구성을 따릅니다.

- synthetic 샘플 데이터 포함
- 예제용 필드명 사용
- YAML 기반 컬럼 순서 및 타입 검증
- YAML 기반 lookup, restore, output 메타데이터 사용

## 하는 일

- top-level 배열 구조의 JSON 파일 하나를 읽음
- YAML `scheme` 기준으로 컬럼 순서를 검증함
- 첫 번째 행을 YAML 타입 기준으로 검증함
- YAML `lookup` 메타데이터 기준으로 lookup parquet를 읽음
- YAML `restore` 메타데이터 기준으로 sparse list 값을 dense list 값으로 복원함
- YAML `output` 메타데이터 기준으로 pass-through 및 derived 출력 컬럼을 구성함
- typed parquet 파일 하나를 씀

## 파일

- [json_to_parquet.py](/c:/Users/super/OneDrive/문서/project/etl2/fetch_open/json_to_parquet.py)
- [generate_sample_data.py](/c:/Users/super/OneDrive/문서/project/etl2/fetch_open/generate_sample_data.py)
- [rust_json_to_parquet](/c:/Users/super/OneDrive/문서/project/etl2/fetch_open/rust_json_to_parquet)
- [BUILD.md](/c:/Users/super/OneDrive/문서/project/etl2/fetch_open/BUILD.md)

## 예시 필드

- `record_id`
- `group_key`
- `metric_id`
- `category_code`
- `value_sparse`
- `coord_a_sparse`
- `coord_b_sparse`
- `event_ts`
- `ingest_ts`

## 실행

synthetic 샘플 파일 생성:

```powershell
.\.venv\Scripts\python.exe fetch_open\generate_sample_data.py
```

Rust extension 빌드 및 설치:

```powershell
.\.venv\Scripts\maturin.exe build --release --interpreter .\.venv\Scripts\python.exe --manifest-path fetch_open\rust_json_to_parquet\Cargo.toml
.\.venv\Scripts\python.exe -m pip install --force-reinstall fetch_open\rust_json_to_parquet\target\wheels\json_to_parquet_rs-*.whl
```

예제 실행:

```powershell
.\.venv\Scripts\python.exe fetch_open\json_to_parquet.py
```
