# `json_to_parquet_rs` 사용법

이 문서는 외부 사용자가 `json_to_parquet_rs` 패키지를 import 해서 호출하는 방법을 설명한다.

## 설치

```bash
pip install -e .
```

Rust 확장을 빌드해야 하므로 Rust toolchain과 `maturin` 기반 빌드 환경이 필요하다.

## 공개 진입점

### `convert_json_to_parquet()`

lookup parquet 을 사용하는 변환 함수다.

```python
from json_to_parquet_rs import convert_json_to_parquet

result = convert_json_to_parquet(
    input_json_path="test/sample_input.json",
    output_parquet_path="out/output.parquet",
    lookup_path="test/lookup_table.parquet",
    columns=["col_a", "col_b"],
    schema={"col_a": "string", "col_b": "int64"},
    config={"table_name": "sample"},
)

print(result)
```

인자:

- `input_json_path`: 입력 JSON 파일 경로
- `output_parquet_path`: 출력 Parquet 파일 경로
- `lookup_path`: lookup parquet 경로
- `columns`: 출력 컬럼 순서
- `schema`: 컬럼별 타입 정의
- `config`: 변환 설정 dict
- `sample_rows`: 선택적 샘플 행 수

### `convert_json_to_parquet_passthrough()`

restore lookup 없이 그대로 변환할 때 사용한다.

```python
from json_to_parquet_rs import convert_json_to_parquet_passthrough

result = convert_json_to_parquet_passthrough(
    input_json_path="test/sample_input.json",
    output_parquet_path="out/output.parquet",
    columns=["col_a", "col_b"],
    schema={"col_a": "string", "col_b": "int64"},
    config={"table_name": "sample"},
)
```

## 반환값

두 함수 모두 dict 형태의 변환 결과 요약을 반환한다.

## 주의사항

- 이 패키지는 파일 경로 기반 API만 제공한다.
- `config` 는 내부에서 JSON 문자열로 직렬화되어 Rust 확장으로 전달된다.
- 사용 예시의 `schema`, `config` 키 구조는 실제 Rust 구현이 기대하는 형식과 맞아야 한다.
