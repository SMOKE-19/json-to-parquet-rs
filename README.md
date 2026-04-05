# json-to-parquet-rs

`json_to_parquet_rs`는 저장된 JSON 파일을 typed Parquet 파일로 변환하는 Python 패키지다.
패키지 내부에서 Rust 확장을 사용하지만, 외부 사용자는 Python API만 알면 된다.

빌드와 배포 절차는 [`build.md`](./build.md)에 정리되어 있다.

## 무엇을 제공하나

- `convert_json_to_parquet(...)`
  - sparse 값과 lookup parquet를 이용해 dense 배열을 복원한 뒤 parquet를 생성한다.
- `convert_json_to_parquet_passthrough(...)`
  - restore 없이 JSON에서 추출한 컬럼을 그대로 parquet로 저장한다.

## 설치

wheel을 받았다면 일반 Python 패키지처럼 설치하면 된다.

```bash
pip install json_to_parquet_rs-0.1.0-*.whl
```

개발 중이라면 저장소 루트에서 editable 설치가 가능하다.

```bash
python -m maturin develop --manifest-path rust_json_to_parquet/Cargo.toml
```

## 빠른 시작

### Dense restore 경로

```python
from json_to_parquet_rs import convert_json_to_parquet

profile = convert_json_to_parquet(
    input_json_path="sample_input.json",
    output_parquet_path="sample_output.parquet",
    lookup_path="lookup_table.parquet",
    columns=["record_id", "group_key", "value_sparse", "coord_a_sparse", "coord_b_sparse"],
    schema={
        "record_id": "TEXT",
        "group_key": "TEXT",
        "value_sparse": "DOUBLE[]",
        "coord_a_sparse": "INTEGER[]",
        "coord_b_sparse": "INTEGER[]",
    },
    config={
        "lookup": {
            "key_column": "group_key",
            "order_column": "order_idx",
            "coord_columns": ["coord_a_sparse", "coord_b_sparse"],
        },
        "restore": {
            "source": {
                "value": "value_sparse",
                "coords": ["coord_a_sparse", "coord_b_sparse"],
            },
            "output": {
                "value": "value_dense",
                "coords": ["coord_a_dense", "coord_b_dense"],
            },
        },
        "output": {
            "pass_through": ["record_id", "group_key"],
            "derived": [],
        },
    },
)

print(profile["rows"])
```

### Passthrough 경로

```python
from json_to_parquet_rs import convert_json_to_parquet_passthrough

profile = convert_json_to_parquet_passthrough(
    input_json_path="sample_input.json",
    output_parquet_path="sample_passthrough.parquet",
    columns=["record_id", "tags", "scores"],
    schema={
        "record_id": "TEXT",
        "tags": "TEXT[]",
        "scores": "DOUBLE[]",
    },
    config={
        "output": {
            "pass_through": ["record_id", "tags", "scores"],
            "derived": [],
        },
    },
)

print(profile["total_sec"])
```

## API 상세

### `convert_json_to_parquet(...)`

시그니처:

```python
convert_json_to_parquet(
    input_json_path: str,
    output_parquet_path: str,
    lookup_path: str,
    columns: list[str],
    schema: dict[str, str],
    config: Mapping[str, object],
    sample_rows: int | None = None,
) -> dict[str, float]
```

인자 설명:

- `input_json_path`
  - 변환할 원본 JSON 파일 경로
- `output_parquet_path`
  - 생성할 parquet 파일 경로
- `lookup_path`
  - dense 복원에 사용할 lookup parquet 경로
- `columns`
  - 입력 JSON에서 읽을 컬럼 순서
- `schema`
  - 각 컬럼의 타입 정의
- `config`
  - lookup, restore, output 구성을 담은 매핑
- `sample_rows`
  - 일부 행만 테스트할 때 사용하는 선택 인자

필수 `config` 구조:

```python
config = {
    "lookup": {
        "key_column": "group_key",
        "order_column": "order_idx",
        "coord_columns": ["coord_a_sparse", "coord_b_sparse"],
    },
    "restore": {
        "source": {
            "value": "value_sparse",
            "coords": ["coord_a_sparse", "coord_b_sparse"],
        },
        "output": {
            "value": "value_dense",
            "coords": ["coord_a_dense", "coord_b_dense"],
        },
    },
    "output": {
        "pass_through": ["record_id", "group_key"],
        "derived": [],
    },
}
```

### `convert_json_to_parquet_passthrough(...)`

시그니처:

```python
convert_json_to_parquet_passthrough(
    input_json_path: str,
    output_parquet_path: str,
    columns: list[str],
    schema: dict[str, str],
    config: Mapping[str, object],
    sample_rows: int | None = None,
) -> dict[str, float]
```

이 경로는 lookup parquet가 필요 없다.
`config`에는 `output.pass_through`만 맞게 넣으면 된다.

```python
config = {
    "output": {
        "pass_through": ["record_id", "tags", "scores"],
        "derived": [],
    },
}
```

## 타입 규약

현재 문서와 테스트 기준으로 아래 타입을 사용한다.

- `TEXT`
- `INTEGER`
- `FLOAT`
- `DOUBLE`
- `DECIMAL(...)`
- `TEXT[]`
- `INTEGER[]`
- `DOUBLE[]`

배열 타입은 입력 JSON에서 JSON 배열 문자열 형태로 들어와야 한다.
예를 들어 `TEXT[]` 컬럼 값은 `"[\\"a\\", \\"b\\"]"` 같은 형태를 기대한다.

## 반환값

두 API 모두 실행 프로파일을 `dict[str, float]` 형태로 반환한다.
대표 키는 아래와 같다.

- `rows`
- `extract_sec`
- `parse_scalar_sec`
- `restore_sec`
- `arrow_batch_build_sec`
- `parquet_write_sec`
- `total_sec`

passthrough 경로에서는 `restore_sec`가 `0.0`으로 반환된다.

## 동작 특성

- 입력은 이미 저장된 JSON 파일 경로를 받는다.
- Python에서 입력 전체를 재구성하지 않고 Rust 확장이 파싱과 parquet 생성을 담당한다.
- 결과 parquet 스키마는 `schema`와 `config.output`에 의해 결정된다.

## 호환성 메모

- 외부 사용자가 알아야 할 계약은 Python import와 함수 시그니처다.
- 내부 구현이 Rust인지, 어떤 상위 프로젝트에서 사용되는지는 패키지 사용에 필수 정보가 아니다.
