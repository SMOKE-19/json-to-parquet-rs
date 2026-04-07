## json-to-parquet-rs Build Guide

이 문서는 `json_to_parquet_rs`를 단독 패키지로 빌드, 설치, 검증하는 절차를 설명한다.
예시는 모두 저장소 루트 `json-to-parquet-rs`에서 실행한다고 가정한다.

## 패키지 개요

- 배포 단위는 Python 패키지 `json_to_parquet_rs` 하나다.
- wheel 안에는 Python 래퍼, type 정보, Rust 확장이 함께 포함된다.
- 외부 사용자는 Rust 소스 구조를 몰라도 된다.

저장소에서 빌드에 직접 관련된 경로는 아래 정도만 알면 충분하다.

- `rust_json_to_parquet/Cargo.toml`
- `rust_json_to_parquet/pyproject.toml`
- `rust_json_to_parquet/python/json_to_parquet_rs/`
- `tests/`

## 필요 도구

- Python 3
- Rust toolchain
- `pip`
- `maturin`

설치 예시:

```bash
python -m pip install maturin
```

가상환경 사용을 권장한다.

## 개발용 설치

소스 수정과 즉시 검증이 목적이라면 `maturin develop`이 가장 편하다.

```bash
python -m maturin develop --manifest-path rust_json_to_parquet/Cargo.toml
```

이 명령은 아래를 수행한다.

- Rust 확장을 컴파일한다.
- 현재 Python 환경에 패키지를 editable 형태로 설치한다.
- 곧바로 `import json_to_parquet_rs` 테스트가 가능해진다.

## wheel 빌드

배포용 산출물이 필요하면 wheel을 빌드한다.

```bash
python -m maturin build --manifest-path rust_json_to_parquet/Cargo.toml
```

성공하면 wheel은 보통 아래 경로에 생성된다.

- `rust_json_to_parquet/target/wheels/`

예시 파일명:

- `json_to_parquet_rs-0.2.0-cp314-cp314-linux_x86_64.whl`

## wheel 설치

빌드한 wheel은 일반 패키지처럼 설치한다.

```bash
pip install --force-reinstall rust_json_to_parquet/target/wheels/json_to_parquet_rs-0.2.0-*.whl
```

설치 후 최소 확인:

```python
import json_to_parquet_rs
from json_to_parquet_rs import convert_json_to_parquet
from json_to_parquet_rs import convert_json_to_parquet_passthrough
```

## 공개 API 계약

단독 패키지 문서 기준으로 외부에 노출되는 핵심 API는 아래다.

- `convert_json_to_parquet(...)`
- `convert_json_to_parquet_passthrough(...)`

권장 사용:

- `convert_json_to_parquet(...)` 또는 `convert_json_to_parquet_passthrough(...)`

## 사용 패턴

### Dense restore

- 입력 JSON 파일 경로
- lookup parquet 경로
- 읽을 컬럼 목록
- 컬럼 타입 schema
- lookup/restore/output 설정

```python
from json_to_parquet_rs import convert_json_to_parquet

profile = convert_json_to_parquet(
    input_json_path="sample_input.json",
    output_parquet_path="output.parquet",
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
```

### Passthrough

- lookup parquet 없이 사용한다.
- `output.pass_through`에 적은 컬럼이 parquet로 그대로 저장된다.

```python
from json_to_parquet_rs import convert_json_to_parquet_passthrough

profile = convert_json_to_parquet_passthrough(
    input_json_path="sample_input.json",
    output_parquet_path="output.parquet",
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
```

## 검증

소스 기준으로 다시 설치한 뒤 테스트를 실행한다.

```bash
python -m maturin develop --manifest-path rust_json_to_parquet/Cargo.toml
python -m unittest discover -s tests -v
```

현재 테스트는 아래를 확인한다.

- dense restore 결과가 정상 생성되는지
- passthrough 배열 컬럼 길이가 유지되는지
- 문자열 배열 입력이 깨지지 않는지

## 운영 팁

- 로컬 개발: `maturin develop`
- 배포 산출물 생성: `maturin build`
- 배포 산출물 검증: 새 환경에서 wheel 설치 후 import와 샘플 변환 실행

## 문서 범위

이 문서는 `json_to_parquet_rs` 패키지 자체만 다룬다.
특정 상위 프로젝트, 별도 어댑터, 저장소 바깥 경로에 대한 설명은 포함하지 않는다.
