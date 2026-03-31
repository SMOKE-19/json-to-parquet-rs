"""`json_to_parquet_rs`의 core Python 래퍼.

역할:
- 이미 저장된 JSON 파일 경로를 받아 Rust mmap 파서 기반 parquet 변환을 수행한다.

주요 책임:
- 입력 JSON 전체를 Python 메모리로 적재하지 않는다.
- Rust 확장이 요구하는 저수준 인자를 Python에서 그대로 전달한다.
- 호출자는 JSON 파일 경로, lookup parquet 경로, 컬럼 순서, schema, config를 직접 제공한다.
"""

from __future__ import annotations

import json
from collections.abc import Mapping

from .json_to_parquet_rs import (
    convert_json_to_parquet as _convert_json_to_parquet_impl,
    convert_json_to_parquet_passthrough as _convert_json_to_parquet_passthrough,
)

__all__ = [
    "convert_json_to_parquet",
    "convert_json_to_parquet_passthrough",
]


def convert_json_to_parquet(
    input_json_path: str,
    output_parquet_path: str,
    lookup_path: str,
    columns: list[str],
    schema: dict[str, str],
    config: Mapping[str, object],
    sample_rows: int | None = None,
) -> dict[str, float]:
    """저장된 JSON 파일을 Rust mmap 파서로 parquet로 변환한다."""

    return _convert_json_to_parquet_impl(
        input_json_path,
        output_parquet_path,
        lookup_path,
        columns,
        schema,
        json.dumps(config, ensure_ascii=True),
        sample_rows,
    )


def convert_json_to_parquet_passthrough(
    input_json_path: str,
    output_parquet_path: str,
    columns: list[str],
    schema: dict[str, str],
    config: Mapping[str, object],
    sample_rows: int | None = None,
) -> dict[str, float]:
    """저장된 JSON 파일을 restore 없이 parquet로 변환한다."""

    return _convert_json_to_parquet_passthrough(
        input_json_path,
        output_parquet_path,
        columns,
        schema,
        json.dumps(config, ensure_ascii=True),
        sample_rows,
    )
