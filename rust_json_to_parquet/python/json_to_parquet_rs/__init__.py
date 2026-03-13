"""`json_to_parquet_rs` 확장을 위한 타입 지정 Python 래퍼."""

from __future__ import annotations

import json
from collections.abc import Mapping

from .json_to_parquet_rs import convert_json_to_parquet as _convert_json_to_parquet

__all__ = ["convert_json_to_parquet"]


def convert_json_to_parquet(
    input_json_path: str,
    output_parquet_path: str,
    lookup_path: str,
    columns: list[str],
    schema: dict[str, str],
    config: Mapping[str, object],
    sample_rows: int | None = None,
) -> dict[str, float]:
    """하나의 column-oriented JSON 파일을 하나의 typed parquet 파일로 변환한다.

    Args:
        input_json_path: 입력 JSON 파일 경로.
        output_parquet_path: 출력 parquet 파일 경로.
        lookup_path: sparse 좌표 복원에 사용할 lookup parquet 파일 경로.
        columns: 입력 파일에서 기대하는 JSON 컬럼 순서.
        schema: 컬럼명과 논리 타입 문자열의 매핑.
        config: lookup, restore, output 컬럼에 대한 변환 메타데이터.
        sample_rows: 부분 변환을 위한 선택적 행 제한값.

    Returns:
        Rust 구현이 반환한 시간 및 행 수 메트릭.
    """

    return _convert_json_to_parquet(
        input_json_path,
        output_parquet_path,
        lookup_path,
        columns,
        schema,
        json.dumps(config, ensure_ascii=True),
        sample_rows,
    )
