from collections.abc import Mapping

def convert_json_to_parquet(
    input_json_path: str,
    output_parquet_path: str,
    lookup_path: str,
    columns: list[str],
    schema: dict[str, str],
    config: Mapping[str, object],
    sample_rows: int | None = ...,
) -> dict[str, float]: ...
