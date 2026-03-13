"""Convert one column-oriented JSON file into one typed parquet file."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import yaml

try:
    import json_to_parquet_rs as rust_json_to_parquet
except ImportError as exc:  # pragma: no cover - environment dependent
    raise SystemExit(
        "Could not import json_to_parquet_rs. "
        "Build and install fetch_open/rust_json_to_parquet first."
    ) from exc


@dataclass(frozen=True)
class ConversionPaths:
    """Paths required for a single-file conversion run."""

    input_file: Path
    output_file: Path
    profile_file: Path
    yaml_path: Path


def load_yaml_scheme(yaml_path: str | Path) -> tuple[list[str], dict[str, str]]:
    """Read ordered columns and schema types from YAML."""

    yaml_path = Path(yaml_path)
    payload = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    scheme = payload.get("scheme")
    if not isinstance(scheme, Mapping) or not scheme:
        raise ValueError(f"Missing scheme definition in YAML: {yaml_path}")

    columns = list(scheme.keys())
    schema = {str(column): str(dtype) for column, dtype in scheme.items()}
    return columns, schema


def load_conversion_config(yaml_path: str | Path) -> dict[str, object]:
    """Read conversion metadata used by the Rust converter."""

    yaml_path = Path(yaml_path)
    payload = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    lookup = payload.get("lookup")
    restore = payload.get("restore")
    output = payload.get("output")
    if not isinstance(lookup, Mapping):
        raise ValueError(f"Missing lookup definition in YAML: {yaml_path}")
    if not isinstance(restore, Mapping):
        raise ValueError(f"Missing restore definition in YAML: {yaml_path}")
    if not isinstance(output, Mapping):
        raise ValueError(f"Missing output definition in YAML: {yaml_path}")
    return {
        "lookup": {
            "key_column": str(lookup.get("key_column") or ""),
            "order_column": str(lookup.get("order_column") or ""),
            "coord_columns": [str(value) for value in lookup.get("coord_columns") or []],
        },
        "restore": {
            "source": {
                "value": str(((restore.get("source") or {}).get("value")) or ""),
                "coords": [
                    str(value)
                    for value in (((restore.get("source") or {}).get("coords")) or [])
                ],
            },
            "output": {
                "value": str(((restore.get("output") or {}).get("value")) or ""),
                "coords": [
                    str(value)
                    for value in (((restore.get("output") or {}).get("coords")) or [])
                ],
            },
        },
        "output": {
            "pass_through": [str(value) for value in output.get("pass_through") or []],
            "derived": [
                {
                    "name": str(item.get("name") or ""),
                    "from": str(item.get("from") or ""),
                    "op": str(item.get("op") or ""),
                }
                for item in output.get("derived") or []
            ],
        },
    }


def load_lookup_table_path(yaml_path: str | Path) -> Path:
    """Read the lookup parquet path from YAML."""

    yaml_path = Path(yaml_path)
    fetch_open_dir = yaml_path.parent.parent
    root_dir = fetch_open_dir.parent
    payload = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    lookup_path = (payload.get("lookup") or {}).get("index_table_parquet")
    if not lookup_path:
        raise ValueError(f"Missing lookup.index_table_parquet in YAML: {yaml_path}")

    candidates = [
        (root_dir / lookup_path).resolve(),
        (fetch_open_dir / lookup_path).resolve(),
        (yaml_path.parent / lookup_path).resolve(),
        (yaml_path.parent / Path(lookup_path).name).resolve(),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Could not find lookup parquet: {lookup_path}")


def build_conversion_paths(
    input_file: str | Path,
    output_dir: str | Path,
    yaml_path: str | Path,
) -> ConversionPaths:
    """Build the paths needed for a single-file conversion run."""

    input_file = Path(input_file)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    profile_dir = output_dir / "profiles"
    profile_dir.mkdir(parents=True, exist_ok=True)

    return ConversionPaths(
        input_file=input_file,
        output_file=output_dir / f"{input_file.stem}.typed.parquet",
        profile_file=profile_dir / f"{input_file.stem}.json_to_parquet.json",
        yaml_path=Path(yaml_path),
    )


def convert_json_to_parquet(
    paths: ConversionPaths,
    *,
    sample_rows: int | None = None,
) -> dict[str, float | str]:
    """Run the Rust converter and return detailed timing statistics."""

    lookup_table_path = load_lookup_table_path(paths.yaml_path)
    columns, schema = load_yaml_scheme(paths.yaml_path)
    config = load_conversion_config(paths.yaml_path)

    started = perf_counter()
    stats = rust_json_to_parquet.convert_json_to_parquet(
        paths.input_file.as_posix(),
        paths.output_file.as_posix(),
        lookup_table_path.as_posix(),
        columns,
        schema,
        config,
        sample_rows,
    )
    elapsed = perf_counter() - started

    payload: dict[str, float | str] = {
        "convert_sec": elapsed,
        "convert_engine": "rust_arrow_direct",
        "input_file": str(paths.input_file),
        "output_file": str(paths.output_file),
        **stats,
    }
    paths.profile_file.write_text(
        json.dumps({"json_to_parquet_profile": payload}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    payload["profile_output"] = str(paths.profile_file)
    return payload


def print_profile_report(profile: dict[str, float | str]) -> None:
    """Print the profile in a stable, easy-to-scan order."""

    ordered_keys = [
        "input_file",
        "output_file",
        "profile_output",
        "convert_engine",
        "convert_sec",
        "rows",
        "extract_rows_total",
        "extract_rows_materialized",
        "extract_sec",
        "extract_file_open_sec",
        "extract_mmap_sec",
        "extract_scan_offsets_sec",
        "extract_materialize_strings_sec",
        "reference_load_sec",
        "parse_scalar_sec",
        "restore_sec",
        "parse_float_sec",
        "parse_il1_sec",
        "parse_il2_sec",
        "dense_fill_sec",
        "arrow_batch_build_sec",
        "parquet_write_sec",
        "total_sec",
    ]
    for key in ordered_keys:
        if key in profile:
            print(f"{key}: {profile[key]}")


def main() -> None:
    fetch_open_dir = Path(__file__).resolve().parent
    test_dir = fetch_open_dir / "test"
    input_file = test_dir / "sample_input.json"
    yaml_path = test_dir / "example_schema.yaml"
    output_dir = fetch_open_dir.parent / ".tmp" / "fetch_open_single"

    paths = build_conversion_paths(
        input_file=input_file,
        output_dir=output_dir,
        yaml_path=yaml_path,
    )
    print(f"Running public json_to_parquet example with input_file={paths.input_file.name}...")
    profile = convert_json_to_parquet(paths)
    print_profile_report(profile)


if __name__ == "__main__":
    main()
