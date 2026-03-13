"""Generate synthetic sample files for the public fetch_open example."""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import yaml


FETCH_OPEN_DIR = Path(__file__).resolve().parent
TEST_DIR = FETCH_OPEN_DIR / "test"


def build_lookup_rows() -> list[dict[str, int | str]]:
    rows: list[dict[str, int | str]] = []
    for group_index, group_key in enumerate(["group_a", "group_b", "group_c", "group_d"], start=1):
        width = 10 + group_index * 2
        for order_idx in range(width):
            rows.append(
                {
                    "group_key": group_key,
                    "order_idx": order_idx,
                    "coord_a": order_idx // 4 + group_index,
                    "coord_b": order_idx % 4 + 1,
                }
            )
    return rows


def build_json_payload(lookup_rows: list[dict[str, int | str]], num_rows: int = 250) -> dict[str, list[str]]:
    rng = random.Random(20260314)
    rows_by_group: dict[str, list[dict[str, int | str]]] = {}
    for row in lookup_rows:
        rows_by_group.setdefault(str(row["group_key"]), []).append(row)

    base_time = datetime(2026, 1, 1, 8, 0, 0)
    payload = {
        "record_id": [],
        "group_key": [],
        "metric_id": [],
        "category_code": [],
        "value_sparse": [],
        "coord_a_sparse": [],
        "coord_b_sparse": [],
        "event_ts": [],
        "ingest_ts": [],
    }

    for row_index in range(num_rows):
        group_key = rng.choice(sorted(rows_by_group))
        dense_rows = rows_by_group[group_key]
        sparse_size = rng.randint(4, min(8, len(dense_rows)))
        sparse_rows = sorted(rng.sample(dense_rows, sparse_size), key=lambda item: int(item["order_idx"]))

        payload["record_id"].append(f"rec_{row_index:05d}")
        payload["group_key"].append(group_key)
        payload["metric_id"].append(str(rng.randint(1, 9)))
        payload["category_code"].append(f"cat_{rng.randint(1, 4)}")
        payload["value_sparse"].append(
            json.dumps([round(rng.uniform(10.0, 999.0), 2) for _ in sparse_rows], ensure_ascii=True)
        )
        payload["coord_a_sparse"].append(
            json.dumps([int(item["coord_a"]) for item in sparse_rows], ensure_ascii=True)
        )
        payload["coord_b_sparse"].append(
            json.dumps([int(item["coord_b"]) for item in sparse_rows], ensure_ascii=True)
        )

        event_ts = base_time + timedelta(minutes=row_index * 7)
        ingest_ts = event_ts + timedelta(seconds=rng.randint(5, 120))
        payload["event_ts"].append(event_ts.strftime("%Y-%m-%d %H:%M:%S"))
        payload["ingest_ts"].append(ingest_ts.strftime("%Y-%m-%d %H:%M:%S"))

    return payload


def main() -> None:
    TEST_DIR.mkdir(parents=True, exist_ok=True)

    schema = {
        "lookup": {
            "index_table_parquet": "test/lookup_table.parquet",
            "key_column": "group_key",
            "order_column": "order_idx",
            "coord_columns": ["coord_a", "coord_b"],
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
            "pass_through": [
                "record_id",
                "group_key",
                "metric_id",
                "category_code",
                "event_ts",
                "ingest_ts",
            ],
            "derived": [
                {"name": "record_prefix", "from": "record_id", "op": "prefix4"},
            ],
        },
        "scheme": {
            "record_id": "TEXT",
            "group_key": "TEXT",
            "metric_id": "TINYINT",
            "category_code": "TEXT",
            "value_sparse": "FLOAT[]",
            "coord_a_sparse": "INTEGER[]",
            "coord_b_sparse": "INTEGER[]",
            "event_ts": "TIMESTAMP",
            "ingest_ts": "TIMESTAMP",
        },
    }

    lookup_rows = build_lookup_rows()
    payload = build_json_payload(lookup_rows)

    (TEST_DIR / "example_schema.yaml").write_text(
        yaml.safe_dump(schema, sort_keys=False),
        encoding="utf-8",
    )
    (TEST_DIR / "sample_input.json").write_text(
        json.dumps(payload, ensure_ascii=True),
        encoding="utf-8",
    )
    lookup_df = pl.DataFrame(lookup_rows).with_columns(
        pl.col("order_idx").cast(pl.Int32),
        pl.col("coord_a").cast(pl.Int32),
        pl.col("coord_b").cast(pl.Int32),
    )
    lookup_df.write_parquet(TEST_DIR / "lookup_table.parquet")


if __name__ == "__main__":
    main()
