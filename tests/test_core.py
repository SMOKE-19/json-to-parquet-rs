from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from json_to_parquet_rs import convert_json_to_parquet, convert_json_to_parquet_passthrough


class CoreConversionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[1]
        self.package_root = self.repo_root / "rust_json_to_parquet"
        self.sample_json = self.package_root / "test" / "sample_input.json"
        self.lookup_parquet = self.package_root / "test" / "lookup_table.parquet"
        self.columns = [
            "record_id",
            "group_key",
            "metric_id",
            "category_code",
            "value_sparse",
            "coord_a_sparse",
            "coord_b_sparse",
            "event_ts",
            "ingest_ts",
        ]
        self.schema = {
            "record_id": "TEXT",
            "group_key": "TEXT",
            "metric_id": "INTEGER",
            "category_code": "TEXT",
            "value_sparse": "DOUBLE[]",
            "coord_a_sparse": "INTEGER[]",
            "coord_b_sparse": "INTEGER[]",
            "event_ts": "TIMESTAMP",
            "ingest_ts": "TIMESTAMP",
        }
        self.config = {
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
                    "value": "value_sparse",
                    "coords": ["coord_a_sparse", "coord_b_sparse"],
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
                "derived": [],
            },
        }

    def test_convert_json_to_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "sample.typed.parquet"
            profile = convert_json_to_parquet(
                input_json_path=str(self.sample_json),
                output_parquet_path=str(output_path),
                lookup_path=str(self.lookup_parquet),
                columns=self.columns,
                schema=self.schema,
                config=self.config,
            )
            self.assertEqual(profile["rows"], 250.0)
            self.assertTrue(output_path.exists())

            con = duckdb.connect()
            result = con.execute(
                """
                WITH output_counts AS (
                  SELECT
                    group_key,
                    array_length(value_sparse) AS value_len,
                    array_length(coord_a_sparse) AS coord_a_len,
                    array_length(coord_b_sparse) AS coord_b_len
                  FROM read_parquet(?)
                ),
                lookup_counts AS (
                  SELECT group_key, COUNT(*) AS expected_len
                  FROM read_parquet(?)
                  GROUP BY 1
                )
                SELECT
                  COUNT(*) AS row_count,
                  SUM(CASE WHEN value_len = expected_len THEN 1 ELSE 0 END) AS value_match_count,
                  SUM(CASE WHEN coord_a_len = expected_len THEN 1 ELSE 0 END) AS coord_a_match_count,
                  SUM(CASE WHEN coord_b_len = expected_len THEN 1 ELSE 0 END) AS coord_b_match_count
                FROM output_counts
                JOIN lookup_counts USING (group_key)
                """,
                [str(output_path), str(self.lookup_parquet)],
            ).fetchone()
            assert result is not None
            row_count, value_match_count, coord_a_match_count, coord_b_match_count = result
            self.assertEqual(row_count, 250)
            self.assertEqual(value_match_count, 250)
            self.assertEqual(coord_a_match_count, 250)
            self.assertEqual(coord_b_match_count, 250)

    def test_string_backslash_quotes_are_preserved_in_input_file(self) -> None:
        payload = json.loads(self.sample_json.read_text(encoding="utf-8"))
        payload["value_sparse"][0] = "[\\\"405.67\\\", \\\"538.07\\\"]"
        with tempfile.TemporaryDirectory() as temp_dir:
            custom_json = Path(temp_dir) / "escaped.json"
            custom_json.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            output_path = Path(temp_dir) / "escaped.parquet"
            profile = convert_json_to_parquet(
                input_json_path=str(custom_json),
                output_parquet_path=str(output_path),
                lookup_path=str(self.lookup_parquet),
                columns=self.columns,
                schema=self.schema,
                config=self.config,
                sample_rows=1,
            )
            self.assertEqual(profile["rows"], 1.0)
            self.assertTrue(output_path.exists())

    def test_convert_json_to_parquet_passthrough(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "sample.passthrough.parquet"
            profile = convert_json_to_parquet_passthrough(
                input_json_path=str(self.sample_json),
                output_parquet_path=str(output_path),
                columns=self.columns,
                schema=self.schema,
                config={
                    "output": {
                        "pass_through": self.columns,
                        "derived": [],
                    }
                },
                sample_rows=3,
            )
            self.assertEqual(profile["rows"], 3.0)
            self.assertTrue(output_path.exists())

            con = duckdb.connect()
            result = con.execute(
                """
                SELECT
                  COUNT(*) AS row_count,
                  array_length(value_sparse) AS value_len,
                  array_length(coord_a_sparse) AS coord_a_len,
                  array_length(coord_b_sparse) AS coord_b_len
                FROM read_parquet(?)
                GROUP BY 2, 3, 4
                """,
                [str(output_path)],
            ).fetchall()
            self.assertEqual(sum(row_count for row_count, *_ in result), 3)
            for _, value_len, coord_a_len, coord_b_len in result:
                self.assertEqual(value_len, coord_a_len)
                self.assertEqual(value_len, coord_b_len)

    def test_convert_json_to_parquet_accepts_print_timing_option(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "sample.typed.parquet"
            profile = convert_json_to_parquet(
                input_json_path=str(self.sample_json),
                output_parquet_path=str(output_path),
                lookup_path=str(self.lookup_parquet),
                columns=self.columns,
                schema=self.schema,
                config=self.config,
                sample_rows=1,
                print_timing=True,
            )
            self.assertEqual(profile["rows"], 1.0)
            self.assertTrue(output_path.exists())

    def test_convert_json_to_parquet_passthrough_accepts_print_timing_option(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "sample.passthrough_timing.parquet"
            profile = convert_json_to_parquet_passthrough(
                input_json_path=str(self.sample_json),
                output_parquet_path=str(output_path),
                columns=self.columns,
                schema=self.schema,
                config={
                    "output": {
                        "pass_through": self.columns,
                        "derived": [],
                    }
                },
                sample_rows=2,
                print_timing=True,
            )
            self.assertEqual(profile["rows"], 2.0)
            self.assertTrue(output_path.exists())


if __name__ == "__main__":
    unittest.main()
