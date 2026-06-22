import json
from datetime import datetime, timezone

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline

from gfw.common.beam.pipeline import Pipeline
from gfw.common.beam.transforms import FakeReadFromBigQuery
from gfw.common.beam.transforms.parquet import FakeParquetSink

from gfw.ops.pipelines.bq_to_parquet.main import (
    _assign_timestamp,
    _build_query,
    run,
)


def test_build_query_contains_table_and_dates():
    q = _build_query("proj.ds.table", "2024-01-01", "2024-02-01", "timestamp")
    assert "proj.ds.table" in q
    assert "DATE(timestamp) >= '2024-01-01'" in q
    assert "DATE(timestamp) < '2024-02-01'" in q


def test_build_query_uses_timestamp_field():
    q = _build_query("proj.ds.table", "2024-06-01", "2024-07-01", "event_time")
    assert "DATE(event_time)" in q


def test_assign_timestamp():
    fn = _assign_timestamp("timestamp")
    ts = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    row = {"ssvid": "123456789", "timestamp": ts}
    result = fn(row)
    assert isinstance(result, beam.window.TimestampedValue)
    assert result.value == row
    assert result.timestamp == ts.timestamp()


def test_dry_run_returns_pipeline(tmp_path):
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps([
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]))
    result = run(
        bq_in="proj.ds.table",
        gcs_out="gs://bucket/path",
        project="proj",
        schema_file=str(schema_file),
        start_date="2024-01-01",
        end_date="2024-02-01",
        dry_run=True,
    )
    assert isinstance(result, Pipeline)


def test_run_pipeline(tmp_path):
    ts = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    rows = [
        {"ssvid": "111", "timestamp": ts, "lat": 1.0},
        {"ssvid": "222", "timestamp": ts, "lat": 2.0},
    ]

    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps([
        {"name": "ssvid", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
    ]))

    pipeline = run(
        bq_in="proj.ds.table",
        gcs_out=str(tmp_path / "out"),
        project="proj",
        schema_file=str(schema_file),
        start_date="2024-01-01",
        end_date="2024-02-01",
        read_from_bigquery_factory=lambda **kwargs: FakeReadFromBigQuery(elements=rows),
        parquet_sink_factory=FakeParquetSink,
        dry_run=True,
    )

    with _TestPipeline() as tp:
        pipeline._dag.apply(tp)
