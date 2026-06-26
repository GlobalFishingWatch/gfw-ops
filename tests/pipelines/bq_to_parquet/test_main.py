import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from google.cloud import bigquery as bq_lib

from gfw.common.beam.pipeline import Pipeline
from gfw.common.beam.transforms import FakeReadFromBigQuery
from gfw.common.beam.transforms.parquet import FakeParquetSink
from gfw.common.bigquery import BigQueryHelper

from gfw.ops.pipelines.bq_to_parquet.main import (
    _assign_timestamp,
    _build_query,
    run,
)


@pytest.fixture
def mock_bq_client():
    client = MagicMock(spec=bq_lib.Client)
    client.get_table.return_value = MagicMock(schema=[], description="")
    return client


@pytest.fixture
def bq_client_factory(mock_bq_client):
    return lambda **kwargs: mock_bq_client


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


def test_dry_run_returns_pipeline(tmp_path, bq_client_factory):
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
        bq_client_factory=bq_client_factory,
    )
    assert isinstance(result, Pipeline)


def test_run_pipeline(tmp_path, bq_client_factory):
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
        bq_client_factory=bq_client_factory,
        dry_run=True,
    )

    with _TestPipeline() as tp:
        pipeline._dag.apply(tp)


def test_external_table_created_with_source_metadata(tmp_path):
    schema_fields = [
        bq_lib.SchemaField("ssvid", "STRING", description="Maritime identifier"),
        bq_lib.SchemaField("timestamp", "TIMESTAMP", description="Event timestamp"),
    ]

    mock_client = MagicMock(spec=bq_lib.Client)
    mock_client.get_table.return_value = MagicMock(
        schema=schema_fields,
        description="Original source table description",
    )

    with patch.object(BigQueryHelper, "create_external_table") as mock_create, \
         patch("gfw.ops.pipelines.bq_to_parquet.main.Pipeline.run"):
        run(
            bq_in="proj.ds.source",
            gcs_out="gs://bucket/out",
            project="proj",
            start_date="2024-01-01",
            end_date="2024-02-01",
            external_table="proj.ds.external",
            bq_client_factory=lambda **kwargs: mock_client,
        )

    mock_client.get_table.assert_called_once_with("proj.ds.source")
    mock_create.assert_called_once()
    kwargs = mock_create.call_args.kwargs
    assert kwargs["table"] == "proj.ds.external"
    assert kwargs["schema"] == schema_fields
    assert kwargs["description"] == "Original source table description"
    assert kwargs["replace"] is True


def test_external_table_uses_schema_file_when_provided(tmp_path):
    schema_fields = [bq_lib.SchemaField("timestamp", "TIMESTAMP", description="From BQ")]
    mock_client = MagicMock(spec=bq_lib.Client)
    mock_client.get_table.return_value = MagicMock(
        schema=schema_fields,
        description="Source description",
    )

    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps([
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]))

    with patch.object(BigQueryHelper, "create_external_table") as mock_create, \
         patch("gfw.ops.pipelines.bq_to_parquet.main.Pipeline.run"):
        run(
            bq_in="proj.ds.source",
            gcs_out="gs://bucket/out",
            project="proj",
            schema_file=str(schema_file),
            start_date="2024-01-01",
            end_date="2024-02-01",
            external_table="proj.ds.external",
            bq_client_factory=lambda **kwargs: mock_client,
        )

    kwargs = mock_create.call_args.kwargs
    # schema comes from file (no field descriptions), description still from BQ
    assert all(not f.description for f in kwargs["schema"])
    assert kwargs["description"] == "Source description"


def test_dry_run_skips_external_table(tmp_path, bq_client_factory):
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps([
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]))

    with patch.object(BigQueryHelper, "create_external_table") as mock_create:
        run(
            bq_in="proj.ds.source",
            gcs_out="gs://bucket/out",
            project="proj",
            schema_file=str(schema_file),
            start_date="2024-01-01",
            end_date="2024-02-01",
            external_table="proj.ds.external",
            dry_run=True,
            bq_client_factory=bq_client_factory,
        )

    mock_create.assert_not_called()
