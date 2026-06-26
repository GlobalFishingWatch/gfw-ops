import json
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery as bq_lib

from gfw.common.bigquery import BigQueryHelper

from gfw.ops.pipelines.create_external_table.main import run


@pytest.fixture
def mock_bq_client():
    schema_fields = [
        bq_lib.SchemaField("ssvid", "STRING", description="Maritime identifier"),
        bq_lib.SchemaField("timestamp", "TIMESTAMP", description="Event timestamp"),
    ]
    client = MagicMock(spec=bq_lib.Client)
    client.get_table.return_value = MagicMock(
        schema=schema_fields,
        description="Source table description",
    )
    return client


def test_creates_external_table_from_reference(mock_bq_client):
    with patch.object(BigQueryHelper, "create_external_table") as mock_create:
        run(
            reference="proj.ds.source",
            gcs_path="gs://bucket/out",
            external_table="proj.ds.external",
            project="proj",
            bq_client_factory=lambda **kwargs: mock_bq_client,
        )

    mock_bq_client.get_table.assert_called_once_with("proj.ds.source")
    mock_create.assert_called_once()
    kwargs = mock_create.call_args.kwargs
    assert kwargs["table"] == "proj.ds.external"
    assert kwargs["source_uris"] == ["gs://bucket/out/*.parquet"]
    assert kwargs["hive_partition_uri_prefix"] == "gs://bucket/out"
    assert kwargs["description"] == "Source table description"
    assert kwargs["replace"] is True
    assert all(f.description for f in kwargs["schema"])


def test_creates_external_table_from_schema_file(tmp_path):
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps([
        {"name": "ssvid", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]))

    with patch.object(BigQueryHelper, "create_external_table") as mock_create:
        run(
            schema_file=str(schema_file),
            gcs_path="gs://bucket/out",
            external_table="proj.ds.external",
            project="proj",
            bq_client_factory=BigQueryHelper.get_client_factory(mocked=True),
        )

    mock_create.assert_called_once()
    kwargs = mock_create.call_args.kwargs
    assert kwargs["table"] == "proj.ds.external"
    assert "gfw-ops" in kwargs["description"]
    assert "gs://bucket/out" in kwargs["description"]
    assert kwargs["replace"] is True


def test_falls_back_to_default_description_when_source_has_none(mock_bq_client):
    mock_bq_client.get_table.return_value.description = None
    with patch.object(BigQueryHelper, "create_external_table") as mock_create:
        run(
            reference="proj.ds.source",
            gcs_path="gs://bucket/out",
            external_table="proj.ds.external",
            project="proj",
            bq_client_factory=lambda **kwargs: mock_bq_client,
        )

    kwargs = mock_create.call_args.kwargs
    assert "gfw-ops" in kwargs["description"]
    assert "gs://bucket/out" in kwargs["description"]


def test_raises_on_unsupported_source_format(tmp_path):
    schema_file = tmp_path / "schema.json"
    schema_file.write_text("[]")
    with pytest.raises(ValueError, match="Unsupported source format"):
        run(
            schema_file=str(schema_file),
            gcs_path="gs://bucket/out",
            external_table="proj.ds.external",
            project="proj",
            source_format="INVALID",
            bq_client_factory=BigQueryHelper.get_client_factory(mocked=True),
        )


def test_raises_when_neither_reference_nor_schema_file():
    with pytest.raises(ValueError, match="--reference or --schema-file"):
        run(
            gcs_path="gs://bucket/out",
            external_table="proj.ds.external",
            project="proj",
            bq_client_factory=BigQueryHelper.get_client_factory(mocked=True),
        )
