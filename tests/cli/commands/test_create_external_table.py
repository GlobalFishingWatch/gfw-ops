import pytest
from unittest.mock import patch

from gfw.common.bigquery import BigQueryHelper
from gfw.common.cli import CLI

from gfw.ops.cli.commands.create_external_table import CreateExternalTable


def test_create_external_table_with_reference():
    with patch.object(BigQueryHelper, "create_external_table"):
        CLI(subcommands=[CreateExternalTable]).execute(
            [
                "create-external-table",
                "--project", "proj",
                "--gcs-path", "gs://bucket/out",
                "--external-table", "proj.ds.external",
                "--reference", "proj.ds.source",
            ],
            bq_client_factory=BigQueryHelper.get_client_factory(mocked=True),
        )


def test_create_external_table_with_schema_file(tmp_path):
    schema_file = tmp_path / "schema.json"
    schema_file.write_text("[]")
    with patch.object(BigQueryHelper, "create_external_table"):
        CLI(subcommands=[CreateExternalTable]).execute(
            [
                "create-external-table",
                "--project", "proj",
                "--gcs-path", "gs://bucket/out",
                "--external-table", "proj.ds.external",
                "--schema-file", str(schema_file),
            ],
            bq_client_factory=BigQueryHelper.get_client_factory(mocked=True),
        )


def test_help():
    with pytest.raises(SystemExit) as exc:
        CLI(subcommands=[CreateExternalTable]).execute(["create-external-table", "--help"])
    assert exc.value.code == 0
