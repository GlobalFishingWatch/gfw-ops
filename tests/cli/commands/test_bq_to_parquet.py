import pytest

from gfw.common.cli import CLI

from gfw.ops.cli.commands.bq_to_parquet import BqToParquet


def test_dry_run(tmp_path):
    schema_file = tmp_path / "schema.json"
    schema_file.write_text("[]")
    CLI(subcommands=[BqToParquet]).execute([
        "bq-to-parquet",
        "--bq-in", "proj.ds.table",
        "--gcs-out", "gs://bucket/output",
        "--schema-file", str(schema_file),
        "--date-range", "2024-01-01", "2024-01-31",
        "--dry-run",
    ])


def test_help():
    with pytest.raises(SystemExit) as exc:
        CLI(subcommands=[BqToParquet]).execute(["bq-to-parquet", "--help"])
    assert exc.value.code == 0
