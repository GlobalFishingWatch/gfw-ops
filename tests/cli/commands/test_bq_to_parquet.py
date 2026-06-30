import pytest
from unittest.mock import MagicMock

from gfw.common.cli import CLI

from gfw.ops.cli.commands.bq_to_parquet import BqToParquet


def test_dry_run():
    CLI(subcommands=[BqToParquet]).execute(
        [
            "bq-to-parquet",
            "--project", "proj",
            "--bq-in", "proj.ds.table",
            "--gcs-out", "gs://bucket/output",
            "--event-source", "wf827-table",
            "--start-date", "2024-01-01",
            "--end-date", "2024-02-01",
            "--dry-run",
        ],
        gcs_client_factory=lambda project: MagicMock(),
    )


def test_help():
    with pytest.raises(SystemExit) as exc:
        CLI(subcommands=[BqToParquet]).execute(["bq-to-parquet", "--help"])
    assert exc.value.code == 0
