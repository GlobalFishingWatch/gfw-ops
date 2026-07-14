import pytest
from unittest.mock import MagicMock

from gfw.common.cli import CLI

from gfw.ops.cli.commands.compact_parquet import CompactParquet


def test_dry_run():
    CLI(subcommands=[CompactParquet]).execute(
        [
            "compact-parquet",
            "--project", "proj",
            "--gcs-input-path", "gs://bucket/messages",
            "--event-source", "src",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-02",
            "--dry-run",
        ],
        gcs_client_factory=lambda project: MagicMock(),
        conn_factory=MagicMock,
    )


def test_help():
    with pytest.raises(SystemExit) as exc:
        CLI(subcommands=[CompactParquet]).execute(["compact-parquet", "--help"])
    assert exc.value.code == 0
