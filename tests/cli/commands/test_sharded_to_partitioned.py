from types import SimpleNamespace
from unittest.mock import MagicMock

from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.cli import CLI

from gfw.ops.cli.commands.sharded_to_partitioned import ShardedToPartitioned


_BASE_ARGS = [
    "sharded-to-partitioned",
    "--bq-in-sharded",
    "proj.ds.table_a",
    "proj.ds.table_b",
    "--bq-out-partitioned",
    "proj.ds.target",
    "--project",
    "proj",
    "--schema-file",
    "schema.json",
    "--start-date",
    "2023-01-01",
    "--end-date",
    "2023-02-01",
]


def test_run_no_pending():
    # Empty range (start == end) → no months to process, no BQ calls needed.
    args = [*_BASE_ARGS[:-1], "2023-01-01"]  # override --end to match --start
    cli = CLI(subcommands=[ShardedToPartitioned])
    cli.execute(args, bq_client_factory=BigQueryHelper.get_client_factory(mocked=True))


def test_run_dry_run():
    col_row = SimpleNamespace(column_name="ts")
    client = MagicMock()
    client.query.return_value.result.side_effect = [
        [],  # _compute_pending (no existing partitions)
        [col_row],  # discover_columns table_a
        [col_row],  # discover_columns table_b
    ]

    cli = CLI(subcommands=[ShardedToPartitioned])
    cli.execute(
        [*_BASE_ARGS, "--dry-run"],
        bq_client_factory=lambda project: client,
    )

    client.create_table.assert_not_called()


def test_run_migration():
    col_row = SimpleNamespace(column_name="ts")
    client = MagicMock()
    client.query.return_value.result.side_effect = [
        [],  # _compute_pending (no existing partitions)
        [col_row],  # discover_columns table_a
        [col_row],  # discover_columns table_b
        None,  # _process_month insert
    ]
    client.query.return_value.total_bytes_processed = 0
    client.schema_from_json.return_value = []

    cli = CLI(subcommands=[ShardedToPartitioned])
    cli.execute(_BASE_ARGS, bq_client_factory=lambda project: client)

    client.create_table.assert_called_once()
