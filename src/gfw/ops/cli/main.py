"""GFW operational tools CLI."""

from __future__ import annotations

import sys

from gfw.common.cli import CLI
from gfw.common.logging import LoggerConfig

from gfw.ops.cli.commands.benchmark_parquet import BenchmarkParquet
from gfw.ops.cli.commands.bq_to_parquet import BqToParquet
from gfw.ops.cli.commands.compact_parquet import CompactParquet
from gfw.ops.cli.commands.create_external_table import CreateExternalTable
from gfw.ops.cli.commands.sharded_to_partitioned import ShardedToPartitioned
from gfw.ops.version import __version__


def run(args: list[str]) -> None:
    """Entry point for the gfw-ops CLI."""
    CLI(
        name="gfw-ops",
        description="GFW operational data engineering tools.",
        subcommands=[
            ShardedToPartitioned,
            BqToParquet,
            CompactParquet,
            CreateExternalTable,
            BenchmarkParquet,
        ],
        version=__version__,
        allow_unknown=True,
        examples=(
            "gfw-ops sharded-to-partitioned --help",
            (
                "gfw-ops sharded-to-partitioned"
                " --bq-in-sharded project.dataset.sharded"
                " --bq-out-partitioned project.dataset.consolidated"
                " --project my-project"
                " --dry-run"
            ),
        ),
        logger_config=LoggerConfig(
            warning_level=[
                "urllib3",
            ],
        ),
    ).execute(args)


def main() -> None:
    """CLI entry point."""
    run(sys.argv[1:])


if __name__ == "__main__":
    main()
