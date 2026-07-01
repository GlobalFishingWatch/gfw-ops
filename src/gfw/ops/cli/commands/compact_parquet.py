"""CLI command for compacting hive-partitioned Parquet files on GCS."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option

from gfw.ops.pipelines import compact_parquet as pipeline


_DESCRIPTION = "Compacts small hive-partitioned Parquet files on GCS in-place into larger files."

HELP_PROJECT = "GCP project for billing."
HELP_GCS_OUTPUT_PATH = "GCS path prefix (gs://bucket/path) of the hive-partitioned files."
HELP_EVENT_SOURCE = "Value for the {prefix}source hive partition key."
HELP_START_DATE = "Start date to compact, inclusive (YYYY-MM-DD)."
HELP_END_DATE = "End date to compact, exclusive (YYYY-MM-DD)."
HELP_PART_PREFIX = "Prefix applied to partition key names in the hive path."
HELP_TARGET_SIZE = "Target output file size in MB."
HELP_THREADS = "Number of DuckDB threads for parallel reads/writes."
HELP_MEMORY_LIMIT = "DuckDB memory cap (e.g. '8GB'). DuckDB spills to disk beyond this limit."
HELP_STAGING_PATH = (
    "When omitted, operates in swap mode: compacted files are staged to an auto-generated "
    "sibling path, then swapped in-place. When set, operates in copy mode: compacted files "
    "are written to this path and source files are left untouched."
)
HELP_DRY_RUN = "Log planned compaction and exit without modifying files."


class CompactParquet(Command):
    """Compact small hive-partitioned Parquet files on GCS in-place into larger files."""

    @property
    def name(self) -> str:
        """Command name."""
        return "compact-parquet"

    @property
    def description(self) -> str:
        """Command description."""
        return _DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """Command options."""
        return [
            Option("--project", type=str, required=True, help=HELP_PROJECT),
            Option("--gcs-output-path", type=str, required=True, help=HELP_GCS_OUTPUT_PATH),
            Option("--event-source", type=str, required=True, help=HELP_EVENT_SOURCE),
            Option("--start-date", type=str, required=True, help=HELP_START_DATE),
            Option("--end-date", type=str, required=True, help=HELP_END_DATE),
            Option("--partition-prefix", type=str, default="event_", help=HELP_PART_PREFIX),
            Option("--target-file-size-mb", type=int, default=512, help=HELP_TARGET_SIZE),
            Option("--memory-limit", type=str, default="12GB", help=HELP_MEMORY_LIMIT),
            Option("--threads", type=int, default=4, help=HELP_THREADS),
            Option("--gcs-staging-path", type=str, default=None, help=HELP_STAGING_PATH),
            Option("--dry-run", type=bool, default=False, help=HELP_DRY_RUN),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> None:
        """Run the compaction."""
        pipeline.run(**vars(config), **kwargs)
