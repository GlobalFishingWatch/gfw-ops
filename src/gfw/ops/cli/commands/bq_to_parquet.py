"""CLI command for exporting a BigQuery table to hive-partitioned Parquet files on GCS."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option

from gfw.ops.pipelines import bq_to_parquet as pipeline


_DESCRIPTION = "Exports data from a BigQuery table to hive-partitioned Parquet files on GCS."

HELP_PROJECT = "GCP project for billing."
HELP_BQ_IN = "Fully-qualified source BigQuery table (project.dataset.table)."
HELP_GCS_OUT = "GCS output path prefix (gs://bucket/path)."
HELP_START_DATE = "Start date to export, inclusive (YYYY-MM-DD)."
HELP_END_DATE = "End date to export, exclusive (YYYY-MM-DD)."
HELP_EVENT_SOURCE = "Value for the event_source hive partition key in the output path."
HELP_SHARDED = "Source is a date-sharded table (table_YYYYMMDD). Missing shards are skipped."
HELP_PART_PREFIX = "Prefix applied to partition key names in the hive output path."
HELP_DRY_RUN = "Log planned exports and exit without submitting jobs."


class BqToParquet(Command):
    """Export a date range from a BigQuery table to hive-partitioned Parquet files on GCS."""

    @property
    def name(self) -> str:
        """Command name."""
        return "bq-to-parquet"

    @property
    def description(self) -> str:
        """Command description."""
        return _DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """Command options."""
        return [
            Option("--project", type=str, required=True, help=HELP_PROJECT),
            Option("--bq-in", type=str, required=True, help=HELP_BQ_IN),
            Option("--gcs-out", type=str, required=True, help=HELP_GCS_OUT),
            Option("--start-date", type=str, required=True, help=HELP_START_DATE),
            Option("--end-date", type=str, required=True, help=HELP_END_DATE),
            Option("--event-source", type=str, required=True, help=HELP_EVENT_SOURCE),
            Option("--sharded", type=bool, default=False, help=HELP_SHARDED),
            Option("--partition-prefix", type=str, default="event_", help=HELP_PART_PREFIX),
            Option("--dry-run", type=bool, default=False, help=HELP_DRY_RUN),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> None:
        """Run the export."""
        pipeline.run(**vars(config), **kwargs)
