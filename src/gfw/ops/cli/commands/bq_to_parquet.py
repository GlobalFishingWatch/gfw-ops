"""CLI command for exporting a BigQuery table to hive-partitioned Parquet files on GCS."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option

from gfw.ops.pipelines import bq_to_parquet as pipeline


_DESCRIPTION = "Exports data from a BigQuery table to hive-partitioned Parquet files on GCS."

HELP_PROJECT = "GCP project for billing and schema fetching."
HELP_BQ_IN = "Fully-qualified source BigQuery table (project.dataset.table)."
HELP_GCS_OUT = "GCS output path prefix (gs://bucket/path)."
HELP_SCHEMA_FILE = "Path to a BigQuery JSON schema. If None, the schema is fetched from the table."
HELP_START_DATE = "Start date to export, inclusive (YYYY-MM-DD)."
HELP_END_DATE = "End date to export, exclusive (YYYY-MM-DD)."
HELP_TIMESTAMP_FIELD = "Field used for windowing and date filtering."
HELP_PART_FIELDS = "Extra hive partition dimensions (field names from the row)."
HELP_PART_TIME = "Time partition granularity: hour or day."
HELP_PART_PREFIX = "Prefix applied to every partition key name in the output path."
HELP_WINDOW_SIZE = "Beam window size in seconds."
HELP_NUM_SHARDS = "Number of output files per partition per window."
HELP_RUNNER = "Beam runner: DirectRunner or DataflowRunner."
HELP_DRY_RUN = "Log the query and exit without writing."
HELP_EXTERNAL_TABLE = (
    "Fully-qualified BigQuery external table to create or replace after the pipeline runs "
    "(project.dataset.table). The table will point to the GCS output with hive partitioning."
)


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
            Option("--schema-file", type=str, required=False, help=HELP_SCHEMA_FILE),
            Option("--start-date", type=str, required=True, help=HELP_START_DATE),
            Option("--end-date", type=str, required=True, help=HELP_END_DATE),
            Option("--timestamp-field", type=str, default="timestamp", help=HELP_TIMESTAMP_FIELD),
            Option("--partition-fields", type=str, nargs="*", default=(), help=HELP_PART_FIELDS),
            Option("--partition-time", type=str, default="hour", help=HELP_PART_TIME),
            Option("--partition-prefix", type=str, default="event_", help=HELP_PART_PREFIX),
            Option("--window-size", type=int, default=3600, help=HELP_WINDOW_SIZE),
            Option("--num-shards", type=int, default=6, help=HELP_NUM_SHARDS),
            Option("--runner", type=str, default="DataflowRunner", help=HELP_RUNNER),
            Option("--dry-run", type=bool, default=False, help=HELP_DRY_RUN),
            Option("--external-table", type=str, required=False, help=HELP_EXTERNAL_TABLE),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> None:
        """Run the export."""
        pipeline.run(**vars(config), **kwargs)
