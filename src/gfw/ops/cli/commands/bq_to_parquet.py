"""CLI command for exporting a BigQuery table to hive-partitioned Parquet files on GCS."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option

from gfw.ops.pipelines import bq_to_parquet as pipeline


_DESCRIPTION = "Exports data from a BigQuery table to hive-partitioned Parquet files on GCS."

HELP_BQ_IN = "Fully-qualified source BigQuery table (project.dataset.table)."
HELP_GCS_OUT = "GCS output path prefix (gs://bucket/path)."
HELP_SCHEMA_FILE = "Path to a BigQuery JSON schema. If None, the schema is fetched from the table."
HELP_DATE_RANGE = "Start and end date to export (YYYY-MM-DD YYYY-MM-DD)."
HELP_TIMESTAMP_FIELD = "Field used for windowing and date filtering."
HELP_PARTITION_FIELDS = "Extra hive partition dimensions (field names from the row)."
HELP_PARTITION_TIME = "Time partition granularity: hour or day."
HELP_PARTITION_PREFIX = "Prefix applied to every partition key name in the output path."
HELP_GCS_WINDOW_SIZE = "Beam window size in seconds."
HELP_GCS_NUM_SHARDS = "Number of output files per partition per window."
HELP_RUNNER = "Beam runner: DirectRunner or DataflowRunner."
HELP_DRY_RUN = "Log the query and exit without writing."


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
            Option("--bq-in", type=str, required=True, help=HELP_BQ_IN),
            Option("--gcs-out", type=str, required=True, help=HELP_GCS_OUT),
            Option("--schema-file", type=str, required=False, help=HELP_SCHEMA_FILE),
            Option("--date-range", type=str, nargs=2, required=True, help=HELP_DATE_RANGE),
            Option("--timestamp-field", type=str, default="timestamp", help=HELP_TIMESTAMP_FIELD),
            Option("--partition-fields", type=str, nargs="*", help=HELP_PARTITION_FIELDS),
            Option("--partition-time", type=str, default="hour", help=HELP_PARTITION_TIME),
            Option("--partition-prefix", type=str, default="event_", help=HELP_PARTITION_PREFIX),
            Option("--gcs-window-size", type=int, default=3600, help=HELP_GCS_WINDOW_SIZE),
            Option("--gcs-num-shards", type=int, default=6, help=HELP_GCS_NUM_SHARDS),
            Option("--runner", type=str, default="DataflowRunner", help=HELP_RUNNER),
            Option("--dry-run", type=bool, default=False, help=HELP_DRY_RUN),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> None:
        """Run the export."""
        pipeline.run(**vars(config), **kwargs)
