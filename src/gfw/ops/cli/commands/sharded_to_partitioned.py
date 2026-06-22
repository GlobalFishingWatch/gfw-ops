"""CLI command for migrating date-sharded BigQuery tables into a partitioned table."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option

from gfw.ops.pipelines import sharded_to_partitioned as pipeline
from gfw.ops.pipelines.sharded_to_partitioned.main import CAVEAT


_DESCRIPTION = f"Migrate date-sharded BigQuery tables into a single partitioned table.\n\n{CAVEAT}"

HELP_BQ_IN_SHARDED = "Fully-qualified source sharded table names (project.dataset.table)."
HELP_BQ_OUT_PARTITIONED = "Fully-qualified target partitioned table name (project.dataset.table)."
HELP_PROJECT = "GCP project used to run BigQuery jobs."
HELP_SCHEMA_FILE = "Path to a JSON schema file."
HELP_START_DATE = "First month to process, inclusive (YYYYMM)."
HELP_END_DATE = "Last month to process, exclusive (YYYYMM)."
HELP_PARTITION_FIELD = "Field to partition the target table on."
HELP_PARTITION_TYPE = "Partitioning granularity: DAY, HOUR, MONTH or YEAR."
HELP_OVERWRITE = "Re-process already-written months."
HELP_DRY_RUN = "Log an example query and exit without writing."
HELP_LIMIT = "Process at most N months."


class ShardedToPartitioned(Command):
    """Migrate date-sharded BigQuery tables into a single partitioned table."""

    @property
    def name(self) -> str:
        """Command name."""
        return "sharded-to-partitioned"

    @property
    def description(self) -> str:
        """Command description."""
        return _DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """Command options."""
        return [
            Option("--bq-in-sharded", type=str, required=True, nargs="+", help=HELP_BQ_IN_SHARDED),
            Option("--bq-out-partitioned", type=str, required=True, help=HELP_BQ_OUT_PARTITIONED),
            Option("--project", type=str, required=True, help=HELP_PROJECT),
            Option("--schema-file", type=str, required=True, help=HELP_SCHEMA_FILE),
            Option("--start-date", type=str, required=True, help=HELP_START_DATE),
            Option("--end-date", type=str, required=True, help=HELP_END_DATE),
            Option("--partition-field", type=str, default="timestamp", help=HELP_PARTITION_FIELD),
            Option("--partition-type", type=str, default="DAY", help=HELP_PARTITION_TYPE),
            Option("--overwrite", type=bool, default=False, help=HELP_OVERWRITE),
            Option("--dry-run", type=bool, default=False, help=HELP_DRY_RUN),
            Option("--limit", type=int, default=0, help=HELP_LIMIT),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> None:
        """Run the migration."""
        pipeline.run(**vars(config), **kwargs)
