"""CLI command for creating a BigQuery external table backed by hive-partitioned files on GCS."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option

from gfw.ops.pipelines import create_external_table as pipeline


_DESCRIPTION = (
    "Create or replace a BigQuery external table backed by hive-partitioned files on GCS. "
    "Schema can be sourced from a reference BigQuery table or a local schema file."
)

HELP_PROJECT = "GCP project for the BigQuery client."
HELP_GCS_PATH = "GCS path prefix where hive-partitioned files are stored (gs://bucket/path)."
HELP_EXTERNAL_TABLE = (
    "Fully-qualified BigQuery external table to create or replace (project.dataset.table)."
)
HELP_REFERENCE = (
    "Fully-qualified BigQuery table (project.dataset.table) used to fetch schema and description. "
    "Takes precedence over --schema-file when both are provided."
)
HELP_SCHEMA_FILE = "Path to a BigQuery JSON schema. Used when no reference table is available."
HELP_SOURCE_FORMAT = (
    "BigQuery external source format (PARQUET, ORC, AVRO, CSV, NEWLINE_DELIMITED_JSON). "
    "Defaults to PARQUET."
)


class CreateExternalTable(Command):
    """Create or replace a BigQuery external table backed by hive-partitioned files on GCS."""

    @property
    def name(self) -> str:
        """Command name."""
        return "create-external-table"

    @property
    def description(self) -> str:
        """Command description."""
        return _DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """Command options."""
        return [
            Option("--project", type=str, required=True, help=HELP_PROJECT),
            Option("--gcs-path", type=str, required=True, help=HELP_GCS_PATH),
            Option("--external-table", type=str, required=True, help=HELP_EXTERNAL_TABLE),
            Option("--reference", type=str, required=False, help=HELP_REFERENCE),
            Option("--schema-file", type=str, required=False, help=HELP_SCHEMA_FILE),
            Option("--source-format", type=str, default="PARQUET", help=HELP_SOURCE_FORMAT),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> None:
        """Run the external table creation."""
        pipeline.run(**vars(config), **kwargs)
