"""Create a BigQuery external table backed by hive-partitioned files on GCS."""
from __future__ import annotations

import logging
from typing import Callable

from google.cloud import bigquery as bq_lib

from gfw.common.bigquery import BigQueryHelper, Schema
from gfw.ops.version import __version__


logger = logging.getLogger(__name__)

_FORMAT_GLOB = {
    "PARQUET": "*.parquet",
    "ORC": "*.orc",
    "AVRO": "*.avro",
    "CSV": "*.csv",
    "NEWLINE_DELIMITED_JSON": "*.json",
}


def run(
    gcs_path: str,
    external_table: str,
    project: str,
    reference: str | None = None,
    schema_file: str | None = None,
    source_format: str = bq_lib.ExternalSourceFormat.PARQUET,
    bq_client_factory: Callable = BigQueryHelper.get_client_factory(),
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict | None = None,
) -> None:
    """Create or replace a BigQuery external table backed by hive-partitioned files on GCS.

    Schema and description can be sourced either from a reference BigQuery table or a
    local schema file. Exactly one of ``reference`` or ``schema_file`` must be provided.

    Args:
        gcs_path:
            GCS path prefix where the hive-partitioned files are stored
            (e.g. ``gs://bucket/path``).

        external_table:
            Fully-qualified BigQuery external table to create or replace
            (``project.dataset.table``).

        project:
            GCP project for the BigQuery client.

        reference:
            Fully-qualified BigQuery table (project.dataset.table) used to fetch the
            schema (with field descriptions) and table description. Takes precedence
            over ``schema_file`` when both are provided.

        schema_file:
            Path to a BigQuery JSON schema file. Used when no reference table is
            available. Table description will be auto-generated.

        source_format:
            BigQuery external source format (e.g. ``PARQUET``, ``ORC``, ``CSV``).
            Defaults to ``PARQUET``.

        bq_client_factory:
            Injectable factory for :class:`~gfw.common.bigquery.BigQueryHelper`.
            Useful for testing.
    """
    if source_format.upper() not in _FORMAT_GLOB:
        raise ValueError(
            f"Unsupported source format: {source_format!r}. "
            f"Must be one of: {', '.join(sorted(_FORMAT_GLOB))}."
        )
    if reference is None and schema_file is None:
        raise ValueError("At least one of --reference or --schema-file must be provided.")

    bq = BigQueryHelper(project=project, client_factory=bq_client_factory)

    default_description = (
        f"External table created with gfw-ops {__version__} pointing to {gcs_path}."
    )

    if reference is not None:
        source_table = bq.client.get_table(reference)
        bq_schema = Schema(list(source_table.schema))
        description = source_table.description or default_description
    else:
        bq_schema = Schema.from_json(schema_file)
        description = default_description

    glob = _FORMAT_GLOB.get(source_format.upper(), "*")
    logger.info(f"Creating external table {external_table} pointing to {gcs_path}")
    bq.create_external_table(
        table=external_table,
        source_uris=[f"{gcs_path}/{glob}"],
        hive_partition_uri_prefix=gcs_path,
        schema=bq_schema.fields,
        description=description,
        source_format=source_format,
        replace=True,
    )
    logger.info("Done.")
