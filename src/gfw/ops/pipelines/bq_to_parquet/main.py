"""Export a date range from a BigQuery table to hive-partitioned Parquet files on GCS."""
from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any, Callable

import apache_beam as beam

from gfw.common.beam.pipeline import LinearDag, Pipeline
from gfw.common.beam.transforms import ReadFromBigQuery
from gfw.common.beam.transforms.parquet import (
    HivePartitionConfig,
    ParquetSink,
    WritePartitionedParquet,
)
from gfw.common.bigquery import BigQueryHelper, Schema
from gfw.ops.version import __version__


logger = logging.getLogger(__name__)


def _build_query(bq_in: str, start_date: str, end_date: str, timestamp_field: str) -> str:
    return (
        f"SELECT * FROM `{bq_in}` "
        f"WHERE DATE({timestamp_field}) >= '{start_date}'"
        f" AND DATE({timestamp_field}) < '{end_date}'"
    )


def _assign_timestamp(timestamp_field: str) -> Callable[[dict], beam.window.TimestampedValue]:
    def _fn(row: dict) -> beam.window.TimestampedValue:
        return beam.window.TimestampedValue(row, row[timestamp_field].timestamp())

    return _fn


def run(
    bq_in: str,
    gcs_out: str,
    start_date: str,
    end_date: str,
    project: str,
    schema_file: str | None = None,
    timestamp_field: str = "timestamp",
    partition_fields: Sequence[str] = (),
    partition_time: str = "hour",
    partition_prefix: str = "event_",
    window_size: int = 3600,
    num_shards: int = 6,
    dry_run: bool = False,
    external_table: str | None = None,
    read_from_bigquery_factory: Callable = ReadFromBigQuery.get_client_factory(),
    bq_client_factory: Callable = BigQueryHelper.get_client_factory(),
    parquet_sink_factory: Callable[..., ParquetSink] = ParquetSink,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict | None = None,
    **kwargs: Any,
) -> Pipeline:
    """Export a date range from a BigQuery table to hive-partitioned Parquet files on GCS.

    Args:
        bq_in:
            Fully-qualified BigQuery source table (project.dataset.table).

        gcs_out:
            GCS output path prefix (gs://bucket/path).

        start_date:
            Start date, inclusive (YYYY-MM-DD).

        end_date:
            End date, exclusive (YYYY-MM-DD).

        project:
            GCP project for schema fetching and Beam pipeline options.

        schema_file:
            Path to a BigQuery JSON schema file. If ``None``, the schema is
            fetched directly from the BigQuery table.

        timestamp_field:
            Field used for windowing and date filtering.

        partition_fields:
            Extra hive partition dimensions (field names from the row).

        partition_time:
            Time partition granularity: "hour" or "day".

        partition_prefix:
            Prefix applied to every partition key name in the output path.
            Defaults to ``"event_"``.

        window_size:
            Beam window size in seconds.

        num_shards:
            Output files per partition per window.

        dry_run:
            Build the pipeline and return it without executing.

        external_table:
            Fully-qualified BigQuery external table to create or replace after the
            pipeline runs (``project.dataset.table``). The table will point to
            ``gcs_out`` with hive partitioning enabled. If ``None``, skipped.

        read_from_bigquery_factory:
            Injectable factory for the BigQuery source — useful for testing.

        bq_client_factory:
            Injectable factory for :class:`~gfw.common.bigquery.BigQueryHelper` — used for
            schema fetching and external table creation. Useful for testing.

        parquet_sink_factory:
            Injectable :class:`~gfw.common.beam.transforms.ParquetSink` factory passed
            to :class:`~gfw.common.beam.transforms.WritePartitionedParquet`. Inject
            :class:`~gfw.common.beam.transforms.FakeParquetSink` in tests to bypass
            GCS writes while still exercising windowing and partitioning logic.

        unknown_unparsed_args:
            Extra unparsed CLI args forwarded to Beam.

        unknown_parsed_args:
            Extra parsed args forwarded to Beam.

        **kwargs:
            Additional keyword args forwarded to Beam PipelineOptions.
    """
    query = _build_query(bq_in, start_date, end_date, timestamp_field)

    logger.info(f"Exporting {bq_in} for [{start_date}, {end_date}) to {gcs_out}")
    logger.info(f"Query:\n{query}")

    bq = BigQueryHelper(project=project, client_factory=bq_client_factory)
    source_table = bq.client.get_table(bq_in)

    if schema_file is not None:
        bq_schema = Schema.from_json(schema_file)
    else:
        bq_schema = Schema(list(source_table.schema))

    partition = HivePartitionConfig(
        fields={f: lambda x: x for f in partition_fields},
        prefix=partition_prefix,
        time_granularity=partition_time,
    )

    dag = LinearDag(
        sources=(
            "ReadFromBigQuery"
            >> ReadFromBigQuery(
                query=query,
                read_from_bigquery_factory=read_from_bigquery_factory,
            ),
        ),
        core="AssignTimestamps" >> beam.Map(_assign_timestamp(timestamp_field)),
        sinks=(
            "WriteToParquet"
            >> WritePartitionedParquet(
                path=gcs_out,
                schema=bq_schema.as_pyarrow(),
                window_size=window_size,
                num_shards=num_shards,
                partition=partition,
                sink_factory=parquet_sink_factory,
            ),
        ),
    )

    beam_options = {**kwargs, **(unknown_parsed_args or {})}

    pipeline = Pipeline(
        name="bq-to-parquet",
        version=__version__,
        dag=dag,
        unparsed_args=unknown_unparsed_args,
        project=project,
        **beam_options,
    )

    if dry_run:
        return pipeline

    pipeline.run()

    if external_table is not None:
        logger.info(f"Creating external table {external_table} pointing to {gcs_out}")
        bq.create_external_table(
            table=external_table,
            source_uris=[f"{gcs_out}/**/*.parquet"],
            hive_partition_uri_prefix=gcs_out,
            schema=bq_schema.fields,
            description=source_table.description or "",
            replace=True,
        )

    return pipeline
