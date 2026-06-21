"""Export a date range from a BigQuery table to hive-partitioned Parquet files on GCS."""
from __future__ import annotations

import logging
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


def _build_query(bq_in: str, date_range: tuple[str, str], timestamp_field: str) -> str:
    start, end = date_range
    return (
        f"SELECT * FROM `{bq_in}` "
        f"WHERE DATE({timestamp_field}) BETWEEN '{start}' AND '{end}'"
    )


def _assign_timestamp(timestamp_field: str) -> Callable[[dict], beam.window.TimestampedValue]:
    def _fn(row: dict) -> beam.window.TimestampedValue:
        return beam.window.TimestampedValue(row, row[timestamp_field].timestamp())

    return _fn


def run(
    bq_in: str,
    gcs_out: str,
    date_range: tuple[str, str],
    schema_file: str | None = None,
    timestamp_field: str = "timestamp",
    partition_fields: list[str] | None = None,
    partition_time_granularity: str = "hour",
    partition_prefix: str = "event_",
    gcs_window_size: int = 3600,
    gcs_num_shards: int = 6,
    dry_run: bool = False,
    read_from_bigquery_factory: Callable = ReadFromBigQuery.get_client_factory(),
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

        date_range:
            Start and end date as (YYYY-MM-DD, YYYY-MM-DD).

        schema_file:
            Path to a BigQuery JSON schema file. If ``None``, the schema is
            fetched directly from the BigQuery table.

        timestamp_field:
            Field used for windowing and date filtering.

        partition_fields:
            Extra hive partition dimensions (field names from the row).

        partition_time_granularity:
            Time partition granularity: "hour" or "day".

        partition_prefix:
            Prefix applied to every partition key name in the output path.
            Defaults to ``"event_"``.

        gcs_window_size:
            Beam window size in seconds.

        gcs_num_shards:
            Output files per partition per window.

        dry_run:
            Build the pipeline and return it without executing.

        read_from_bigquery_factory:
            Injectable factory for the BigQuery source — useful for testing.

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
    query = _build_query(bq_in, date_range, timestamp_field)

    logger.info(f"Exporting {bq_in} for date range {date_range} to {gcs_out}")
    logger.info(f"Query:\n{query}")

    if schema_file is not None:
        schema = Schema.from_json(schema_file).as_pyarrow()
    else:
        schema = BigQueryHelper(project=kwargs.get("project")).fetch_schema(bq_in).as_pyarrow()

    partition = HivePartitionConfig(
        fields={f: lambda x: x for f in (partition_fields or [])},
        prefix=partition_prefix,
        time_granularity=partition_time_granularity,
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
                schema=schema,
                window_size=gcs_window_size,
                num_shards=gcs_num_shards,
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
        **beam_options,
    )

    if dry_run:
        return pipeline

    pipeline.run()
    return pipeline
