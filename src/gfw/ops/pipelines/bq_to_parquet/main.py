"""Export a date range from a BigQuery table to hive-partitioned Parquet files on GCS."""
from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from typing import Any, Callable

from cloudpathlib import GSPath
from google.cloud import bigquery, storage

from gfw.ops.pipelines.bq_to_parquet.destination import HiveDestination
from gfw.ops.pipelines.bq_to_parquet.job import ExportJob, ExportJobResults
from gfw.ops.pipelines.bq_to_parquet.source import Source


logger = logging.getLogger(__name__)


def _date_range(start_date: str, end_date: str) -> list[datetime.date]:
    # TODO: move this to gfw-common.
    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)
    dates: list[datetime.date] = []
    current = start
    while current < end:
        dates.append(current)
        current += datetime.timedelta(days=1)

    return dates


@dataclass
class Exporter:
    """Orchestrates BQ extract jobs for a source/destination pair."""

    bq_client: bigquery.Client
    source: Source
    destination: HiveDestination

    def run(self, dates: list[datetime.date]) -> ExportJobResults:
        """Submit extract jobs for dates with no existing output and wait for completion."""
        pending = [self.submit_job(date) for date in self.remaining_dates(dates)]
        return ExportJobResults([job.wait() for job in pending])

    def submit_job(self, date: datetime.date) -> ExportJob:
        """Submit a BQ extract job for a single date and return immediately."""
        bq_reference = self.source.ref(date)
        gcs_reference = self.destination.uri(date)

        job = self.bq_client.extract_table(
            bq_reference,
            gcs_reference,
            job_config=self.destination.extract_job_config
        )
        logger.info(f"Submitted {job.job_id}: {bq_reference} -> {gcs_reference}")
        return ExportJob(date=date, job=job)

    def remaining_dates(self, dates: list[datetime.date]) -> list[datetime.date]:
        exported = self.destination.existing_dates(dates)
        return [d for d in dates if d not in exported]


def run(
    bq_in: str,
    gcs_out: str,
    start_date: str,
    end_date: str,
    project: str,
    event_source: str,
    sharded: bool = False,
    partition_prefix: str = "event_",
    dry_run: bool = False,
    bq_client_factory: Callable[[str], bigquery.Client] = bigquery.Client,
    gcs_client_factory: Callable[[str], storage.Client] = storage.Client,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict | None = None,
    **kwargs: Any,
) -> None:
    """Export a date range from a BigQuery table to hive-partitioned Parquet files on GCS.

    Submits one BQ extract job per day for dates with no existing output, then waits for
    completion. Dates that already have files in GCS are skipped, making Airflow retries safe.
    Output path: ``{gcs_out}/{prefix}source={event_source}/{prefix}date=YYYY-MM-DD/*.parquet``.

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
            GCP project used for billing.

        event_source:
            Value written as the ``{prefix}source`` hive partition key in the output path
            (e.g. ``"wf827-pipe-nmea-parsed"``).

        sharded:
            Set to ``True`` for date-sharded tables (``table_YYYYMMDD``). Each shard is
            addressed directly. Missing shards are skipped with a warning.

        partition_prefix:
            Prefix applied to partition key names in the hive output path.
            Defaults to ``"event_"``, producing keys like ``event_source`` and ``event_date``.

        dry_run:
            Log the planned exports and return without submitting any jobs.

        bq_client_factory:
            Callable used to instantiate the BQ client. Override in tests to inject a mock.
            Defaults to :class:`google.cloud.bigquery.Client`.

        gcs_client_factory:
            Callable used to instantiate the GCS client. Override in tests to inject a mock.
            Defaults to :class:`google.cloud.storage.Client`.

        unknown_unparsed_args:
            Extra unparsed CLI args (ignored).

        unknown_parsed_args:
            Extra parsed args (ignored).
    """
    dates = _date_range(start_date, end_date)
    source = Source.create(bq_in, sharded)
    destination = HiveDestination(
        gcs_out=GSPath(gcs_out),
        event_source=event_source,
        partition_prefix=partition_prefix,
        gcs_client=gcs_client_factory(project=project),
    )

    logger.info(
        f"Exporting {bq_in} for [{start_date}, {end_date}) ({len(dates)} days) to {gcs_out}"
    )

    if dry_run:
        for date in dates:
            logger.info(f"[dry-run] {source.ref(date)} -> {destination.uri(date)}")

        return

    exporter = Exporter(
        bq_client=bq_client_factory(project=project),
        source=source,
        destination=destination,
    )

    results = exporter.run(dates)

    if results.failed:
        raise RuntimeError(
            f"Export failed for {len(results.failed)} date(s): "
            f"{[job.date for job in results.failed]}. "
            "Retrying this task will skip already-exported dates and resume from the failures."
        )

    logger.info(
        f"Done: {len(results.succeeded)} exported, {len(results.skipped)} skipped (not found)"
    )
