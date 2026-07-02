"""Compact small hive-partitioned Parquet files on GCS into larger files."""
from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from typing import Any, Callable

import duckdb
from cloudpathlib import GSPath
from google.cloud import storage


logger = logging.getLogger(__name__)

_MB = 1024 * 1024


def _date_range(start_date: str, end_date: str) -> list[datetime.date]:
    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)
    dates: list[datetime.date] = []
    current = start
    while current < end:
        dates.append(current)
        current += datetime.timedelta(days=1)
    return dates


def _duckdb_conn(memory_limit: str = "8GB") -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection authenticated against GCS via application default credentials."""
    import gcsfs

    conn = duckdb.connect()
    conn.execute(f"SET memory_limit='{memory_limit}'")
    conn.register_filesystem(gcsfs.GCSFileSystem())
    return conn


@dataclass
class Compactor:
    """Compacts hive-partitioned Parquet files for a given source and date range.

    Uses DuckDB to read all source files in parallel and write compacted output
    to a staging path. The staging files are then swapped into the original
    partition path so the external table path never changes. If the process is
    interrupted between the delete and copy steps, the next run detects the
    staging files and resumes the swap automatically.
    """

    gcs_client: storage.Client
    gcs_path: GSPath
    staging_path: GSPath
    event_source: str
    partition_prefix: str
    target_file_size_mb: int
    conn_factory: Callable[[], duckdb.DuckDBPyConnection] = _duckdb_conn

    def run(self, dates: list[datetime.date]) -> None:
        """Compact each date partition, resuming any interrupted swaps first."""
        for date in dates:
            self._compact(date)

    def _partition_path(self, base: GSPath, date: datetime.date) -> GSPath:
        p = self.partition_prefix
        return base / f"{p}source={self.event_source}" / f"{p}date={date.isoformat()}"

    def _list_parquet_blobs(self, base: GSPath, date: datetime.date) -> list[storage.Blob]:
        part = self._partition_path(base, date)
        blobs = self.gcs_client.list_blobs(part.bucket, prefix=f"{part.blob}/")
        return [b for b in blobs if b.name.endswith(".parquet")]

    def _compact(self, date: datetime.date) -> None:
        source_blobs = self._list_parquet_blobs(self.gcs_path, date)
        staging_blobs = self._list_parquet_blobs(self.staging_path, date)

        if staging_blobs and not source_blobs:
            # Interrupted between delete-originals and copy-from-staging: resume the swap.
            logger.warning(
                f"Resuming interrupted swap for {date}: "
                f"{len(staging_blobs)} staged file(s) found, originals already deleted"
            )
            self._copy_to_partition(date, staging_blobs)
            self._delete_blobs(staging_blobs)
            return

        if not source_blobs:
            logger.info(f"No source files for {date}, skipping")
            return

        if len(source_blobs) == 1:
            logger.info(f"Skipping {date}: already a single file")
            return

        if staging_blobs:
            logger.warning(
                f"Cleaning up {len(staging_blobs)} leftover staging file(s) for {date}"
            )
            self._delete_blobs(staging_blobs)

        source_part = self._partition_path(self.gcs_path, date)
        source_uris = [f"gs://{source_part.bucket}/{b.name}" for b in source_blobs]
        total_mb = sum(b.size for b in source_blobs) / _MB

        logger.info(
            f"Compacting {len(source_uris)} file(s) for {date} "
            f"({total_mb:.0f} MB compressed)"
        )
        staged_blobs = self._write_compacted_to_staging(date, source_uris)

        # Originals are deleted before the copy so we never have duplicate data visible
        # to the external table. The staging files serve as the recovery point if the
        # copy step is interrupted.
        self._delete_blobs(source_blobs)
        self._copy_to_partition(date, staged_blobs)
        self._delete_blobs(staged_blobs)

    def _write_compacted_to_staging(
        self, date: datetime.date, source_uris: list[str]
    ) -> list[storage.Blob]:
        """Compact source files into the staging path using DuckDB.

        DuckDB reads all source files in parallel, splits output at
        target_file_size_mb boundaries, and writes directly to GCS.
        """
        staging_part = self._partition_path(self.staging_path, date)
        staging_dir = f"gs://{staging_part.bucket}/{staging_part.blob}"
        files_sql = "[" + ", ".join(f"'{u}'" for u in source_uris) + "]"
        target_bytes = self.target_file_size_mb * _MB

        conn = self.conn_factory()
        logger.info(f"Writing compacted output to {staging_dir}")
        conn.execute(f"""
            COPY (SELECT * FROM read_parquet({files_sql}))
            TO '{staging_dir}'
            (FORMAT PARQUET, COMPRESSION SNAPPY, FILE_SIZE_BYTES {target_bytes})
        """)
        conn.close()

        staged = self._list_parquet_blobs(self.staging_path, date)
        logger.info(f"Staged {len(staged)} file(s) to {staging_dir}")
        return staged

    def _copy_to_partition(self, date: datetime.date, blobs: list[storage.Blob]) -> None:
        dest_part = self._partition_path(self.gcs_path, date)
        bucket = self.gcs_client.bucket(dest_part.bucket)
        for blob in blobs:
            filename = blob.name.rsplit("/", 1)[-1]
            dest_name = f"{dest_part.blob}/{filename}"
            bucket.copy_blob(blob, bucket, new_name=dest_name)
            logger.info(f"Copied to gs://{dest_part.bucket}/{dest_name}")

    def _delete_blobs(self, blobs: list[storage.Blob]) -> None:
        for blob in blobs:
            blob.delete()
            logger.info(f"Deleted gs://{blob.bucket.name}/{blob.name}")


def run(
    project: str,
    gcs_path: str,
    event_source: str,
    start_date: str,
    end_date: str,
    partition_prefix: str = "event_",
    target_file_size_mb: int = 512,
    memory_limit: str = "8GB",
    staging_path: str | None = None,
    dry_run: bool = False,
    gcs_client_factory: Callable[[str], storage.Client] = storage.Client,
    conn_factory: Callable[[], duckdb.DuckDBPyConnection] | None = None,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict | None = None,
    **kwargs: Any,
) -> None:
    """Compact small hive-partitioned Parquet files on GCS into larger files.

    For each date partition, DuckDB reads all source files in parallel and writes
    compacted output to a staging path. The staged files are then swapped into the
    original partition path so the external table path never changes. If the process
    is interrupted after deleting originals, the next run detects the staged files
    and resumes the copy step automatically.

    Path structure::

        {gcs_path}/{prefix}source={event_source}/{prefix}date=YYYY-MM-DD/*.parquet

    Staging path defaults to ``gs://{bucket}/{parent}/_compact_{table}_staging``
    (sibling of the table directory, same bucket so copies are server-side).

    Args:
        project:
            GCP project used for billing.

        gcs_path:
            GCS path prefix (gs://bucket/path) of the hive-partitioned files.

        event_source:
            Value of the ``{prefix}source`` hive partition key
            (e.g. ``"wf827-pipe-nmea-parsed"``).

        start_date:
            Start date, inclusive (YYYY-MM-DD).

        end_date:
            End date, exclusive (YYYY-MM-DD).

        partition_prefix:
            Prefix applied to partition key names. Defaults to ``"event_"``.

        target_file_size_mb:
            Target size for each output file in MB. Defaults to 512.

        staging_path:
            GCS path used for staging compacted files before the swap. Defaults to
            ``gs://{bucket}/{parent}/_compact_{table}_staging`` (sibling of the table
            directory, same bucket so copies are server-side).

        dry_run:
            Log planned compaction and exit without modifying files.

        gcs_client_factory:
            Injectable factory for :class:`~google.cloud.storage.Client`.

        memory_limit:
            DuckDB memory cap (e.g. ``"8GB"``). DuckDB spills to disk beyond this limit.
            Defaults to ``"8GB"``. Set lower on machines with less RAM, higher on GKE pods.

        conn_factory:
            Injectable factory for a configured DuckDB connection. Defaults to
            :func:`_duckdb_conn` which authenticates via application default credentials.
            Override in tests to avoid real GCS calls.

        unknown_unparsed_args:
            Extra unparsed CLI args (ignored).

        unknown_parsed_args:
            Extra parsed args (ignored).
    """
    source = GSPath(gcs_path.rstrip("/"))
    if staging_path is None:
        staging_path = str(source.parent / f"_compact_{source.name}_staging")

    staging = GSPath(staging_path)

    dates = _date_range(start_date, end_date)

    logger.info(
        f"Compacting {gcs_path} for [{start_date}, {end_date}) ({len(dates)} days)"
    )

    if dry_run:
        for date in dates:
            p = partition_prefix
            logger.info(
                f"[dry-run] {gcs_path}/{p}source={event_source}/{p}date={date}"
            )
        return

    resolved_conn_factory = conn_factory or (lambda: _duckdb_conn(memory_limit=memory_limit))
    gcs_client = gcs_client_factory(project=project)
    compactor = Compactor(
        gcs_client=gcs_client,
        gcs_path=source,
        staging_path=staging,
        event_source=event_source,
        partition_prefix=partition_prefix,
        target_file_size_mb=target_file_size_mb,
        conn_factory=resolved_conn_factory,
    )
    compactor.run(dates)
