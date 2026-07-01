"""Compact small hive-partitioned Parquet files on GCS into larger files."""
from __future__ import annotations

import datetime
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import gcsfs
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


def _duckdb_conn(memory_limit: str = "8GB", threads: int = 4) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection authenticated against GCS via application default credentials."""
    conn = duckdb.connect()
    conn.execute(f"SET memory_limit='{memory_limit}'")
    conn.execute(f"SET threads={threads}")
    conn.execute("SET preserve_insertion_order=false")
    conn.register_filesystem(gcsfs.GCSFileSystem())
    return conn


@dataclass
class Compactor:
    """Compacts hive-partitioned Parquet files for a given source and date range.

    When ``gcs_staging_path`` is ``None`` (the default), operates in *swap* mode: DuckDB
    writes compacted output to an auto-generated staging sibling path, the originals are
    deleted, and the staged files are moved back so the external table path never changes.
    If the process is interrupted between the delete and copy steps, the next run detects
    the staging files and resumes automatically.

    When ``gcs_staging_path`` is set, operates in *copy* mode: compacted files are written
    directly to ``gcs_staging_path`` without touching the source. Use this when you want
    to keep the original uncompacted files alongside the compacted output (e.g. for
    benchmarking both versions via separate external tables).
    """

    gcs_client: storage.Client
    gcs_output_path: GSPath
    event_source: str
    partition_prefix: str
    target_file_size_mb: int
    gcs_staging_path: Optional[GSPath] = None
    conn_factory: Callable[[], duckdb.DuckDBPyConnection] = _duckdb_conn
    swap: bool = field(init=False)

    def __post_init__(self):
        self.swap = False
        if self.gcs_staging_path is None:
            self.gcs_staging_path = self.default_staging_path
            self.swap = True

    @property
    def default_staging_path(self) -> GSPath:
        return self.gcs_output_path.parent / f"_compact_{self.gcs_output_path.name}_staging"

    def run(self, dates: list[datetime.date]) -> None:
        """Compact each date partition, resuming any interrupted swaps first."""
        total = len(dates)
        logger.info(f"Starting compaction: {total} date(s) to process")
        for i, date in enumerate(dates, 1):
            logger.info(f"[{i}/{total}] {date}")
            self._compact(date)

        logger.info(f"Finished compaction: {total} date(s) processed")

    def _partition_path(self, base: GSPath, date: datetime.date) -> GSPath:
        p = self.partition_prefix
        return base / f"{p}source={self.event_source}" / f"{p}date={date.isoformat()}"

    def _list_parquet_blobs(self, base: GSPath, date: datetime.date) -> list[storage.Blob]:
        part = self._partition_path(base, date)
        blobs = self.gcs_client.list_blobs(part.bucket, prefix=f"{part.blob}/")
        return [b for b in blobs if b.name.endswith(".parquet")]

    def _compact(self, date: datetime.date) -> None:
        source_blobs = self._list_parquet_blobs(self.gcs_output_path, date)
        dest_blobs = self._list_parquet_blobs(self.gcs_staging_path, date)

        if self.swap and dest_blobs and not source_blobs:
            # Interrupted between delete-originals and copy-from-staging: resume the swap.
            logger.warning(
                f"Resuming interrupted swap for {date}: "
                f"{len(dest_blobs)} staged file(s) found, originals already deleted"
            )
            self._copy_to_partition(date, dest_blobs, self.gcs_output_path)
            self._delete_blobs(dest_blobs)
            return

        if not source_blobs:
            logger.info(f"No source files for {date}, skipping")
            return

        if self.swap and len(source_blobs) == 1:
            logger.info(f"Skipping {date}: already a single file")
            return

        if dest_blobs:
            logger.info(f"Removing {len(dest_blobs)} existing file(s) from dest for {date}")
            self._delete_blobs(dest_blobs)

        source_part = self._partition_path(self.gcs_output_path, date)
        source_uris = [f"gs://{source_part.bucket}/{b.name}" for b in source_blobs]
        total_mb = sum(b.size for b in source_blobs) / _MB

        logger.info(
            f"Compacting {len(source_uris)} file(s) for {date} ({total_mb:.0f} MB compressed)"
        )

        t0 = time.monotonic()
        written = self._write_compacted(date, source_uris, self.gcs_staging_path)
        logger.info(f"DuckDB done in {time.monotonic() - t0:.1f}s — {len(written)} file(s)")

        if self.swap:
            # Originals are deleted before the copy so we never have duplicate data visible
            # to the external table. The staging files serve as the recovery point if the
            # copy step is interrupted.
            t0 = time.monotonic()
            self._delete_blobs(source_blobs)
            self._copy_to_partition(date, written, self.gcs_output_path)
            self._delete_blobs(written)
            logger.info(f"Swap done in {time.monotonic() - t0:.1f}s")

    def _write_compacted(
        self, date: datetime.date, source_uris: list[str], dest_path: GSPath
    ) -> list[storage.Blob]:
        """Compact source files into dest_path using DuckDB."""
        dest_part = self._partition_path(dest_path, date)
        dest_dir = f"gs://{dest_part.bucket}/{dest_part.blob}"
        files_sql = "[" + ", ".join(f"'{u}'" for u in source_uris) + "]"
        target_bytes = self.target_file_size_mb * _MB

        conn = self.conn_factory()
        logger.info(f"Writing compacted output to {dest_dir}")
        conn.execute(
            f"""
            COPY (SELECT * FROM read_parquet({files_sql}))
            TO '{dest_dir}'
            (
                FORMAT PARQUET,
                COMPRESSION SNAPPY,
                FILE_SIZE_BYTES {target_bytes},
                PER_THREAD_OUTPUT false
            )
        """
        )
        conn.close()

        written = self._list_parquet_blobs(dest_path, date)
        logger.info(f"Wrote {len(written)} file(s) to {dest_dir}")
        return written

    def _copy_to_partition(
        self, date: datetime.date, blobs: list[storage.Blob], dest_path: GSPath
    ) -> None:
        dest_part = self._partition_path(dest_path, date)
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
    gcs_output_path: str,
    event_source: str,
    start_date: str,
    end_date: str,
    partition_prefix: str = "event_",
    target_file_size_mb: int = 512,
    memory_limit: str = "8GB",
    gcs_staging_path: str | None = None,
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

        {gcs_output_path}/{prefix}source={event_source}/{prefix}date=YYYY-MM-DD/*.parquet

    Staging path defaults to ``gs://{bucket}/{parent}/_compact_{table}_staging``
    (sibling of the table directory, same bucket so copies are server-side).

    Args:
        project:
            GCP project used for billing.

        gcs_output_path:
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

        gcs_staging_path:
            When ``None`` (default), operates in swap mode: compacted files are written
            to an auto-generated staging sibling path, the originals are deleted, and the
            staged files are moved back in-place. When set, operates in copy mode: compacted
            files are written to this path and the source files are left untouched, giving
            you both uncompacted and compacted paths for separate external tables.

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
    source = GSPath(gcs_output_path.rstrip("/"))
    dates = _date_range(start_date, end_date)

    logger.info(f"Compacting {gcs_output_path} for [{start_date}, {end_date}) ({len(dates)} days)")

    if dry_run:
        for date in dates:
            p = partition_prefix
            logger.info(f"[dry-run] {gcs_output_path}/{p}source={event_source}/{p}date={date}")
        return

    resolved_conn_factory = conn_factory or (lambda: _duckdb_conn(memory_limit=memory_limit))
    gcs_client = gcs_client_factory(project=project)
    compactor = Compactor(
        gcs_client=gcs_client,
        gcs_output_path=source,
        event_source=event_source,
        partition_prefix=partition_prefix,
        target_file_size_mb=target_file_size_mb,
        gcs_staging_path=GSPath(gcs_staging_path.rstrip("/")) if gcs_staging_path else None,
        conn_factory=resolved_conn_factory,
    )
    compactor.run(dates)
