"""Compact small hive-partitioned Parquet files on GCS into larger files."""
from __future__ import annotations

import datetime
import json
import logging
import time

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import duckdb
import gcsfs

from cloudpathlib import GSPath
from google.api_core.exceptions import NotFound
from google.cloud import storage
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt


logger = logging.getLogger(__name__)

_MB = 1024 * 1024
_MANIFEST_NAME = "_manifest.json"


def _date_range(start_date: str, end_date: str) -> list[datetime.date]:
    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)
    dates: list[datetime.date] = []
    current = start
    while current < end:
        dates.append(current)
        current += datetime.timedelta(days=1)
    return dates


def _duckdb_conn(memory_limit: int = 8, threads: int = 4) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(
        config={
            "memory_limit": f"{memory_limit}GB",
            "threads": threads,
            "preserve_insertion_order": False,
        }
    )
    conn.register_filesystem(gcsfs.GCSFileSystem())
    return conn


@dataclass
class CompactionQuery:
    """Renders the DuckDB COPY statement for a single compaction job."""

    source_uris: list[str]
    dest_dir: str
    target_file_size_mb: int

    _TEMPLATE = (
        "COPY (SELECT * FROM read_parquet({files_sql}))\n"
        "TO '{dest_dir}'\n"
        "(\n"
        "    FORMAT PARQUET,\n"
        "    COMPRESSION SNAPPY,\n"
        "    FILE_SIZE_BYTES {target_bytes},\n"
        "    PER_THREAD_OUTPUT false\n"
        ")"
    )

    def render(self) -> str:
        files_sql = "[" + ", ".join(f"'{u}'" for u in self.source_uris) + "]"
        return self._TEMPLATE.format(
            files_sql=files_sql,
            dest_dir=self.dest_dir,
            target_bytes=self.target_file_size_mb * _MB,
        )


@dataclass
class Compactor:
    """Compacts hive-partitioned Parquet files for a given source and date range.

    When ``gcs_staging_path`` is ``None`` (the default), operates in *swap* mode: DuckDB
    writes compacted output to an auto-generated staging sibling path, the originals are
    deleted, and the staged files are moved back so the external table path never changes.
    Before deleting any originals, the exact list of blobs to delete is committed to a
    manifest file in staging. If the process is interrupted at any point during the delete
    (even partway through), the next run finds the manifest and knows staging already holds
    a complete, verified replacement — so it finishes the delete from the recorded list and
    copies back, rather than re-deriving state from what's currently on disk.

    When ``gcs_staging_path`` is set, operates in *copy* mode: compacted files are written
    directly to ``gcs_staging_path`` without touching the source. Use this when you want
    to keep the original uncompacted files alongside the compacted output (e.g. for
    benchmarking both versions via separate external tables).
    """

    gcs_client: storage.Client
    gcs_input_path: GSPath
    event_source: str
    partition_prefix: str
    target_file_size_mb: int
    gcs_staging_path: GSPath | None = None
    memory_limit_gb: int = 8
    threads: int = 4
    conn_factory: Callable[..., duckdb.DuckDBPyConnection] = _duckdb_conn
    max_retries: int = 3
    swap: bool = field(init=False)

    _connection: Optional[duckdb.DuckDBPyConnection] = field(default=None, init=False, repr=False)

    def __post_init__(self):
        self.swap = False
        if self.gcs_staging_path is None:
            self.gcs_staging_path = self.default_staging_path
            self.swap = True

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            self._connection = self.conn_factory(
                memory_limit=self.memory_limit_gb, threads=self.threads
            )
        return self._connection

    def close_connection(self) -> None:
        if self._connection is None:
            return

        self._connection.close()
        self._connection = None

    @property
    def default_staging_path(self) -> GSPath:
        return self.gcs_input_path.parent / f"_compact_{self.gcs_input_path.name}_staging"

    def run(self, dates: list[datetime.date]) -> None:
        """Compact each date partition, resuming any interrupted swaps first."""
        total = len(dates)
        logger.info(f"Starting compaction: {total} date(s) to process")
        for i, date in enumerate(dates, 1):
            logger.info(f"[{i}/{total}] {date}")
            self._compact_with_retry(date)
        self.close_connection()
        logger.info(f"Finished compaction: {total} date(s) processed")

    def _compact_with_retry(self, date: datetime.date) -> None:
        def before_sleep(retry_state: Retrying) -> None:
            logger.warning(
                f"Attempt {retry_state.attempt_number}/{self.max_retries} failed for {date}: "
                f"{retry_state.outcome.exception()}. Retrying..."
            )
            self.close_connection()

        for attempt in Retrying(
            # InvalidInputException covers Snappy decompression failures from partial GCS reads,
            # which DuckDB surfaces as a parse/input error rather than an I/O error. Confirmed
            # transient: retrying the same date succeeds.
            retry=retry_if_exception_type((duckdb.IOException, duckdb.InvalidInputException)),
            stop=stop_after_attempt(self.max_retries),
            before_sleep=before_sleep,
            reraise=True,
        ):
            with attempt:
                self._compact(date)

    def _partition_path(self, base: GSPath, date: datetime.date) -> GSPath:
        p = self.partition_prefix
        return base / f"{p}source={self.event_source}" / f"{p}date={date.isoformat()}"

    def _list_parquet_blobs(self, base: GSPath, date: datetime.date) -> list[storage.Blob]:
        part = self._partition_path(base, date)
        blobs = self.gcs_client.list_blobs(part.bucket, prefix=f"{part.blob}/")
        return [b for b in blobs if b.name.endswith(".parquet")]

    def _compact(self, date: datetime.date) -> None:
        manifest = self._read_manifest(date) if self.swap else None
        source_blobs = self._list_parquet_blobs(self.gcs_input_path, date)
        dest_blobs = self._list_parquet_blobs(self.gcs_staging_path, date)

        if manifest is not None:
            # A manifest exists only once compaction fully succeeded and originals were
            # about to be deleted, so staging is guaranteed to hold the complete replacement
            # regardless of how far the delete-of-originals got before the interruption.
            logger.warning(
                f"Resuming committed swap for {date}: finishing delete of "
                f"{len(manifest)} recorded original(s) and copy-back from staging"
            )
            self._finish_swap(date, manifest, dest_blobs)
            return

        if self.swap and dest_blobs and not source_blobs:
            # Unreachable under this code's own logic: the manifest is written before any
            # delete starts and removed only after the swap fully completes, so a crash
            # anywhere in between always leaves a manifest behind. Reaching this state means
            # either leftover data from a pre-manifest run, or something outside this code's
            # control touched the paths — either way, there's no way to verify dest is the
            # complete, correct replacement. Stop and surface it rather than guessing.
            raise RuntimeError(
                f"Ambiguous state for {date}: {len(dest_blobs)} staged file(s) in "
                f"{self.gcs_staging_path}, no source files in {self.gcs_input_path}, and no "
                "manifest recording a committed swap. Refusing to auto-recover — verify "
                "manually whether the staged files are the complete replacement before "
                "deleting or restoring anything."
            )

        if self.swap and dest_blobs and source_blobs:
            # Originals are still present and no manifest was recorded, so the
            # delete-of-originals step never started. Any staging output here is leftover
            # from an interrupted compaction attempt — safe to discard and recompact.
            logger.warning(
                f"Clearing {len(dest_blobs)} stale staged file(s) for {date} "
                "(interrupted compaction attempt)"
            )
            self._delete_blobs(dest_blobs)
            dest_blobs = []

        if not source_blobs:
            logger.info(f"No source files for {date}, skipping")
            return

        if self.swap and len(source_blobs) == 1:
            logger.info(f"Skipping {date}: already a single file")
            return

        if not self.swap and dest_blobs:
            logger.info(f"Removing {len(dest_blobs)} existing file(s) from dest for {date}")
            self._delete_blobs(dest_blobs)

        source_part = self._partition_path(self.gcs_input_path, date)
        source_uris = [f"gs://{source_part.bucket}/{b.name}" for b in source_blobs]
        total_mb = sum(b.size for b in source_blobs) / _MB

        logger.info(
            f"Compacting {len(source_uris)} file(s) for {date} ({total_mb:.0f} MB compressed)"
        )

        t0 = time.monotonic()
        written = self._write_compacted(date, source_uris, self.gcs_staging_path)
        logger.info(f"DuckDB done in {time.monotonic() - t0:.1f}s — {len(written)} file(s)")

        if self.swap:
            # The manifest is written before anything is deleted, so its existence is what
            # tells a resumed run that staging already holds a verified, complete replacement
            # — independent of how much of the delete loop below actually completed.
            t0 = time.monotonic()
            names = [b.name for b in source_blobs]
            self._write_manifest(date, names)
            self._finish_swap(date, names, written)
            logger.info(f"Swap done in {time.monotonic() - t0:.1f}s")

    def _write_compacted(
        self, date: datetime.date, source_uris: list[str], dest_path: GSPath
    ) -> list[storage.Blob]:
        """Compact source files into dest_path using DuckDB."""
        dest_part = self._partition_path(dest_path, date)
        dest_dir = f"gs://{dest_part.bucket}/{dest_part.blob}"
        query = CompactionQuery(
            source_uris=source_uris,
            dest_dir=dest_dir,
            target_file_size_mb=self.target_file_size_mb,
        )
        logger.info(f"Writing compacted output to {dest_dir}")
        self.connection.execute(query.render())

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

    def _delete_named_blobs(self, base: GSPath, names: list[str]) -> None:
        bucket = self.gcs_client.bucket(base.bucket)
        for name in names:
            try:
                bucket.blob(name).delete()
                logger.info(f"Deleted gs://{base.bucket}/{name}")
            except NotFound:
                logger.info(f"Already deleted gs://{base.bucket}/{name}")

    def _finish_swap(
        self, date: datetime.date, original_names: list[str], staged_blobs: list[storage.Blob]
    ) -> None:
        """Delete originals, copy staged output into place, and clear the manifest.

        Shared by both a fresh compaction and a manifest-driven resume: every step here
        is idempotent (deleting an already-gone original, re-copying an already-copied
        file, deleting an already-gone manifest all no-op), so re-running this from the
        top after any partial failure is always safe.
        """
        self._delete_named_blobs(self.gcs_input_path, original_names)
        self._copy_to_partition(date, staged_blobs, self.gcs_input_path)
        self._delete_blobs(staged_blobs)
        self._delete_manifest(date)

    def _manifest_blob(self, date: datetime.date) -> storage.Blob:
        part = self._partition_path(self.gcs_staging_path, date)
        bucket = self.gcs_client.bucket(part.bucket)
        return bucket.blob(f"{part.blob}/{_MANIFEST_NAME}")

    def _read_manifest(self, date: datetime.date) -> Optional[list[str]]:
        blob = self._manifest_blob(date)
        if not blob.exists():
            return None
        return list(json.loads(blob.download_as_text()))

    def _write_manifest(self, date: datetime.date, names: list[str]) -> None:
        blob = self._manifest_blob(date)
        blob.upload_from_string(json.dumps(names), content_type="application/json")

    def _delete_manifest(self, date: datetime.date) -> None:
        blob = self._manifest_blob(date)
        try:
            blob.delete()
        except NotFound:
            pass


def run(
    project: str,
    gcs_input_path: str,
    event_source: str,
    start_date: str,
    end_date: str,
    partition_prefix: str = "event_",
    target_file_size_mb: int = 512,
    memory_limit_gb: int = 8,
    threads: int = 4,
    max_retries: int = 3,
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
    compacted output to an auto-generated staging sibling path. The staged files are
    then swapped into the original partition path so the external table path never
    changes. If the process is interrupted after deleting originals, the next run
    detects the staged files and resumes the copy step automatically.

    Path structure::

        {gcs_input_path}/{prefix}source={event_source}/{prefix}date=YYYY-MM-DD/*.parquet

    Staging path: ``gs://{bucket}/{parent}/_compact_{table}_staging``
    (sibling of the table directory, same bucket so copies are server-side).

    Args:
        project:
            GCP project used for billing.

        gcs_input_path:
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

        max_retries:
            Number of retries on transient DuckDB I/O errors before failing the task.
            Defaults to 3.

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

        unknown_unparsed_args:
            Extra unparsed CLI args (ignored).

        unknown_parsed_args:
            Extra parsed args (ignored).
    """
    source = GSPath(gcs_input_path.rstrip("/"))
    dates = _date_range(start_date, end_date)

    logger.info(f"Compacting {gcs_input_path} for [{start_date}, {end_date}) ({len(dates)} days)")

    if dry_run:
        for date in dates:
            p = partition_prefix
            logger.info(f"[dry-run] {gcs_input_path}/{p}source={event_source}/{p}date={date}")
        return

    gcs_client = gcs_client_factory(project=project)
    compactor = Compactor(
        gcs_client=gcs_client,
        gcs_input_path=source,
        event_source=event_source,
        partition_prefix=partition_prefix,
        target_file_size_mb=target_file_size_mb,
        gcs_staging_path=GSPath(gcs_staging_path.rstrip("/")) if gcs_staging_path else None,
        memory_limit_gb=memory_limit_gb,
        threads=threads,
        max_retries=max_retries,
        conn_factory=conn_factory or _duckdb_conn,
    )
    compactor.run(dates)
