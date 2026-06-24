"""Generic utility for migrating date-sharded BigQuery tables into a partitioned table."""

from __future__ import annotations

import logging

from collections.abc import Iterator
from datetime import date, datetime
from dataclasses import dataclass
from functools import cached_property
from importlib.resources import files
from pathlib import Path
from typing import Any, Callable

from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

import gfw.ops.assets.queries.sharded_to_partitioned as _sql_pkg


logger = logging.getLogger(__name__)

CAVEAT = (
    "NOTE: Best-effort tool — does not cover all edge cases. Key assumptions:\n"
    "\n"
    "  • Input tables are daily sharded (e.g. table_YYYYMMDD). Other conventions\n"
    "    (e.g. monthly shards table_YYYYMM) require adapting the SQL templates and\n"
    "    shard-discovery logic.\n"
    "\n"
    "  • Skip logic (overwrite=False) only works correctly when the target table uses\n"
    "    DAY partitioning. With MONTH or YEAR partitioning the pending-month check always\n"
    "    returns all months as pending, so every run rewrites everything.\n"
    "\n"
    "  • Column schema is assumed stable across shards of the same source table.\n"
    "    Column discovery scans INFORMATION_SCHEMA across all shards at once; if a column\n"
    "    was dropped in newer shards the generated SELECT will fail against those shards.\n"
    "\n"
    "  • The partition field must be of type TIMESTAMP or DATE. Integer-range partitioning\n"
    "    is not supported.\n"
    "\n"
    "  • If the target table already exists its schema is not updated, even if the provided\n"
    "    schema file has changed.\n"
    "\n"
    "  • Month-level errors do not halt the run — all months are attempted. Failed months\n"
    "    are listed in a summary and the run exits with an error at the end."
)


def _as_shard(d: date) -> str:
    return d.strftime("%Y%m%d")


def _progress_console() -> Console:
    h = next((h for h in logging.root.handlers if isinstance(h, RichHandler)), None)
    return h.console if h else Console()


_env = Environment(
    loader=FileSystemLoader(str(files(_sql_pkg))),
    trim_blocks=True,
    lstrip_blocks=True,
)

_DISCOVER_COLUMNS = _env.get_template("discover_columns.sql")
_EXISTING_PARTITIONS = _env.get_template("existing_partitions.sql")
_DELETE_MONTH = _env.get_template("delete_month.sql")
_CONSOLIDATE = _env.get_template("consolidate.sql")

_PARTITION_TYPE_MAP = {
    "DAY": bigquery.TimePartitioningType.DAY,
    "HOUR": bigquery.TimePartitioningType.HOUR,
    "MONTH": bigquery.TimePartitioningType.MONTH,
    "YEAR": bigquery.TimePartitioningType.YEAR,
}


@dataclass(frozen=True)
class Month:
    """A calendar month with arithmetic and string support."""

    year: int
    month: int

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    def next(self) -> Month:
        """Return the following month."""
        return Month(self.year + self.month // 12, self.month % 12 + 1)

    @property
    def start(self) -> date:
        """First day of this month."""
        return date(self.year, self.month, 1)

    @classmethod
    def from_date(cls, d: date) -> Month:
        """Construct from a date, truncating to the month."""
        return cls(d.year, d.month)

    @classmethod
    def from_partition_id(cls, partition_id: str) -> Month:
        """Construct from a BigQuery YYYYMMDD partition_id string."""
        return cls.from_date(datetime.strptime(partition_id, "%Y%m%d").date())


@dataclass(frozen=True)
class Table:
    """A BigQuery table reference with unpacking and string support."""

    project: str
    dataset_id: str
    table_id: str

    def __iter__(self) -> Iterator[str]:
        """Unpack as (project, dataset_id, table_id)."""
        return iter((self.project, self.dataset_id, self.table_id))

    def __str__(self) -> str:
        """Return the fully-qualified table name."""
        return self.fully_qualified

    @classmethod
    def from_fully_qualified(cls, fqn: str) -> Table:
        """Parse a fully-qualified table name (project.dataset.table)."""
        project, dataset_id, table_id = fqn.split(".")
        return cls(project=project, dataset_id=dataset_id, table_id=table_id)

    @property
    def fully_qualified(self) -> str:
        """Return the fully-qualified table name (project.dataset.table)."""
        return f"{self.project}.{self.dataset_id}.{self.table_id}"

    @property
    def dataset(self) -> str:
        """Return the fully-qualified dataset name (project.dataset)."""
        return f"{self.project}.{self.dataset_id}"


class ShardedToPartitioned:
    """Migrate one or more date-sharded BigQuery tables into a single partitioned table.

    This utility consolidates tables that follow the ``base_name_YYYYMMDD`` naming
    convention into a single time-partitioned destination table. It is designed for
    use cases where data from multiple providers or pipeline versions must be merged
    into one canonical table.

    **Date range**

    The caller supplies a ``start`` (inclusive) and ``end`` (exclusive) month in
    ``YYYY-MM-DD`` format. The tool iterates over all months in that range and processes
    each one by querying all source tables with a ``_TABLE_SUFFIX`` filter — one
    wildcard branch per source table, covering the full calendar month.

    **Multiple source tables**

    Any number of source tables can be supplied. For each calendar month the tool
    issues a ``UNION ALL`` query with one branch per source table, using BigQuery
    wildcard syntax (``table_*``) filtered by ``_TABLE_SUFFIX``. Days that appear
    in more than one source table are merged, not deduplicated.

    **Schema mismatch handling**

    Each source table may have a different set of columns. The tool discovers
    the columns present in every source table and fills any gaps with
    ``CAST(NULL AS <type>)`` using the provided schema. This ensures every row
    in the target partition has the same structure regardless of which source
    table it came from.

    **Incremental operation**

    By default the tool queries the existing partitions in the target table and
    skips months that already have any data written. Pass ``overwrite=True`` to
    re-process and replace already-written months.

    **Dry run**

    When ``dry_run=True`` the tool logs an example ``UNION ALL`` query for the
    first pending month and exits. No data is written and the target table is
    not created or modified.

    **Limitations**

    See :data:`CAVEAT` for a summary of the assumptions and edge cases not covered.

    Args:
        tables:
            Fully-qualified source table names (``project.dataset.table``).
            Each name is the base without the ``_YYYYMMDD`` suffix.

        target:
            Fully-qualified destination table name (``project.dataset.table``).

        project:
            GCP project used to run BigQuery jobs and bear their costs.

        schema:
            Target schema, supplied as a path to a JSON schema file or a
            pre-loaded list of :class:`~google.cloud.bigquery.SchemaField` objects.
            Required — used to align columns across sources via ``CAST(NULL AS <type>)``
            for any column absent from a given source table.

        start_date:
            First month to process, inclusive, in ``YYYY-MM-DD`` format.

        end_date:
            Last month to process, exclusive, in ``YYYY-MM-DD`` format.

        partition_type:
            Partitioning granularity — one of ``DAY``, ``HOUR``, ``MONTH``, or ``YEAR``.
            Defaults to ``DAY``.

        partition_field:
            Name of the TIMESTAMP or DATE column used for partitioning.
            Defaults to ``timestamp``.

        bq_client_factory:
            Optional factory callable that receives a project string and returns a
            :class:`~google.cloud.bigquery.Client`.
            Defaults to :class:`~google.cloud.bigquery.Client`.
            Useful for injecting mocked clients in tests.
    """

    def __init__(
        self,
        tables: list[str],
        target: str,
        project: str,
        schema: str | Path | list[bigquery.SchemaField],
        start_date: str,
        end_date: str,
        partition_type: str = "DAY",
        partition_field: str = "timestamp",
        bq_client_factory: Callable[[str], bigquery.Client] = bigquery.Client,
    ) -> None:
        self._tables = tables
        self._project = project
        self._client_factory = bq_client_factory
        self._target = target
        self._schema = schema
        self._start_date = start_date
        self._end_date = end_date
        self._partition_type = partition_type
        self._partition_field = partition_field

    @cached_property
    def tables(self) -> list[Table]:
        """Source tables parsed from their fully-qualified names."""
        return [Table.from_fully_qualified(t) for t in self._tables]

    @cached_property
    def target(self) -> Table:
        """Target table parsed from its fully-qualified name."""
        return Table.from_fully_qualified(self._target)

    @cached_property
    def client(self) -> bigquery.Client:
        """BigQuery client for the execution project."""
        return self._client_factory(self._project)

    @cached_property
    def schema(self) -> list[bigquery.SchemaField]:
        """Schema fields: loaded from file if a path is given, used as-is if already a list."""
        if isinstance(self._schema, list):
            return self._schema

        return self.client.schema_from_json(str(self._schema))

    @cached_property
    def start_date(self) -> date:
        """Start date parsed from the ``start_date`` string."""
        return date.fromisoformat(self._start_date)

    @cached_property
    def end_date(self) -> date:
        """End date parsed from the ``end_date`` string."""
        return date.fromisoformat(self._end_date)

    @cached_property
    def partition_type(self) -> str:
        """Partition type resolved from string (DAY, HOUR, MONTH, YEAR)."""
        if self._partition_type not in _PARTITION_TYPE_MAP:
            raise ValueError(
                f"Invalid partition_type {self._partition_type!r}. "
                f"Must be one of: {list(_PARTITION_TYPE_MAP)}"
            )
        return _PARTITION_TYPE_MAP[self._partition_type]

    def run(self, dry_run: bool = False, overwrite: bool = False, limit: int = 0) -> None:
        """Run the migration.

        Args:
            dry_run: Log an example consolidation query for the first pending month and exit.
            overwrite: Re-process months that already exist in the target table.
            limit: Process at most this many months.

        Note:
            Logging is not configured automatically. The caller is responsible for
            setting up logging before calling this method (e.g. via ``LoggerConfig().setup()``).
            TODO: adopt PipelineConfig as the base class and move logging setup there.
        """
        logger.info(f"Target: {self.target}")
        logger.info(f"Date range: {self._start_date} – {self._end_date} (exclusive)")
        logger.info(f"Months in range: {len(self.months)}")

        logger.info("Computing pending months...")
        pending = self.months
        if not overwrite:
            pending = self._compute_pending()

        skipped = len(self.months) - len(pending)
        if skipped:
            logger.info(f"Skipping {skipped} already-written months (use overwrite=True to redo).")

        logger.info(f"Pending months: {len(pending)}")
        if limit > 0:
            pending = pending[:limit]
            logger.info(f"Limit applied: processing {len(pending)} month(s).")

        if not pending:
            logger.info("Nothing to do. No pending months to process.")
            return

        logger.info("Discovering source columns…")
        table_columns = self._discover_columns(self.tables)

        if dry_run:
            month = pending[0]
            logger.info(f"Example query for {self.target} ({month}):")
            logger.info(self._build_query(month, table_columns))
            return

        self._ensure_table()

        failed = []

        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=_progress_console(),
        ) as progress:
            task = progress.add_task("Migrating...", total=len(pending))
            for month in pending:
                progress.update(task, description=f"Migrating {month}...")
                if not self._process_month(month, table_columns, overwrite=overwrite):
                    failed.append(month)
                progress.advance(task)

        if failed:
            failed_str = [str(m) for m in failed]
            logger.error(f"{len(failed)} month(s) failed: {', '.join(failed_str)}")
            raise RuntimeError(
                f"Migration completed with {len(failed)} failed month(s): {failed_str}"
            )

        logger.info("Done.")

    @cached_property
    def months(self) -> list[Month]:
        """All months touched by [start_date, end_date)."""
        months = []
        current = Month.from_date(self.start_date)
        while current.start < self.end_date:
            months.append(current)
            current = current.next()
        return months

    def _compute_pending(self) -> list[Month]:
        project, dataset, table = self.target
        query = _EXISTING_PARTITIONS.render(project=project, dataset=dataset, table=table)
        logger.debug(f"Fetching existing partitions for {self.target}:\n{query}")

        try:
            rows = list(self.client.query(query).result())
        except NotFound:
            logger.warning(f"Target table {self.target} not found, all months are pending.")
            return self.months

        existing_months = {Month.from_partition_id(row.partition_id) for row in rows}
        return [m for m in self.months if m not in existing_months]

    def _discover_columns(self, tables: list[Table]) -> dict[str, frozenset[str]]:
        result = {}
        for table in tables:
            query = _DISCOVER_COLUMNS.render(fq_dataset=table.dataset, table_id=table.table_id)
            logger.debug(f"Discovering columns for {table}:\n{query}")
            result[table.fully_qualified] = frozenset(
                row.column_name for row in self.client.query(query).result()
            )
        return result

    def _ensure_table(self) -> None:
        bq_table = bigquery.Table(str(self.target), schema=self.schema)
        bq_table.time_partitioning = bigquery.TimePartitioning(
            type_=self.partition_type, field=self._partition_field
        )
        self.client.create_table(bq_table, exists_ok=True)
        logger.info("Target table ready.")

    def _process_month(
        self,
        month: Month,
        table_columns: dict[str, frozenset[str]],
        overwrite: bool,
    ) -> bool:
        query = self._build_query(month, table_columns)
        logger.debug(f"Consolidation query for {self.target} ({month}):\n{query}")

        try:
            if overwrite:
                logger.info(f"Overwrite: deleting {month} from target...")
                self._delete_month(month)

            job_config = bigquery.QueryJobConfig(
                destination=str(self.target),
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                time_partitioning=bigquery.TimePartitioning(
                    type_=self.partition_type,
                    field=self._partition_field,
                ),
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            )
            job = self.client.query(query, job_config=job_config)
            job.result()
            logger.info(f"{month} — {job.total_bytes_processed / 1e9:.2f} GB processed")
            return True
        except GoogleAPIError as e:
            logger.error(f"{month} failed: {e}")
            return False

    def _build_query(
        self,
        month: Month,
        table_columns: dict[str, frozenset[str]],
    ) -> str:
        start = _as_shard(max(month.start, self.start_date))
        end = _as_shard(min(month.next().start, self.end_date))

        target_cols = [(f.name, f.field_type) for f in self.schema]
        sources = [
            {
                "fqn": table.fully_qualified,
                "start": start,
                "end": end,
                "cols": [
                    name
                    if name in table_columns[table.fully_qualified]
                    else f"CAST(NULL AS {ftype}) AS {name}"
                    for name, ftype in target_cols
                ],
            }
            for table in self.tables
        ]
        return _CONSOLIDATE.render(sources=sources)

    def _delete_month(self, month: Month) -> None:
        query = _DELETE_MONTH.render(
            target=self.target,
            year=month.year,
            month=month.month,
            partition_field=self._partition_field,
        )
        self.client.query(query).result()


def run(
    bq_in_sharded: list[str],
    bq_out_partitioned: str,
    project: str,
    schema_file: str,
    start_date: str,
    end_date: str,
    partition_type: str = "DAY",
    partition_field: str = "timestamp",
    overwrite: bool = False,
    dry_run: bool = False,
    limit: int = 0,
    bq_client_factory: Callable[[str], bigquery.Client] = bigquery.Client,
    **kwargs: Any,
) -> None:
    """Run the sharded-to-partitioned migration."""
    ShardedToPartitioned(
        tables=bq_in_sharded,
        target=bq_out_partitioned,
        schema=schema_file,
        project=project,
        start_date=start_date,
        end_date=end_date,
        partition_type=partition_type,
        partition_field=partition_field,
        bq_client_factory=bq_client_factory,
    ).run(
        dry_run=dry_run,
        overwrite=overwrite,
        limit=limit,
    )
