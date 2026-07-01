"""Benchmark query performance of a native BigQuery table vs a Parquet external table."""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from google.cloud import bigquery
from rich.console import Console
from rich.table import Table


logger = logging.getLogger(__name__)

_DATE_FILTERS = {
    "native": "DATE({column}) >= '{start_date}' AND DATE({column}) < '{end_date}'",
    "external": "{column} >= '{start_date}' AND {column} < '{end_date}'",
}

_QUERIES = {
    "select_star": "SELECT * FROM `{table}` WHERE {date_filter}",
    "aggregation": (
        "SELECT EXTRACT(HOUR FROM {agg_column}) AS hour, COUNT(*) AS cnt"
        " FROM `{table}` WHERE {date_filter}"
        " GROUP BY 1 ORDER BY 1"
    ),
}


@dataclass
class QueryStats:
    """Stats captured from a single BigQuery job execution."""

    bytes_processed: int
    slot_ms: int
    elapsed_s: float


@dataclass
class BenchmarkResult:
    """Aggregated stats for one query × one table across N runs."""

    label: str
    query: str
    runs: list[QueryStats] = field(default_factory=list)

    @property
    def avg_bytes(self) -> float:
        """Average bytes processed across runs."""
        return sum(r.bytes_processed for r in self.runs) / len(self.runs)

    @property
    def avg_slot_ms(self) -> float:
        """Average slot milliseconds across runs."""
        return sum(r.slot_ms for r in self.runs) / len(self.runs)

    @property
    def avg_elapsed_s(self) -> float:
        """Average wall-clock elapsed time across runs."""
        return sum(r.elapsed_s for r in self.runs) / len(self.runs)

    @property
    def min_elapsed_s(self) -> float:
        """Minimum wall-clock elapsed time across runs."""
        return min(r.elapsed_s for r in self.runs)


_SCRATCH_DATASET = "gfw_benchmark_scratch"


def _ensure_scratch_dataset(client: bigquery.Client, location: str) -> None:
    dataset_ref = bigquery.DatasetReference(client.project, _SCRATCH_DATASET)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)


def _run_query(
    client: bigquery.Client,
    sql: str,
    location: str,
    max_bytes_billed: int | None,
) -> QueryStats:
    dest = f"{client.project}.{_SCRATCH_DATASET}.run_{uuid.uuid4().hex[:12]}"
    job_config = bigquery.QueryJobConfig(
        use_query_cache=False,
        maximum_bytes_billed=max_bytes_billed,
        allow_large_results=True,
        destination=dest,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.query(sql, job_config=job_config, location=location)

    while job.state != "DONE":
        time.sleep(1)
        job = client.get_job(job.job_id, location=location)

    client.delete_table(dest, not_found_ok=True)

    if job.errors:
        raise RuntimeError(f"Query job failed: {job.errors}")

    elapsed_s = (job.ended - job.started).total_seconds()

    return QueryStats(
        bytes_processed=job.total_bytes_processed or 0,
        slot_ms=job.slot_millis or 0,
        elapsed_s=elapsed_s,
    )


def _fmt_bytes(n: float) -> str:
    if n < 1024:
        return f"{n:.0f} B"

    if n < 1024**2:
        return f"{n/1024:.1f} KB"

    if n < 1024**3:
        return f"{n/1024**2:.1f} MB"

    return f"{n/1024**3:.2f} GB"


def _ratio(a: float, b: float) -> str:
    if b == 0:
        return "—"

    return f"{a/b:.1f}×"


def _build_queries(
    queries_to_include: set[str],
    table_ref: str,
    date_filter: str,
    agg_column: str | None,
) -> list[tuple[str, str]]:
    result = []
    for name in queries_to_include:
        if name not in _QUERIES:
            raise ValueError(f"Unknown query {name!r}. Valid values: {set(_QUERIES)}.")

        result.append(
            (
                name,
                _QUERIES[name].format(
                    table=table_ref, date_filter=date_filter, agg_column=agg_column
                ),
            )
        )

    return result


def _print_results(results: list[BenchmarkResult]) -> None:
    console = Console()
    query_names = list(dict.fromkeys(r.query for r in results))

    for qname in query_names:
        rows = [r for r in results if r.query == qname]
        t = Table(title=f"Query: {qname}", show_lines=True)
        t.add_column("Table")
        t.add_column("Bytes Processed", justify="right")
        t.add_column("Slot ms (avg)", justify="right")
        t.add_column("Elapsed avg", justify="right")
        t.add_column("Elapsed min", justify="right")

        for r in rows:
            t.add_row(
                r.label,
                _fmt_bytes(r.avg_bytes),
                f"{r.avg_slot_ms:,.0f}",
                f"{r.avg_elapsed_s:.2f}s",
                f"{r.min_elapsed_s:.2f}s",
            )

        if len(rows) == 2:
            ref, cmp = rows[0], rows[1]
            t.add_row(
                f"[bold]ratio ({ref.label}/{cmp.label})[/bold]",
                _ratio(ref.avg_bytes, cmp.avg_bytes),
                _ratio(ref.avg_slot_ms, cmp.avg_slot_ms),
                _ratio(ref.avg_elapsed_s, cmp.avg_elapsed_s),
                "—",
                style="bold",
            )

        console.print(t)


def run(
    project: str,
    start_date: str,
    end_date: str,
    tables: dict[str, dict] | None = None,
    queries: list[str] | None = None,
    agg_column: str | None = None,
    runs: int = 1,
    location: str = "US",
    max_gb_billed: float = 100.0,
    bq_client_factory: Callable[..., bigquery.Client] = bigquery.Client,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict | None = None,
    **kwargs: Any,
) -> None:
    """Benchmark query performance across one or more BigQuery tables.

    Runs ``SELECT *`` and (optionally) an aggregation query against each table,
    filtered to a single date partition. Prints a comparison of bytes processed,
    slot milliseconds, and wall-clock elapsed time from BQ job stats. Query
    cache is disabled so each run reflects real scan cost.

    Tables are specified as a dict of dicts — the key is the display label, and
    each value must contain:

    - ``table``: fully-qualified BigQuery table (``project.dataset.table``).
    - ``column``: column used for date filtering.
    - ``type``: ``"native"`` wraps the column in ``DATE()`` (for TIMESTAMP
      columns); ``"external"`` compares directly (for hive partition DATE keys).
      Defaults to ``"external"``.

    Args:
        project:
            GCP project used for billing.

        start_date:
            Start of date range, inclusive (``YYYY-MM-DD``).

        end_date:
            End of date range, exclusive (``YYYY-MM-DD``).

        tables:
            Dict of table configurations keyed by display label. See above.

        queries:
            List of query names to run. Valid values: ``select_star``,
            ``aggregation``. Defaults to all queries.

        agg_column:
            Timestamp column name to aggregate by hour, applied to all tables
            (e.g. ``tagblock_timestamp``). If omitted, the aggregation query is
            skipped.

        runs:
            Number of times each query is repeated. Results are averaged.

        location:
            BigQuery job location. Defaults to ``US``.

        max_gb_billed:
            Safety cap on bytes billed per query (in GB). Defaults to 100 GB.
            Pass 0 to disable.
    """
    if not tables:
        raise ValueError("--tables is required: specify at least one table configuration.")

    queries_to_include = set(queries) if queries else set(_QUERIES)

    if "aggregation" in queries_to_include and not agg_column:
        raise ValueError("--agg-column is required when running the aggregation query.")

    max_bytes_billed = int(max_gb_billed * 1024**3) if max_gb_billed else None
    client = bq_client_factory(project=project)
    _ensure_scratch_dataset(client, location)

    results: list[BenchmarkResult] = []

    for label, cfg in tables.items():
        table_ref = cfg["table"]
        column = cfg["column"]
        table_type = cfg.get("type", "external")

        if table_type not in _DATE_FILTERS:
            raise ValueError(
                f"Unknown table type {table_type!r} for {label!r}. Use 'native' or 'external'."
            )

        date_filter = _DATE_FILTERS[table_type].format(
            column=column, start_date=start_date, end_date=end_date
        )
        queries_to_run = _build_queries(queries_to_include, table_ref, date_filter, agg_column)

        for query_name, sql in queries_to_run:
            result = BenchmarkResult(label=label, query=query_name)
            logger.info(f"[{label}:{query_name}] {sql}")

            for i in range(runs):
                logger.info(f"  run {i + 1}/{runs}")
                stats = _run_query(
                    client, sql, location=location, max_bytes_billed=max_bytes_billed
                )
                result.runs.append(stats)
                logger.info(
                    f"  → {_fmt_bytes(stats.bytes_processed)}, "
                    f"{stats.slot_ms:,} slot ms, {stats.elapsed_s:.2f}s"
                )

            results.append(result)

    _print_results(results)
