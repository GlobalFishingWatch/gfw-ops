"""CLI command for benchmarking BigQuery table query performance."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option
from gfw.common.cli.actions import NestedKeyValueAction

from gfw.ops.pipelines import benchmark_parquet as pipeline


_DESCRIPTION = "Benchmark SELECT * query performance across one or more BigQuery tables."

HELP_PROJECT = "GCP project for billing."
HELP_START_DATE = "Start of date range, inclusive (YYYY-MM-DD)."
HELP_END_DATE = "End of date range, exclusive (YYYY-MM-DD)."
HELP_TABLES = (
    "Table configurations as dotted key=value pairs. "
    "Each table is keyed by its display label. "
    "Required fields: table, column. "
    "Optional: type (native|external, default: external). "
    "Example: --tables native.table=project.dataset.t native.column=ts native.type=native "
    "external.table=project.dataset.ext external.column=event_date"
)
HELP_QUERIES = (
    "Which queries to run. Valid values: select_star, aggregation. "
    "Defaults to all. Example: --queries aggregation"
)
HELP_AGG_COLUMN = (
    "Timestamp column name to aggregate by hour, applied to all tables. "
    "Example: --agg-column tagblock_timestamp. "
    "If omitted, the aggregation query is skipped."
)
HELP_RUNS = "Number of times each query is repeated. Results are averaged."
HELP_LOCATION = "BigQuery job location."
HELP_MAX_GB = "Safety cap on bytes billed per query in GB. Pass 0 to disable."


class BenchmarkParquet(Command):
    """Benchmark SELECT * query performance across one or more BigQuery tables."""

    @property
    def name(self) -> str:
        """Command name."""
        return "benchmark-parquet"

    @property
    def description(self) -> str:
        """Command description."""
        return _DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """Command options."""
        return [
            Option("--project", type=str, required=True, help=HELP_PROJECT),
            Option("--start-date", type=str, required=True, help=HELP_START_DATE),
            Option("--end-date", type=str, required=True, help=HELP_END_DATE),
            Option("--tables", type=str, nargs="+", action=NestedKeyValueAction, help=HELP_TABLES),
            Option("--queries", type=str, nargs="+", default=None, help=HELP_QUERIES),
            Option("--agg-column", type=str, default=None, help=HELP_AGG_COLUMN),
            Option("--runs", type=int, default=1, help=HELP_RUNS),
            Option("--location", type=str, default="US", help=HELP_LOCATION),
            Option("--max-gb-billed", type=float, default=100.0, help=HELP_MAX_GB),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> None:
        """Run the benchmark."""
        pipeline.run(**vars(config), **kwargs)
