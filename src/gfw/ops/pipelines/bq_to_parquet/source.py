"""BigQuery source types for date-based table addressing."""
from __future__ import annotations

import datetime

from dataclasses import dataclass
from typing import ClassVar, NamedTuple

from google.cloud import bigquery


class DatesWithDataQuery(NamedTuple):
    """A metadata-only query whose rows identify dates that have data.

    Executing ``sql`` (with ``parameters`` bound) is expected to return one row
    per date that has data, each exposing that date as ``YYYYMMDD`` under a
    ``date_id`` column.
    """

    sql: str
    parameters: list[bigquery.ScalarQueryParameter]


@dataclass(frozen=True)
class Source:
    """Base class for a BigQuery table source."""

    table: str
    separator: ClassVar[str]

    @classmethod
    def create(cls, table: str, sharded: bool) -> Source:
        """Return a :class:`ShardedSource` or :class:`PartitionedSource` for the given table."""
        if sharded:
            return ShardedSource(table)

        return PartitionedSource(table)

    def ref(self, date: datetime.date) -> str:
        """Return the BQ table reference for the given date."""
        return f"{self.table}{self.separator}{date.strftime('%Y%m%d')}"

    def dates_with_data_query(self, dates: list[datetime.date]) -> DatesWithDataQuery:
        """Return a query to find which of ``dates`` have data.

        This describes the check as data (SQL + parameters) without touching
        BigQuery -- callers (e.g. :class:`~.main.Exporter`, which owns the BQ
        client) are responsible for running it.

        Subclasses must implement this, same as ``separator``: there's no
        safe generic default, since silently skipping the check would mean
        every date is assumed to have data.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class PartitionedSource(Source):
    """A time-partitioned BigQuery table. Addresses each day as ``table$YYYYMMDD``."""

    separator: ClassVar[str] = "$"

    def dates_with_data_query(self, dates: list[datetime.date]) -> DatesWithDataQuery:
        """Return a query for dates whose partition has at least one row.

        Extracting an empty or non-existent partition via the ``table$YYYYMMDD``
        decorator does not raise an error: BigQuery returns zero rows and still
        writes a schema-only output file. Checking partition metadata up front
        avoids submitting jobs -- and writing placeholder files -- for dates
        with no data.
        """
        project, dataset, table_name = self.table.split(".")
        sql = f"""
            SELECT partition_id AS date_id
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
            WHERE table_name = @table_name
              AND partition_id BETWEEN @min_id AND @max_id
              AND total_rows > 0
        """
        parameters = [
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
            bigquery.ScalarQueryParameter("min_id", "STRING", min(dates).strftime("%Y%m%d")),
            bigquery.ScalarQueryParameter("max_id", "STRING", max(dates).strftime("%Y%m%d")),
        ]
        return DatesWithDataQuery(sql, parameters)


@dataclass(frozen=True)
class ShardedSource(Source):
    """A date-sharded BigQuery table. Addresses each shard as ``table_YYYYMMDD``."""

    separator: ClassVar[str] = "_"

    def dates_with_data_query(self, dates: list[datetime.date]) -> DatesWithDataQuery:
        """Return a query for dates whose shard table exists.

        Checks table existence directly via ``INFORMATION_SCHEMA.TABLES``,
        rather than submitting an extract job and inferring "the shard doesn't
        exist" from a ``NotFound`` error: that exception is raised for *any*
        missing resource (table, dataset, project), so catching it alone can't
        tell a genuinely missing shard apart from, say, a typo'd dataset name
        that would make every date look like a benign gap.
        """
        project, dataset, table_name = self.table.split(".")
        sql = f"""
            SELECT SUBSTR(table_name, -8) AS date_id
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE STARTS_WITH(table_name, @table_prefix)
              AND SUBSTR(table_name, -8) BETWEEN @min_id AND @max_id
        """
        parameters = [
            bigquery.ScalarQueryParameter(
                "table_prefix", "STRING", f"{table_name}{self.separator}"
            ),
            bigquery.ScalarQueryParameter("min_id", "STRING", min(dates).strftime("%Y%m%d")),
            bigquery.ScalarQueryParameter("max_id", "STRING", max(dates).strftime("%Y%m%d")),
        ]
        return DatesWithDataQuery(sql, parameters)
