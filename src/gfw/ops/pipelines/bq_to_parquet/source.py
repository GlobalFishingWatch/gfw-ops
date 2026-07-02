"""BigQuery source types for date-based table addressing."""
from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import ClassVar


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


@dataclass(frozen=True)
class PartitionedSource(Source):
    """A time-partitioned BigQuery table. Addresses each day as ``table$YYYYMMDD``."""

    separator: ClassVar[str] = "$"


@dataclass(frozen=True)
class ShardedSource(Source):
    """A date-sharded BigQuery table. Addresses each shard as ``table_YYYYMMDD``."""

    separator: ClassVar[str] = "_"
