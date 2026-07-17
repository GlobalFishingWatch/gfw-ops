"""Compaction units: the independent partitions a Compactor can compact."""
from __future__ import annotations

import datetime

from abc import ABC, abstractmethod
from dataclasses import dataclass

from cloudpathlib import GSPath


def date_partition_path(
    base: GSPath, event_source: str, partition_prefix: str, date: datetime.date
) -> GSPath:
    """Return the {prefix}source=/{prefix}date= partition path for `date` (no hour
    subpartition). Used by ``DailyCompactionUnit.path()``, and directly by callers that
    just need the date-level directory itself — e.g. to discover which hour
    subpartitions exist under it — without constructing a unit for it.
    """
    p = partition_prefix
    return base / f"{p}source={event_source}" / f"{p}date={date.isoformat()}"


@dataclass(frozen=True)
class CompactionUnit(ABC):
    """One independent partition to compact.

    ``DailyCompactionUnit`` and ``HourlyCompactionUnit`` are siblings, not a
    specialization of one another: an hourly unit isn't "a kind of" daily unit, it's a
    different, mutually exclusive way of dividing the same date. This base class holds
    what both share (the date, and the default string label); ``path()`` is abstract
    because that's exactly where the two differ.
    """

    date: datetime.date

    @abstractmethod
    def path(self, base: GSPath, event_source: str, partition_prefix: str) -> GSPath:
        """Return the GCS partition path for this unit, under `base`."""
        ...

    def __str__(self) -> str:
        return str(self.date)


@dataclass(frozen=True)
class DailyCompactionUnit(CompactionUnit):
    """A whole date to compact as one partition — the flat layout, with no hour
    subpartition.
    """

    def path(self, base: GSPath, event_source: str, partition_prefix: str) -> GSPath:
        return date_partition_path(base, event_source, partition_prefix, self.date)

    def with_hour(self, hour: str) -> HourlyCompactionUnit:
        """Return the HourlyCompactionUnit for `hour` within this same date."""
        return HourlyCompactionUnit(self.date, hour)


@dataclass(frozen=True)
class HourlyCompactionUnit(CompactionUnit):
    """One ``{prefix}hour=`` subpartition within a date to compact independently —
    the hourly layout.
    """

    hour: str

    def path(self, base: GSPath, event_source: str, partition_prefix: str) -> GSPath:
        date_part = date_partition_path(base, event_source, partition_prefix, self.date)
        return date_part / f"{partition_prefix}hour={self.hour}"

    def __str__(self) -> str:
        return f"{self.date} hour={self.hour}"
