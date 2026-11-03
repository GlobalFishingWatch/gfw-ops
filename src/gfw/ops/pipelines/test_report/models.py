"""Types shared by the sources, the statistics and the report."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any


class Environment(Enum):
    """Environment a suite runs against."""

    PRODUCTION = "production"
    DEVELOPMENT = "development"


@dataclass(frozen=True)
class Run:
    """A finished run of a suite that either passed or failed."""

    started_at: datetime
    finished_at: datetime
    passed: bool

    @property
    def duration(self) -> timedelta:
        """Wall-clock time the run took."""
        return self.finished_at - self.started_at


@dataclass(frozen=True)
class Suite:
    """A test suite to include in the report."""

    name: str
    environment: Environment
    # Anything with ``fetch(start, end) -> list[Run]`` that returns the finished runs created in
    # ``[start, end)``, leaving out the ones skipped, cancelled or still in progress.
    source: Any


@dataclass(frozen=True)
class SuiteStats:
    """How a suite did over the reporting window."""

    runs: int
    passed: int
    avg_duration: timedelta | None

    @property
    def pass_rate(self) -> float | None:
        """Share of runs that passed (0 to 1), or None if there were no runs."""
        return self.passed / self.runs if self.runs else None


@dataclass(frozen=True)
class SuiteResult:
    """A suite with its statistics, or ``None`` stats when its source could not be read."""

    suite: Suite
    stats: SuiteStats | None
