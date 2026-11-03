"""Statistics over the runs of a suite."""

from __future__ import annotations

from datetime import timedelta

from .models import Run, SuiteStats


def compute_stats(runs: list[Run]) -> SuiteStats:
    """Count the runs and average their duration."""
    avg_duration = sum((run.duration for run in runs), timedelta()) / len(runs) if runs else None
    return SuiteStats(
        runs=len(runs),
        passed=sum(run.passed for run in runs),
        avg_duration=avg_duration,
    )
