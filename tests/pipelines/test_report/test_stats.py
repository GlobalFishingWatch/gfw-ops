from datetime import datetime, timedelta, timezone

from gfw.ops.pipelines.test_report.models import Run
from gfw.ops.pipelines.test_report.stats import compute_stats


def _run(minutes, passed):
    started = datetime(2026, 10, 27, tzinfo=timezone.utc)
    return Run(started, started + timedelta(minutes=minutes), passed)


def test_compute_stats():
    stats = compute_stats([_run(10, True), _run(20, True), _run(30, False)])

    assert stats.runs == 3
    assert stats.passed == 2
    assert stats.avg_duration == timedelta(minutes=20)
    assert stats.pass_rate == 2 / 3


def test_compute_stats_without_runs():
    stats = compute_stats([])

    assert stats.runs == 0
    assert stats.avg_duration is None
    assert stats.pass_rate is None
