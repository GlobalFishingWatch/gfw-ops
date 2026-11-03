from datetime import datetime, timedelta, timezone

from gfw.ops.pipelines.test_report.models import (
    Environment,
    Suite,
    SuiteResult,
    SuiteStats,
)
from gfw.ops.pipelines.test_report.report import format_report


START = datetime(2026, 10, 27, tzinfo=timezone.utc)
END = datetime(2026, 11, 3, tzinfo=timezone.utc)


def _result(name, environment, stats):
    return SuiteResult(Suite(name, environment, source=None), stats)


def test_format_report_matches_the_agreed_layout():
    results = [
        _result(
            "API (daily)",
            Environment.PRODUCTION,
            SuiteStats(runs=7, passed=6, avg_duration=timedelta(minutes=12, seconds=40)),
        ),
        _result(
            "Frontend (daily)",
            Environment.PRODUCTION,
            SuiteStats(runs=4, passed=0, avg_duration=timedelta(minutes=7, seconds=52)),
        ),
        _result(
            "Frontend (on every PR)",
            Environment.DEVELOPMENT,
            SuiteStats(runs=37, passed=24, avg_duration=timedelta(minutes=5, seconds=20)),
        ),
    ]

    assert format_report(START, END, results) == (
        "📊 *Weekly test report* · 27 Oct – 2 Nov\n"
        "\n"
        "*Live monitoring* (production)\n"
        "• API (daily): 86% passed (6/7) · avg 13m\n"
        "• Frontend (daily): 0% passed (0/4) · avg 8m 🔴\n"
        "\n"
        "*CI checks* (dev/staging)\n"
        "• Frontend (on every PR): 65% passed (24/37) · avg 5m"
    )


def test_suite_without_runs():
    results = [_result("API (daily)", Environment.PRODUCTION, SuiteStats(0, 0, None))]

    assert "• API (daily): no runs" in format_report(START, END, results)


def test_suite_with_unreadable_source_is_flagged():
    results = [_result("API (daily)", Environment.PRODUCTION, None)]

    assert "• API (daily): data unavailable ⚠️" in format_report(START, END, results)


def test_section_without_suites_is_left_out():
    results = [
        _result("API (daily)", Environment.PRODUCTION, SuiteStats(1, 1, timedelta(minutes=1)))
    ]

    assert "CI checks" not in format_report(START, END, results)


def test_pass_rate_at_the_flag_threshold_is_not_flagged():
    results = [
        _result("API (daily)", Environment.PRODUCTION, SuiteStats(2, 1, timedelta(minutes=1)))
    ]

    assert "🔴" not in format_report(START, END, results)


def test_durations_are_shown_in_seconds_minutes_or_hours():
    def line(duration):
        results = [_result("S", Environment.PRODUCTION, SuiteStats(1, 1, duration))]
        return format_report(START, END, results).splitlines()[-1]

    assert line(timedelta(seconds=45)).endswith("avg 45s")
    assert line(timedelta(minutes=12, seconds=29)).endswith("avg 12m")
    assert line(timedelta(hours=1, minutes=5)).endswith("avg 1h 05m")
