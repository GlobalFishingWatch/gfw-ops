"""Formats the weekly report as a Slack message."""

from __future__ import annotations

from datetime import datetime, timedelta

from .models import Environment, SuiteResult, SuiteStats


# A suite that passes less than this share of its runs is flagged.
FLAG_BELOW_PASS_RATE = 0.5

_SECTIONS = (
    (Environment.PRODUCTION, "*Live monitoring* (production)"),
    (Environment.DEVELOPMENT, "*CI checks* (dev/staging)"),
)


def format_report(start: datetime, end: datetime, results: list[SuiteResult]) -> str:
    """Build the Slack message for the window ``[start, end)``."""
    last_day = end - timedelta(days=1)
    lines = [f"📊 *Weekly test report* · {_day(start)} – {_day(last_day)}"]  # noqa: RUF001
    for environment, heading in _SECTIONS:
        section = [r for r in results if r.suite.environment is environment]
        if section:
            lines += ["", heading, *(_suite_line(r) for r in section)]
    return "\n".join(lines)


def _day(moment: datetime) -> str:
    return f"{moment.day} {moment:%b}"


def _suite_line(result: SuiteResult) -> str:
    name, stats = result.suite.name, result.stats
    if stats is None:
        return f"• {name}: data unavailable ⚠️"

    if stats.runs == 0:
        return f"• {name}: no runs"

    line = f"• {name}: {_percent(stats)} passed ({stats.passed}/{stats.runs})"
    if stats.avg_duration is not None:
        line += f" · avg {_duration(stats.avg_duration)}"

    if stats.pass_rate is not None and stats.pass_rate < FLAG_BELOW_PASS_RATE:
        line += " 🔴"
    return line


def _percent(stats: SuiteStats) -> str:
    assert stats.pass_rate is not None
    return f"{int(stats.pass_rate * 100 + 0.5)}%"


def _duration(duration: timedelta) -> str:
    seconds = round(duration.total_seconds())
    if seconds < 60:
        return f"{seconds}s"

    minutes = (seconds + 30) // 60
    if minutes < 60:
        return f"{minutes}m"

    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"
