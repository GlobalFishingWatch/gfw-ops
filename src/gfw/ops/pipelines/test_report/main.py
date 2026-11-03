"""Report how the test suites ran over the past week and post it to Slack."""

from __future__ import annotations

import logging
import os

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Sequence

from .models import Suite, SuiteResult
from .report import format_report
from .slack import post_message
from .stats import compute_stats
from .suites import SUITES


logger = logging.getLogger(__name__)


def run(
    slack_channel: str | None = None,
    days: int = 7,
    end_date: str | None = None,
    dry_run: bool = False,
    suites: Sequence[Suite] = SUITES,
    post: Callable[[str, str, str], None] = post_message,
    unknown_unparsed_args: tuple[str, ...] = (),
    unknown_parsed_args: dict[str, Any] | None = None,
) -> None:
    """Post a report of how the suites ran over the last ``days`` days to Slack.

    The window is made of whole UTC days and ends at the start of ``end_date`` (today by
    default), so a run on Monday morning reports the previous Monday to Sunday.

    Credentials come from the environment: Application Default Credentials for Cloud Build,
    ``GITHUB_TOKEN`` for GitHub Actions and ``SLACK_TOKEN`` for Slack.

    Args:
        slack_channel:
            Slack channel ID (or name, for public channels) to post to. Required unless
            ``dry_run`` is set. The Slack app must be a member of the channel.

        days:
            Length of the reporting window, in days.

        end_date:
            Day the window ends on, exclusive (``YYYY-MM-DD``). Defaults to today (UTC).

        dry_run:
            Print the report instead of posting it.

        suites:
            Suites to include in the report.

        post:
            Function that posts ``(token, channel, text)`` to Slack.

        unknown_unparsed_args:
            Unknown arguments received by the CLI, unused.

        unknown_parsed_args:
            Unknown arguments received by the CLI, unused.

    Raises:
        ValueError: If ``slack_channel`` is missing and ``dry_run`` is not set.
        RuntimeError: After posting, if the runs of some suite could not be read.
    """
    if not dry_run and not slack_channel:
        raise ValueError("--slack-channel is required unless --dry-run is set.")

    end = _window_end(end_date)
    start = end - timedelta(days=days)
    results = [_collect(suite, start, end) for suite in suites]
    message = format_report(start, end, results)

    if dry_run:
        print(message)
    else:
        assert slack_channel is not None
        post(_slack_token(), slack_channel, message)
        logger.info("Report posted to %s", slack_channel)

    unavailable = [r.suite.name for r in results if r.stats is None]
    if unavailable:
        raise RuntimeError(f"Could not read the runs of: {', '.join(unavailable)}")


def _window_end(end_date: str | None) -> datetime:
    day = date.fromisoformat(end_date) if end_date else datetime.now(timezone.utc).date()
    return datetime.combine(day, time.min, tzinfo=timezone.utc)


def _collect(suite: Suite, start: datetime, end: datetime) -> SuiteResult:
    try:
        return SuiteResult(suite, compute_stats(suite.source.fetch(start, end)))
    except Exception:
        logger.exception("Could not read the runs of '%s'", suite.name)
        return SuiteResult(suite, None)


def _slack_token() -> str:
    token = os.environ.get("SLACK_TOKEN")
    if not token:
        raise RuntimeError("SLACK_TOKEN is required to post to Slack.")

    return token
