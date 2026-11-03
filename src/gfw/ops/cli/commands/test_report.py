"""CLI command: post the weekly test report to Slack."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from gfw.common.cli import Command, Option
from gfw.ops.pipelines import test_report as pipeline


_DESCRIPTION = (
    "Post a report of how the test suites ran over the past week (success rate and average "
    "duration) to Slack. Credentials are read from the environment: Application Default "
    "Credentials for Cloud Build, GITHUB_TOKEN for GitHub Actions and SLACK_TOKEN for Slack."
)

HELP_SLACK_CHANNEL = (
    "Slack channel ID to post to. The Slack app must be a member of the channel. "
    "Required unless --dry-run is set."
)
HELP_DAYS = "Length of the reporting window, in days."
HELP_END_DATE = (
    "Day the window ends on, exclusive (YYYY-MM-DD, UTC). Defaults to today, so the window is "
    "the last --days whole days."
)
HELP_DRY_RUN = "Print the report instead of posting it to Slack."


class WeeklyTestReport(Command):
    """Post the weekly test report to Slack."""

    @property
    def name(self) -> str:
        """Command name."""
        return "test-report"

    @property
    def description(self) -> str:
        """Command description."""
        return _DESCRIPTION

    @property
    def options(self) -> list[Option]:
        """Command options."""
        return [
            Option("--slack-channel", type=str, required=False, help=HELP_SLACK_CHANNEL),
            Option("--days", type=int, default=7, help=HELP_DAYS),
            Option("--end-date", type=str, required=False, help=HELP_END_DATE),
            Option("--dry-run", type=bool, default=False, help=HELP_DRY_RUN),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any) -> None:
        """Run the report."""
        pipeline.run(**vars(config), **kwargs)
