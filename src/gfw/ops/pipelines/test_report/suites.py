"""The suites included in the weekly report.

To add a suite, append a :class:`Suite` below with the source its runs come from.
"""

from __future__ import annotations

from .models import Environment, Suite
from .sources import CloudBuildSource, GitHubActionsSource


SUITES: tuple[Suite, ...] = (
    Suite(
        name="API (daily)",
        environment=Environment.PRODUCTION,
        source=CloudBuildSource(
            project="gfw-int-infrastructure",
            location="us-central1",
            trigger_name="pro-api-tests",
        ),
    ),
    Suite(
        name="Frontend (daily)",
        environment=Environment.PRODUCTION,
        source=GitHubActionsSource(
            repo="GlobalFishingWatch/frontend",
            workflow_file="e2e-extended-daily.yml",
            event="schedule",
        ),
    ),
    Suite(
        name="Frontend (on every PR)",
        environment=Environment.DEVELOPMENT,
        source=GitHubActionsSource(
            repo="GlobalFishingWatch/frontend",
            workflow_file="e2e-tests.yml",
            event="pull_request",
        ),
    ),
)
