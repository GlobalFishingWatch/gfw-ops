"""Sources of suite runs: Cloud Build triggers and GitHub Actions workflows."""

from __future__ import annotations

import os

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import google.auth
import requests

from google.auth.transport.requests import AuthorizedSession

from .models import Run


_CLOUD_BUILD_API = "https://cloudbuild.googleapis.com/v1"
_GITHUB_API = "https://api.github.com"
_PAGE_SIZE = 100
_TIMEOUT_SECONDS = 60


def _parse_time(value: str) -> datetime:
    """Parse an RFC 3339 timestamp, including nanosecond precision (Cloud Build)."""
    value = value.replace("Z", "+00:00")
    if "." in value:
        head, tail = value.split(".", 1)
        fraction, _, offset = tail.partition("+")
        value = f"{head}.{fraction[:6]}+{offset}"
    return datetime.fromisoformat(value)


def _rfc3339(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class CloudBuildSource:
    """Builds started by a Cloud Build trigger.

    Authenticates with Application Default Credentials.

    Args:
        project:
            GCP project that owns the trigger.

        location:
            Region of the trigger (e.g. ``us-central1``).

        trigger_name:
            Name of the trigger.
    """

    project: str
    location: str
    trigger_name: str
    session: Any = field(default=None, repr=False)

    # SUCCESS passes; these statuses fail. Anything else (QUEUED, WORKING, CANCELLED, ...) is ignored.
    FAILED_STATUSES = frozenset({"FAILURE", "TIMEOUT", "INTERNAL_ERROR"})

    def fetch(self, start: datetime, end: datetime) -> list[Run]:
        """Return the finished builds created in ``[start, end)``."""
        session = self.session or self._default_session()
        url = f"{_CLOUD_BUILD_API}/projects/{self.project}/locations/{self.location}/builds"
        params: dict[str, Any] = {
            "filter": (
                f'substitutions.TRIGGER_NAME="{self.trigger_name}"'
                f' AND create_time>="{_rfc3339(start)}"'
                f' AND create_time<"{_rfc3339(end)}"'
            ),
            "pageSize": _PAGE_SIZE,
        }
        runs = []
        while True:
            response = session.get(url, params=params, timeout=_TIMEOUT_SECONDS)
            response.raise_for_status()
            body = response.json()
            runs.extend(self._to_runs(body.get("builds", [])))
            if not body.get("nextPageToken"):
                return runs
            params["pageToken"] = body["nextPageToken"]

    def _to_runs(self, builds: list[dict[str, Any]]) -> list[Run]:
        runs = []
        for build in builds:
            status = build.get("status")
            if status != "SUCCESS" and status not in self.FAILED_STATUSES:
                continue

            if not build.get("startTime") or not build.get("finishTime"):
                continue

            runs.append(
                Run(
                    started_at=_parse_time(build["startTime"]),
                    finished_at=_parse_time(build["finishTime"]),
                    passed=status == "SUCCESS",
                )
            )
        return runs

    @staticmethod
    def _default_session() -> AuthorizedSession:
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return AuthorizedSession(credentials)


@dataclass
class GitHubActionsSource:
    """Runs of a GitHub Actions workflow.

    Authenticates with the token in the ``GITHUB_TOKEN`` environment variable.

    Args:
        repo:
            Repository in ``owner/name`` form.

        workflow_file:
            File name of the workflow (e.g. ``e2e-tests.yml``).

        event:
            Only count runs triggered by this event (e.g. ``schedule``). All events if None.
    """

    repo: str
    workflow_file: str
    event: str | None = None
    session: Any = field(default=None, repr=False)

    # `success` passes; these conclusions fail. Anything else (skipped, cancelled, ...) is ignored.
    FAILED_CONCLUSIONS = frozenset({"failure", "timed_out"})

    def fetch(self, start: datetime, end: datetime) -> list[Run]:
        """Return the completed runs created in ``[start, end)``."""
        session = self.session or self._default_session()
        url = f"{_GITHUB_API}/repos/{self.repo}/actions/workflows/{self.workflow_file}/runs"
        params: dict[str, Any] = {
            "created": f"{_rfc3339(start)}..{_rfc3339(end)}",
            "status": "completed",
            "per_page": _PAGE_SIZE,
            "page": 1,
        }
        if self.event:
            params["event"] = self.event

        runs = []
        while True:
            response = session.get(url, params=params, timeout=_TIMEOUT_SECONDS)
            response.raise_for_status()
            workflow_runs = response.json().get("workflow_runs", [])
            runs.extend(self._to_runs(workflow_runs, start, end))
            if len(workflow_runs) < _PAGE_SIZE:
                return runs
            params["page"] += 1

    def _to_runs(
        self, workflow_runs: list[dict[str, Any]], start: datetime, end: datetime
    ) -> list[Run]:
        runs = []
        for run in workflow_runs:
            conclusion = run.get("conclusion")
            if conclusion != "success" and conclusion not in self.FAILED_CONCLUSIONS:
                continue

            # The API's `created` range is inclusive on both ends; keep ours half-open.
            if not start <= _parse_time(run["created_at"]) < end:
                continue

            runs.append(
                Run(
                    started_at=_parse_time(run["run_started_at"]),
                    finished_at=_parse_time(run["updated_at"]),
                    passed=conclusion == "success",
                )
            )
        return runs

    @staticmethod
    def _default_session() -> requests.Session:
        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            raise RuntimeError("GITHUB_TOKEN is required to read GitHub Actions runs.")

        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        return session
