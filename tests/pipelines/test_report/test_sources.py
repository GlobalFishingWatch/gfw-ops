from datetime import datetime, timedelta, timezone

from gfw.ops.pipelines.test_report.sources import CloudBuildSource, GitHubActionsSource


START = datetime(2026, 10, 27, tzinfo=timezone.utc)
END = datetime(2026, 11, 3, tzinfo=timezone.utc)


class FakeResponse:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


class FakeSession:
    """Returns the given bodies in order and records the requests."""

    def __init__(self, *bodies):
        self._bodies = list(bodies)
        self.requests = []

    def get(self, url, params, timeout):
        self.requests.append((url, dict(params)))
        return FakeResponse(self._bodies.pop(0))


def _build(status, start="2026-10-28T00:00:10.123456789Z", finish="2026-10-28T00:10:10.5Z"):
    return {"status": status, "startTime": start, "finishTime": finish}


def test_cloud_build_counts_only_finished_builds_and_follows_pages():
    session = FakeSession(
        {"builds": [_build("SUCCESS"), _build("CANCELLED")], "nextPageToken": "next"},
        {"builds": [_build("FAILURE"), _build("TIMEOUT"), _build("INTERNAL_ERROR")]},
    )
    source = CloudBuildSource("proj", "us-central1", "pro-api-tests", session=session)

    runs = source.fetch(START, END)

    assert [run.passed for run in runs] == [True, False, False, False]
    assert runs[0].duration == timedelta(minutes=10, seconds=0.376544)
    url, params = session.requests[0]
    assert url.endswith("/projects/proj/locations/us-central1/builds")
    assert params["filter"] == (
        'substitutions.TRIGGER_NAME="pro-api-tests"'
        ' AND create_time>="2026-10-27T00:00:00Z" AND create_time<"2026-11-03T00:00:00Z"'
    )
    assert session.requests[1][1]["pageToken"] == "next"


def test_cloud_build_ignores_builds_without_timings():
    session = FakeSession({"builds": [{"status": "SUCCESS"}]})
    source = CloudBuildSource("proj", "us-central1", "pro-api-tests", session=session)

    assert source.fetch(START, END) == []


def _workflow_run(conclusion, created="2026-10-28T10:00:00Z"):
    return {
        "conclusion": conclusion,
        "created_at": created,
        "run_started_at": created,
        "updated_at": "2026-10-28T10:05:00Z",
    }


def test_github_actions_counts_only_passed_and_failed_runs():
    session = FakeSession(
        {
            "workflow_runs": [
                _workflow_run("success"),
                _workflow_run("failure"),
                _workflow_run("timed_out"),
                _workflow_run("skipped"),
                _workflow_run("cancelled"),
            ]
        }
    )
    source = GitHubActionsSource(
        "org/repo", "e2e-tests.yml", event="pull_request", session=session
    )

    runs = source.fetch(START, END)

    assert [run.passed for run in runs] == [True, False, False]
    assert runs[0].duration == timedelta(minutes=5)
    url, params = session.requests[0]
    assert url.endswith("/repos/org/repo/actions/workflows/e2e-tests.yml/runs")
    assert params["event"] == "pull_request"
    assert params["status"] == "completed"
    assert params["created"] == "2026-10-27T00:00:00Z..2026-11-03T00:00:00Z"


def test_github_actions_drops_runs_created_at_the_window_end():
    # GitHub's `created` range includes both ends; the window is half-open.
    session = FakeSession(
        {"workflow_runs": [_workflow_run("success", created="2026-11-03T00:00:00Z")]}
    )
    source = GitHubActionsSource("org/repo", "e2e-tests.yml", session=session)

    assert source.fetch(START, END) == []


def test_github_actions_follows_pages():
    full_page = [_workflow_run("success")] * 100
    session = FakeSession(
        {"workflow_runs": full_page}, {"workflow_runs": [_workflow_run("failure")]}
    )
    source = GitHubActionsSource("org/repo", "e2e-tests.yml", session=session)

    runs = source.fetch(START, END)

    assert len(runs) == 101
    assert session.requests[1][1]["page"] == 2
