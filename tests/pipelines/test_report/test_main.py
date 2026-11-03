from datetime import datetime, timedelta, timezone

import pytest

from gfw.ops.pipelines.test_report import run
from gfw.ops.pipelines.test_report.models import Environment, Run, Suite


class FakeSource:
    def __init__(self, runs=(), error=None):
        self.runs = list(runs)
        self.error = error
        self.window = None

    def fetch(self, start, end):
        self.window = (start, end)
        if self.error:
            raise self.error
        return self.runs


def _run(passed):
    started = datetime(2026, 10, 28, tzinfo=timezone.utc)
    return Run(started, started + timedelta(minutes=10), passed)


def _suite(source, name="API (daily)"):
    return Suite(name, Environment.PRODUCTION, source)


def test_posts_the_report_to_the_channel(monkeypatch):
    monkeypatch.setenv("SLACK_TOKEN", "xoxb-token")
    posted = []
    source = FakeSource([_run(True), _run(False)])

    run(
        slack_channel="C123",
        end_date="2026-11-03",
        suites=[_suite(source)],
        post=lambda *args: posted.append(args),
    )

    assert source.window == (
        datetime(2026, 10, 27, tzinfo=timezone.utc),
        datetime(2026, 11, 3, tzinfo=timezone.utc),
    )
    token, channel, text = posted[0]
    assert (token, channel) == ("xoxb-token", "C123")
    assert "• API (daily): 50% passed (1/2) · avg 10m" in text


def test_dry_run_prints_without_posting(capsys):
    def fail(*args):
        raise AssertionError("must not post")

    run(dry_run=True, end_date="2026-11-03", suites=[_suite(FakeSource([_run(True)]))], post=fail)

    assert "• API (daily): 100% passed (1/1)" in capsys.readouterr().out


def test_window_length_follows_days():
    source = FakeSource()

    run(dry_run=True, days=1, end_date="2026-11-03", suites=[_suite(source)])

    assert source.window[0] == datetime(2026, 11, 2, tzinfo=timezone.utc)


def test_channel_is_required_unless_dry_run():
    with pytest.raises(ValueError, match="--slack-channel"):
        run(suites=[])


def test_slack_token_is_required(monkeypatch):
    monkeypatch.delenv("SLACK_TOKEN", raising=False)

    with pytest.raises(RuntimeError, match="SLACK_TOKEN"):
        run(slack_channel="C123", suites=[], post=lambda *args: None)


def test_unreadable_suite_is_reported_then_fails_the_run(monkeypatch):
    monkeypatch.setenv("SLACK_TOKEN", "xoxb-token")
    posted = []
    suites = [
        _suite(FakeSource(error=OSError("boom")), name="API (daily)"),
        _suite(FakeSource([_run(True)]), name="Frontend (daily)"),
    ]

    with pytest.raises(RuntimeError, match="API \\(daily\\)"):
        run(slack_channel="C123", suites=suites, post=lambda *args: posted.append(args))

    text = posted[0][2]
    assert "• API (daily): data unavailable ⚠️" in text
    assert "• Frontend (daily): 100% passed (1/1)" in text
