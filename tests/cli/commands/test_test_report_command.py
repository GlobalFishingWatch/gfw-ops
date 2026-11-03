from gfw.common.cli import CLI

from gfw.ops.cli.commands.test_report import WeeklyTestReport
from gfw.ops.pipelines.test_report.models import Environment, Run, Suite


class _Source:
    def fetch(self, start, end):
        return [Run(start, end, passed=True)]


def test_run_dry_run(capsys):
    cli = CLI(subcommands=[WeeklyTestReport])

    cli.execute(
        ["test-report", "--dry-run", "--end-date", "2026-11-03"],
        suites=[Suite("API (daily)", Environment.PRODUCTION, _Source())],
    )

    assert "• API (daily): 100% passed (1/1)" in capsys.readouterr().out
