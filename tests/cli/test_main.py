from unittest.mock import patch

import pytest

from gfw.ops.cli.main import main


def test_main_shows_help():
    with patch("sys.argv", ["gfw-ops", "--help"]):
        with pytest.raises(SystemExit) as exc:
            main()
    assert exc.value.code == 0
