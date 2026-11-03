from unittest.mock import MagicMock, patch

import pytest

from gfw.ops.pipelines.test_report.slack import post_message


def _response(body):
    response = MagicMock()
    response.json.return_value = body
    return response


def test_post_message():
    with patch("requests.post", return_value=_response({"ok": True})) as post:
        post_message("xoxb-token", "C123", "hello")

    kwargs = post.call_args.kwargs
    assert kwargs["headers"] == {"Authorization": "Bearer xoxb-token"}
    assert kwargs["json"] == {"channel": "C123", "text": "hello", "mrkdwn": True}


def test_post_message_raises_when_slack_rejects_it():
    with patch("requests.post", return_value=_response({"ok": False, "error": "not_in_channel"})):
        with pytest.raises(RuntimeError, match="not_in_channel"):
            post_message("xoxb-token", "C123", "hello")
