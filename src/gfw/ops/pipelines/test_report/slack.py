"""Posts messages to Slack."""

from __future__ import annotations

import requests


_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"


def post_message(token: str, channel: str, text: str) -> None:
    """Post ``text`` to ``channel`` as the Slack app that owns ``token``.

    Raises:
        RuntimeError: If Slack rejects the message (for example when the app is not
            a member of the channel).
    """
    response = requests.post(
        _POST_MESSAGE_URL,
        headers={"Authorization": f"Bearer {token}"},
        json={"channel": channel, "text": text, "mrkdwn": True},
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    if not body.get("ok"):
        raise RuntimeError(f"Slack chat.postMessage failed: {body.get('error')}")
