import time
from typing import Any, Optional

import requests

from config import VK_ACCESS_TOKEN, VK_API_VERSION

_session = requests.Session()
_session.trust_env = True


def vk_api_enabled() -> bool:
    return bool(VK_ACCESS_TOKEN)


def vk_api_post(method: str, data: dict[str, Any]) -> dict[str, Any]:
    if not VK_ACCESS_TOKEN:
        raise RuntimeError("VK_ACCESS_TOKEN missing")

    payload = {
        **data,
        "access_token": VK_ACCESS_TOKEN,
        "v": VK_API_VERSION,
    }

    max_attempts = 4
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = _session.post(
                f"https://api.vk.com/method/{method}",
                data=payload,
                timeout=20,
            )
            response.raise_for_status()
            parsed = response.json()
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            time.sleep(0.5 * attempt)
            continue

        if parsed.get("error"):
            error = parsed["error"]
            raise RuntimeError(
                f"VK API error in {method}: "
                f"code={error.get('error_code')} message={error.get('error_msg')}"
            )

        return parsed.get("response") or {}

    raise RuntimeError(f"VK API request failed for {method}: {last_error}")


def vk_send_message(peer_id: int, text: str, *, random_id: Optional[int] = None) -> dict[str, Any]:
    message_text = str(text or "").strip()
    if not message_text:
        raise ValueError("VK message text is empty")

    return vk_api_post(
        "messages.send",
        {
            "peer_id": int(peer_id),
            "random_id": int(random_id if random_id is not None else time.time_ns() % 2147483647),
            "message": message_text,
        },
    )
