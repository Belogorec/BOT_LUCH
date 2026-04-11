import json
import time
from typing import Any, Optional

import requests

from config import VK_API_VERSION, get_vk_bot_config

_session = requests.Session()
_session.trust_env = True


def vk_api_enabled(bot_key: str = "hostess") -> bool:
    bot = get_vk_bot_config(bot_key)
    return bool(bot.get("access_token"))


def vk_api_post(method: str, data: dict[str, Any], *, bot_key: str = "hostess") -> dict[str, Any]:
    bot = get_vk_bot_config(bot_key)
    access_token = str(bot.get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError(f"VK access token missing for bot '{bot_key}'")

    payload = {
        **data,
        "access_token": access_token,
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


def vk_send_message(
    peer_id: int,
    text: str,
    *,
    bot_key: str = "hostess",
    random_id: Optional[int] = None,
    keyboard: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    message_text = str(text or "").strip()
    if not message_text:
        raise ValueError("VK message text is empty")

    payload: dict[str, Any] = {
        "peer_id": int(peer_id),
        "random_id": int(random_id if random_id is not None else time.time_ns() % 2147483647),
        "message": message_text,
    }
    if keyboard is not None:
        payload["keyboard"] = json.dumps(keyboard, ensure_ascii=False)

    return vk_api_post(
        "messages.send",
        payload,
        bot_key=bot_key,
    )


def vk_answer_message_event(
    *,
    event_id: str,
    user_id: int,
    peer_id: int,
    event_data: dict[str, Any],
    bot_key: str = "hostess",
) -> dict[str, Any]:
    payload = {
        "event_id": str(event_id or "").strip(),
        "user_id": int(user_id),
        "peer_id": int(peer_id),
        "event_data": json.dumps(event_data, ensure_ascii=False),
    }
    return vk_api_post(
        "messages.sendMessageEventAnswer",
        payload,
        bot_key=bot_key,
    )
