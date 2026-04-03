import traceback


def _normalize_value(value: object) -> str:
    if value is None:
        return "-"

    text = str(value)
    return text if text else "-"


def log_event(prefix: str, **fields: object) -> None:
    parts = [f"{key}={_normalize_value(value)}" for key, value in fields.items()]
    message = f"[{prefix}]"
    if parts:
        message = f"{message} {' '.join(parts)}"
    print(message, flush=True)


def log_exception(prefix: str, **fields: object) -> None:
    log_event(prefix, **fields)
    print(traceback.format_exc(), flush=True)
