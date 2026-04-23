import traceback

try:
    from flask import g, has_request_context
except ModuleNotFoundError:
    g = None

    def has_request_context() -> bool:
        return False


def _normalize_value(value: object) -> str:
    if value is None:
        return "-"

    text = str(value)
    return text if text else "-"


def log_event(prefix: str, **fields: object) -> None:
    if "request_id" not in fields and has_request_context() and g is not None:
        request_id = getattr(g, "request_id", "")
        if request_id:
            fields = {"request_id": request_id, **fields}
    parts = [f"{key}={_normalize_value(value)}" for key, value in fields.items()]
    message = f"[{prefix}]"
    if parts:
        message = f"{message} {' '.join(parts)}"
    print(message, flush=True)


def log_exception(prefix: str, **fields: object) -> None:
    log_event(prefix, **fields)
    print(traceback.format_exc(), flush=True)
