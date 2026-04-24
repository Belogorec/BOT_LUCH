from __future__ import annotations

from datetime import datetime
import sqlite3
from typing import Optional


def _parse_expires_at(value: object) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def delete_expired_pending_replies(
    conn: sqlite3.Connection,
    *,
    kind: Optional[str] = None,
    chat_id: Optional[str] = None,
    actor_tg_id: Optional[str] = None,
) -> int:
    clauses: list[str] = []
    params: list[object] = []
    if kind is not None:
        clauses.append("kind = ?")
        params.append(str(kind))
    if chat_id is not None:
        clauses.append("chat_id = ?")
        params.append(str(chat_id))
    if actor_tg_id is not None:
        clauses.append("actor_tg_id = ?")
        params.append(str(actor_tg_id))

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        f"""
        SELECT id, expires_at
        FROM pending_replies
        {where}
        ORDER BY id ASC
        """,
        tuple(params),
    ).fetchall()

    now = datetime.utcnow()
    expired_ids: list[int] = []
    for row in rows:
        exp = _parse_expires_at(row["expires_at"])
        if exp is None or now >= exp:
            expired_ids.append(int(row["id"]))
    for pending_reply_id in expired_ids:
        conn.execute("DELETE FROM pending_replies WHERE id = ?", (pending_reply_id,))
    return len(expired_ids)


def replace_pending_reply(
    conn: sqlite3.Connection,
    *,
    kind: str,
    booking_id: int,
    payload_text: str,
    chat_id: str,
    actor_tg_id: str,
    prompt_message_id: str,
    expires_at: str,
) -> int:
    normalized_kind = str(kind or "").strip()
    normalized_chat_id = str(chat_id or "").strip()
    normalized_actor_id = str(actor_tg_id or "").strip()
    if not normalized_kind or not normalized_chat_id or not normalized_actor_id:
        raise ValueError("pending_reply_scope_required")

    delete_expired_pending_replies(
        conn,
        kind=normalized_kind,
        chat_id=normalized_chat_id,
        actor_tg_id=normalized_actor_id,
    )
    conn.execute(
        """
        DELETE FROM pending_replies
        WHERE kind = ?
          AND chat_id = ?
          AND actor_tg_id = ?
        """,
        (normalized_kind, normalized_chat_id, normalized_actor_id),
    )
    cur = conn.execute(
        """
        INSERT INTO pending_replies (
            kind, booking_id, phone_e164, chat_id, actor_tg_id, prompt_message_id, expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            normalized_kind,
            int(booking_id or 0),
            str(payload_text or ""),
            normalized_chat_id,
            normalized_actor_id,
            str(prompt_message_id or ""),
            str(expires_at or "").strip(),
        ),
    )
    return int(cur.lastrowid)


def delete_superseded_pending_replies(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        """
        SELECT
          pr.id
        FROM pending_replies pr
        JOIN pending_replies newer
          ON newer.kind = pr.kind
         AND newer.chat_id = pr.chat_id
         AND newer.actor_tg_id = pr.actor_tg_id
         AND newer.id > pr.id
        WHERE datetime(pr.expires_at) > datetime('now')
          AND datetime(newer.expires_at) > datetime('now')
        GROUP BY pr.id
        ORDER BY pr.id ASC
        """
    ).fetchall()
    ids = [int(row["id"]) for row in rows]
    for pending_reply_id in ids:
        conn.execute("DELETE FROM pending_replies WHERE id = ?", (pending_reply_id,))
    return len(ids)
