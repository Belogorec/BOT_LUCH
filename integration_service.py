import json
import sqlite3
from typing import Optional


def upsert_bot_peer(
    conn: sqlite3.Connection,
    *,
    platform: str,
    bot_scope: str,
    external_peer_id: str,
    external_user_id: Optional[str] = None,
    display_name: str = "",
    username: str = "",
) -> int:
    conn.execute(
        """
        INSERT INTO bot_peers (
          platform,
          bot_scope,
          external_peer_id,
          external_user_id,
          display_name,
          username,
          is_active,
          last_seen_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
        ON CONFLICT(platform, bot_scope, external_peer_id) DO UPDATE SET
          external_user_id = COALESCE(excluded.external_user_id, bot_peers.external_user_id),
          display_name = CASE
            WHEN trim(COALESCE(excluded.display_name, '')) <> '' THEN excluded.display_name
            ELSE bot_peers.display_name
          END,
          username = CASE
            WHEN trim(COALESCE(excluded.username, '')) <> '' THEN excluded.username
            ELSE bot_peers.username
          END,
          is_active = 1,
          last_seen_at = datetime('now'),
          updated_at = datetime('now')
        """,
        (
            platform,
            bot_scope,
            external_peer_id,
            external_user_id,
            display_name.strip(),
            username.strip(),
        ),
    )
    row = conn.execute(
        """
        SELECT id
        FROM bot_peers
        WHERE platform = ? AND bot_scope = ? AND external_peer_id = ?
        """,
        (platform, bot_scope, external_peer_id),
    ).fetchone()
    return int(row["id"])


def create_outbox_message(
    conn: sqlite3.Connection,
    *,
    reservation_id: Optional[int],
    platform: str,
    bot_scope: str,
    message_type: str,
    payload: dict,
    target_peer_id: Optional[int] = None,
    target_external_id: Optional[str] = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO bot_outbox (
          reservation_id,
          platform,
          bot_scope,
          target_peer_id,
          target_external_id,
          message_type,
          payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            reservation_id,
            platform,
            bot_scope,
            target_peer_id,
            target_external_id,
            message_type,
            json.dumps(payload or {}, ensure_ascii=False),
        ),
    )
    return int(cur.lastrowid)


def record_inbound_event(
    conn: sqlite3.Connection,
    *,
    platform: str,
    bot_scope: str,
    event_type: str,
    payload: dict,
    external_event_id: Optional[str] = None,
    actor_external_id: Optional[str] = None,
    actor_display_name: str = "",
    peer_external_id: Optional[str] = None,
    reservation_id: Optional[int] = None,
) -> int:
    existing = None
    if external_event_id:
        existing = conn.execute(
            """
            SELECT id
            FROM bot_inbound_events
            WHERE platform = ? AND bot_scope = ? AND external_event_id = ?
            """,
            (platform, bot_scope, external_event_id),
        ).fetchone()
    if existing:
        return int(existing["id"])

    cur = conn.execute(
        """
        INSERT INTO bot_inbound_events (
          platform,
          bot_scope,
          external_event_id,
          event_type,
          actor_external_id,
          actor_display_name,
          peer_external_id,
          reservation_id,
          payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            platform,
            bot_scope,
            external_event_id,
            event_type,
            actor_external_id,
            actor_display_name.strip(),
            peer_external_id,
            reservation_id,
            json.dumps(payload or {}, ensure_ascii=False),
        ),
    )
    return int(cur.lastrowid)


def mark_inbound_event_processed(
    conn: sqlite3.Connection,
    event_id: int,
    *,
    status: str = "processed",
    error_text: str = "",
):
    conn.execute(
        """
        UPDATE bot_inbound_events
        SET processing_status = ?,
            error_text = ?,
            processed_at = datetime('now')
        WHERE id = ?
        """,
        (status, error_text.strip() or None, int(event_id)),
    )


def link_message_to_reservation(
    conn: sqlite3.Connection,
    *,
    reservation_id: int,
    platform: str,
    bot_scope: str,
    external_chat_id: str,
    external_message_id: str,
    message_kind: str = "reservation_card",
    peer_id: Optional[int] = None,
):
    existing = conn.execute(
        """
        SELECT id
        FROM bot_message_links
        WHERE platform = ?
          AND bot_scope = ?
          AND external_chat_id = ?
          AND external_message_id = ?
          AND message_kind = ?
        """,
        (platform, bot_scope, external_chat_id, external_message_id, message_kind),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE bot_message_links
            SET reservation_id = ?,
                peer_id = COALESCE(?, peer_id),
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (reservation_id, peer_id, int(existing["id"])),
        )
        return int(existing["id"])

    cur = conn.execute(
        """
        INSERT INTO bot_message_links (
          reservation_id,
          platform,
          bot_scope,
          peer_id,
          external_chat_id,
          external_message_id,
          message_kind
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            reservation_id,
            platform,
            bot_scope,
            peer_id,
            external_chat_id,
            external_message_id,
            message_kind,
        ),
    )
    return int(cur.lastrowid)
