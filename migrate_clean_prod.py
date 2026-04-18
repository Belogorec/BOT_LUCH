import argparse
import os
import sqlite3
from typing import Optional

from contact_schema import init_contact_schema
from core_schema import init_core_schema
from integration_schema import init_integration_schema
from db import init_schema


def connect_sqlite(path: str) -> sqlite3.Connection:
    db_path = str(path or "").strip()
    if not db_path:
        raise ValueError("db_path_required")

    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row)


def upsert_contact(target: sqlite3.Connection, *, phone_e164: str, display_name: str, tags_json: str = "[]") -> int:
    phone = str(phone_e164 or "").strip()
    if not phone:
        raise ValueError("phone_e164_required")

    existing = target.execute("SELECT id, display_name FROM contacts WHERE phone_e164=?", (phone,)).fetchone()
    if existing:
        target.execute(
            """
            UPDATE contacts
            SET display_name = CASE
                  WHEN trim(COALESCE(display_name, '')) = '' AND trim(COALESCE(?, '')) <> '' THEN ?
                  ELSE display_name
                END,
                tags_json = CASE
                  WHEN trim(COALESCE(?, '')) <> '' THEN ?
                  ELSE tags_json
                END,
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (display_name, display_name, tags_json, tags_json, int(existing["id"])),
        )
        return int(existing["id"])

    cur = target.execute(
        """
        INSERT INTO contacts (phone_e164, display_name, tags_json, source)
        VALUES (?, ?, ?, 'legacy_import')
        """,
        (phone, str(display_name or "").strip() or None, tags_json or "[]"),
    )
    return int(cur.lastrowid)


def upsert_contact_channel(
    target: sqlite3.Connection,
    *,
    platform: str,
    external_user_id: str,
    contact_id: Optional[int] = None,
    external_peer_id: Optional[str] = None,
    username: str = "",
    display_name: str = "",
    channel_kind: str = "user",
):
    external_id = str(external_user_id or "").strip()
    if not external_id:
        return

    existing = target.execute(
        "SELECT id FROM contact_channels WHERE platform=? AND external_user_id=?",
        (platform, external_id),
    ).fetchone()
    if existing:
        target.execute(
            """
            UPDATE contact_channels
            SET contact_id = COALESCE(?, contact_id),
                external_peer_id = COALESCE(?, external_peer_id),
                username = CASE WHEN trim(COALESCE(?, '')) <> '' THEN ? ELSE username END,
                display_name = CASE WHEN trim(COALESCE(?, '')) <> '' THEN ? ELSE display_name END,
                channel_kind = COALESCE(?, channel_kind),
                status = 'active',
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (
                contact_id,
                external_peer_id,
                username,
                username,
                display_name,
                display_name,
                channel_kind,
                int(existing["id"]),
            ),
        )
        return

    target.execute(
        """
        INSERT INTO contact_channels (
          contact_id,
          platform,
          channel_kind,
          external_user_id,
          external_peer_id,
          username,
          display_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            contact_id,
            platform,
            channel_kind,
            external_id,
            external_peer_id,
            username.strip() or None,
            display_name.strip() or None,
        ),
    )


def migrate_contacts(source: sqlite3.Connection, target: sqlite3.Connection) -> dict:
    migrated_contacts = 0
    migrated_channels = 0
    migrated_peers = 0

    if table_exists(source, "guests"):
        rows = source.execute(
            """
            SELECT phone_e164, name_last, tags_json
            FROM guests
            WHERE phone_e164 IS NOT NULL AND trim(phone_e164) <> ''
            ORDER BY phone_e164 ASC
            """
        ).fetchall()
        for row in rows:
            upsert_contact(
                target,
                phone_e164=row["phone_e164"],
                display_name=row["name_last"] or "",
                tags_json=row["tags_json"] or "[]",
            )
            migrated_contacts += 1

    if table_exists(source, "tg_bot_users"):
        rows = source.execute(
            """
            SELECT tg_user_id, username, first_name, last_name, phone_e164, has_shared_phone
            FROM tg_bot_users
            ORDER BY tg_user_id ASC
            """
        ).fetchall()
        for row in rows:
            contact_id = None
            phone = str(row["phone_e164"] or "").strip()
            name = " ".join(
                part for part in [str(row["first_name"] or "").strip(), str(row["last_name"] or "").strip()] if part
            ).strip()
            if phone and int(row["has_shared_phone"] or 0):
                contact_id = upsert_contact(
                    target,
                    phone_e164=phone,
                    display_name=name or row["username"] or "",
                )
                migrated_contacts += 1
            upsert_contact_channel(
                target,
                platform="telegram",
                external_user_id=str(row["tg_user_id"] or "").strip(),
                contact_id=contact_id,
                username=str(row["username"] or "").strip(),
                display_name=name,
                channel_kind="user",
            )
            migrated_channels += 1

    if table_exists(source, "guest_channel_bindings"):
        rows = source.execute(
            """
            SELECT guest_phone_e164, channel_type, external_user_id, external_username, external_display_name
            FROM guest_channel_bindings
            ORDER BY id ASC
            """
        ).fetchall()
        for row in rows:
            contact_id = None
            phone = str(row["guest_phone_e164"] or "").strip()
            if phone:
                existing_contact = target.execute(
                    "SELECT id FROM contacts WHERE phone_e164=?",
                    (phone,),
                ).fetchone()
                if existing_contact:
                    contact_id = int(existing_contact["id"])
            upsert_contact_channel(
                target,
                platform=str(row["channel_type"] or "").strip() or "unknown",
                external_user_id=str(row["external_user_id"] or "").strip(),
                contact_id=contact_id,
                username=str(row["external_username"] or "").strip(),
                display_name=str(row["external_display_name"] or "").strip(),
                channel_kind="guest_binding",
            )
            migrated_channels += 1

    if table_exists(source, "vk_staff_peers"):
        rows = source.execute(
            """
            SELECT peer_id, peer_external_id, bot_key, from_id, role_hint, last_message_text, is_active
            FROM vk_staff_peers
            ORDER BY peer_id ASC
            """
        ).fetchall()
        for row in rows:
            external_peer_id = str(row["peer_external_id"] or row["peer_id"] or "").strip()
            if not external_peer_id:
                continue
            target.execute(
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
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
                  is_active = excluded.is_active,
                  last_seen_at = datetime('now'),
                  updated_at = datetime('now')
                """,
                (
                    "vk",
                    str(row["bot_key"] or "").strip() or "hostess",
                    external_peer_id,
                    str(row["from_id"] or "").strip() or None,
                    str(row["last_message_text"] or row["role_hint"] or "").strip() or None,
                    None,
                    int(row["is_active"] or 0),
                ),
            )
            migrated_peers += 1

    return {
        "contacts": migrated_contacts,
        "channels": migrated_channels,
        "bot_peers": migrated_peers,
    }


def bootstrap_target_schema(target: sqlite3.Connection):
    init_schema(target)
    init_core_schema(target)
    init_integration_schema(target)
    init_contact_schema(target)
    target.commit()


def main():
    parser = argparse.ArgumentParser(description="Clean production migration: contacts and users only.")
    parser.add_argument("--source-db", required=True, help="Path to legacy SQLite DB.")
    parser.add_argument("--target-db", required=True, help="Path to new target SQLite DB.")
    parser.add_argument("--apply", action="store_true", help="Apply migration. Without this flag only schema bootstrap is validated.")
    args = parser.parse_args()

    source = connect_sqlite(args.source_db)
    target = connect_sqlite(args.target_db)
    try:
        bootstrap_target_schema(target)

        if not args.apply:
            print("DRY-RUN", f"source={args.source_db}", f"target={args.target_db}")
            target.rollback()
            return

        result = migrate_contacts(source, target)
        target.commit()
        print(
            "DONE",
            f"contacts={result['contacts']}",
            f"channels={result['channels']}",
            f"bot_peers={result['bot_peers']}",
            f"target={args.target_db}",
        )
    finally:
        source.close()
        target.close()


if __name__ == "__main__":
    main()
