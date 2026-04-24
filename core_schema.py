import sqlite3


def _ensure_column(conn: sqlite3.Connection, table: str, col: str, ddl: str) -> None:
    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not table_exists:
        return
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")


def init_core_schema(conn: sqlite3.Connection):
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS reservations (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          source        TEXT NOT NULL DEFAULT 'manual',
          external_ref  TEXT,
          guest_name    TEXT,
          guest_phone   TEXT,
          reservation_at TEXT NOT NULL,
          party_size    INTEGER NOT NULL,
          comment       TEXT,
          deposit_amount INTEGER,
          deposit_comment TEXT,
          deposit_set_at TEXT,
          deposit_set_by TEXT,
          status        TEXT NOT NULL DEFAULT 'pending',
          version       INTEGER NOT NULL DEFAULT 1,
          created_at    TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_reservations_external_ref
          ON reservations(source, external_ref)
          WHERE external_ref IS NOT NULL AND trim(external_ref) <> '';
        CREATE INDEX IF NOT EXISTS idx_reservations_at
          ON reservations(reservation_at);
        CREATE INDEX IF NOT EXISTS idx_reservations_phone
          ON reservations(guest_phone);
        CREATE INDEX IF NOT EXISTS idx_reservations_status
          ON reservations(status, reservation_at);
        CREATE INDEX IF NOT EXISTS idx_reservations_deposit
          ON reservations(deposit_amount, reservation_at);

        CREATE TABLE IF NOT EXISTS tables_core (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          code          TEXT NOT NULL UNIQUE,
          title         TEXT,
          capacity      INTEGER,
          zone          TEXT,
          is_active     INTEGER NOT NULL DEFAULT 1,
          created_at    TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_tables_core_active
          ON tables_core(is_active, code);

        CREATE TABLE IF NOT EXISTS reservation_tables (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          reservation_id INTEGER NOT NULL,
          table_id       INTEGER NOT NULL,
          assigned_at    TEXT NOT NULL DEFAULT (datetime('now')),
          assigned_by    TEXT,
          released_at    TEXT,
          version        INTEGER NOT NULL DEFAULT 1,
          FOREIGN KEY (reservation_id) REFERENCES reservations(id) ON DELETE CASCADE,
          FOREIGN KEY (table_id) REFERENCES tables_core(id) ON DELETE RESTRICT
        );

        CREATE INDEX IF NOT EXISTS idx_reservation_tables_reservation
          ON reservation_tables(reservation_id, released_at, assigned_at);
        CREATE INDEX IF NOT EXISTS idx_reservation_tables_table
          ON reservation_tables(table_id, released_at, assigned_at);

        CREATE TABLE IF NOT EXISTS table_blocks (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          table_id       INTEGER NOT NULL,
          starts_at      TEXT NOT NULL,
          ends_at        TEXT NOT NULL,
          reason         TEXT,
          block_type     TEXT NOT NULL DEFAULT 'manual',
          reservation_id INTEGER,
          created_by     TEXT,
          version        INTEGER NOT NULL DEFAULT 1,
          created_at     TEXT NOT NULL DEFAULT (datetime('now')),
          FOREIGN KEY (table_id) REFERENCES tables_core(id) ON DELETE CASCADE,
          FOREIGN KEY (reservation_id) REFERENCES reservations(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_table_blocks_table_window
          ON table_blocks(table_id, starts_at, ends_at);
        CREATE INDEX IF NOT EXISTS idx_table_blocks_reservation
          ON table_blocks(reservation_id);

        CREATE TABLE IF NOT EXISTS table_sessions_core (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          table_id       INTEGER NOT NULL,
          reservation_id INTEGER,
          session_type   TEXT NOT NULL,
          session_status TEXT NOT NULL DEFAULT 'active',
          starts_at      TEXT NOT NULL,
          ends_at        TEXT,
          deposit_amount INTEGER,
          comment        TEXT,
          created_by     TEXT,
          version        INTEGER NOT NULL DEFAULT 1,
          created_at     TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at     TEXT NOT NULL DEFAULT (datetime('now')),
          archived_at    TEXT,
          FOREIGN KEY (table_id) REFERENCES tables_core(id) ON DELETE CASCADE,
          FOREIGN KEY (reservation_id) REFERENCES reservations(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_table_sessions_core_table
          ON table_sessions_core(table_id, session_status, starts_at);
        CREATE INDEX IF NOT EXISTS idx_table_sessions_core_reservation
          ON table_sessions_core(reservation_id, session_status, starts_at);
        CREATE INDEX IF NOT EXISTS idx_table_sessions_core_type
          ON table_sessions_core(session_type, session_status, starts_at);

        CREATE TABLE IF NOT EXISTS reservation_events (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          reservation_id INTEGER NOT NULL,
          event_type     TEXT NOT NULL,
          actor          TEXT,
          payload_json   TEXT,
          created_at     TEXT NOT NULL DEFAULT (datetime('now')),
          FOREIGN KEY (reservation_id) REFERENCES reservations(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_reservation_events_reservation
          ON reservation_events(reservation_id, created_at);

        CREATE TABLE IF NOT EXISTS reservation_metadata (
          reservation_id  INTEGER PRIMARY KEY,
          formname        TEXT,
          tranid          TEXT,
          phone_raw       TEXT,
          user_chat_id    TEXT,
          guest_segment   TEXT,
          raw_payload_json TEXT,
          utm_source      TEXT,
          utm_medium      TEXT,
          utm_campaign    TEXT,
          utm_content     TEXT,
          utm_term        TEXT,
          created_at      TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
          FOREIGN KEY (reservation_id) REFERENCES reservations(id) ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_reservation_metadata_tranid
          ON reservation_metadata(tranid)
          WHERE tranid IS NOT NULL AND trim(tranid) <> '';
        """
    )


def run_core_schema_migrations(conn: sqlite3.Connection):
    init_core_schema(conn)
    _ensure_column(conn, "reservations", "version", "INTEGER NOT NULL DEFAULT 1")
    _ensure_column(conn, "reservation_tables", "version", "INTEGER NOT NULL DEFAULT 1")
    _ensure_column(conn, "table_blocks", "version", "INTEGER NOT NULL DEFAULT 1")
    _ensure_column(conn, "table_sessions_core", "version", "INTEGER NOT NULL DEFAULT 1")
    _ensure_column(conn, "reservation_metadata", "formname", "TEXT")
    _ensure_column(conn, "reservation_metadata", "tranid", "TEXT")
    _ensure_column(conn, "reservation_metadata", "phone_raw", "TEXT")
    _ensure_column(conn, "reservation_metadata", "user_chat_id", "TEXT")
    _ensure_column(conn, "reservation_metadata", "guest_segment", "TEXT")
    _ensure_column(conn, "reservation_metadata", "raw_payload_json", "TEXT")
    _ensure_column(conn, "reservation_metadata", "utm_source", "TEXT")
    _ensure_column(conn, "reservation_metadata", "utm_medium", "TEXT")
    _ensure_column(conn, "reservation_metadata", "utm_campaign", "TEXT")
    _ensure_column(conn, "reservation_metadata", "utm_content", "TEXT")
    _ensure_column(conn, "reservation_metadata", "utm_term", "TEXT")
    _ensure_column(conn, "reservation_metadata", "created_at", "TEXT NOT NULL DEFAULT (datetime('now'))")
    _ensure_column(conn, "reservation_metadata", "updated_at", "TEXT NOT NULL DEFAULT (datetime('now'))")
    conn.execute("UPDATE reservations SET version = 1 WHERE version IS NULL OR version <= 0")
    conn.execute("UPDATE reservation_tables SET version = 1 WHERE version IS NULL OR version <= 0")
    conn.execute("UPDATE table_blocks SET version = 1 WHERE version IS NULL OR version <= 0")
    conn.execute("UPDATE table_sessions_core SET version = 1 WHERE version IS NULL OR version <= 0")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_reservation_metadata_tranid
          ON reservation_metadata(tranid)
          WHERE tranid IS NOT NULL AND trim(tranid) <> ''
        """
    )
    conn.commit()
