import sqlite3


def init_contact_schema(conn: sqlite3.Connection):
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS contacts (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          phone_e164    TEXT UNIQUE,
          display_name  TEXT,
          tags_json     TEXT NOT NULL DEFAULT '[]',
          source        TEXT NOT NULL DEFAULT 'legacy_import',
          created_at    TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_contacts_display_name
          ON contacts(display_name);

        CREATE TABLE IF NOT EXISTS contact_channels (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          contact_id          INTEGER,
          platform            TEXT NOT NULL,
          channel_kind        TEXT NOT NULL DEFAULT 'user',
          external_user_id    TEXT NOT NULL,
          external_peer_id    TEXT,
          username            TEXT,
          display_name        TEXT,
          status              TEXT NOT NULL DEFAULT 'active',
          linked_at           TEXT NOT NULL DEFAULT (datetime('now')),
          created_at          TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
          FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE SET NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_contact_channels_unique
          ON contact_channels(platform, external_user_id);
        CREATE INDEX IF NOT EXISTS idx_contact_channels_contact
          ON contact_channels(contact_id, platform, status, updated_at);
        """
    )


def run_contact_schema_migrations(conn: sqlite3.Connection):
    init_contact_schema(conn)
    conn.commit()
