import sqlite3


def init_integration_schema(conn: sqlite3.Connection):
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS bot_peers (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          platform            TEXT NOT NULL,
          bot_scope           TEXT NOT NULL,
          external_peer_id    TEXT NOT NULL,
          external_user_id    TEXT,
          display_name        TEXT,
          username            TEXT,
          is_active           INTEGER NOT NULL DEFAULT 1,
          last_seen_at        TEXT,
          created_at          TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_peers_unique
          ON bot_peers(platform, bot_scope, external_peer_id);
        CREATE INDEX IF NOT EXISTS idx_bot_peers_active
          ON bot_peers(platform, bot_scope, is_active, updated_at);

        CREATE TABLE IF NOT EXISTS bot_message_links (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          reservation_id      INTEGER NOT NULL,
          platform            TEXT NOT NULL,
          bot_scope           TEXT NOT NULL,
          peer_id             INTEGER,
          external_chat_id    TEXT,
          external_message_id TEXT,
          message_kind        TEXT NOT NULL DEFAULT 'reservation_card',
          created_at          TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
          FOREIGN KEY (reservation_id) REFERENCES reservations(id) ON DELETE CASCADE,
          FOREIGN KEY (peer_id) REFERENCES bot_peers(id) ON DELETE SET NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_message_links_unique
          ON bot_message_links(platform, bot_scope, external_chat_id, external_message_id, message_kind);
        CREATE INDEX IF NOT EXISTS idx_bot_message_links_reservation
          ON bot_message_links(reservation_id, platform, bot_scope, message_kind);

        CREATE TABLE IF NOT EXISTS bot_inbound_events (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          platform            TEXT NOT NULL,
          bot_scope           TEXT NOT NULL,
          external_event_id   TEXT,
          event_type          TEXT NOT NULL,
          actor_external_id   TEXT,
          actor_display_name  TEXT,
          peer_external_id    TEXT,
          reservation_id      INTEGER,
          payload_json        TEXT NOT NULL,
          processing_status   TEXT NOT NULL DEFAULT 'new',
          error_text          TEXT,
          created_at          TEXT NOT NULL DEFAULT (datetime('now')),
          processed_at        TEXT,
          FOREIGN KEY (reservation_id) REFERENCES reservations(id) ON DELETE SET NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_inbound_events_unique
          ON bot_inbound_events(platform, bot_scope, external_event_id)
          WHERE external_event_id IS NOT NULL AND trim(external_event_id) <> '';
        CREATE INDEX IF NOT EXISTS idx_bot_inbound_events_status
          ON bot_inbound_events(platform, bot_scope, processing_status, created_at);
        CREATE INDEX IF NOT EXISTS idx_bot_inbound_events_reservation
          ON bot_inbound_events(reservation_id, created_at);

        CREATE TABLE IF NOT EXISTS bot_outbox (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          reservation_id      INTEGER,
          platform            TEXT NOT NULL,
          bot_scope           TEXT NOT NULL,
          target_peer_id      INTEGER,
          target_external_id  TEXT,
          message_type        TEXT NOT NULL,
          payload_json        TEXT NOT NULL,
          delivery_status     TEXT NOT NULL DEFAULT 'new',
          attempts            INTEGER NOT NULL DEFAULT 0,
          last_error          TEXT,
          created_at          TEXT NOT NULL DEFAULT (datetime('now')),
          sent_at             TEXT,
          FOREIGN KEY (reservation_id) REFERENCES reservations(id) ON DELETE SET NULL,
          FOREIGN KEY (target_peer_id) REFERENCES bot_peers(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_bot_outbox_status
          ON bot_outbox(platform, bot_scope, delivery_status, created_at);
        CREATE INDEX IF NOT EXISTS idx_bot_outbox_reservation
          ON bot_outbox(reservation_id, created_at);

        CREATE TABLE IF NOT EXISTS public_reservation_tokens (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          reservation_id      INTEGER NOT NULL,
          public_token        TEXT NOT NULL,
          token_kind          TEXT NOT NULL DEFAULT 'guest_access',
          status              TEXT NOT NULL DEFAULT 'active',
          expires_at          TEXT,
          created_at          TEXT NOT NULL DEFAULT (datetime('now')),
          used_at             TEXT,
          FOREIGN KEY (reservation_id) REFERENCES reservations(id) ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_public_reservation_tokens_unique
          ON public_reservation_tokens(public_token);
        CREATE INDEX IF NOT EXISTS idx_public_reservation_tokens_reservation
          ON public_reservation_tokens(reservation_id, token_kind, status);

        CREATE TABLE IF NOT EXISTS channel_binding_tokens (
          id                       INTEGER PRIMARY KEY AUTOINCREMENT,
          reservation_id           INTEGER NOT NULL,
          token_hash               TEXT NOT NULL,
          guest_phone_e164         TEXT,
          channel_type             TEXT NOT NULL,
          status                   TEXT NOT NULL DEFAULT 'active',
          expires_at               TEXT NOT NULL,
          used_at                  TEXT,
          used_by_external_user_id TEXT,
          created_at               TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at               TEXT NOT NULL DEFAULT (datetime('now')),
          FOREIGN KEY (reservation_id) REFERENCES reservations(id) ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_binding_tokens_hash
          ON channel_binding_tokens(token_hash);
        CREATE INDEX IF NOT EXISTS idx_channel_binding_tokens_reservation
          ON channel_binding_tokens(reservation_id, channel_type, status, expires_at);
        """
    )


def run_integration_schema_migrations(conn: sqlite3.Connection):
    init_integration_schema(conn)
    conn.commit()
