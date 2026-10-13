"""Persist Meta's customer-service window independently of agent conversation activity."""

from __future__ import annotations

import logging
import sqlite3
import time
from contextlib import closing
from pathlib import Path

logger = logging.getLogger(__name__)
CUSTOMER_SERVICE_WINDOW_SECONDS = 24 * 60 * 60


def _path(home: Path) -> Path:
    return home / "state" / "whatsapp_cloud_windows.db"


def record_inbound(home: Path, phone_id: str, chat_id: str, timestamp: object) -> None:
    """Keep the newest provider timestamp; delayed webhooks cannot extend the window."""
    try:
        if isinstance(timestamp, bool):
            return
        received_at = int(str(timestamp))
    except (TypeError, ValueError):
        return
    if not phone_id or not chat_id or not 0 < received_at <= time.time():
        return
    try:
        path = _path(home)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(mode=0o600, exist_ok=True)
        with closing(sqlite3.connect(path, timeout=2)) as db, db:
            db.execute("""CREATE TABLE IF NOT EXISTS chat_windows (
                phone_id TEXT NOT NULL, chat_id TEXT NOT NULL,
                last_inbound_at INTEGER NOT NULL,
                PRIMARY KEY (phone_id, chat_id)
            )""")
            db.execute("""INSERT INTO chat_windows VALUES (?, ?, ?)
                ON CONFLICT(phone_id, chat_id) DO UPDATE SET
                last_inbound_at = MAX(last_inbound_at, excluded.last_inbound_at)
            """, (phone_id, chat_id, received_at))
    except (OSError, sqlite3.Error):
        logger.warning("Could not persist WhatsApp customer-service window", exc_info=True)


def is_open(home: Path, phone_id: str, chat_id: str) -> bool:
    """Unknown, expired and unreadable windows require a template."""
    try:
        path = _path(home)
        if not path.exists():
            return False
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
            row = db.execute(
                "SELECT last_inbound_at FROM chat_windows WHERE phone_id = ? AND chat_id = ?",
                (phone_id, chat_id),
            ).fetchone()
        return row is not None and 0 <= time.time() - row[0] < CUSTOMER_SERVICE_WINDOW_SECONDS
    except (OSError, sqlite3.Error):
        logger.warning("Could not read WhatsApp customer-service window; using template", exc_info=True)
        return False
