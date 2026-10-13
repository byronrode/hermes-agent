"""Persist Meta's customer-service window independently of agent conversation activity."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from contextlib import closing, contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)
CUSTOMER_SERVICE_WINDOW_SECONDS = 24 * 60 * 60
FALLBACK_RECEIPT_SECONDS = 10 * 60


def _path(home: Path) -> Path:
    return home / "state" / "whatsapp_cloud_windows.db"


@contextmanager
def _write_database(home: Path):
    path = _path(home)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(mode=0o600, exist_ok=True)
    with closing(sqlite3.connect(path, timeout=2)) as db, db:
        db.execute("""CREATE TABLE IF NOT EXISTS chat_windows (
            phone_id TEXT NOT NULL, chat_id TEXT NOT NULL,
            last_inbound_at INTEGER NOT NULL,
            PRIMARY KEY (phone_id, chat_id)
        )""")
        db.execute("""CREATE TABLE IF NOT EXISTS scheduled_fallbacks (
            phone_id TEXT NOT NULL, message_id TEXT NOT NULL, chat_id TEXT NOT NULL,
            content TEXT, template TEXT, job_name TEXT, rejected INTEGER NOT NULL, delivered INTEGER NOT NULL,
            claimed INTEGER NOT NULL DEFAULT 0, expires_at REAL NOT NULL,
            PRIMARY KEY (phone_id, message_id)
        )""")
        db.execute("DELETE FROM scheduled_fallbacks WHERE expires_at <= ?", (time.time(),))
        yield db


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
        with _write_database(home) as db:
            db.execute("""INSERT INTO chat_windows VALUES (?, ?, ?)
                ON CONFLICT(phone_id, chat_id) DO UPDATE SET
                last_inbound_at = MAX(last_inbound_at, excluded.last_inbound_at)
            """, (phone_id, chat_id, received_at))
    except (OSError, sqlite3.Error):
        logger.warning("Could not persist WhatsApp customer-service window", exc_info=True)


def is_open(home: Path, phone_id: str, chat_id: str) -> bool | None:
    """None means unknown; Meta can authoritatively reject a first free-form send."""
    try:
        path = _path(home)
        if not path.exists():
            return None
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
            row = db.execute(
                "SELECT last_inbound_at FROM chat_windows WHERE phone_id = ? AND chat_id = ?",
                (phone_id, chat_id),
            ).fetchone()
        if row is None:
            return None
        return 0 <= time.time() - row[0] < CUSTOMER_SERVICE_WINDOW_SECONDS
    except (OSError, sqlite3.Error):
        logger.warning("Could not read WhatsApp customer-service window; deferring to Meta", exc_info=True)
        return None


def remember_send(home: Path, phone_id: str, chat_id: str, message_id: str,
                  content: str | None = None, template: dict | None = None,
                  job_name: str | None = None, *, rejected: bool = False, delivered: bool = False) -> None:
    """Join accepted sends and signed rejection receipts, in either arrival order."""
    try:
        with _write_database(home) as db:
            db.execute("""INSERT INTO scheduled_fallbacks
                (phone_id, message_id, chat_id, content, template, job_name, rejected, delivered, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(phone_id, message_id) DO UPDATE SET
                content = COALESCE(excluded.content, scheduled_fallbacks.content),
                template = COALESCE(excluded.template, scheduled_fallbacks.template),
                job_name = COALESCE(excluded.job_name, scheduled_fallbacks.job_name),
                rejected = MAX(scheduled_fallbacks.rejected, excluded.rejected),
                delivered = MAX(scheduled_fallbacks.delivered, excluded.delivered)
                WHERE scheduled_fallbacks.chat_id = excluded.chat_id
            """, (phone_id, message_id, chat_id, content,
                   json.dumps(template) if template is not None else None, job_name,
                   int(rejected), int(delivered), time.time() + FALLBACK_RECEIPT_SECONDS))
    except (OSError, sqlite3.Error):
        logger.warning("Could not persist WhatsApp scheduled-send receipt", exc_info=True)


def retry_keys(home: Path, phone_id: str) -> list[tuple[str, str]]:
    path = _path(home)
    if not path.exists():
        return []
    try:
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
            return db.execute("""SELECT chat_id, message_id FROM scheduled_fallbacks
                WHERE phone_id = ? AND rejected = 1 AND delivered = 0 AND claimed = 0
                AND content IS NOT NULL AND expires_at > ?""", (phone_id, time.time())).fetchall()
    except sqlite3.Error:
        return []


def claim_retry(home: Path, phone_id: str, chat_id: str, message_id: str) -> tuple | None:
    """Claim before POST; duplicate receipts or a crash cannot send the fallback twice."""
    try:
        with closing(sqlite3.connect(_path(home), timeout=2)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT content, template, job_name FROM scheduled_fallbacks
                WHERE phone_id = ? AND chat_id = ? AND message_id = ?
                AND rejected = 1 AND delivered = 0 AND claimed = 0 AND content IS NOT NULL
                AND template IS NOT NULL
                AND expires_at > ?""", (phone_id, chat_id, message_id, time.time())).fetchone()
            if row is None:
                return None
            db.execute("UPDATE scheduled_fallbacks SET claimed = 1 WHERE phone_id = ? AND message_id = ?",
                       (phone_id, message_id))
            return row[0], json.loads(row[1]), row[2]
    except (OSError, sqlite3.Error, ValueError):
        logger.warning("Could not claim WhatsApp window-rejection fallback", exc_info=True)
        return None
