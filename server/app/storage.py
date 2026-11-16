from contextlib import closing
from pathlib import Path
import sqlite3
import threading
import uuid

import msgpack

from app.core import pack_message

class Storage:
    def __init__(self, path: str, origin: str) -> None:
        self.path = path
        self.origin = origin
        self.lock = threading.RLock()
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 15000")
        return conn

    def _initialize(self) -> None:
        with closing(self._connect()) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    payload BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS logins (
                    event_id TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    login_at INTEGER NOT NULL,
                    lamport INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS channels (
                    name TEXT PRIMARY KEY,
                    created_by TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    event_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS messages (
                    message_id TEXT PRIMARY KEY,
                    channel TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    content TEXT NOT NULL,
                    sent_at INTEGER NOT NULL,
                    lamport INTEGER NOT NULL,
                    origin TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_messages_channel_date
                    ON messages(channel, sent_at);
                CREATE INDEX IF NOT EXISTS idx_events_origin ON events(origin);
                CREATE TABLE IF NOT EXISTS replication_cursors (
                    peer TEXT PRIMARY KEY, seq INTEGER NOT NULL
                );
            """)
            conn.commit()

    def create_event(self, kind: str, payload: dict, timestamp: int, *, event_id: str | None = None) -> bool:
        event = {
            "event_id": event_id or str(uuid.uuid4()),
            "kind": kind,
            "origin": self.origin,
            "timestamp": int(timestamp),
            "payload": payload,
        }
        return self.apply_event(event)

    def apply_event(self, event: dict) -> bool:
        with self.lock, closing(self._connect()) as conn:
            with conn:
                cursor = conn.execute(
                    "INSERT OR IGNORE INTO events (event_id,kind,origin,timestamp,payload) VALUES (?,?,?,?,?)",
                    (event["event_id"], event["kind"], event["origin"], int(event["timestamp"]),
                     pack_message(event["payload"])),
                )
                if cursor.rowcount != 1:
                    return False
                p = event["payload"]
                if event["kind"] == "login":
                    conn.execute(
                        "INSERT OR IGNORE INTO logins VALUES (?,?,?,?)",
                        (event["event_id"], p["username"], int(p["login_at"]), int(p["lamport"])),
                    )
                elif event["kind"] == "channel":
                    conn.execute(
                        "INSERT OR IGNORE INTO channels VALUES (?,?,?,?)",
                        (p["name"], p["created_by"], int(p["created_at"]), event["event_id"]),
                    )
                elif event["kind"] == "message":
                    conn.execute(
                        "INSERT OR IGNORE INTO messages VALUES (?,?,?,?,?,?,?)",
                        (event["event_id"], p["channel"], p["sender"], p["content"],
                         int(p["sent_at"]), int(p["lamport"]), event["origin"]),
                    )
                return True

    def get_channels(self) -> list[str]:
        with closing(self._connect()) as conn:
            return [row["name"] for row in conn.execute("SELECT name FROM channels ORDER BY name")]

    def channel_exists(self, name: str) -> bool:
        with closing(self._connect()) as conn:
            return conn.execute("SELECT 1 FROM channels WHERE name=?", (name,)).fetchone() is not None

    def get_history(self, channel: str | None, limit: int = 50) -> list[dict]:
        with closing(self._connect()) as conn:
            query = "SELECT message_id,channel,sender,content,sent_at,lamport,origin FROM messages"
            params: tuple = ()
            if channel:
                query += " WHERE channel=?"
                params = (channel,)
            query += " ORDER BY sent_at DESC, message_id DESC LIMIT ?"
            rows = conn.execute(query, (*params, max(1, min(int(limit), 200)))).fetchall()
            return [dict(row) for row in reversed(rows)]

    def pull_events(self, since_sequence: int, limit: int = 100) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT sequence,event_id,kind,origin,timestamp,payload FROM events "
                "WHERE sequence > ? ORDER BY sequence LIMIT ?",
                (max(0, int(since_sequence)), max(1, min(int(limit), 200))),
            ).fetchall()
        return [{"sequence": row["sequence"], "event_id": row["event_id"],
                 "kind": row["kind"], "origin": row["origin"],
                 "timestamp": row["timestamp"], "payload": self._read_event_payload(row["payload"])}
                for row in rows]

    @staticmethod
    def _read_event_payload(payload: bytes) -> dict:
        return msgpack.unpackb(payload, raw=False)

    def count_events(self) -> int:
        with closing(self._connect()) as conn:
            return int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])

    def get_cursor(self, peer: str) -> int:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT seq FROM replication_cursors WHERE peer=?", (peer,)).fetchone()
            return int(row[0]) if row else 0

    def set_cursor(self, peer: str, sequence: int) -> None:
        with self.lock, closing(self._connect()) as conn:
            with conn:
                conn.execute("""INSERT INTO replication_cursors(peer, seq) VALUES (?,?)
                    ON CONFLICT(peer) DO UPDATE SET seq=MAX(seq, excluded.seq)""", (peer, int(sequence)))