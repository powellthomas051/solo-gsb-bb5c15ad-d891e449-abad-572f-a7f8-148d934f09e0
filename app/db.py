"""SQLite 连接与建表。

数据库路径通过环境变量 ``DB_PATH`` 配置，默认当前目录下的
``teleprompter.db``。``init_db()`` 在服务启动时调用，幂等建表。
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = "teleprompter.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS rooms (
    id                     TEXT PRIMARY KEY,
    name                   TEXT NOT NULL,
    advance_threshold      INTEGER NOT NULL CHECK (advance_threshold >= 1),
    offline_timeout_seconds INTEGER NOT NULL CHECK (offline_timeout_seconds >= 1),
    public_position        INTEGER NOT NULL DEFAULT -1,
    revision               INTEGER NOT NULL DEFAULT 0,
    paused                 INTEGER NOT NULL DEFAULT 0,  -- 排练是否暂停
    clock_version          INTEGER NOT NULL DEFAULT 0,  -- 暂停/恢复每切换一次递增
    paused_at              REAL,                        -- 本次暂停开始的 epoch 秒
    progress_epoch         INTEGER NOT NULL DEFAULT 0,  -- 进度代际：导演跳转/重导台本递增
    created_at             REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS lines (
    room_id  TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    text     TEXT NOT NULL,
    PRIMARY KEY (room_id, position)
);

CREATE TABLE IF NOT EXISTS participants (
    room_id        TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    participant_id TEXT NOT NULL,
    last_seq       INTEGER NOT NULL,
    position       INTEGER NOT NULL,
    last_seen_at   REAL NOT NULL,
    PRIMARY KEY (room_id, participant_id)
);

CREATE TABLE IF NOT EXISTS advancements (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id      TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    position     INTEGER NOT NULL,
    reason       TEXT NOT NULL,
    contributors TEXT NOT NULL,  -- JSON 数组：促成本次推进的参与者
    created_at   REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_advancements_room ON advancements(room_id, id);
"""


def db_path() -> str:
    return os.environ.get("DB_PATH", DEFAULT_DB_PATH)


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    path = db_path()
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = connect()
    try:
        conn.executescript(SCHEMA)
        _migrate(conn)
        conn.commit()
    finally:
        conn.close()


def _migrate(conn: sqlite3.Connection) -> None:
    """对已存在的旧库幂等补齐后加的列。

    - rooms.revision：台本版本号
    - rooms.paused / clock_version / paused_at：排练暂停/恢复时钟
    - rooms.progress_epoch：进度代际（导演校准公共位置 / 重导台本时递增）
    """
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(rooms)")}
    if "revision" not in columns:
        conn.execute(
            "ALTER TABLE rooms ADD COLUMN revision INTEGER NOT NULL DEFAULT 0"
        )
    if "paused" not in columns:
        conn.execute(
            "ALTER TABLE rooms ADD COLUMN paused INTEGER NOT NULL DEFAULT 0"
        )
    if "clock_version" not in columns:
        conn.execute(
            "ALTER TABLE rooms ADD COLUMN clock_version INTEGER NOT NULL DEFAULT 0"
        )
    if "paused_at" not in columns:
        conn.execute(
            "ALTER TABLE rooms ADD COLUMN paused_at REAL"
        )
    if "progress_epoch" not in columns:
        conn.execute(
            "ALTER TABLE rooms ADD COLUMN progress_epoch INTEGER NOT NULL DEFAULT 0"
        )
