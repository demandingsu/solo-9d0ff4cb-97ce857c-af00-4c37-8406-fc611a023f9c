"""SQLite 持久层。

关键的"不漏对、不重对"由 :func:`commit_pair` 的单事务保证：
只有当两端偏移都成功前推、配对记录落库后，事务才提交；
任何一步失败则整体回滚，下次轮询会从旧偏移重新拉取、重新配对。
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    base_url      TEXT NOT NULL,
    status        TEXT NOT NULL,            -- running|completed|error
    r1_offset     INTEGER NOT NULL DEFAULT 0,
    r2_offset     INTEGER NOT NULL DEFAULT 0,
    pairs         INTEGER NOT NULL DEFAULT 0,
    error         TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS pairs (
    run_id     TEXT NOT NULL,
    seq        INTEGER NOT NULL,             -- 配对顺序号，从 0 开始
    name       TEXT NOT NULL,
    r1_record  BLOB NOT NULL,
    r2_record  BLOB NOT NULL,
    PRIMARY KEY (run_id, seq),
    UNIQUE (run_id, name)
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def create_run(conn: sqlite3.Connection, run_id: str, base_url: str, now: float) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO runs(run_id, base_url, status, created_at, updated_at)"
        " VALUES (?, ?, 'running', ?, ?)",
        (run_id, base_url, now, now),
    )
    conn.commit()


def get_run(conn: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()


def list_runs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM runs ORDER BY created_at").fetchall()


def mark_status(
    conn: sqlite3.Connection, run_id: str, status: str, now: float, error: dict | None = None
) -> None:
    conn.execute(
        "UPDATE runs SET status=?, error=?, updated_at=? WHERE run_id=?",
        (status, json.dumps(error, ensure_ascii=False) if error else None, now, run_id),
    )
    conn.commit()


def commit_pair(
    conn: sqlite3.Connection,
    run_id: str,
    seq: int,
    name: str,
    r1_record: bytes,
    r2_record: bytes,
    r1_offset: int,
    r2_offset: int,
    now: float,
) -> None:
    """单事务写入一条配对并前推两端字节进度。"""
    with conn:  # 自动 BEGIN/COMMIT，异常自动 ROLLBACK
        conn.execute(
            "INSERT INTO pairs(run_id, seq, name, r1_record, r2_record)"
            " VALUES (?, ?, ?, ?, ?)",
            (run_id, seq, name, r1_record, r2_record),
        )
        conn.execute(
            "UPDATE runs SET r1_offset=?, r2_offset=?, pairs=pairs+1, updated_at=?"
            " WHERE run_id=?",
            (r1_offset, r2_offset, now, run_id),
        )


def iter_records(conn: sqlite3.Connection, run_id: str, side: str):
    """按配对顺序流式产出某一端的完整 FASTQ 记录字节。"""
    col = "r1_record" if side == 1 else "r2_record"
    cur = conn.execute(
        f"SELECT {col} AS rec FROM pairs WHERE run_id=? ORDER BY seq", (run_id,)
    )
    for row in cur:
        yield row["rec"]


def snapshot(row: sqlite3.Row) -> dict[str, Any]:
    error = json.loads(row["error"]) if row["error"] else None
    return {
        "run_id": row["run_id"],
        "base_url": row["base_url"],
        "status": row["status"],
        "r1_offset": row["r1_offset"],
        "r2_offset": row["r2_offset"],
        "pairs": row["pairs"],
        "error": error,
    }
