"""SQLite 元数据层。

单进程内所有连接访问都通过一把 RLock 串行化；SQLite 开启 WAL，
写事务由 `with conn:` 保证提交/回滚。表设计：

- blobs       全局内容寻址 blob（按摘要去重）
- repo_blobs  repo -> blob 的命名空间关联（同一 blob 可被多个 repo 可见）
- uploads     上传会话（落盘持久化，重启后可恢复/续传）
- manifests   repo 内按摘要不可变存储的 manifest
- tags        repo 内 tag -> manifest digest（CAS 更新）
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS blobs (
    digest     TEXT PRIMARY KEY,
    size       INTEGER NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS repo_blobs (
    repo       TEXT NOT NULL,
    digest     TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (repo, digest)
);
CREATE TABLE IF NOT EXISTS uploads (
    id         TEXT PRIMARY KEY,
    repo       TEXT NOT NULL,
    state      TEXT NOT NULL,              -- active | committed | cancelled
    size       INTEGER NOT NULL DEFAULT 0,
    digest     TEXT,                       -- committed 时记录最终 digest（幂等重试用）
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    expires_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
    repo       TEXT NOT NULL,
    digest     TEXT NOT NULL,
    media_type TEXT NOT NULL,
    content    BLOB NOT NULL,
    size       INTEGER NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (repo, digest)
);
CREATE TABLE IF NOT EXISTS tags (
    repo       TEXT NOT NULL,
    name       TEXT NOT NULL,
    digest     TEXT NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (repo, name)
);
"""


class Database:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock, self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(SCHEMA)

    # ---------------- uploads ----------------

    def create_upload(self, upload_id: str, repo: str, now: float, expires_at: float):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO uploads (id, repo, state, size, digest, created_at, updated_at, expires_at)"
                " VALUES (?, ?, 'active', 0, NULL, ?, ?, ?)",
                (upload_id, repo, now, now, expires_at),
            )

    def get_upload(self, upload_id: str) -> "dict | None":
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM uploads WHERE id = ?", (upload_id,)
            ).fetchone()
            return dict(row) if row else None

    def update_upload_progress(self, upload_id: str, size: int, expires_at: float):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE uploads SET size = ?, updated_at = ?, expires_at = ? WHERE id = ?",
                (size, time.time(), expires_at, upload_id),
            )

    def set_upload_state(self, upload_id: str, state: str, digest: "str | None" = None):
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE uploads SET state = ?, digest = COALESCE(?, digest), updated_at = ? WHERE id = ?",
                (state, digest, time.time(), upload_id),
            )

    def delete_upload(self, upload_id: str):
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM uploads WHERE id = ?", (upload_id,))

    def list_uploads(self, state: "str | None" = None) -> list:
        with self._lock:
            if state:
                rows = self._conn.execute(
                    "SELECT * FROM uploads WHERE state = ?", (state,)
                ).fetchall()
            else:
                rows = self._conn.execute("SELECT * FROM uploads").fetchall()
            return [dict(r) for r in rows]

    # ---------------- blobs ----------------

    def create_blob(self, digest: str, size: int):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO blobs (digest, size, created_at) VALUES (?, ?, ?)",
                (digest, size, time.time()),
            )

    def get_blob(self, digest: str) -> "dict | None":
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM blobs WHERE digest = ?", (digest,)
            ).fetchone()
            return dict(row) if row else None

    def blob_exists(self, digest: str) -> bool:
        return self.get_blob(digest) is not None

    def all_blobs(self) -> list:
        with self._lock:
            return [dict(r) for r in self._conn.execute("SELECT * FROM blobs").fetchall()]

    def link_repo_blob(self, repo: str, digest: str):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO repo_blobs (repo, digest, created_at) VALUES (?, ?, ?)",
                (repo, digest, time.time()),
            )

    def repo_has_blob(self, repo: str, digest: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM repo_blobs WHERE repo = ? AND digest = ?", (repo, digest)
            ).fetchone()
            return row is not None

    def repos_for_blobs(self, digests: list) -> dict:
        if not digests:
            return {}
        marks = ",".join("?" for _ in digests)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT digest, repo FROM repo_blobs WHERE digest IN ({marks})",
                digests,
            ).fetchall()
        out: dict = {d: [] for d in digests}
        for r in rows:
            out.setdefault(r["digest"], []).append(r["repo"])
        return out

    def delete_blobs(self, digests: list):
        """删除 blob 记录及其 repo 关联，并清理任何悬空关联。"""
        if not digests:
            return
        marks = ",".join("?" for _ in digests)
        with self._lock, self._conn:
            self._conn.execute(f"DELETE FROM repo_blobs WHERE digest IN ({marks})", digests)
            self._conn.execute(f"DELETE FROM blobs WHERE digest IN ({marks})", digests)
            self._conn.execute(
                "DELETE FROM repo_blobs WHERE digest NOT IN (SELECT digest FROM blobs)"
            )

    # ---------------- manifests / tags ----------------

    def put_manifest(self, repo: str, digest: str, media_type: str, content: bytes):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO manifests (repo, digest, media_type, content, size, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (repo, digest, media_type, content, len(content), time.time()),
            )

    def get_manifest_by_digest(self, repo: str, digest: str) -> "dict | None":
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM manifests WHERE repo = ? AND digest = ?", (repo, digest)
            ).fetchone()
            return dict(row) if row else None

    def get_manifest_by_tag(self, repo: str, tag: str) -> "dict | None":
        with self._lock:
            row = self._conn.execute(
                "SELECT m.* FROM manifests m JOIN tags t"
                " ON t.repo = m.repo AND t.digest = m.digest"
                " WHERE t.repo = ? AND t.name = ?",
                (repo, tag),
            ).fetchone()
            return dict(row) if row else None

    def manifest_exists(self, repo: str, digest: str) -> bool:
        return self.get_manifest_by_digest(repo, digest) is not None

    def all_manifests(self) -> list:
        with self._lock:
            return [dict(r) for r in self._conn.execute(
                "SELECT repo, digest, media_type, size, created_at FROM manifests"
            ).fetchall()]

    def delete_manifests(self, keys: list):
        """keys: [(repo, digest), ...]"""
        if not keys:
            return
        marks = ",".join("(?, ?)" for _ in keys)
        params = [v for key in keys for v in key]
        with self._lock, self._conn:
            self._conn.execute(
                f"DELETE FROM manifests WHERE (repo, digest) IN ({marks})", params
            )

    def cas_tag(self, repo: str, name: str, expected: "str | None", new_digest: str):
        """比较并交换 tag。返回 (status, current_digest)。

        - created: 标签不存在且未提供 expected -> 创建
        - updated: 标签存在且 expected == 当前值 -> 移动
        - conflict: 其余情况，current_digest 为当前实际值（不存在则为 None）
        """
        now = time.time()
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT digest FROM tags WHERE repo = ? AND name = ?", (repo, name)
            ).fetchone()
            if row is None:
                if expected is not None:
                    return "conflict", None
                self._conn.execute(
                    "INSERT INTO tags (repo, name, digest, updated_at) VALUES (?, ?, ?, ?)",
                    (repo, name, new_digest, now),
                )
                return "created", None
            current = row["digest"]
            if expected is None or expected != current:
                return "conflict", current
            self._conn.execute(
                "UPDATE tags SET digest = ?, updated_at = ? WHERE repo = ? AND name = ?",
                (new_digest, now, repo, name),
            )
            return "updated", current

    def all_tags(self) -> list:
        with self._lock:
            return [dict(r) for r in self._conn.execute("SELECT * FROM tags").fetchall()]

    def list_tags(self, repo: str) -> list:
        with self._lock:
            rows = self._conn.execute(
                "SELECT name FROM tags WHERE repo = ? ORDER BY name", (repo,)
            ).fetchall()
            return [r["name"] for r in rows]
