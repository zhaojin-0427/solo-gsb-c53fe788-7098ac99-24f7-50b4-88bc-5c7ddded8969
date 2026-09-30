"""核心仓库逻辑：分块上传、manifest 发布（CAS）、垃圾回收。

并发模型：进程内一把 threading.RLock 串行化所有元数据写事务与 GC；
拉取 blob 时在锁内打开文件句柄，锁外流式发送——即使 GC 随后删除文件，
POSIX 下已打开的 fd 依然可读，因此并发拉取不会被误删打断。
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import errors as E
from .config import Settings
from .db import Database
from .digest import is_digest, is_tag, sha256_bytes, sha256_file
from .storage import FileStorage

DEFAULT_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def parse_manifest_refs(content: bytes):
    """解析 manifest，返回 (blob_digests, manifest_digests, media_type|None)。

    blob 引用：config、layers[]；manifest 引用：manifests[]（index）、subject（referrers）。
    结构非法时抛 ValueError。
    """
    doc = json.loads(content.decode("utf-8"))
    if not isinstance(doc, dict):
        raise ValueError("manifest must be a JSON object")
    blobs, manifests = [], []

    def _digest_of(descriptor, field):
        if not isinstance(descriptor, dict) or "digest" not in descriptor:
            raise ValueError(f"invalid {field} descriptor: missing digest")
        d = descriptor["digest"]
        if not is_digest(d):
            raise ValueError(f"invalid digest in {field}: {d!r}")
        return d

    if "config" in doc:
        blobs.append(_digest_of(doc["config"], "config"))
    layers = doc.get("layers")
    if layers is not None:
        if not isinstance(layers, list):
            raise ValueError("layers must be a list")
        for layer in layers:
            blobs.append(_digest_of(layer, "layers[]"))
    children = doc.get("manifests")
    if children is not None:
        if not isinstance(children, list):
            raise ValueError("manifests must be a list")
        for child in children:
            manifests.append(_digest_of(child, "manifests[]"))
    if "subject" in doc:
        manifests.append(_digest_of(doc["subject"], "subject"))
    return blobs, manifests, doc.get("mediaType")


def parse_content_range_start(value: str) -> int:
    """接受 '0-511' / 'bytes 0-511' / 'bytes 0-511/1024'，返回起始偏移。"""
    text = value.strip()
    if text.lower().startswith("bytes "):
        text = text[6:]
    text = text.split("/", 1)[0]
    start = text.split("-", 1)[0].strip()
    if not start.isdigit():
        raise E.upload_invalid(f"invalid Content-Range header: {value!r}")
    return int(start)


class Registry:
    def __init__(self, settings: Settings):
        self.settings = settings
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        self.storage = FileStorage(settings)
        self.db = Database(settings.db_path)
        self.lock = threading.RLock()
        self._reconcile()

    # ---------------- 启动恢复 ----------------

    def _reconcile(self):
        """重启后恢复：以上传临时文件为准修正会话进度；清理孤儿 blob 文件。"""
        with self.lock:
            for u in self.db.list_uploads(state="active"):
                path = self.storage.upload_path(u["id"])
                if not path.exists():
                    self.db.set_upload_state(u["id"], "cancelled")
                    continue
                actual = path.stat().st_size
                if actual != u["size"]:
                    self.db.update_upload_progress(u["id"], actual, u["expires_at"])
            known = {b["digest"] for b in self.db.all_blobs()}
            self.storage.sweep_orphan_blobs(
                known, older_than_ts=time.time() - self.settings.gc_grace_seconds
            )

    # ---------------- 上传会话 ----------------

    def start_upload(self, repo: str) -> dict:
        with self.lock:
            upload_id = uuid.uuid4().hex
            now = time.time()
            self.db.create_upload(
                upload_id, repo, now, now + self.settings.upload_lease_seconds
            )
            self.storage.upload_path(upload_id).touch()
            return self.db.get_upload(upload_id)

    def _active_upload(self, repo: str, upload_id: str) -> dict:
        u = self.db.get_upload(upload_id)
        if u is None or u["repo"] != repo or u["state"] != "active":
            raise E.upload_unknown(upload_id)
        if u["expires_at"] <= time.time():
            raise E.upload_unknown(upload_id)  # 租约已过期，等 GC 回收
        return u

    def _renew_lease(self, upload_id: str) -> float:
        return time.time() + self.settings.upload_lease_seconds

    def get_upload_status(self, repo: str, upload_id: str) -> dict:
        with self.lock:
            u = self._active_upload(repo, upload_id)
            self.db.update_upload_progress(u["id"], u["size"], self._renew_lease(u["id"]))
            return self.db.get_upload(upload_id)

    def append_chunk(self, repo: str, upload_id: str, chunk: bytes,
                     content_range: "str | None" = None) -> int:
        with self.lock:
            u = self._active_upload(repo, upload_id)
            path = self.storage.upload_path(upload_id)
            if not path.exists():
                raise E.upload_unknown(upload_id)
            if content_range:
                start = parse_content_range_start(content_range)
                if start != u["size"]:
                    raise E.range_invalid(u["size"], content_range)
            with open(path, "ab") as f:
                f.write(chunk)
            new_size = u["size"] + len(chunk)
            self.db.update_upload_progress(upload_id, new_size, self._renew_lease(upload_id))
            return new_size

    def finalize_upload(self, repo: str, upload_id: str, digest: str,
                        chunk: "bytes | None" = None):
        """校验摘要并落库。已 committed 的会话允许同 digest 幂等重试。"""
        with self.lock:
            u = self.db.get_upload(upload_id)
            if u is None or u["repo"] != repo:
                raise E.upload_unknown(upload_id)
            if u["state"] == "committed":
                if u["digest"] == digest and self.db.repo_has_blob(repo, digest):
                    blob = self.db.get_blob(digest)
                    return digest, blob["size"] if blob else 0
                raise E.upload_invalid(
                    "upload already committed with a different digest",
                    {"upload_id": upload_id},
                )
            if u["state"] != "active" or u["expires_at"] <= time.time():
                raise E.upload_unknown(upload_id)

            path = self.storage.upload_path(upload_id)
            if not path.exists():
                raise E.upload_unknown(upload_id)
            if chunk:
                with open(path, "ab") as f:
                    f.write(chunk)
            computed = sha256_file(path)
            if computed != digest:
                # 数据不可信，取消会话让客户端重新上传
                self.storage.delete_upload_file(upload_id)
                self.db.set_upload_state(upload_id, "cancelled")
                raise E.digest_mismatch(expected=digest, computed=computed)

            size = self.storage.commit_blob(path, digest)  # 按摘要去重
            self.db.create_blob(digest, size)
            self.db.link_repo_blob(repo, digest)
            self.db.set_upload_state(upload_id, "committed", digest)
            return digest, size

    def monolithic_upload(self, repo: str, payload: bytes, digest: str):
        computed = sha256_bytes(payload)
        if computed != digest:
            raise E.digest_mismatch(expected=digest, computed=computed)
        with self.lock:
            tmp = self.storage.upload_path(uuid.uuid4().hex)
            with open(tmp, "wb") as f:
                f.write(payload)
            size = self.storage.commit_blob(tmp, digest)
            self.db.create_blob(digest, size)
            self.db.link_repo_blob(repo, digest)
            return digest, size

    def mount_blob(self, repo: str, digest: str) -> bool:
        """跨仓库挂载：blob 已存在则直接关联，实现秒级去重推送。"""
        with self.lock:
            if not self.db.blob_exists(digest) or not self.storage.has_blob(digest):
                return False
            self.db.link_repo_blob(repo, digest)
            return True

    def cancel_upload(self, repo: str, upload_id: str):
        with self.lock:
            u = self.db.get_upload(upload_id)
            if u is None or u["repo"] != repo:
                raise E.upload_unknown(upload_id)
            self.storage.delete_upload_file(upload_id)
            self.db.delete_upload(upload_id)

    # ---------------- blob 读取 ----------------

    def open_blob(self, repo: str, digest: str):
        """返回 (file, size)。文件句柄在锁内打开，GC 之后删文件不影响读取。"""
        with self.lock:
            if not self.db.repo_has_blob(repo, digest):
                raise E.blob_unknown(repo, digest)
            try:
                f = self.storage.open_blob(digest)
            except FileNotFoundError:
                raise E.blob_unknown(repo, digest)
            return f, os.fstat(f.fileno()).st_size

    def stat_blob(self, repo: str, digest: str) -> int:
        with self.lock:
            f, size = self.open_blob(repo, digest)
            f.close()
            return size

    # ---------------- manifest 发布 / 读取 ----------------

    def put_manifest(self, repo: str, ref: str, content: bytes,
                     content_type: "str | None", expected: "str | None") -> str:
        if not content:
            raise E.manifest_invalid("empty manifest body")
        digest = sha256_bytes(content)
        if is_digest(ref):
            if ref != digest:  # digest 寻址的 manifest 不可伪造
                raise E.digest_mismatch(expected=ref, computed=digest)
            tag = None
        elif is_tag(ref):
            tag = ref
        else:
            raise E.tag_invalid(ref)

        try:
            blob_refs, child_refs, doc_media_type = parse_manifest_refs(content)
        except (ValueError, UnicodeDecodeError) as exc:
            raise E.manifest_invalid(str(exc))
        media_type = (
            (content_type or "").split(";")[0].strip()
            or doc_media_type
            or DEFAULT_MANIFEST_MEDIA_TYPE
        )

        with self.lock:
            # 校验层引用：config/layers 必须是本 repo 已存在的 blob，
            # index 子 manifest / subject 必须已存在，杜绝悬空引用
            missing_blobs = [d for d in blob_refs if not self.db.repo_has_blob(repo, d)]
            if missing_blobs:
                raise E.manifest_blob_unknown(repo, missing_blobs)
            missing_manifests = [d for d in child_refs if not self.db.manifest_exists(repo, d)]
            if missing_manifests:
                raise E.manifest_blob_unknown(repo, missing_manifests)

            self.db.put_manifest(repo, digest, media_type, content)
            if tag is not None:
                status, current = self.db.cas_tag(repo, tag, expected, digest)
                if status == "conflict":
                    raise E.tag_conflict(repo, tag, current, expected)
            return digest

    def get_manifest(self, repo: str, ref: str) -> dict:
        with self.lock:
            if is_digest(ref):
                row = self.db.get_manifest_by_digest(repo, ref)
            elif is_tag(ref):
                row = self.db.get_manifest_by_tag(repo, ref)
            else:
                raise E.tag_invalid(ref)
            if row is None:
                raise E.manifest_unknown(repo, ref)
            return row

    def list_tags(self, repo: str) -> list:
        with self.lock:
            return self.db.list_tags(repo)

    # ---------------- 垃圾回收 ----------------

    def gc(self, dry_run: bool = True, grace_seconds: "int | None" = None) -> dict:
        """可达性 GC。

        根集合：所有 tag 指向的 manifest；未过期上传租约保护其会话不被回收。
        manifest 图沿 config/layers（blob）与 manifests[]/subject（manifest）遍历。
        未被任何 tag 可达的 manifest、未被可达 manifest 引用且超出宽限期的 blob、
        已过期的上传会话，进入回收清单。dry_run=True 只生成报告不删除。
        """
        grace = self.settings.gc_grace_seconds if grace_seconds is None else grace_seconds
        with self.lock:
            now = time.time()

            reachable_manifests: set = set()
            reachable_blobs: set = set()
            stack = [(t["repo"], t["digest"]) for t in self.db.all_tags()]
            while stack:
                key = stack.pop()
                if key in reachable_manifests:
                    continue
                row = self.db.get_manifest_by_digest(*key)
                if row is None:
                    continue
                reachable_manifests.add(key)
                try:
                    blobs, children, _ = parse_manifest_refs(row["content"])
                except (ValueError, UnicodeDecodeError):
                    continue  # 发布时已校验，理论上不会到这里
                reachable_blobs.update(blobs)
                stack.extend((key[0], c) for c in children)

            dead_manifests = [
                m for m in self.db.all_manifests()
                if (m["repo"], m["digest"]) not in reachable_manifests
            ]

            grace_cutoff = now - grace
            dead_blobs, grace_protected = [], []
            for b in self.db.all_blobs():
                if b["digest"] in reachable_blobs:
                    continue
                (grace_protected if b["created_at"] > grace_cutoff else dead_blobs).append(b)

            active_uploads = self.db.list_uploads(state="active")
            expired_uploads = [u for u in active_uploads if u["expires_at"] <= now]
            leased_uploads = [u for u in active_uploads if u["expires_at"] > now]

            repos_map = self.db.repos_for_blobs([b["digest"] for b in dead_blobs])
            report = {
                "dry_run": dry_run,
                "executed": not dry_run,
                "grace_seconds": grace,
                "generated_at": _iso(now),
                "summary": {
                    "blobs_to_delete": len(dead_blobs),
                    "manifests_to_delete": len(dead_manifests),
                    "uploads_to_delete": len(expired_uploads),
                    "bytes_to_reclaim": sum(b["size"] for b in dead_blobs)
                    + sum(u["size"] for u in expired_uploads),
                    "manifests_reachable": len(reachable_manifests),
                    "blobs_reachable": len(reachable_blobs),
                    "blobs_kept_by_grace": len(grace_protected),
                    "uploads_kept_by_lease": len(leased_uploads),
                },
                "delete": {
                    "blobs": [
                        {"digest": b["digest"], "size": b["size"],
                         "repos": sorted(repos_map.get(b["digest"], [])),
                         "reason": "unreferenced and older than grace period"}
                        for b in sorted(dead_blobs, key=lambda x: x["digest"])
                    ],
                    "manifests": [
                        {"repo": m["repo"], "digest": m["digest"],
                         "media_type": m["media_type"], "reason": "untagged"}
                        for m in sorted(dead_manifests, key=lambda x: (x["repo"], x["digest"]))
                    ],
                    "uploads": [
                        {"id": u["id"], "repo": u["repo"], "size": u["size"],
                         "expired_at": _iso(u["expires_at"]), "reason": "lease expired"}
                        for u in sorted(expired_uploads, key=lambda x: x["id"])
                    ],
                },
                "keep": {
                    "blobs_grace_protected": [
                        {"digest": b["digest"], "size": b["size"],
                         "age_seconds": round(now - b["created_at"], 3)}
                        for b in sorted(grace_protected, key=lambda x: x["digest"])
                    ],
                    "uploads_leased": [
                        {"id": u["id"], "repo": u["repo"], "size": u["size"],
                         "expires_at": _iso(u["expires_at"])}
                        for u in sorted(leased_uploads, key=lambda x: x["id"])
                    ],
                },
            }

            if not dry_run:
                for u in expired_uploads:
                    self.storage.delete_upload_file(u["id"])
                    self.db.delete_upload(u["id"])
                self.db.delete_manifests(
                    [(m["repo"], m["digest"]) for m in dead_manifests]
                )
                digests = [b["digest"] for b in dead_blobs]
                self.db.delete_blobs(digests)  # 先清元数据……
                for d in digests:
                    self.storage.delete_blob(d)  # ……再删文件；崩溃残留由启动时 sweep 兜底
            return report
