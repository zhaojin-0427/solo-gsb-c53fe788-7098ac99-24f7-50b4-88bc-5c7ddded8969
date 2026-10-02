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
        """重启后恢复：以上传临时文件为准修正会话进度；清理孤儿 blob 文件；
        清理过期/久远的 GC 计划。"""
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
            # 已执行计划留档 24h（供客户端复核响应/排查），过期未执行计划清掉
            self.db.prune_gc_plans(time.time() - 24 * 3600)

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
    #
    # 两阶段复核模型：
    #   1. gc_plan_create（dry-run）：在某一时刻计算候选集合，**原样持久化**为计划，
    #      返回 plan_id 与候选清单。计划生成后任何标签移动/续租产生的新垃圾都不在
    #      计划内，执行时不会被删除——必须等待下一次 dry-run 复核。
    #   2. gc_plan_execute（dry_run:false + plan_id）：校验计划有效后，在同一把写锁
    #      下用**计划记录的宽限期**重新计算可达性与租约，只删除「计划内且当下仍符合
    #      条件」的对象；重新可达（标签回移等）或续租中的对象跳过。
    # 计划持久化在 SQLite，跨进程重启有效；pending→executed 的原子条件更新保证
    # 重复/并发执行最多只有一次真正删除。

    def _reachable_set(self):
        """以「所有 tag 指向的 manifest」为根遍历可达图。

        manifest 图沿 config/layers（blob）与 manifests[]/subject（manifest）遍历。
        返回 (reachable_manifest_keys, reachable_blob_digests)。
        """
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
        return reachable_manifests, reachable_blobs

    def _build_plan_payload(self, now: float, grace: int) -> dict:
        """计算当前时刻的候选集合（报告格式），供持久化与返回。"""
        reachable_manifests, reachable_blobs = self._reachable_set()

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
        return {
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

    def gc_plan_create(self, grace_seconds: "int | None" = None) -> dict:
        """dry-run：生成并持久化复核计划，返回 plan_id、有效期与候选清单。"""
        if grace_seconds is not None and grace_seconds < 0:
            raise E.upload_invalid("grace_seconds must be non-negative")
        grace = self.settings.gc_grace_seconds if grace_seconds is None else grace_seconds
        with self.lock:
            now = time.time()
            payload = self._build_plan_payload(now, grace)
            plan_id = "gcplan-" + uuid.uuid4().hex
            expires_at = now + self.settings.gc_plan_ttl_seconds
            self.db.create_gc_plan(
                plan_id, json.dumps(payload, separators=(",", ":")),
                grace, now, expires_at,
            )
            return {
                "dry_run": True,
                "plan_id": plan_id,
                "expires_at": _iso(expires_at),
                "plan_ttl_seconds": self.settings.gc_plan_ttl_seconds,
                **payload,
            }

    def gc_plan_execute(self, plan_id: str) -> dict:
        """执行已复核计划：校验 → 原子认领 → 锁内按计划宽限重新检查 → 只删计划内对象。"""
        with self.lock:
            # 1) 计划必须存在、未过期、未执行
            plan = self.db.get_gc_plan(plan_id)
            if plan is None:
                raise E.gc_plan_unknown(plan_id)
            if plan["status"] == "executed":
                raise E.gc_plan_already_executed(plan_id)
            now = time.time()
            if now > plan["expires_at"]:
                raise E.gc_plan_expired(plan_id)

            # 2) 原子认领：并发/重复执行只有一方能把 pending 改成 executed
            if self.db.claim_gc_plan(plan_id, now) == 0:
                row = self.db.get_gc_plan(plan_id)
                if row is not None and row["status"] == "executed":
                    raise E.gc_plan_already_executed(plan_id)
                raise E.gc_plan_conflict(plan_id)

            payload = json.loads(plan["payload"])
            grace = plan["grace_seconds"]

            # 3) 用**计划记录的宽限期**重新计算当下可达性与租约
            reachable_manifests, reachable_blobs = self._reachable_set()
            grace_cutoff = now - grace
            active_uploads = {
                u["id"]: u for u in self.db.list_uploads(state="active")
            }
            current_blobs = {b["digest"]: b for b in self.db.all_blobs()}
            current_manifests = {
                (m["repo"], m["digest"]): m for m in self.db.all_manifests()
            }

            deleted = {"blobs": [], "manifests": [], "uploads": []}
            skipped = {"blobs": [], "manifests": [], "uploads": []}

            # 4) 逐个复核计划内对象：只删当下仍满足条件的；重新可达/续租的跳过
            delete_uploads = []
            for item in payload["delete"]["uploads"]:
                uid = item["id"]
                u = active_uploads.get(uid)
                # 状态不再是 active（已提交/取消/删除），或租约已续到当下仍有效 → 跳过
                if u is None or u["state"] != "active":
                    skipped["uploads"].append({
                        "id": uid, "repo": item["repo"],
                        "reason": "upload no longer active",
                    })
                elif u["expires_at"] > now:
                    skipped["uploads"].append({
                        "id": uid, "repo": item["repo"],
                        "reason": "lease renewed",
                        "expires_at": _iso(u["expires_at"]),
                    })
                else:
                    delete_uploads.append((uid, item["repo"], u["size"]))

            delete_manifest_keys = []
            for item in payload["delete"]["manifests"]:
                key = (item["repo"], item["digest"])
                if key not in current_manifests:
                    skipped["manifests"].append({
                        "repo": key[0], "digest": key[1],
                        "reason": "manifest no longer exists",
                    })
                elif key in reachable_manifests:
                    skipped["manifests"].append({
                        "repo": key[0], "digest": key[1],
                        "reason": "reachable again (tag moved back)",
                    })
                else:
                    delete_manifest_keys.append(key)

            delete_digests = []
            for item in payload["delete"]["blobs"]:
                digest = item["digest"]
                b = current_blobs.get(digest)
                if b is None:
                    skipped["blobs"].append({
                        "digest": digest,
                        "reason": "blob no longer exists",
                    })
                elif digest in reachable_blobs:
                    skipped["blobs"].append({
                        "digest": digest, "size": b["size"],
                        "reason": "reachable again",
                    })
                elif b["created_at"] > grace_cutoff:
                    # 计划生成后同摘要内容又被重新推送（created_at 变新）：受宽限保护
                    skipped["blobs"].append({
                        "digest": digest, "size": b["size"],
                        "reason": "grace protected again",
                    })
                else:
                    delete_digests.append(digest)

            # 5) 实际删除：先过期上传、再 manifest 元数据、最后 blob（先元数据后文件）
            deleted_bytes = 0
            for uid, repo, size in delete_uploads:
                self.storage.delete_upload_file(uid)
                self.db.delete_upload(uid)
                deleted["uploads"].append({"id": uid, "repo": repo, "size": size})
                deleted_bytes += size

            self.db.delete_manifests(delete_manifest_keys)
            for repo, digest in delete_manifest_keys:
                deleted["manifests"].append({"repo": repo, "digest": digest})

            # 关联关系随 delete_blobs 一并清除，先取出用于响应
            repos_map = self.db.repos_for_blobs(delete_digests)
            self.db.delete_blobs(delete_digests)
            for digest in delete_digests:
                b = current_blobs[digest]
                self.storage.delete_blob(digest)
                deleted["blobs"].append({
                    "digest": digest, "size": b["size"],
                    "repos": sorted(repos_map.get(digest, [])),
                })
                deleted_bytes += b["size"]

            return {
                "dry_run": False,
                "executed": True,
                "plan_id": plan_id,
                "grace_seconds": grace,
                "generated_at": payload["generated_at"],
                "executed_at": _iso(now),
                "summary": {
                    "blobs_deleted": len(deleted["blobs"]),
                    "manifests_deleted": len(deleted["manifests"]),
                    "uploads_deleted": len(deleted["uploads"]),
                    "blobs_skipped": len(skipped["blobs"]),
                    "manifests_skipped": len(skipped["manifests"]),
                    "uploads_skipped": len(skipped["uploads"]),
                    "bytes_reclaimed": deleted_bytes,
                },
                "deleted": deleted,
                "skipped": skipped,
            }
