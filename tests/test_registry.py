"""端到端测试：上传/续传/去重、manifest CAS、GC dry-run 与并发安全。"""
import hashlib
import json
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app

OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
OCI_LAYER = "application/vnd.oci.image.layer.v1.tar"


def digest_of(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def make_manifest(config: bytes, layers: list) -> bytes:
    return json.dumps({
        "schemaVersion": 2,
        "mediaType": OCI_MANIFEST,
        "config": {"mediaType": OCI_CONFIG, "digest": digest_of(config), "size": len(config)},
        "layers": [
            {"mediaType": OCI_LAYER, "digest": digest_of(l), "size": len(l)}
            for l in layers
        ],
    }).encode()


@pytest.fixture()
def settings(tmp_path):
    return Settings(data_dir=tmp_path / "data", upload_lease_seconds=3600,
                    gc_grace_seconds=3600)


@pytest.fixture()
def client(settings):
    return TestClient(create_app(settings))


def push_blob(client: TestClient, repo: str, data: bytes) -> str:
    d = digest_of(data)
    r = client.post(f"/v2/{repo}/blobs/uploads?digest={d}", content=data)
    assert r.status_code == 201, r.text
    assert r.headers["Docker-Content-Digest"] == d
    return d


def push_manifest(client: TestClient, repo: str, tag: str, manifest: bytes,
                  expected: str = None, status: int = 201):
    headers = {"Content-Type": OCI_MANIFEST}
    if expected:
        headers["If-Match"] = expected
    r = client.put(f"/v2/{repo}/manifests/{tag}", content=manifest, headers=headers)
    assert r.status_code == status, r.text
    return r


# ---------------- 基础 ----------------

def test_v2_ping(client):
    r = client.get("/v2/")
    assert r.status_code == 200
    assert r.headers["Docker-Distribution-API-Version"] == "registry/2.0"


def test_invalid_repo_name(client):
    r = client.post("/v2/UPPER/case/blobs/uploads")
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "NAME_INVALID"


# ---------------- 上传 ----------------

def test_monolithic_upload_and_pull(client):
    data = b"hello registry" * 100
    d = push_blob(client, "team/app", data)
    r = client.get(f"/v2/team/app/blobs/{d}")
    assert r.status_code == 200
    assert r.content == data
    assert r.headers["Docker-Content-Digest"] == d
    h = client.head(f"/v2/team/app/blobs/{d}")
    assert h.headers["Content-Length"] == str(len(data))


def test_chunked_upload_resume_and_verify(client):
    chunks = [b"aaa" * 100, b"bbb" * 100, b"ccc" * 100]
    data = b"".join(chunks)

    r = client.post("/v2/app/blobs/uploads")
    assert r.status_code == 202
    loc = r.headers["Location"]
    uid = r.headers["Docker-Upload-UUID"]
    assert r.headers["Range"] == "0-0"

    r = client.patch(loc, content=chunks[0])
    assert r.status_code == 202
    assert r.headers["Range"] == f"0-{len(chunks[0]) - 1}"

    # 续传探测：GET 返回当前进度
    r = client.get(loc)
    assert r.status_code == 204
    assert r.headers["Range"] == f"0-{len(chunks[0]) - 1}"
    assert r.headers["Docker-Upload-UUID"] == uid

    # 带 Content-Range 续传第二块
    r = client.patch(loc, content=chunks[1],
                     headers={"Content-Range": f"{len(chunks[0])}-{len(chunks[0]) + len(chunks[1]) - 1}"})
    assert r.status_code == 202

    # 最后一块随 PUT 一起提交并校验摘要
    r = client.put(f"{loc}?digest={digest_of(data)}", content=chunks[2])
    assert r.status_code == 201
    assert r.headers["Docker-Content-Digest"] == digest_of(data)

    assert client.get(f"/v2/app/blobs/{digest_of(data)}").content == data


def test_chunked_upload_wrong_content_range(client):
    client.post("/v2/app/blobs/uploads")
    loc = client.post("/v2/app/blobs/uploads").headers["Location"]
    client.patch(loc, content=b"abc")
    r = client.patch(loc, content=b"def", headers={"Content-Range": "0-2"})
    assert r.status_code == 416
    assert r.json()["errors"][0]["code"] == "RANGE_INVALID"


def test_finalize_digest_mismatch_cancels_upload(client):
    loc = client.post("/v2/app/blobs/uploads").headers["Location"]
    client.patch(loc, content=b"some data")
    bad = "sha256:" + "0" * 64
    r = client.put(f"{loc}?digest={bad}")
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "DIGEST_INVALID"
    assert client.get(loc).status_code == 404  # 会话已取消


def test_finalize_idempotent_retry(client):
    data = b"retry me"
    loc = client.post("/v2/app/blobs/uploads").headers["Location"]
    client.patch(loc, content=data)
    d = digest_of(data)
    assert client.put(f"{loc}?digest={d}").status_code == 201
    # 客户端没收到响应而重试：同 digest 幂等返回 201
    assert client.put(f"{loc}?digest={d}").status_code == 201


def test_cancel_upload(client):
    loc = client.post("/v2/app/blobs/uploads").headers["Location"]
    assert client.delete(loc).status_code == 204
    assert client.get(loc).status_code == 404


def test_blob_dedup_across_repos(client, settings):
    data = b"shared layer" * 50
    d = push_blob(client, "repo/one", data)
    push_blob(client, "repo/two", data)
    push_blob(client, "repo/one", data)  # 重复推送
    blobs = list(settings.blobs_dir.rglob("*"))
    blob_files = [p for p in blobs if p.is_file()]
    assert len(blob_files) == 1  # 全局只有一份内容
    assert client.get(f"/v2/repo/one/blobs/{d}").status_code == 200
    assert client.get(f"/v2/repo/two/blobs/{d}").status_code == 200


def test_cross_repo_mount(client):
    data = b"mountable"
    d = push_blob(client, "src/repo", data)
    r = client.post(f"/v2/dst/repo/blobs/uploads?mount={d}&from=src/repo")
    assert r.status_code == 201
    assert client.get(f"/v2/dst/repo/blobs/{d}").status_code == 200


def test_blob_namespace_isolation(client):
    d = push_blob(client, "repo/a", b"private")
    r = client.get(f"/v2/repo/b/blobs/{d}")
    assert r.status_code == 404
    assert r.json()["errors"][0]["code"] == "BLOB_UNKNOWN"


# ---------------- manifest ----------------

def test_manifest_requires_existing_layers(client):
    m = make_manifest(b"cfg", [b"missing-layer"])
    r = client.put("/v2/app/manifests/v1", content=m,
                   headers={"Content-Type": OCI_MANIFEST})
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "MANIFEST_BLOB_UNKNOWN"


def test_manifest_publish_and_pull(client):
    cfg, layer = b'{"cfg":1}', b"layer-data"
    push_blob(client, "app", cfg)
    push_blob(client, "app", layer)
    m = make_manifest(cfg, [layer])
    r = push_manifest(client, "app", "v1", m)
    d = digest_of(m)
    assert r.headers["Docker-Content-Digest"] == d

    by_tag = client.get("/v2/app/manifests/v1")
    assert by_tag.content == m
    assert by_tag.headers["Content-Type"].startswith(OCI_MANIFEST)
    by_digest = client.get(f"/v2/app/manifests/{d}")
    assert by_digest.content == m
    assert client.head("/v2/app/manifests/v1").headers["Content-Length"] == str(len(m))
    assert client.get("/v2/app/tags/list").json()["tags"] == ["v1"]


def test_manifest_digest_ref_is_immutable(client):
    cfg = b"cfg"
    push_blob(client, "app", cfg)
    m1 = make_manifest(cfg, [])
    other = make_manifest(b"cfg2", [])
    # digest 引用与内容不符 -> 拒绝
    r = client.put(f"/v2/app/manifests/{digest_of(m1)}", content=other,
                   headers={"Content-Type": OCI_MANIFEST})
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "DIGEST_INVALID"


def test_tag_move_requires_cas(client):
    cfg_a, cfg_b = b"config-a", b"config-b"
    push_blob(client, "app", cfg_a)
    push_blob(client, "app", cfg_b)
    m_a = make_manifest(cfg_a, [])
    m_b = make_manifest(cfg_b, [])
    d_a, d_b = digest_of(m_a), digest_of(m_b)

    push_manifest(client, "app", "latest", m_a)

    # 移动标签必须携带期望摘要
    r = push_manifest(client, "app", "latest", m_b, status=409)
    err = r.json()["errors"][0]
    assert err["code"] == "TAG_CONFLICT"
    assert err["detail"]["current"] == d_a

    # 期望摘要错误 -> 409
    r = push_manifest(client, "app", "latest", m_b,
                      expected="sha256:" + "1" * 64, status=409)
    assert r.json()["errors"][0]["code"] == "TAG_CONFLICT"

    # 期望摘要正确 -> CAS 成功
    push_manifest(client, "app", "latest", m_b, expected=d_a)
    assert client.get("/v2/app/manifests/latest").content == m_b

    # 重复推送同一 manifest 到同一标签：当前值==内容 digest，仍需 If-Match
    push_manifest(client, "app", "latest", m_b, expected=d_b)


def test_tag_cas_on_absent_tag(client):
    cfg = b"cfg"
    push_blob(client, "app", cfg)
    m = make_manifest(cfg, [])
    # 标签不存在却声称期望某个摘要 -> 冲突
    r = push_manifest(client, "app", "v1", m, expected="sha256:" + "2" * 64, status=409)
    assert r.json()["errors"][0]["detail"]["current"] is None


def test_manifest_unknown(client):
    r = client.get("/v2/app/manifests/nope")
    assert r.status_code == 404
    assert r.json()["errors"][0]["code"] == "MANIFEST_UNKNOWN"


# ---------------- GC ----------------

def gc_dry_run(client, grace_seconds=None):
    body = {"dry_run": True}
    if grace_seconds is not None:
        body["grace_seconds"] = grace_seconds
    r = client.post("/gc", json=body)
    assert r.status_code == 200, r.text
    return r.json()


def gc_execute(client, plan_id, status=200):
    r = client.post("/gc", json={"dry_run": False, "plan_id": plan_id})
    assert r.status_code == status, r.text
    return r.json() if r.headers.get("content-type", "").startswith("application/json") else r


def _push_image(client, repo, tag, cfg, layers, expected=None):
    push_blob(client, repo, cfg)
    for l in layers:
        push_blob(client, repo, l)
    m = make_manifest(cfg, layers)
    push_manifest(client, repo, tag, m, expected=expected)
    return m


def test_gc_dry_run_persists_plan_and_execute_deletes_planned(client):
    shared, layer1, layer2 = b"shared", b"layer-1", b"layer-2"
    cfg_a, cfg_b = b"cfg-a", b"cfg-b"
    m_a = _push_image(client, "app", "keep", cfg_a, [shared, layer1])
    m_b = _push_image(client, "app", "temp", cfg_b, [shared, layer2])
    # 用 CAS 把 temp 移走，使 m_b 成为无标签垃圾
    m_c = _push_image(client, "app", "temp", cfg_a, [shared], expected=digest_of(m_b))

    # grace_seconds=0：跳过宽限期，立刻回收无引用对象
    report = gc_dry_run(client, grace_seconds=0)
    assert report["dry_run"] is True
    assert report["plan_id"].startswith("gcplan-")
    assert report["plan_ttl_seconds"] == 3600  # 计划生成后一小时内必须执行
    assert "expires_at" in report
    dead_blobs = {b["digest"] for b in report["delete"]["blobs"]}
    assert digest_of(layer2) in dead_blobs
    assert digest_of(cfg_b) in dead_blobs
    assert digest_of(shared) not in dead_blobs  # 共享层可达
    dead_manifests = {m["digest"] for m in report["delete"]["manifests"]}
    assert digest_of(m_b) in dead_manifests
    assert digest_of(m_a) not in dead_manifests
    assert digest_of(m_c) not in dead_manifests

    # dry-run 不删除任何东西
    assert client.get(f"/v2/app/blobs/{digest_of(layer2)}").status_code == 200

    result = gc_execute(client, report["plan_id"])
    assert result["executed"] is True
    assert result["plan_id"] == report["plan_id"]
    assert result["summary"]["blobs_deleted"] == 2
    assert result["summary"]["manifests_deleted"] == 1
    deleted_blobs = {b["digest"] for b in result["deleted"]["blobs"]}
    assert digest_of(layer2) in deleted_blobs
    assert digest_of(cfg_b) in deleted_blobs
    assert digest_of(shared) not in deleted_blobs

    assert client.get(f"/v2/app/blobs/{digest_of(layer2)}").status_code == 404
    assert client.get(f"/v2/app/blobs/{digest_of(shared)}").status_code == 200
    assert client.get(f"/v2/app/manifests/{digest_of(m_b)}").status_code == 404
    assert client.get("/v2/app/manifests/keep").status_code == 200
    assert client.get("/v2/app/manifests/temp").status_code == 200


def test_gc_respects_grace_period(client):
    d = push_blob(client, "app", b"fresh unreferenced blob")
    plan = gc_dry_run(client)
    assert plan["summary"]["blobs_to_delete"] == 0
    assert plan["summary"]["blobs_kept_by_grace"] == 1

    plan = gc_dry_run(client, grace_seconds=0)
    assert d in {b["digest"] for b in plan["delete"]["blobs"]}

    gc_execute(client, plan["plan_id"])
    assert client.get(f"/v2/app/blobs/{d}").status_code == 404


def test_gc_upload_leases(client, settings):
    live = client.post("/v2/app/blobs/uploads").headers["Docker-Upload-UUID"]
    stale = client.post("/v2/app/blobs/uploads").headers["Docker-Upload-UUID"]
    # 手动把 stale 的租约改到过去
    registry = client.app.state.registry
    with registry.lock:
        registry.db.update_upload_progress(stale, 0, time.time() - 1)

    plan = gc_dry_run(client)
    assert stale in {u["id"] for u in plan["delete"]["uploads"]}
    assert live in {u["id"] for u in plan["keep"]["uploads_leased"]}

    result = gc_execute(client, plan["plan_id"])
    assert stale in {u["id"] for u in result["deleted"]["uploads"]}
    assert client.get(f"/v2/app/blobs/uploads/{stale}").status_code == 404
    assert client.get(f"/v2/app/blobs/uploads/{live}").status_code == 204


# ---- 复核漏洞修复：计划外的新候选绝不删除 ----

def test_gc_new_candidates_after_dry_run_are_not_deleted(client):
    """核心漏洞回归：dry-run 后标签移动产生的新垃圾不在计划内，执行不得删除。"""
    old_garbage = b"garbage known at review"
    push_blob(client, "app", old_garbage)
    cfg_v1, cfg_v2 = b"cfg-v1", b"cfg-v2"
    m_v1 = _push_image(client, "app", "tag", cfg_v1, [b"v1-layer"])
    registry = client.app.state.registry
    with registry.lock:
        registry.db._conn.execute(
            "UPDATE blobs SET created_at = 0 WHERE digest = ?",
            (digest_of(old_garbage),),
        )
        registry.db._conn.commit()

    # 复核时刻：只有老垃圾 blob 在计划内；m_v1 仍被 tag 引用，是可达的
    plan = gc_dry_run(client, grace_seconds=0)
    planned_blobs = {b["digest"] for b in plan["delete"]["blobs"]}
    planned_manifests = {m["digest"] for m in plan["delete"]["manifests"]}
    assert digest_of(old_garbage) in planned_blobs
    assert digest_of(m_v1) not in planned_manifests

    # 复核之后：移动 tag -> m_v1 的 manifest、cfg_v1、v1-layer 此刻才变成不可达
    m_v2 = _push_image(client, "app", "tag", cfg_v2, [b"v2-layer"],
                       expected=digest_of(m_v1))

    result = gc_execute(client, plan["plan_id"])
    deleted_manifests = {m["digest"] for m in result["deleted"]["manifests"]}
    deleted_blobs = {b["digest"] for b in result["deleted"]["blobs"]}
    assert digest_of(m_v1) not in deleted_manifests  # 新候选，未审阅，不能删
    assert digest_of(cfg_v1) not in deleted_blobs
    assert digest_of(b"v1-layer") not in deleted_blobs

    # 计划内的老垃圾被删除；标签移动新产生的对象原封不动
    assert client.get(f"/v2/app/blobs/{digest_of(old_garbage)}").status_code == 404
    assert client.get(f"/v2/app/manifests/{digest_of(m_v1)}").status_code == 200
    assert client.get(f"/v2/app/blobs/{digest_of(cfg_v1)}").status_code == 200
    assert client.get(f"/v2/app/blobs/{digest_of(b'v1-layer')}").status_code == 200
    assert client.get("/v2/app/manifests/tag").content == m_v2

    # 新候选留待下次 dry-run 复核后才会进入计划
    next_plan = gc_dry_run(client, grace_seconds=0)
    next_manifests = {m["digest"] for m in next_plan["delete"]["manifests"]}
    assert digest_of(m_v1) in next_manifests


def test_gc_object_reachable_again_is_skipped(client):
    """计划生成后对象重新可达（标签回移），执行时跳过而不是删除。"""
    cfg, layer = b"cfg-once-dead", b"layer-once-dead"
    m = _push_image(client, "app", "v1", cfg, [layer])
    registry = client.app.state.registry
    with registry.lock:
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()
    # 移走标签使其成为计划内垃圾
    _push_image(client, "app", "v1", b"other-cfg", [], expected=digest_of(m))

    plan = gc_dry_run(client, grace_seconds=0)
    assert digest_of(m) in {x["digest"] for x in plan["delete"]["manifests"]}

    # 复核窗口内标签又指回 m（digest 寻址 PUT 内容不变；这里用 CAS 回移需要 old 摘要，
    # 直接构造一个把 m 重新挂回标签的发布：先以 digest PUT 无需移动，
    # 简化为发布同内容 manifest 到新标签使其可达）
    push_manifest(client, "app", "rollback", m)

    result = gc_execute(client, plan["plan_id"])
    assert result["deleted"]["manifests"] == []
    skipped = {(s["digest"], s["reason"]) for s in result["skipped"]["manifests"]}
    assert (digest_of(m), "reachable again (tag moved back)") in skipped
    # blob 重新可达同样跳过
    assert all(s["reason"] == "reachable again" for s in result["skipped"]["blobs"])
    assert client.get(f"/v2/app/manifests/{digest_of(m)}").status_code == 200
    assert client.get(f"/v2/app/blobs/{digest_of(layer)}").status_code == 200


def test_gc_renewed_lease_is_skipped(client):
    stale = client.post("/v2/app/blobs/uploads").headers["Docker-Upload-UUID"]
    registry = client.app.state.registry
    with registry.lock:
        registry.db.update_upload_progress(stale, 0, time.time() - 1)
    plan = gc_dry_run(client)
    assert stale in {u["id"] for u in plan["delete"]["uploads"]}

    # 复核后上传方在执行前完成续租（例如另一持有会话的进程直接续了租约，
    # 或时钟边界场景）：执行时重新检查租约，仍有效就跳过
    with registry.lock:
        registry.db.update_upload_progress(stale, 0, time.time() + 600)

    result = gc_execute(client, plan["plan_id"])
    assert result["deleted"]["uploads"] == []
    assert len(result["skipped"]["uploads"]) == 1
    assert result["skipped"]["uploads"][0]["id"] == stale
    assert result["skipped"]["uploads"][0]["reason"] == "lease renewed"
    assert client.get(f"/v2/app/blobs/uploads/{stale}").status_code == 204


# ---- 计划生命周期：缺失 / 未知 / 过期 / 已执行 / 重启 / 幂等 ----

def test_gc_execute_without_plan_id_is_rejected(client):
    r = client.post("/gc", json={"dry_run": False})
    assert r.status_code == 400
    assert r.json()["errors"][0]["code"] == "GC_PLAN_ID_REQUIRED"


def test_gc_execute_unknown_plan_is_rejected_and_deletes_nothing(client):
    push_blob(client, "app", b"whatever")
    r = client.post("/gc", json={"dry_run": False, "plan_id": "gcplan-deadbeef"})
    assert r.status_code == 404
    assert r.json()["errors"][0]["code"] == "GC_PLAN_UNKNOWN"
    assert client.get(f"/v2/app/blobs/{digest_of(b'whatever')}").status_code == 200


def test_gc_execute_expired_plan_is_rejected(client):
    push_blob(client, "app", b"old")
    registry = client.app.state.registry
    with registry.lock:
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()
    plan = gc_dry_run(client, grace_seconds=0)
    # 把计划过期时间改到过去（模拟超过生成后一小时）
    with registry.lock:
        registry.db._conn.execute(
            "UPDATE gc_plans SET expires_at = ? WHERE id = ?",
            (time.time() - 1, plan["plan_id"]),
        )
        registry.db._conn.commit()

    r = client.post("/gc", json={"dry_run": False, "plan_id": plan["plan_id"]})
    assert r.status_code == 410
    assert r.json()["errors"][0]["code"] == "GC_PLAN_EXPIRED"
    # 拒绝执行且不删数据
    assert client.get(f"/v2/app/blobs/{digest_of(b'old')}").status_code == 200
    # 计划仍未被标记为 executed
    with registry.lock:
        row = registry.db.get_gc_plan(plan["plan_id"])
    assert row["status"] == "pending"


def test_gc_repeat_execute_deletes_nothing_again(client):
    push_blob(client, "app", b"once")
    registry = client.app.state.registry
    with registry.lock:
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()
    plan = gc_dry_run(client, grace_seconds=0)
    first = gc_execute(client, plan["plan_id"])
    assert first["summary"]["blobs_deleted"] == 1

    # 重复执行：拒绝（409），不会再次删除
    r = client.post("/gc", json={"dry_run": False, "plan_id": plan["plan_id"]})
    assert r.status_code == 409
    assert r.json()["errors"][0]["code"] == "GC_PLAN_ALREADY_EXECUTED"


def test_gc_plan_survives_restart(settings):
    c1 = TestClient(create_app(settings))
    push_blob(c1, "app", b"persisted garbage")
    registry = c1.app.state.registry
    with registry.lock:
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()
    plan = gc_dry_run(c1, grace_seconds=0)
    plan_id = plan["plan_id"]

    # 重启：同一数据目录创建新应用实例，计划仍然有效并可执行
    c2 = TestClient(create_app(settings))
    result = gc_execute(c2, plan_id)
    assert result["summary"]["blobs_deleted"] == 1
    assert c2.get(f"/v2/app/blobs/{digest_of(b'persisted garbage')}").status_code == 404


def test_gc_concurrent_pull_and_publish(client, settings):
    """GC 与并发拉取/发布同时进行：可达数据不丢、新发布不被误删。"""
    shared = b"hot shared layer" * 100
    m = _push_image(client, "app", "live", b"cfg", [shared])
    # 制造一些垃圾
    push_blob(client, "app", b"garbage-old")
    registry = client.app.state.registry
    with registry.lock:  # 让垃圾老过宽限期
        registry.db._conn.execute("UPDATE blobs SET created_at = 0")
        registry.db._conn.commit()

    errors = []

    def puller():
        try:
            for _ in range(30):
                r = client.get(f"/v2/app/blobs/{digest_of(shared)}")
                assert r.status_code == 200 and r.content == shared
                time.sleep(0.001)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def publisher():
        try:
            for i in range(10):
                _push_image(client, "app", f"v{i}", f"cfg-{i}".encode(), [shared])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def collector():
        try:
            for _ in range(10):
                # 真实宽限窗口（settings 默认 3600s）：进行中发布刚推的新 blob
                # 受宽限保护，绝不被回收
                plan = gc_dry_run(client)
                client.post("/gc", json={"dry_run": False, "plan_id": plan["plan_id"]})
                time.sleep(0.001)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = ([threading.Thread(target=puller) for _ in range(3)]
               + [threading.Thread(target=publisher)]
               + [threading.Thread(target=collector)])
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert client.get(f"/v2/app/blobs/{digest_of(shared)}").status_code == 200
    assert client.get("/v2/app/manifests/live").content == m
    assert client.get("/v2/app/manifests/v9").status_code == 200
    # 垃圾在某轮复核中被回收（新发布产生的垃圾不在计划内，不影响这一保证）
    assert client.get(f"/v2/app/blobs/{digest_of(b'garbage-old')}").status_code == 404


# ---------------- 重启恢复 ----------------

def test_upload_survives_restart(settings):
    data = b"resume after restart" * 10
    c1 = TestClient(create_app(settings))
    r = c1.post("/v2/app/blobs/uploads")
    loc = r.headers["Location"]
    c1.patch(loc, content=data[:50])

    # 模拟重启：同一数据目录创建新应用实例
    c2 = TestClient(create_app(settings))
    r = c2.get(loc)
    assert r.status_code == 204
    assert r.headers["Range"] == "0-49"  # 进度从磁盘恢复

    c2.patch(loc, content=data[50:], headers={"Content-Range": f"50-{len(data) - 1}"})
    r = c2.put(f"{loc}?digest={digest_of(data)}")
    assert r.status_code == 201
    assert c2.get(f"/v2/app/blobs/{digest_of(data)}").content == data
