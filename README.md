# 本地容器制品仓库（Local OCI Registry）

基于 **Python + FastAPI + SQLite + 本地文件存储** 的轻量 OCI 风格制品仓库。
支持分块/断点续传 blob、按摘要全局去重、重启后恢复上传、不可变 manifest
发布校验、标签 CAS 移动，以及带 dry-run 复核的可达性垃圾回收。

## 功能特性

| 能力 | 说明 |
| --- | --- |
| OCI 风格 API | `/v2/<repo>/blobs/uploads`、`/v2/<repo>/manifests/<ref>`、`/v2/<repo>/blobs/<digest>`、`/v2/<repo>/tags/list` |
| 分块上传 / 续传 | `POST → PATCH* → PUT?digest=`；`GET` 会话返回 `Range` 进度，可断点续传 |
| 摘要校验 | 上传完成时按内容计算 sha256，与客户端摘要不一致即拒绝并取消会话 |
| 按摘要去重 | blob 全局内容寻址存储，跨仓库/重复推送只占一份磁盘；支持 `?mount=&from=` 跨仓库挂载 |
| 重启恢复 | 上传会话与进度持久化在 SQLite，重启后自动对齐磁盘实际进度，可继续续传 |
| 不可变 manifest | digest 寻址的 manifest 内容与摘要不符即拒绝；发布时校验 config/layers/subject/子 manifest 引用必须已存在，杜绝悬空引用 |
| 标签 CAS | 移动已存在的标签必须携带期望摘要（`If-Match` 头或 `?expected=`），不匹配返回 `409 TAG_CONFLICT` |
| 安全 GC | 以「标签 + 未过期上传租约」为根建立可达图；**两阶段复核**——dry-run 持久化候选计划并返回 `plan_id`，执行只删计划内、执行时仍不可达的对象；宽限期保护新上传 blob；与并发拉取/发布互斥，绝不误删可达数据 |

## 快速开始（Docker Compose 一键启动）

```bash
docker compose up --build -d
```

服务监听在 **http://localhost:5000**（容器内 8000 端口），数据持久化在
`registry-data` 卷中。验证：

```bash
curl http://localhost:5000/v2/        # -> {} ，响应头含 Docker-Distribution-API-Version: registry/2.0
curl http://localhost:5000/healthz    # -> {"status":"ok"}
```

也可以本地直接运行（Python 3.11+）：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 5000
```

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REGISTRY_DATA_DIR` | `./data`（镜像内 `/data`） | 数据目录（SQLite + blob 文件 + 上传临时文件） |
| `REGISTRY_UPLOAD_LEASE_SECONDS` | `3600` | 上传会话租约；超时无读写即被 GC 回收 |
| `REGISTRY_GC_GRACE_SECONDS` | `3600` | GC 宽限期；创建不足该时长的未引用 blob 不回收，保护「已推送、待发布」窗口 |
| `REGISTRY_GC_PLAN_TTL_SECONDS` | `3600` | GC 复核计划有效期；dry-run 生成的计划超过该时长（默认一小时）未执行即作废 |

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v2/` | API 版本探测 |
| POST | `/v2/{repo}/blobs/uploads` | 创建上传会话（202）；`?digest=` 整体上传（201）；`?mount=&from=` 跨仓库挂载 |
| GET | `/v2/{repo}/blobs/uploads/{id}` | 查询上传进度（204 + `Range`），用于续传 |
| PATCH | `/v2/{repo}/blobs/uploads/{id}` | 追加分块，可选 `Content-Range` 校验偏移 |
| PUT | `/v2/{repo}/blobs/uploads/{id}?digest=` | 完成上传（可携带最后一块），校验摘要 |
| DELETE | `/v2/{repo}/blobs/uploads/{id}` | 取消上传 |
| GET/HEAD | `/v2/{repo}/blobs/{digest}` | 拉取 blob |
| PUT | `/v2/{repo}/manifests/{ref}` | 发布 manifest；`ref` 为标签时需 CAS（`If-Match` 或 `?expected=`） |
| GET/HEAD | `/v2/{repo}/manifests/{ref}` | 拉取 manifest（`ref` 可为标签或 digest） |
| GET | `/v2/{repo}/tags/list` | 列出标签 |
| POST | `/gc` | 两阶段垃圾回收。`{"dry_run": true, "grace_seconds": 3600}` 生成并持久化复核计划（返回 `plan_id`）；`{"dry_run": false, "plan_id": "..."}` 执行该计划 |

错误统一为 OCI 格式：`{"errors":[{"code","message","detail"}]}`。

## curl 示例

以下假设服务在 `localhost:5000`，仓库为 `demo/app`。

### 1. 分块上传 blob（断点续传）

```bash
# 准备数据并计算摘要
printf 'hello-layer-content' > layer.bin
DIGEST="sha256:$(sha256sum layer.bin | cut -d' ' -f1)"

# 创建上传会话，记录 Location
LOC=$(curl -s -D - -o /dev/null -X POST \
  http://localhost:5000/v2/demo/app/blobs/uploads | awk '/^[Ll]ocation:/{print $2}' | tr -d '\r')
echo "$LOC"   # /v2/demo/app/blobs/uploads/<uuid>

# 上传前 5 字节（模拟中断）
curl -s -X PATCH -H 'Content-Type: application/octet-stream' \
  --data-binary @- "http://localhost:5000$LOC" <<< '' >/dev/null  # 仅占位
head -c 5 layer.bin | curl -s -D - -o /dev/null -X PATCH \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @- "http://localhost:5000$LOC" | grep -i range
# Range: 0-4

# 断点续传：先查询进度，再带 Content-Range 续传剩余部分
curl -s -D - -o /dev/null "http://localhost:5000$LOC" | grep -i range
tail -c +6 layer.bin | curl -s -X PATCH \
  -H 'Content-Type: application/octet-stream' -H 'Content-Range: 5-18' \
  --data-binary @- "http://localhost:5000$LOC" -o /dev/null

# 完成上传并校验摘要（服务按内容重算 sha256，不符则 400 并取消会话）
curl -s -D - -o /dev/null -X PUT "http://localhost:5000$LOC?digest=$DIGEST"
# 201 Created, Docker-Content-Digest: sha256:...
```

小文件也可整体上传：

```bash
curl -s -X POST --data-binary @layer.bin \
  "http://localhost:5000/v2/demo/app/blobs/uploads?digest=$DIGEST"   # 201，重复推送会去重秒回
```

### 2. 发布 manifest（标签 CAS）

```bash
# 先推 config 与 layer 两个 blob（略，同上），然后组装 manifest
cat > manifest.json <<EOF
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config":   {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "$CONFIG_DIGEST", "size": $CONFIG_SIZE},
  "layers":   [{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": "$DIGEST", "size": 19}]
}
EOF

# 首次打 tag：无需期望摘要
curl -s -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
  --data-binary @manifest.json \
  http://localhost:5000/v2/demo/app/manifests/v1     # 201

# 移动 tag 必须携带期望摘要（CAS），否则 409 TAG_CONFLICT
curl -s -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
  --data-binary @manifest-v2.json \
  http://localhost:5000/v2/demo/app/manifests/v1     # 409

# 正确姿势：If-Match 携带 tag 当前指向的 digest
CUR=$(curl -sI http://localhost:5000/v2/demo/app/manifests/v1 | awk '/^[Dd]ocker-[Cc]ontent-[Dd]igest:/{print $2}' | tr -d '\r')
curl -s -X PUT -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' \
  -H "If-Match: $CUR" \
  --data-binary @manifest-v2.json \
  http://localhost:5000/v2/demo/app/manifests/v1     # 201，tag 完成移动
```

发布时服务端会校验 manifest 引用的 config/layers（以及 index 的子
manifest、referrers 的 subject）必须已存在于本仓库，否则返回
`400 MANIFEST_BLOB_UNKNOWN`，从源头杜绝悬空引用。

### 3. 拉取

```bash
curl -s http://localhost:5000/v2/demo/app/manifests/v1            # 按 tag 拉 manifest
curl -s http://localhost:5000/v2/demo/app/manifests/$MANIFEST_DIGEST
curl -s http://localhost:5000/v2/demo/app/blobs/$DIGEST -o layer.bin
curl -s http://localhost:5000/v2/demo/app/tags/list
```

### 4. 垃圾回收（先 dry-run 复核，再凭 plan_id 执行）

GC 采用**两阶段复核**，修复了「dry-run 后状态又变化、执行时重算会删掉未审阅
对象」的问题：dry-run 把候选清单**原样持久化**为计划，执行只允许删除计划内、
且执行时刻仍然不可达/已过期的对象。

#### 第一步：dry-run —— 生成并持久化复核计划

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"dry_run": true, "grace_seconds": 0}' http://localhost:5000/gc | jq .
```

请求体：`dry_run`（默认 `true`）、可选 `grace_seconds`（不传则用
`REGISTRY_GC_GRACE_SECONDS`，该值会记录在计划里）。

真实响应（`delete.blobs/manifests/uploads` 即本次复核记录的候选；此时不删任何数据）：

```json
{
  "dry_run": true,
  "plan_id": "gcplan-3a2f5c54109e42b58e0f4828485a7089",
  "expires_at": "2026-10-02T02:29:25.708776+00:00",
  "plan_ttl_seconds": 3600,
  "grace_seconds": 0,
  "generated_at": "2026-10-02T01:29:25.708776+00:00",
  "summary": {
    "blobs_to_delete": 2,
    "manifests_to_delete": 1,
    "uploads_to_delete": 0,
    "bytes_to_reclaim": 17,
    "manifests_reachable": 1,
    "blobs_reachable": 1,
    "blobs_kept_by_grace": 0,
    "uploads_kept_by_lease": 0
  },
  "delete": {
    "blobs": [
      {"digest": "sha256:72ca7be537ce7477c009da27266088b1544644464a8751212181112899041b4b",
       "size": 8, "repos": ["demo/app"],
       "reason": "unreferenced and older than grace period"},
      {"digest": "sha256:e3155b20e134632816c8611c4e9ee5cbd0e00689f7c4c955ee9f896580d02fdb",
       "size": 9, "repos": ["demo/app"],
       "reason": "unreferenced and older than grace period"}
    ],
    "manifests": [
      {"repo": "demo/app",
       "digest": "sha256:f299f420c1dc013ea7e1f777f6ba5c876f08960908cb61fa29da7868a0e3b1b0",
       "media_type": "application/vnd.oci.image.manifest.v1+json", "reason": "untagged"}
    ],
    "uploads": []
  },
  "keep": {"blobs_grace_protected": [], "uploads_leased": []}
}
```

> 复核窗口（默认 1 小时，由 `REGISTRY_GC_PLAN_TTL_SECONDS` 控制）内完成下一步。

#### 第二步：执行 —— 提交 `dry_run:false` + `plan_id`

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"dry_run": false, "plan_id": "gcplan-3a2f5c54109e42b58e0f4828485a7089"}' \
  http://localhost:5000/gc | jq .
```

执行时会在写锁内、用**计划记录的宽限期**重新检查可达性和租约。真实响应分别列出
**实际删除（`deleted`）**与**跳过（`skipped`，计划内但已重新可达/续租）**：

```json
{
  "dry_run": false,
  "executed": true,
  "plan_id": "gcplan-3a2f5c54109e42b58e0f4828485a7089",
  "grace_seconds": 0,
  "generated_at": "2026-10-02T01:29:25.708776+00:00",
  "executed_at": "2026-10-02T01:29:39.458406+00:00",
  "summary": {
    "blobs_deleted": 2,
    "manifests_deleted": 1,
    "uploads_deleted": 0,
    "blobs_skipped": 0,
    "manifests_skipped": 0,
    "uploads_skipped": 0,
    "bytes_reclaimed": 17
  },
  "deleted": {
    "blobs": [
      {"digest": "sha256:72ca7be537ce7477c009da27266088b1544644464a8751212181112899041b4b",
       "size": 8, "repos": ["demo/app"]},
      {"digest": "sha256:e3155b20e134632816c8611c4e9ee5cbd0e00689f7c4c955ee9f896580d02fdb",
       "size": 9, "repos": ["demo/app"]}
    ],
    "manifests": [
      {"repo": "demo/app",
       "digest": "sha256:f299f420c1dc013ea7e1f777f6ba5c876f08960908cb61fa29da7868a0e3b1b0"}
    ],
    "uploads": []
  },
  "skipped": {"blobs": [], "manifests": [], "uploads": []}
}
```

对象在复核后被标签重新引用或上传被续租时，会出现在 `skipped` 中，reason 为
`reachable again`、`reachable again (tag moved back)`、`lease renewed`、
`blob no longer exists` 等；dry-run 之后**新产生**的不可达对象不在计划内，本轮
一律保留，留待下一次 dry-run 审阅。

#### 拒绝执行的情况（一律不删数据）

| 场景 | 状态码 | `errors[0].code` |
| --- | --- | --- |
| `dry_run:false` 但未带 `plan_id` | 400 | `GC_PLAN_ID_REQUIRED` |
| `plan_id` 未知/已被清理 | 404 | `GC_PLAN_UNKNOWN` |
| 计划生成已超过一小时（TTL） | 410 | `GC_PLAN_EXPIRED` |
| 计划已执行过（重复/并发执行） | 409 | `GC_PLAN_ALREADY_EXECUTED` |

例如重复执行同一计划：

```bash
curl -i -X POST -H 'Content-Type: application/json' \
  -d '{"dry_run": false, "plan_id": "gcplan-3a2f5c54109e42b58e0f4828485a7089"}' \
  http://localhost:5000/gc
# HTTP/1.1 409 Conflict
# {"errors":[{"code":"GC_PLAN_ALREADY_EXECUTED", ...}]}
```

计划持久化在 SQLite，**重启后依然有效**（未过期即可执行）；计划是一次性的，
由数据库中 `pending → executed` 的原子条件更新保证重复执行不会再次删除任何数据。

GC 语义：

- **根集合**：所有 tag 指向的 manifest；沿 `config`/`layers`/`manifests[]`/`subject`
  遍历出全部可达 blob 与 manifest。
- **回收对象**：计划内、执行时仍不可达且超出计划宽限期的 blob，仍不可达（无标签）
  的 manifest，以及租约仍过期的上传会话。
- **复核边界**：只删 dry-run 记录的候选；dry-run 之后标签移动/新推送产生的新垃圾
  不在计划内，必须经下一次 dry-run 复核才会回收。
- **执行时复核**：执行与生成是两个时刻——执行瞬间重新计算可达性与租约，重新可达
  （标签回移等）或续租中的对象跳过；跳过项在响应中逐项列出。
- **保护机制**：未过期上传租约不会被清理；宽限期内的“新 blob”不会被清理
  （覆盖“已推送 blob、尚未发布 manifest”的并发窗口）；GC 与发布/上传在同一
  写锁下串行，拉取在打开文件句柄后再流式发送，文件随后被删也不影响进行中的下载。
- 删除顺序为先清元数据再删文件，崩溃残留由下次启动时的孤儿文件清扫兜底。

## 目录结构

```
app/
  main.py       # FastAPI 路由（/v2/*、/gc 两阶段复核）
  registry.py   # 核心逻辑：上传、manifest CAS、GC 可达图与持久化计划
  db.py         # SQLite 元数据（WAL，进程内 RLock 串行化；gc_plans 持久化复核计划）
  storage.py    # 本地文件存储：blobs/sha256/<hex> 内容寻址 + uploads/<uuid>
  digest.py     # digest/tag/repo 校验与 sha256 计算
  errors.py     # OCI 错误模型（含 GC_PLAN_* 复核计划错误）
  config.py     # 环境变量配置
tests/test_registry.py   # 30 个端到端测试
Dockerfile / docker-compose.yml
```

## 本地开发与测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -q
```

测试覆盖：分块上传与续传、摘要校验失败、幂等重试、跨仓库去重与挂载、
manifest 引用校验、digest 不可变性、标签 CAS 冲突、GC 两阶段复核与执行、
**dry-run 后标签移动产生的新候选不被删除**、重新可达/续租对象跳过、
计划缺失/未知/过期/已执行拒绝、计划跨重启有效与重复执行幂等、
宽限期与租约保护、GC 与并发拉取/发布竞争、重启后恢复上传。

## 设计说明与限制

- **单进程单写**：SQLite（WAL）+ 进程内写锁保证元数据一致；请用单 worker
  运行（compose 默认如此）。多实例水平扩展需替换为外部数据库与共享存储。
- **命名空间隔离**：blob 物理上全局去重，但 `GET /v2/<repo>/blobs/<digest>`
  要求该 blob 已在 `<repo>` 命名空间内关联，跨仓库不可直接读。
- **manifest 不可变**：同一 digest 的内容恒定；digest 寻址的 PUT 会校验内容
  与摘要一致；标签只是指向 digest 的可变指针，且移动必须走 CAS。
