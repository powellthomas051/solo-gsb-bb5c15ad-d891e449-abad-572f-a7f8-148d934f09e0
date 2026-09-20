# 排练提词同步 API

基于 **Python + FastAPI + SQLite** 的排练提词同步服务：房间导入有序台词并设置
**推进人数** 与 **离线超时**；演员上报参与者 ID、单调递增事件序号和已到达的
台词位置（须携带当前**进度代际** `progress_epoch`）；服务幂等处理重复或乱序
上报，公共位置只推进到达到指定人数的最高台词且 **绝不回退**；超时参与者自动
退出推进人数计算。导演可随时**校准公共位置**（前跳或回退）：校验版本与范围后
原子更新公共位置、递增进度代际并清空参与者进度，旧代际的迟到/重试上报返回
409。支持 **排练暂停/恢复**（冻结超时判定、暂停期间拒绝上报，恢复时扣除整段
暂停时长）。台本支持 **带版本号的增量修订**：按当前行号执行
insert/replace/delete，整批原子生效，参与者与公共位置按台词身份自动重排。
导演可批量设置 **台词止点**：演员上报仍按 quorum 汇聚，但自动推进不得越过
最靠前的未释放止点；放行止点时原子递增 `hold_version` 并立即用当前活跃
参与者重算，满足条件可在同一响应中继续推进。

## 快速启动（单条命令）

```bash
docker compose up --build
```

服务地址：**http://localhost:8000**

- 交互式 API 文档（Swagger UI）：http://localhost:8000/docs
- 健康检查：http://localhost:8000/health

依赖安装随镜像构建完成，SQLite 建表随应用启动自动完成，无需任何手工初始化。

## 配置

| 环境变量   | 默认值                    | 说明                     |
| ---------- | ------------------------- | ------------------------ |
| `DB_PATH`  | `/data/teleprompter.db`（Compose 内） | SQLite 数据库文件路径 |

端口映射在 `docker-compose.yml` 中（默认 `8000:8000`）；数据库文件通过
`teleprompter-data` 卷持久化。

## 快速体验

```bash
# 1. 创建房间：推进人数 2，离线超时 30 秒
curl -X POST http://localhost:8000/rooms \
  -H 'Content-Type: application/json' \
  -d '{"id":"demo","name":"雷雨第一幕","advance_threshold":2,"offline_timeout_seconds":30}'

# 2. 导入有序台词（列表顺序即台词顺序，位置从 0 开始）
curl -X POST http://localhost:8000/rooms/demo/lines \
  -H 'Content-Type: application/json' \
  -d '{"lines":["周朴园：你姓什么？","鲁侍萍：我姓鲁。","周朴园：你是谁？"]}'

# 3. 演员上报：参与者 ID + 单调递增序号 + 已到达的台词位置 + 当前进度代际
#    （台词导入后 progress_epoch 为 1；之后通过 /state 获取最新代际）
curl -X POST http://localhost:8000/rooms/demo/events \
  -H 'Content-Type: application/json' \
  -d '{"participant_id":"actor-zhou","seq":1,"position":2,"progress_epoch":1}'
curl -X POST http://localhost:8000/rooms/demo/events \
  -H 'Content-Type: application/json' \
  -d '{"participant_id":"actor-lu","seq":1,"position":2,"progress_epoch":1}'

# 4. 查询当前活跃成员、公共位置、进度代际及推进原因
curl http://localhost:8000/rooms/demo/state

# 5. 台本增量修订：基于 revision 1 整批执行插入/替换/删除（原子生效）
curl -X PATCH http://localhost:8000/rooms/demo/lines \
  -H 'Content-Type: application/json' \
  -d '{"base_revision":1,"operations":[
        {"op":"insert","position":1,"text":"周朴园：（沉思）哦。"},
        {"op":"replace","position":2,"text":"周朴园：你到底是谁？"},
        {"op":"delete","position":0}
      ]}'
# → {"room_id":"demo","revision":2,"lines_count":3,"public_position":2,"participants_remapped":0}

# 6. 台词与状态都会携带当前 revision；以过期 base_revision 修订将返回 409
curl http://localhost:8000/rooms/demo/lines

# 7. 暂停排练（冻结超时判定、拒绝位置上报），再恢复（扣除整段暂停时长）
curl -X PUT http://localhost:8000/rooms/demo/clock \
  -H 'Content-Type: application/json' \
  -d '{"expected_clock_version":0,"paused":true}'
# → {"changed":true,"paused":true,"clock_version":1,"paused_at":...,"state":{...}}
curl -X PUT http://localhost:8000/rooms/demo/clock \
  -H 'Content-Type: application/json' \
  -d '{"expected_clock_version":1,"paused":false}'

# 8. 导演校准公共位置（可前跳或回退）：携带当前 progress_epoch 做乐观锁
#    成功后公共位置变为 0、progress_epoch 递增、全部参与者进度被清空
curl -X PUT http://localhost:8000/rooms/demo/cursor \
  -H 'Content-Type: application/json' \
  -d '{"expected_progress_epoch":1,"position":0}'
# → {"changed":true,"public_position":0,"progress_epoch":2,"state":{...}}
# 此后演员必须携带 progress_epoch=2 上报；携带旧代际 1 的迟到/重试事件返回 409

# 9. 台词止点：批量设置（位置必须属于当前台本；追加语义，hold_version 乐观锁）
curl -X PUT http://localhost:8000/rooms/demo/holds \
  -H 'Content-Type: application/json' \
  -d '{"expected_hold_version":0,"positions":[1,2]}'
# → {"changed":true,"hold_version":1,"added":[1,2],"pending_holds":[1,2],"state":{...}}

# 演员照常上报；quorum 候选位置越过止点时，公共位置停在最靠前的止点，
# 参与者最新位置保留；state 中可见 pending_holds 与 last_block
curl http://localhost:8000/rooms/demo/state

# 10. 放行止点：原子递增 hold_version 并立即重算，满足条件时同响应继续推进
curl -X POST http://localhost:8000/rooms/demo/holds/release \
  -H 'Content-Type: application/json' \
  -d '{"expected_hold_version":1,"positions":[1]}'
# → {"released":[1],"hold_version":2,"advancement":{"position":...},"pending_holds":[2],"state":{...}}
```

## API 一览

| 方法   | 路径                            | 说明                                       |
| ------ | ------------------------------- | ------------------------------------------ |
| POST   | `/rooms`                        | 创建房间（推进人数、离线超时）             |
| GET    | `/rooms/{room_id}`              | 房间配置（含当前台本版本 `revision`）      |
| POST   | `/rooms/{room_id}/lines`        | 导入有序台词（整体替换，重置进度并开启新版本） |
| GET    | `/rooms/{room_id}/lines`        | 查看台词（含当前 `revision`）              |
| PATCH  | `/rooms/{room_id}/lines`        | 台本增量修订（版本校验 + 整批原子生效）    |
| POST   | `/rooms/{room_id}/events`       | 演员上报位置（须携带 `progress_epoch`；旧代际/暂停时 409） |
| PUT    | `/rooms/{room_id}/clock`        | 暂停/恢复排练（时钟版本乐观锁）            |
| PUT    | `/rooms/{room_id}/cursor`       | 导演校准公共位置，可前跳/回退（进度代际乐观锁） |
| PUT    | `/rooms/{room_id}/holds`        | 批量设置未释放止点（`hold_version` 乐观锁，追加语义） |
| POST   | `/rooms/{room_id}/holds/release` | 放行止点：版本递增后立即重算，可同响应继续推进 |
| GET    | `/rooms/{room_id}/state`        | 当前活跃成员、公共位置、止点与最近一次阻挡信息 |
| GET    | `/rooms/{room_id}/advancements` | 完整推进历史（含每次推进原因）             |
| GET    | `/health`                       | 健康检查                                   |

## 核心语义

- **幂等上报**：同一参与者的 `seq` 必须严格递增。重复（`seq` 相同）或乱序
  （`seq` 更小）的上报会被忽略，返回 `applied: false`，不产生任何状态变化，
  客户端可安全重试。
- **进度代际（progress_epoch）**：演员上报必须携带当前 `progress_epoch`
  （由 `/state` 获取；房间创建时为 0，首次导入台本后为 1）。导演校准公共
  位置或台本整体重导都会开启新代际；**旧代际的延迟或重试事件返回 409**，
  既不被记录也不能重新推进房间。`GET /rooms/{room_id}`、`/state` 以及上报
  响应均返回当前代际。
- **导演校准公共位置**（`PUT /rooms/{room_id}/cursor`，请求体
  `{"expected_progress_epoch": <当前代际>, "position": <目标行号>}`）：
  允许**前跳或回退**，与台本版本、时钟状态相互独立（暂停期间也可校准）。
  - 目标位置与当前公共位置一致时为**幂等空操作**：返回当前状态、
    `changed: false`，`progress_epoch` **不递增**（即使携带过期代际也视为
    成功重试）；
  - 目标不同时校验 `expected_progress_epoch`，与当前代际不一致 → **409**，
    状态不变；房间尚未导入台词 → **409**；目标位置不在
    `[0, 台词总数)` 内 → **422**，状态不变；
  - 校验通过后在同一事务中原子完成：公共位置更新为目标位置（可低于当前
    位置）、`progress_epoch` 递增、**清空全部参与者进度**（新代际从干净
    状态重新汇聚），并在推进历史中写入一条“导演校准”记录。
- **推进规则**：公共位置 = 活跃参与者中第 `advance_threshold` 高的位置，即
  “达到指定人数的最高台词”。活跃指在 `offline_timeout_seconds` 内有上报。
  存在未释放止点时，自动推进不得越过最靠前（位置不小于当前公共位置）的止点。
- **绝不回退**：公共位置只升不降。参与者超时离线、演员自己回报更低位置，
  都不会使公共位置下降。
- **台词止点（holds）**：导演用 `PUT /rooms/{room_id}/holds`（请求体
  `{"expected_hold_version": <当前版本>, "positions": [<台词位置>, ...]}`）
  批量设置未释放止点，止点位置必须全部属于当前台本（越界 → **422** 整批
  不落库；未导入台本 → **409**）。设置为**追加语义**：已存在未释放止点的
  位置保持不变，其余新增；只要有新增就原子递增 `hold_version`，全部重复
  （或空列表）为幂等空操作、版本不递增。
  - 演员上报照常按 quorum 计算候选位置，参与者的最新位置**始终保留**；但
    候选位置越过最靠前的未释放止点时，公共位置最多停在该止点，并在
    `/state` 的 `last_block` 中记录阻挡信息（止点位置、quorum 候选位置、
    越过止点的参与者、时间）。已经停在止点后又有越过它的上报时，只刷新
    阻挡信息，位置不动、不重复写推进记录。
  - 放行（`POST /rooms/{room_id}/holds/release`，请求体
    `{"expected_hold_version": <当前版本>, "positions": [...]}`）：每个位置
    都必须是当前未释放止点（任一不是 → **422** 整批不落库），版本不一致 →
    **409**。校验通过后原子标记放行、`hold_version` 递增，并**立即用当前
    活跃参与者重算**：满足 quorum 时在同一响应中继续推进（`advancement`
    字段），仍不得越过最靠前的其余未释放止点；不满足 quorum 则不推进。
  - **增量修订**后未释放止点按原**台词身份**重排：保留（含被替换）的台词
    跟随新行号，**被删台词上的止点直接移除**（不做回退）；修订不改变
    `hold_version`（`base_revision` 已防止过期操作）。
  - **整体导入**清空全部止点（含留档）、清除 `last_block`；当此前确实
    存在止点时递增 `hold_version`，使在途的设置/放行请求失效。
  - **手动校准不受止点限制**：前跳时越过（位置严格小于目标）的未释放
    止点自动释放并递增 `hold_version`；目标位置上的止点保留，仍可阻挡
    之后越过它的自动推进；回退不释放任何止点。
  - 时钟与止点相互独立：暂停期间导演仍可设置/放行止点；放行时按暂停冻结
    的活跃集合重算。`hold_version` 与 `revision`、`progress_epoch`、
    `clock_version` 互不影响。
- **离线超时**：超过超时未上报的参与者退出活跃成员列表与推进人数计算；
  重新上报后自动回到活跃集合。
- **排练暂停/恢复**（`PUT /rooms/{room_id}/clock`，请求体
  `{"expected_clock_version": <当前版本>, "paused": true|false}`）：
  时钟状态与台本版本相互独立。
  - 目标状态与当前一致时为幂等空操作：返回当前状态、`changed: false`，
    `clock_version` **不递增**（便于客户端安全重试）；
  - 目标状态不同时校验 `expected_clock_version`，与当前 `clock_version`
    不一致 → **409**，状态不变；校验通过后 `clock_version` 递增；
  - **暂停**：记录 `paused_at`，活跃成员的超时判定冻结在暂停瞬间——暂停
    多久都不会有人因墙钟流逝而离线；期间一切位置上报（含新参与者）均被
    拒绝并返回 **409**，不产生任何状态变化；
  - **恢复**：把整段暂停时长（`now - paused_at`）补到每个成员的
    `last_seen_at` 上，暂停前的**剩余在线时长原样延续**；暂停前已离线的
    成员补时后仍然离线，**不会被重新激活**；恢复后上报与推进恢复正常；
  - 暂停/恢复本身**不产生推进记录**；台本整体导入或增量修订不改变暂停
    状态与 `clock_version`。
- **时钟状态字段**：`/state` 返回 `paused`（布尔）、`clock_version`
  （暂停/恢复每实际切换一次递增，初值 0）与 `paused_at`（本次暂停开始的
  epoch 秒；运行中为 `null`）。时钟版本与进度代际互不影响。
- **推进原因**：每次推进记录促成的参与者名单与可读原因，可在
  `/state`（最近一次）与 `/advancements`（全部历史）中查询。导演校准记录
  的 `contributors` 为空数组、原因为“导演校准公共位置”；公共位置被止点
  挡下时也会写入一条位置为止点的推进记录（`contributors` 为空）。
- **台本版本（revision）**：房间创建时为 0，每次整体导入或增量修订后递增；
  `GET /rooms`、`GET /rooms/{room_id}/lines` 与 `GET /rooms/{room_id}/state`
  均返回当前版本。revision 与 progress_epoch 相互独立：增量修订不改变
  进度代际，导演校准不改变台本版本。
- **台词导入**：整体替换台本并重置房间进度（公共位置、参与者、推进历史），
  同时开启新版本（`revision` 递增）与新进度代际（`progress_epoch` 递增），
  使重导前在途的旧代际上报全部失效。
- **台本增量修订**：`PATCH /rooms/{room_id}/lines` 携带 `base_revision` 与一组
  按**当前行号**执行的 `insert` / `replace` / `delete` 操作，整批在事务中校验、
  原子生效：
  - `base_revision` 与当前版本不一致 → **409**，台本不变；
  - 任一操作越界（`insert` 允许 `position == 台词总数` 表示追加）或同一行上
    操作冲突（重复删除、重复替换、替换又删除）→ **422**，**全部不落库**；
  - 修订成功后 `revision` 递增，参与者与公共位置按原先指向的**台词身份**
    重排：保留（含被替换）的台词跟随新位置，被删除的回退到最近仍保留的
    前一行，无前一行则置为 `-1`；未释放**止点**同样按台词身份重排，被删
    台词上的止点移除。修订本身**不产生推进记录**。
- **状态接口字段**：`/state` 除活跃成员、公共位置、推进原因外，还返回
  `hold_version`（止点版本，初值 0）、`pending_holds`（当前未释放止点
  位置列表，升序）与 `last_block`（最近一次阻挡信息；无阻挡为 `null`，
  正常推进、校准或修订后清除）。
- 时间字段均为 epoch 秒（浮点）；`state.server_time` 可用于对齐超时判断。

## 本地开发（不用 Docker）

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --reload          # http://localhost:8000
pytest                                 # 运行测试
```

## 项目结构

```
app/
  main.py       # FastAPI 路由与推进/幂等核心逻辑
  db.py         # SQLite 连接与启动时建表
  schemas.py    # 请求体校验
tests/test_api.py
Dockerfile
docker-compose.yml
requirements.txt / requirements-dev.txt
```
