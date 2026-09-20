"""排练提词同步 API。

房间导入有序台词并设置推进人数与离线超时；演员上报参与者 ID、
单调递增事件序号和已到达的台词位置。服务幂等处理重复或乱序上报，
公共位置只推进到达到指定人数的最高台词且绝不回退；超时参与者退出
推进人数计算。状态接口返回当前活跃成员、公共位置及最近一次推进原因。

排练可暂停/恢复（PUT /rooms/{room_id}/clock，独立的 clock_version
乐观锁）：暂停期间冻结活跃成员的超时判定并拒绝一切位置上报；恢复时
扣除整段暂停时长，暂停前的剩余在线时长原样延续，已离线成员不会被
重新激活。

台本支持整体导入（重置进度并开启新版本）与增量修订（PATCH）：修订按
当前行号执行 insert/replace/delete，整批在事务中校验，版本不匹配返回
409，任一越界则全部不落库；修订后参与者与公共位置按原先指向的台词
身份重排——保留的台词跟随新位置，被删除的回退到最近仍保留的前一行，
无前一行则置为 -1，且修订本身不产生推进记录。

导演可校准公共位置（PUT /rooms/{room_id}/cursor，独立的 progress_epoch
乐观锁）：校验代际与目标范围后允许前跳或回退，原子更新公共位置、递增
progress_epoch 并清空全部参与者进度；旧代际的迟到或重试上报返回 409，
不能再推进房间。目标位置与当前一致时为幂等空操作，代际不递增。演员
上报必须携带当前 progress_epoch（/state 可查）。台本整体导入同样开启
新代际，使重导前在途的旧上报全部失效。
"""

from __future__ import annotations

import json
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Response

from .db import connect, init_db
from .schemas import (
    ClockUpdate,
    CursorUpdate,
    EventReport,
    LinesImport,
    LinesPatch,
    RoomCreate,
)


def _now() -> float:
    """当前时间（epoch 秒）。独立成函数便于测试注入假时钟。"""
    return time.time()


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()  # 数据库初始化随服务启动完成
    yield


app = FastAPI(
    title="排练提词同步 API",
    version="1.3.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# 内部辅助
# ---------------------------------------------------------------------------

def _get_room(conn, room_id: str):
    row = conn.execute("SELECT * FROM rooms WHERE id = ?", (room_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"房间不存在: {room_id}")
    return row


def _reference_now(room, now: float) -> float:
    """用于超时判定的“当前时间”：暂停时冻结在暂停开始时刻。"""
    if room["paused"]:
        return room["paused_at"]
    return now


def _active_participants(conn, room_id: str, ref_now: float, timeout_seconds: int) -> List[Any]:
    """离线超时内有过上报的参与者才算活跃。

    ``ref_now`` 是计时基准：排练暂停时传入暂停开始时刻，暂停期间不再有
    任何成员因墙钟流逝而变为离线。
    """
    cutoff = ref_now - timeout_seconds
    return conn.execute(
        """
        SELECT participant_id, position, last_seq, last_seen_at
        FROM participants
        WHERE room_id = ? AND last_seen_at >= ?
        ORDER BY participant_id
        """,
        (room_id, cutoff),
    ).fetchall()


def _maybe_advance(conn, room, now: float) -> Optional[Dict[str, Any]]:
    """按活跃参与者重算公共位置；仅在提升时落库并返回推进记录。

    活跃参与者中第 ``advance_threshold`` 高的位置，即为“达到指定人数的
    最高台词”。公共位置只在算出的候选值更高时才更新，因此绝不回退。
    """
    threshold = room["advance_threshold"]
    ref_now = _reference_now(room, now)
    rows = _active_participants(conn, room["id"], ref_now, room["offline_timeout_seconds"])
    if len(rows) < threshold:
        return None
    positions = sorted((row["position"] for row in rows), reverse=True)
    candidate = positions[threshold - 1]
    if candidate <= room["public_position"]:
        return None
    contributors = sorted(row["participant_id"] for row in rows if row["position"] >= candidate)
    reason = (
        f"{len(contributors)} 名活跃参与者已到达台词 {candidate}"
        f"（要求 {threshold} 人）：{', '.join(contributors)}"
    )
    conn.execute(
        "UPDATE rooms SET public_position = ? WHERE id = ?",
        (candidate, room["id"]),
    )
    conn.execute(
        """
        INSERT INTO advancements (room_id, position, reason, contributors, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (room["id"], candidate, reason, json.dumps(contributors, ensure_ascii=False), now),
    )
    return {"position": candidate, "reason": reason, "contributors": contributors, "at": now}


def _room_payload(room) -> Dict[str, Any]:
    return {
        "room_id": room["id"],
        "name": room["name"],
        "advance_threshold": room["advance_threshold"],
        "offline_timeout_seconds": room["offline_timeout_seconds"],
        "public_position": room["public_position"],
        "revision": room["revision"],
        "progress_epoch": room["progress_epoch"],
        "created_at": room["created_at"],
    }


def _state_payload(conn, room, now: float) -> Dict[str, Any]:
    room_id = room["id"]
    public_position = room["public_position"]
    # 暂停时以暂停开始时刻为基准，冻结活跃/超时判定
    ref_now = _reference_now(room, now)

    current_line = None
    if public_position >= 0:
        row = conn.execute(
            "SELECT position, text FROM lines WHERE room_id = ? AND position = ?",
            (room_id, public_position),
        ).fetchone()
        if row is not None:
            current_line = {"position": row["position"], "text": row["text"]}

    participants = [
        {
            "participant_id": row["participant_id"],
            "position": row["position"],
            "last_seq": row["last_seq"],
            "last_seen_at": row["last_seen_at"],
        }
        for row in _active_participants(conn, room_id, ref_now, room["offline_timeout_seconds"])
    ]

    last_advancement = None
    adv = conn.execute(
        """
        SELECT position, reason, contributors, created_at
        FROM advancements WHERE room_id = ? ORDER BY id DESC LIMIT 1
        """,
        (room_id,),
    ).fetchone()
    if adv is not None:
        last_advancement = {
            "position": adv["position"],
            "reason": adv["reason"],
            "contributors": json.loads(adv["contributors"]),
            "at": adv["created_at"],
        }

    lines_count = conn.execute(
        "SELECT COUNT(*) AS c FROM lines WHERE room_id = ?", (room_id,)
    ).fetchone()["c"]

    return {
        "room_id": room_id,
        "name": room["name"],
        "advance_threshold": room["advance_threshold"],
        "offline_timeout_seconds": room["offline_timeout_seconds"],
        "lines_count": lines_count,
        "revision": room["revision"],
        "progress_epoch": room["progress_epoch"],
        "public_position": public_position,
        "current_line": current_line,
        "active_participants": participants,
        "last_advancement": last_advancement,
        "paused": bool(room["paused"]),
        "clock_version": room["clock_version"],
        "paused_at": room["paused_at"],
        "server_time": now,
    }


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/rooms", status_code=201)
def create_room(payload: RoomCreate, response: Response) -> Dict[str, Any]:
    """创建房间，设定推进人数与离线超时。

    自带 ID 重复创建时返回已存在的房间（HTTP 200），便于客户端重试。
    """
    conn = connect()
    try:
        room_id = payload.id or uuid.uuid4().hex[:12]
        existing = conn.execute("SELECT * FROM rooms WHERE id = ?", (room_id,)).fetchone()
        if existing is not None:
            response.status_code = 200
            return _room_payload(existing)
        conn.execute(
            """
            INSERT INTO rooms (id, name, advance_threshold, offline_timeout_seconds,
                               public_position, created_at)
            VALUES (?, ?, ?, ?, -1, ?)
            """,
            (room_id, payload.name, payload.advance_threshold,
             payload.offline_timeout_seconds, _now()),
        )
        conn.commit()
        room = conn.execute("SELECT * FROM rooms WHERE id = ?", (room_id,)).fetchone()
        return _room_payload(room)
    finally:
        conn.close()


@app.get("/rooms/{room_id}")
def get_room(room_id: str) -> Dict[str, Any]:
    conn = connect()
    try:
        room = _get_room(conn, room_id)
        payload = _room_payload(room)
        payload["lines_count"] = conn.execute(
            "SELECT COUNT(*) AS c FROM lines WHERE room_id = ?", (room_id,)
        ).fetchone()["c"]
        return payload
    finally:
        conn.close()


@app.post("/rooms/{room_id}/lines")
def import_lines(room_id: str, payload: LinesImport) -> Dict[str, Any]:
    """导入有序台词（整体替换），重置进度并开启新的台本版本。

    台本变化后旧的位置上报不再有意义，因此导入会重置公共位置、
    参与者与推进历史；每次导入版本号（revision）递增，同时进度代际
    （progress_epoch）递增，使重导前在途的旧代际上报全部失效（返回 409）。
    """
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        room = _get_room(conn, room_id)
        conn.execute("DELETE FROM lines WHERE room_id = ?", (room_id,))
        conn.executemany(
            "INSERT INTO lines (room_id, position, text) VALUES (?, ?, ?)",
            [(room_id, position, text) for position, text in enumerate(payload.lines)],
        )
        new_revision = room["revision"] + 1
        new_progress_epoch = room["progress_epoch"] + 1
        conn.execute(
            """
            UPDATE rooms
            SET public_position = -1, revision = ?, progress_epoch = ?
            WHERE id = ?
            """,
            (new_revision, new_progress_epoch, room_id),
        )
        conn.execute("DELETE FROM participants WHERE room_id = ?", (room_id,))
        conn.execute("DELETE FROM advancements WHERE room_id = ?", (room_id,))
        conn.commit()
        return {
            "room_id": room_id,
            "lines_imported": len(payload.lines),
            "progress_reset": True,
            "revision": new_revision,
            "progress_epoch": new_progress_epoch,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.get("/rooms/{room_id}/lines")
def list_lines(room_id: str) -> Dict[str, Any]:
    conn = connect()
    try:
        room = _get_room(conn, room_id)
        rows = conn.execute(
            "SELECT position, text FROM lines WHERE room_id = ? ORDER BY position",
            (room_id,),
        ).fetchall()
        return {
            "room_id": room_id,
            "revision": room["revision"],
            "lines": [dict(row) for row in rows],
        }
    finally:
        conn.close()


@app.patch("/rooms/{room_id}/lines")
def patch_lines(room_id: str, payload: LinesPatch) -> Dict[str, Any]:
    """台本增量修订：按当前行号执行 insert/replace/delete，整批原子生效。

    所有操作的 ``position`` 都指修订前（``base_revision`` 对应）的行号；
    insert 允许 ``position == 台词总数``（追加到末尾）。``base_revision``
    与当前版本不一致返回 409；任一操作越界或与同一行的其他操作冲突，
    整批不落库。修订成功后版本递增，参与者与公共位置按原先指向的台词
    身份重排：保留的台词跟随新位置，被删除的回退到最近仍保留的前一行，
    无前一行则置为 -1。修订本身不写入推进记录。
    """
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        room = _get_room(conn, room_id)
        if payload.base_revision != room["revision"]:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"台本版本不匹配: base_revision={payload.base_revision}, "
                    f"当前 revision={room['revision']}"
                ),
            )

        old_rows = conn.execute(
            "SELECT text FROM lines WHERE room_id = ? ORDER BY position",
            (room_id,),
        ).fetchall()
        old_texts = [row["text"] for row in old_rows]
        total = len(old_texts)

        # ---- 整批校验：任一越界或冲突都抛错，事务回滚后全部不落库 ----
        inserts: Dict[int, List[str]] = {}   # 锚定行号 -> 依次插入的文本
        replaced: Dict[int, str] = {}        # 被替换的行号 -> 新文本
        deleted: set[int] = set()            # 被删除的行号
        for op in payload.operations:
            if op.op == "insert":
                if op.position > total:
                    raise HTTPException(
                        status_code=422,
                        detail=f"insert 位置越界: position={op.position}, 台词总数={total}",
                    )
                inserts.setdefault(op.position, []).append(op.text)
            else:
                if op.position >= total:
                    raise HTTPException(
                        status_code=422,
                        detail=f"{op.op} 位置越界: position={op.position}, 台词总数={total}",
                    )
                if op.position in replaced or op.position in deleted:
                    raise HTTPException(
                        status_code=422,
                        detail=f"台词 {op.position} 上存在冲突的修订操作",
                    )
                if op.op == "replace":
                    replaced[op.position] = op.text
                else:
                    deleted.add(op.position)

        # ---- 应用：构建新台本，并记录每个新行来自的旧行号（台词身份）----
        new_texts: List[str] = []
        origin: List[Optional[int]] = []  # 新行号 -> 旧行号；None 表示新插入
        for position in range(total):
            for text in inserts.get(position, ()):
                new_texts.append(text)
                origin.append(None)
            if position in deleted:
                continue
            new_texts.append(replaced.get(position, old_texts[position]))
            origin.append(position)
        for text in inserts.get(total, ()):
            new_texts.append(text)
            origin.append(None)

        # 仍保留的旧行号 -> 新行号
        kept = {old_pos: new_pos for new_pos, old_pos in enumerate(origin)
                if old_pos is not None}

        def remap(position: int) -> int:
            """把旧行号映射到新行号：保留则跟随；被删除则回退到最近仍保留的
            前一行，无前一行则置为 -1。"""
            probe = position
            while probe >= 0:
                if probe in kept:
                    return kept[probe]
                probe -= 1
            return -1

        # ---- 重排参与者与公共位置（不触发推进、不写推进记录）----
        participants = conn.execute(
            "SELECT participant_id, position FROM participants WHERE room_id = ?",
            (room_id,),
        ).fetchall()
        remapped = 0
        for row in participants:
            new_position = remap(row["position"])
            if new_position != row["position"]:
                conn.execute(
                    """
                    UPDATE participants SET position = ?
                    WHERE room_id = ? AND participant_id = ?
                    """,
                    (new_position, room_id, row["participant_id"]),
                )
                remapped += 1
        new_public_position = remap(room["public_position"])

        # ---- 落库新台本并递增版本 ----
        conn.execute("DELETE FROM lines WHERE room_id = ?", (room_id,))
        conn.executemany(
            "INSERT INTO lines (room_id, position, text) VALUES (?, ?, ?)",
            [(room_id, position, text) for position, text in enumerate(new_texts)],
        )
        new_revision = room["revision"] + 1
        conn.execute(
            "UPDATE rooms SET public_position = ?, revision = ? WHERE id = ?",
            (new_public_position, new_revision, room_id),
        )
        conn.commit()
        return {
            "room_id": room_id,
            "revision": new_revision,
            "lines_count": len(new_texts),
            "public_position": new_public_position,
            "participants_remapped": remapped,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.post("/rooms/{room_id}/events")
def report_event(room_id: str, event: EventReport) -> Dict[str, Any]:
    """演员上报已到达的台词位置。

    上报必须携带当前 ``progress_epoch``（由 ``GET /state`` 获取）。导演
    校准公共位置或台本整体重导会开启新代际；旧代际的迟到或重试事件返回
    409，既不会被记录也不能重新推进房间。

    同一参与者的 ``seq`` 必须严格递增；小于等于已见序号的上报（重复或
    乱序）会被忽略并返回 ``applied: false``，不产生任何状态变化，因此
    重试是安全的。被接受的上报会刷新参与者的活跃时间，并触发一次公共
    位置重算。排练暂停期间一律拒绝上报并返回 409。
    """
    now = _now()
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        room = _get_room(conn, room_id)
        if room["paused"]:
            raise HTTPException(status_code=409, detail="排练已暂停：暂停期间拒绝位置上报")
        if event.progress_epoch != room["progress_epoch"]:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"进度代际不匹配: progress_epoch={event.progress_epoch}, "
                    f"当前 progress_epoch={room['progress_epoch']}"
                ),
            )
        lines_count = conn.execute(
            "SELECT COUNT(*) AS c FROM lines WHERE room_id = ?", (room_id,)
        ).fetchone()["c"]
        if lines_count == 0:
            raise HTTPException(status_code=409, detail="房间尚未导入台词")
        if event.position >= lines_count:
            raise HTTPException(
                status_code=422,
                detail=f"台词位置越界: position={event.position}, 台词总数={lines_count}",
            )

        existing = conn.execute(
            "SELECT last_seq FROM participants WHERE room_id = ? AND participant_id = ?",
            (room_id, event.participant_id),
        ).fetchone()

        applied = existing is None or event.seq > existing["last_seq"]
        advancement = None
        if applied:
            conn.execute(
                """
                INSERT INTO participants (room_id, participant_id, last_seq, position, last_seen_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (room_id, participant_id) DO UPDATE SET
                    last_seq     = excluded.last_seq,
                    position     = excluded.position,
                    last_seen_at = excluded.last_seen_at
                """,
                (room_id, event.participant_id, event.seq, event.position, now),
            )
            advancement = _maybe_advance(conn, room, now)
        conn.commit()

        fresh_room = _get_room(conn, room_id)
        return {
            "applied": applied,
            "duplicate": not applied,
            "progress_epoch": fresh_room["progress_epoch"],
            "advancement": advancement,
            "state": _state_payload(conn, fresh_room, now),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.put("/rooms/{room_id}/clock")
def update_clock(room_id: str, payload: ClockUpdate) -> Dict[str, Any]:
    """暂停或恢复排练。

    目标状态与当前一致时为幂等空操作：直接返回当前状态，``clock_version``
    不递增（即便 ``expected_clock_version`` 已过期也视为成功重试）。否则
    校验 ``expected_clock_version``，与当前版本不一致返回 409、状态不变；
    校验通过后 ``clock_version`` 递增：

    - 暂停（paused=true）：记录 ``paused_at``，活跃成员的超时判定随即冻结；
    - 恢复（paused=false）：把整段暂停时长（now - paused_at）补到每个成员
      的 ``last_seen_at`` 上，使暂停前的剩余在线时长原样延续。暂停前已离线
      的成员补时后仍然离线，不会被重新激活。
    """
    now = _now()
    conn = connect()
    try:
        # 先在无锁状态下判断是否为幂等空操作，并重试友好
        room = _get_room(conn, room_id)
        if bool(room["paused"]) == payload.paused:
            return {
                "changed": False,
                "paused": bool(room["paused"]),
                "clock_version": room["clock_version"],
                "paused_at": room["paused_at"],
                "state": _state_payload(conn, room, now),
            }

        try:
            conn.execute("BEGIN IMMEDIATE")
            room = _get_room(conn, room_id)
            if bool(room["paused"]) == payload.paused:
                conn.commit()  # 并发下可能已被其他请求切换
                fresh = _get_room(conn, room_id)
                return {
                    "changed": False,
                    "paused": bool(fresh["paused"]),
                    "clock_version": fresh["clock_version"],
                    "paused_at": fresh["paused_at"],
                    "state": _state_payload(conn, fresh, now),
                }
            if payload.expected_clock_version != room["clock_version"]:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"时钟版本不匹配: expected_clock_version="
                        f"{payload.expected_clock_version}, 当前 clock_version="
                        f"{room['clock_version']}"
                    ),
                )

            new_clock_version = room["clock_version"] + 1
            if payload.paused:
                conn.execute(
                    """
                    UPDATE rooms
                    SET paused = 1, clock_version = ?, paused_at = ?
                    WHERE id = ?
                    """,
                    (new_clock_version, now, room_id),
                )
            else:
                # 扣除整段暂停时长：成员的“已在线时长”在恢复瞬间与暂停瞬间相同
                pause_duration = now - room["paused_at"]
                conn.execute(
                    "UPDATE participants SET last_seen_at = last_seen_at + ? WHERE room_id = ?",
                    (pause_duration, room_id),
                )
                conn.execute(
                    """
                    UPDATE rooms
                    SET paused = 0, clock_version = ?, paused_at = NULL
                    WHERE id = ?
                    """,
                    (new_clock_version, room_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        fresh = _get_room(conn, room_id)
        return {
            "changed": True,
            "paused": bool(fresh["paused"]),
            "clock_version": fresh["clock_version"],
            "paused_at": fresh["paused_at"],
            "state": _state_payload(conn, fresh, now),
        }
    finally:
        conn.close()


@app.put("/rooms/{room_id}/cursor")
def update_cursor(room_id: str, payload: CursorUpdate) -> Dict[str, Any]:
    """导演校准公共位置（允许前跳或回退）。

    目标位置与当前公共位置一致时为幂等空操作：直接返回当前状态，
    ``progress_epoch`` 不递增（即便 ``expected_progress_epoch`` 已过期也
    视为成功重试）。否则校验 ``expected_progress_epoch``，与当前代际不
    一致返回 409、状态不变；目标位置越界（不在 ``[0, 台词总数)`` 内）
    返回 422、状态不变。校验通过后在同一事务中：

    1. 公共位置原子更新为目标位置（可以低于当前位置，即回退）；
    2. ``progress_epoch`` 递增，旧代际在途的演员上报随即失效；
    3. 清空全部参与者进度，使公共位置从干净状态重新由上报汇聚；
    4. 写入一条推进记录，标明本次位置变化由导演校准触发。

    校准不受暂停状态影响：暂停期间导演仍可调整公共位置；时钟状态
    （paused / clock_version）不随校准改变。
    """
    conn = connect()
    try:
        # 先在无锁状态下判断是否为幂等空操作，并重试友好
        room = _get_room(conn, room_id)
        if room["public_position"] == payload.position:
            return {
                "changed": False,
                "public_position": room["public_position"],
                "progress_epoch": room["progress_epoch"],
                "state": _state_payload(conn, room, _now()),
            }

        try:
            conn.execute("BEGIN IMMEDIATE")
            room = _get_room(conn, room_id)
            now = _now()
            if room["public_position"] == payload.position:
                conn.commit()  # 并发下可能已被其他请求校准到同一位置
                fresh = _get_room(conn, room_id)
                return {
                    "changed": False,
                    "public_position": fresh["public_position"],
                    "progress_epoch": fresh["progress_epoch"],
                    "state": _state_payload(conn, fresh, now),
                }
            if payload.expected_progress_epoch != room["progress_epoch"]:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"进度代际不匹配: expected_progress_epoch="
                        f"{payload.expected_progress_epoch}, 当前 progress_epoch="
                        f"{room['progress_epoch']}"
                    ),
                )
            lines_count = conn.execute(
                "SELECT COUNT(*) AS c FROM lines WHERE room_id = ?", (room_id,)
            ).fetchone()["c"]
            if lines_count == 0:
                raise HTTPException(status_code=409, detail="房间尚未导入台词")
            if payload.position >= lines_count:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"台词位置越界: position={payload.position}, "
                        f"台词总数={lines_count}"
                    ),
                )

            new_epoch = room["progress_epoch"] + 1
            direction = "前跳" if payload.position > room["public_position"] else "回退"
            reason = (
                f"导演校准公共位置：{direction} {room['public_position']} → "
                f"{payload.position}（progress_epoch {room['progress_epoch']} → {new_epoch}）"
            )
            conn.execute(
                "UPDATE rooms SET public_position = ?, progress_epoch = ? WHERE id = ?",
                (payload.position, new_epoch, room_id),
            )
            # 清空参与者进度：新代际从干净状态重新汇聚，旧代际上报一律失效
            conn.execute("DELETE FROM participants WHERE room_id = ?", (room_id,))
            conn.execute(
                """
                INSERT INTO advancements (room_id, position, reason, contributors, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (room_id, payload.position, reason, json.dumps([], ensure_ascii=False), now),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        fresh = _get_room(conn, room_id)
        return {
            "changed": True,
            "public_position": fresh["public_position"],
            "progress_epoch": fresh["progress_epoch"],
            "state": _state_payload(conn, fresh, _now()),
        }
    finally:
        conn.close()


@app.get("/rooms/{room_id}/state")
def get_state(room_id: str) -> Dict[str, Any]:
    """查询当前活跃成员、公共位置及最近一次推进原因。"""
    now = _now()
    conn = connect()
    try:
        room = _get_room(conn, room_id)
        return _state_payload(conn, room, now)
    finally:
        conn.close()


@app.get("/rooms/{room_id}/advancements")
def list_advancements(room_id: str) -> Dict[str, Any]:
    """公共位置的完整推进历史（含每次推进原因）。"""
    conn = connect()
    try:
        _get_room(conn, room_id)
        rows = conn.execute(
            """
            SELECT position, reason, contributors, created_at
            FROM advancements WHERE room_id = ? ORDER BY id
            """,
            (room_id,),
        ).fetchall()
        return {
            "room_id": room_id,
            "advancements": [
                {
                    "position": row["position"],
                    "reason": row["reason"],
                    "contributors": json.loads(row["contributors"]),
                    "at": row["created_at"],
                }
                for row in rows
            ],
        }
    finally:
        conn.close()
