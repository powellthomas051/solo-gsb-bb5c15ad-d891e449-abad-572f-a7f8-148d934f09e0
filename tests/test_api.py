"""核心行为测试：幂等、quorum 推进、绝不回退、离线超时。"""

import os
import tempfile
import uuid

# 必须在导入应用前指定独立数据库文件
os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")

import pytest
from fastapi.testclient import TestClient

import app.main as main
from app.main import app


class FakeClock:
    def __init__(self, start: float = 1_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture()
def client(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(main, "_now", clock)
    with TestClient(app) as test_client:
        test_client.clock = clock
        yield test_client


def make_room(client, threshold=2, timeout=30, lines=("甲：你好。", "乙：来了？", "甲：请坐。", "乙：谢谢。", "甲：开始吧。")):
    room_id = "room-" + uuid.uuid4().hex[:8]
    resp = client.post("/rooms", json={
        "id": room_id,
        "name": "排练室",
        "advance_threshold": threshold,
        "offline_timeout_seconds": timeout,
    })
    assert resp.status_code == 201, resp.text
    resp = client.post(f"/rooms/{room_id}/lines", json={"lines": list(lines)})
    assert resp.status_code == 200, resp.text
    return room_id


def report(client, room_id, participant, seq, position, progress_epoch=None):
    # 默认携带房间当前代际；测试旧代际失效时显式传入过期值
    if progress_epoch is None:
        progress_epoch = state(client, room_id)["progress_epoch"]
    return client.post(f"/rooms/{room_id}/events", json={
        "participant_id": participant,
        "seq": seq,
        "position": position,
        "progress_epoch": progress_epoch,
    })


def state(client, room_id):
    resp = client.get(f"/rooms/{room_id}/state")
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_no_advance_until_threshold_met(client):
    room_id = make_room(client, threshold=2)
    resp = report(client, room_id, "actor-a", 1, 4)
    assert resp.status_code == 200
    body = resp.json()
    assert body["applied"] is True
    assert body["advancement"] is None
    assert state(client, room_id)["public_position"] == -1


def test_advance_to_highest_line_reached_by_threshold(client):
    room_id = make_room(client, threshold=2)
    report(client, room_id, "actor-a", 1, 4)
    resp = report(client, room_id, "actor-b", 1, 2)
    body = resp.json()
    # 两人共同到达的最高台词是 2（第 2 高的位置）
    assert body["advancement"]["position"] == 2
    assert body["advancement"]["contributors"] == ["actor-a", "actor-b"]
    assert "2" in body["advancement"]["reason"]
    assert state(client, room_id)["public_position"] == 2
    # 落后者赶上后，公共位置推进到 4
    resp = report(client, room_id, "actor-b", 2, 4)
    assert resp.json()["advancement"]["position"] == 4
    assert state(client, room_id)["public_position"] == 4


def test_duplicate_and_out_of_order_reports_are_idempotent(client):
    room_id = make_room(client, threshold=1)
    report(client, room_id, "actor-a", 5, 3)

    # 完全相同的重试：不产生任何状态变化
    dup = report(client, room_id, "actor-a", 5, 3).json()
    assert dup["applied"] is False
    assert dup["duplicate"] is True
    assert dup["advancement"] is None

    # 乱序（序号更小）但位置更高：同样被忽略
    stale = report(client, room_id, "actor-a", 2, 4).json()
    assert stale["applied"] is False

    current = state(client, room_id)
    assert current["public_position"] == 3
    assert current["active_participants"][0]["position"] == 3
    assert current["active_participants"][0]["last_seq"] == 5


def test_public_position_never_regresses(client):
    room_id = make_room(client, threshold=2, timeout=10)
    report(client, room_id, "actor-a", 1, 4)
    report(client, room_id, "actor-b", 1, 4)
    assert state(client, room_id)["public_position"] == 4

    # actor-a 离线超时后，活跃人数不足，但公共位置绝不回退
    client.clock.advance(11)
    report(client, room_id, "actor-b", 2, 4)  # 刷新 actor-b，使其保持活跃
    current = state(client, room_id)
    assert current["public_position"] == 4
    assert [p["participant_id"] for p in current["active_participants"]] == ["actor-b"]

    # 演员自己回退上报（如重排），公共位置也不回退
    report(client, room_id, "actor-b", 3, 1)
    assert state(client, room_id)["public_position"] == 4


def test_timed_out_participants_leave_quorum(client):
    room_id = make_room(client, threshold=2, timeout=10)
    report(client, room_id, "actor-a", 1, 4)
    report(client, room_id, "actor-b", 1, 4)
    report(client, room_id, "actor-c", 1, 1)
    assert state(client, room_id)["public_position"] == 4

    # a、b 离线后，只剩 actor-c，无法凑够 2 人推进
    client.clock.advance(11)
    resp = report(client, room_id, "actor-c", 2, 3)
    assert resp.json()["advancement"] is None
    current = state(client, room_id)
    assert current["public_position"] == 4
    assert [p["participant_id"] for p in current["active_participants"]] == ["actor-c"]

    # actor-b 回来，两人共同到达 3 → 推进到 3？不，公共位置已是 4，不会回退也不会记录
    resp = report(client, room_id, "actor-b", 2, 3)
    assert resp.json()["advancement"] is None
    assert state(client, room_id)["public_position"] == 4


def test_advancement_reason_and_history(client):
    room_id = make_room(client, threshold=2)
    report(client, room_id, "actor-a", 1, 1)
    report(client, room_id, "actor-b", 1, 1)
    report(client, room_id, "actor-a", 2, 3)
    report(client, room_id, "actor-b", 2, 3)

    current = state(client, room_id)
    last = current["last_advancement"]
    assert last["position"] == 3
    assert last["contributors"] == ["actor-a", "actor-b"]
    assert "actor-a" in last["reason"] and "actor-b" in last["reason"]

    history = client.get(f"/rooms/{room_id}/advancements").json()["advancements"]
    assert [a["position"] for a in history] == [1, 3]


def test_position_out_of_range_rejected(client):
    room_id = make_room(client)
    resp = report(client, room_id, "actor-a", 1, 99)
    assert resp.status_code == 422
    resp = report(client, room_id, "actor-a", 1, -1)
    assert resp.status_code == 422


def test_report_before_lines_imported_rejected(client):
    room_id = "room-" + uuid.uuid4().hex[:8]
    client.post("/rooms", json={
        "id": room_id, "name": "空房间",
        "advance_threshold": 1, "offline_timeout_seconds": 30,
    })
    resp = report(client, room_id, "actor-a", 1, 0)
    assert resp.status_code == 409


def test_reimport_lines_resets_progress(client):
    room_id = make_room(client, threshold=1)
    report(client, room_id, "actor-a", 1, 4)
    assert state(client, room_id)["public_position"] == 4

    resp = client.post(f"/rooms/{room_id}/lines", json={"lines": ["新台词一", "新台词二"]})
    assert resp.json() == {
        "room_id": room_id,
        "lines_imported": 2,
        "progress_reset": True,
        "revision": 2,  # 首次导入为 1，整体替换再开启新版本
        "progress_epoch": 2,  # 首次导入为 1，整体导入再开启新代际
    }

    current = state(client, room_id)
    assert current["public_position"] == -1
    assert current["active_participants"] == []
    assert current["lines_count"] == 2
    assert current["revision"] == 2


def test_create_room_with_same_id_is_idempotent(client):
    payload = {"id": "fixed-room", "name": "固定房间",
               "advance_threshold": 3, "offline_timeout_seconds": 15}
    first = client.post("/rooms", json=payload)
    second = client.post("/rooms", json=payload)
    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["room_id"] == second.json()["room_id"] == "fixed-room"


def test_state_endpoint_shape(client):
    room_id = make_room(client, threshold=2, timeout=30)
    report(client, room_id, "actor-a", 1, 2)
    report(client, room_id, "actor-b", 1, 2)
    current = state(client, room_id)

    assert current["room_id"] == room_id
    assert current["advance_threshold"] == 2
    assert current["offline_timeout_seconds"] == 30
    assert current["lines_count"] == 5
    assert current["public_position"] == 2
    assert current["current_line"] == {"position": 2, "text": "甲：请坐。"}
    assert {p["participant_id"] for p in current["active_participants"]} == {"actor-a", "actor-b"}
    assert current["last_advancement"]["position"] == 2
    assert "server_time" in current


def test_unknown_room_returns_404(client):
    assert client.get("/rooms/nope/state").status_code == 404
    assert client.post("/rooms/nope/lines", json={"lines": ["x"]}).status_code == 404
    assert client.patch("/rooms/nope/lines", json={
        "base_revision": 0, "operations": [{"op": "delete", "position": 0}],
    }).status_code == 404
    # 旧代际上报不存在的房间：404（代际校验在房间存在之后）
    assert client.post("/rooms/nope/events", json={
        "participant_id": "actor-a", "seq": 1, "position": 0, "progress_epoch": 0,
    }).status_code == 404


# ---------------------------------------------------------------------------
# 台本增量修订（PATCH /rooms/{room_id}/lines）
# ---------------------------------------------------------------------------

def patch_lines(client, room_id, base_revision, operations):
    return client.patch(f"/rooms/{room_id}/lines", json={
        "base_revision": base_revision,
        "operations": operations,
    })


def lines(client, room_id):
    resp = client.get(f"/rooms/{room_id}/lines")
    assert resp.status_code == 200, resp.text
    return resp.json()


def line_texts(client, room_id):
    return [line["text"] for line in lines(client, room_id)["lines"]]


def participant_positions(client, room_id):
    return {p["participant_id"]: p["position"]
            for p in state(client, room_id)["active_participants"]}


def test_patch_lines_insert_replace_delete(client):
    room_id = make_room(client)  # 5 行台词，revision=1
    resp = patch_lines(client, room_id, 1, [
        {"op": "insert", "position": 0, "text": "导演：开机。"},
        {"op": "replace", "position": 2, "text": "甲：请坐吧。"},
        {"op": "delete", "position": 3},
    ])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["revision"] == 2
    assert body["lines_count"] == 5

    payload = lines(client, room_id)
    assert payload["revision"] == 2
    assert [line["text"] for line in payload["lines"]] == [
        "导演：开机。",    # 行首插入
        "甲：你好。",
        "乙：来了？",
        "甲：请坐吧。",    # 行 2 被替换
        "甲：开始吧。",    # 行 3 被删，行 4 前移
    ]
    # 行号重新连续编号
    assert [line["position"] for line in payload["lines"]] == [0, 1, 2, 3, 4]


def test_patch_lines_insert_at_end_appends(client):
    room_id = make_room(client)
    resp = patch_lines(client, room_id, 1, [
        {"op": "insert", "position": 5, "text": "全体：谢幕。"},  # == 台词总数：追加
    ])
    assert resp.status_code == 200
    assert resp.json()["lines_count"] == 6
    assert line_texts(client, room_id)[-1] == "全体：谢幕。"


def test_patch_lines_revision_mismatch_returns_409(client):
    room_id = make_room(client)
    resp = patch_lines(client, room_id, 99, [{"op": "delete", "position": 0}])
    assert resp.status_code == 409
    # 台本与版本均未变化
    payload = lines(client, room_id)
    assert payload["revision"] == 1
    assert len(payload["lines"]) == 5


def test_patch_lines_out_of_range_rolls_back_everything(client):
    room_id = make_room(client)
    resp = patch_lines(client, room_id, 1, [
        {"op": "replace", "position": 0, "text": "甲：改过了。"},
        {"op": "insert", "position": 6, "text": "越界一行。"},  # 6 > 台词总数 5
        {"op": "delete", "position": 99},
    ])
    assert resp.status_code == 422
    # 整批不落库：replace 与版本号都未生效
    payload = lines(client, room_id)
    assert payload["revision"] == 1
    assert [line["text"] for line in payload["lines"]][0] == "甲：你好。"


def test_patch_lines_conflicting_operations_rejected(client):
    room_id = make_room(client)
    for operations in (
        [{"op": "replace", "position": 1, "text": "乙：改。"},
         {"op": "delete", "position": 1}],
        [{"op": "delete", "position": 1},
         {"op": "delete", "position": 1}],
        [{"op": "replace", "position": 1, "text": "乙：改一。"},
         {"op": "replace", "position": 1, "text": "乙：改二。"}],
    ):
        resp = patch_lines(client, room_id, 1, operations)
        assert resp.status_code == 422, operations
    assert lines(client, room_id)["revision"] == 1
    assert len(lines(client, room_id)["lines"]) == 5


def test_patch_lines_request_validation(client):
    room_id = make_room(client)
    # insert/replace 缺少 text 或文本为空白
    assert patch_lines(client, room_id, 1, [{"op": "insert", "position": 0}]).status_code == 422
    assert patch_lines(client, room_id, 1, [
        {"op": "replace", "position": 0, "text": "  "},
    ]).status_code == 422
    # delete 不允许携带 text
    assert patch_lines(client, room_id, 1, [
        {"op": "delete", "position": 0, "text": "多余"},
    ]).status_code == 422
    # 空操作列表 / 负行号 / 未知操作
    assert patch_lines(client, room_id, 1, []).status_code == 422
    assert patch_lines(client, room_id, 1, [{"op": "delete", "position": -1}]).status_code == 422
    assert patch_lines(client, room_id, 1, [{"op": "move", "position": 0}]).status_code == 422
    assert lines(client, room_id)["revision"] == 1


def test_patch_lines_remaps_participant_positions(client):
    room_id = make_room(client, threshold=2)
    report(client, room_id, "actor-a", 1, 1)  # 指向「乙：来了？」
    report(client, room_id, "actor-b", 1, 3)  # 指向「乙：谢谢。」

    # 行首插入：保留的台词跟随新位置
    resp = patch_lines(client, room_id, 1, [
        {"op": "insert", "position": 0, "text": "导演：开机。"},
    ])
    assert resp.json()["participants_remapped"] == 2
    assert participant_positions(client, room_id) == {"actor-a": 2, "actor-b": 4}

    # 删除 actor-a 指向的台词：回退到最近仍保留的前一行（「甲：你好。」）
    patch_lines(client, room_id, 2, [{"op": "delete", "position": 2}])
    assert participant_positions(client, room_id) == {"actor-a": 1, "actor-b": 3}

    # 再删除其之前所有行：无前一行 → -1；actor-b 指向的台词仍保留并跟随
    patch_lines(client, room_id, 3, [
        {"op": "delete", "position": 0},
        {"op": "delete", "position": 1},
    ])
    positions = participant_positions(client, room_id)
    assert positions["actor-a"] == -1
    assert positions["actor-b"] == 1  # 「乙：谢谢。」现在位于行 1
    assert line_texts(client, room_id) == ["甲：请坐。", "乙：谢谢。", "甲：开始吧。"]


def test_patch_lines_replace_keeps_line_identity(client):
    room_id = make_room(client, threshold=1)
    report(client, room_id, "actor-a", 1, 2)  # 指向「甲：请坐。」

    # replace 只改文本，台词身份保留：参与者与公共位置原地不动
    resp = patch_lines(client, room_id, 1, [
        {"op": "replace", "position": 2, "text": "甲：请坐吧。"},
    ])
    assert resp.json()["participants_remapped"] == 0
    current = state(client, room_id)
    assert current["public_position"] == 2
    assert current["current_line"] == {"position": 2, "text": "甲：请坐吧。"}
    assert participant_positions(client, room_id) == {"actor-a": 2}


def test_patch_lines_remaps_public_position_without_fake_advancement(client):
    room_id = make_room(client, threshold=1)
    report(client, room_id, "actor-a", 1, 3)  # 公共位置推进到 3（「乙：谢谢。」）
    before = client.get(f"/rooms/{room_id}/advancements").json()["advancements"]
    assert len(before) == 1

    # 行首插入：公共位置跟随原台词
    patch_lines(client, room_id, 1, [{"op": "insert", "position": 0, "text": "导演：开机。"}])
    assert state(client, room_id)["public_position"] == 4

    # 删除公共位置指向的台词：回退到最近仍保留的前一行
    patch_lines(client, room_id, 2, [{"op": "delete", "position": 4}])
    assert state(client, room_id)["public_position"] == 3

    # 连同其之前所有行一起删除：无前一行 → -1
    patch_lines(client, room_id, 3, [
        {"op": "delete", "position": 0},
        {"op": "delete", "position": 1},
        {"op": "delete", "position": 2},
        {"op": "delete", "position": 3},
    ])
    current = state(client, room_id)
    assert current["public_position"] == -1
    assert current["current_line"] is None

    # 修订全程不产生任何新的推进记录
    after = client.get(f"/rooms/{room_id}/advancements").json()["advancements"]
    assert after == before
    assert current["last_advancement"]["position"] == 3  # 历史记录保持原样


def test_import_lines_bumps_revision_and_resets(client):
    room_id = make_room(client)  # 首次导入 → revision 1
    resp = patch_lines(client, room_id, 1, [{"op": "delete", "position": 0}])
    assert resp.json()["revision"] == 2
    report(client, room_id, "actor-a", 1, 2)

    # 整体导入仍重置进度，并再开启一个新版本
    resp = client.post(f"/rooms/{room_id}/lines", json={"lines": ["新台词一", "新台词二"]})
    assert resp.json()["revision"] == 3
    current = state(client, room_id)
    assert current["revision"] == 3
    assert current["public_position"] == -1
    assert current["active_participants"] == []
    assert current["lines_count"] == 2


def test_revision_exposed_in_room_lines_and_state(client):
    room_id = make_room(client)
    assert client.get(f"/rooms/{room_id}").json()["revision"] == 1
    assert lines(client, room_id)["revision"] == 1
    assert state(client, room_id)["revision"] == 1

    patch_lines(client, room_id, 1, [{"op": "insert", "position": 0, "text": "导演：开机。"}])
    assert client.get(f"/rooms/{room_id}").json()["revision"] == 2
    assert lines(client, room_id)["revision"] == 2
    assert state(client, room_id)["revision"] == 2


# ---------------------------------------------------------------------------
# 排练暂停/恢复（PUT /rooms/{room_id}/clock）
# ---------------------------------------------------------------------------

def set_clock(client, room_id, expected_clock_version, paused):
    return client.put(f"/rooms/{room_id}/clock", json={
        "expected_clock_version": expected_clock_version,
        "paused": paused,
    })


def pause(client, room_id, expected=0):
    return set_clock(client, room_id, expected, True)


def resume(client, room_id, expected=1):
    return set_clock(client, room_id, expected, False)


def active_ids(client, room_id):
    return [p["participant_id"] for p in state(client, room_id)["active_participants"]]


def test_state_exposes_clock_fields_for_running_room(client):
    room_id = make_room(client)
    current = state(client, room_id)
    assert current["paused"] is False
    assert current["clock_version"] == 0
    assert current["paused_at"] is None


def test_pause_freezes_timeout_judgement(client):
    room_id = make_room(client, threshold=1, timeout=10)
    report(client, room_id, "actor-a", 1, 4)

    client.clock.advance(5)
    resp = pause(client, room_id, expected=0)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["changed"] is True
    assert body["paused"] is True
    assert body["clock_version"] == 1
    assert body["paused_at"] == client.clock.t
    assert body["state"]["paused"] is True

    # 暂停远超超时窗口：成员依旧活跃，冻结在暂停瞬间的集合上
    client.clock.advance(1000)
    current = state(client, room_id)
    assert current["paused"] is True
    assert current["clock_version"] == 1
    assert current["paused_at"] == client.clock.t - 1000
    assert active_ids(client, room_id) == ["actor-a"]


def test_events_rejected_while_paused(client):
    room_id = make_room(client, threshold=1, timeout=10)
    report(client, room_id, "actor-a", 1, 4)
    pause(client, room_id)

    client.clock.advance(5)
    # 暂停期间一切位置上报均被拒绝
    resp = report(client, room_id, "actor-a", 2, 4)
    assert resp.status_code == 409, resp.text
    # 新参与者同样无法在暂停期间加入
    resp = report(client, room_id, "actor-b", 1, 4)
    assert resp.status_code == 409

    # 拒绝不产生任何状态变化
    current = state(client, room_id)
    assert active_ids(client, room_id) == ["actor-a"]
    assert current["active_participants"][0]["last_seq"] == 1
    assert current["public_position"] == 4


def test_resume_subtracts_full_pause_duration(client):
    room_id = make_room(client, threshold=1, timeout=10)
    report(client, room_id, "actor-a", 1, 4)
    client.clock.advance(5)           # 暂停前剩余在线时长 5 秒
    pause(client, room_id)
    client.clock.advance(1000)        # 暂停多久都不计入在线时长

    resp = resume(client, room_id, expected=1)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["changed"] is True
    assert body["paused"] is False
    assert body["clock_version"] == 2
    assert body["paused_at"] is None

    # 恢复瞬间成员仍活跃；再等 4 秒（剩余 5 - 4）仍在线
    client.clock.advance(4)
    assert active_ids(client, room_id) == ["actor-a"]

    # 再等 2 秒：累计超过暂停前剩余的 5 秒，正常离线
    client.clock.advance(2)
    assert active_ids(client, room_id) == []


def test_already_offline_members_are_not_reactivated_on_resume(client):
    room_id = make_room(client, threshold=1, timeout=10)
    report(client, room_id, "actor-a", 1, 4)

    # actor-a 在暂停前就已离线
    client.clock.advance(11)
    assert active_ids(client, room_id) == []

    pause(client, room_id)
    client.clock.advance(1000)
    resume(client, room_id, expected=1)

    # 扣除暂停时长后：1011 - 1000 = 11 > 10，离线成员不得被重新激活
    assert active_ids(client, room_id) == []
    current = state(client, room_id)
    assert current["paused"] is False
    assert current["clock_version"] == 2


def test_idempotent_pause_and_resume_do_not_bump_version(client):
    room_id = make_room(client)
    pause(client, room_id)
    paused_at_first = state(client, room_id)["paused_at"]

    # 重复暂停（即使携带过期版本号）：目标一致，直接返回当前状态且不增版
    resp = set_clock(client, room_id, 0, True)
    assert resp.status_code == 200
    body = resp.json()
    assert body["changed"] is False
    assert body["clock_version"] == 1
    assert body["paused_at"] == paused_at_first

    client.clock.advance(30)
    # 幂等暂停不刷新 paused_at：冻结时刻保持不变
    assert state(client, room_id)["paused_at"] == paused_at_first

    resume(client, room_id, expected=1)
    # 重复恢复同样不增版
    resp = set_clock(client, room_id, 1, False)
    assert resp.status_code == 200
    assert resp.json()["changed"] is False
    assert resp.json()["clock_version"] == 2


def test_clock_version_mismatch_returns_409_and_keeps_state(client):
    room_id = make_room(client)
    pause(client, room_id)  # clock_version -> 1

    # 以过期版本恢复：409，房间保持暂停
    resp = resume(client, room_id, expected=0)
    assert resp.status_code == 409, resp.text
    current = state(client, room_id)
    assert current["paused"] is True
    assert current["clock_version"] == 1

    # 暂停仍在生效：上报继续被拒
    assert report(client, room_id, "actor-a", 1, 0).status_code == 409

    # 正确版本恢复成功，clock_version 递增
    assert resume(client, room_id, expected=1).status_code == 200
    current = state(client, room_id)
    assert current["paused"] is False
    assert current["clock_version"] == 2

    # 运行中再以过期版本暂停：409，状态不变
    resp = pause(client, room_id, expected=1)
    assert resp.status_code == 409
    assert state(client, room_id)["paused"] is False
    assert state(client, room_id)["clock_version"] == 2


def test_clock_unknown_room_returns_404(client):
    assert set_clock(client, "nope", 0, True).status_code == 404


def test_clock_state_is_independent_of_script_revision(client):
    room_id = make_room(client, threshold=1)
    report(client, room_id, "actor-a", 1, 4)
    pause(client, room_id)

    # 暂停期间台本整体导入：重置进度并递增 revision，但不改变暂停状态
    client.clock.advance(100)
    resp = client.post(f"/rooms/{room_id}/lines", json={"lines": ["新台词一", "新台词二"]})
    assert resp.status_code == 200
    current = state(client, room_id)
    assert current["revision"] == 2
    assert current["paused"] is True
    assert current["clock_version"] == 1
    # 暂停期间依然拒绝上报
    assert report(client, room_id, "actor-a", 1, 0).status_code == 409

    # 恢复后按整段暂停时长扣减，导入清空了参与者，可正常重新上报
    resume(client, room_id, expected=1)
    assert report(client, room_id, "actor-a", 1, 1).status_code == 200

    # 增量修订同样不影响时钟版本
    set_clock(client, room_id, 2, True)
    patch_resp = patch_lines(client, room_id, 2, [
        {"op": "insert", "position": 0, "text": "导演：开机。"},
    ])
    assert patch_resp.status_code == 200
    current = state(client, room_id)
    assert current["revision"] == 3
    assert current["paused"] is True
    assert current["clock_version"] == 3


def test_pause_resume_preserves_advancement_history(client):
    room_id = make_room(client, threshold=2)
    report(client, room_id, "actor-a", 1, 2)
    report(client, room_id, "actor-b", 1, 2)
    before = client.get(f"/rooms/{room_id}/advancements").json()["advancements"]
    assert [a["position"] for a in before] == [2]

    pause(client, room_id)
    client.clock.advance(60)
    resume(client, room_id, expected=1)

    # 暂停/恢复本身不产生推进记录
    after = client.get(f"/rooms/{room_id}/advancements").json()["advancements"]
    assert after == before

    # 恢复后推进语义照常工作
    client.clock.advance(1)
    resp = report(client, room_id, "actor-a", 2, 4)
    assert resp.status_code == 200  # 单人力推进时也正常；此处阈值 2
    report(client, room_id, "actor-b", 2, 4)
    history = client.get(f"/rooms/{room_id}/advancements").json()["advancements"]
    assert [a["position"] for a in history] == [2, 4]


def test_resume_with_zero_length_pause(client):
    room_id = make_room(client, threshold=1, timeout=10)
    report(client, room_id, "actor-a", 1, 4)
    pause(client, room_id)
    # 立刻恢复：暂停时长为 0，成员仍按正常超时规则计时
    resume(client, room_id, expected=1)
    assert active_ids(client, room_id) == ["actor-a"]
    client.clock.advance(10)
    assert active_ids(client, room_id) == ["actor-a"]
    client.clock.advance(1)
    assert active_ids(client, room_id) == []


# ---------------------------------------------------------------------------
# 导演校准公共位置（PUT /rooms/{room_id}/cursor）与进度代际 progress_epoch
# ---------------------------------------------------------------------------

def put_cursor(client, room_id, expected_progress_epoch, position):
    return client.put(f"/rooms/{room_id}/cursor", json={
        "expected_progress_epoch": expected_progress_epoch,
        "position": position,
    })


def test_state_exposes_progress_epoch_starting_at_zero(client):
    # 房间创建后、台词导入前：代际为 0
    room_id = "room-" + uuid.uuid4().hex[:8]
    client.post("/rooms", json={
        "id": room_id, "name": "空房间",
        "advance_threshold": 1, "offline_timeout_seconds": 30,
    })
    current = state(client, room_id)
    assert current["progress_epoch"] == 0
    assert client.get(f"/rooms/{room_id}").json()["progress_epoch"] == 0

    # 首次导入台本即开启第一个进度代际
    client.post(f"/rooms/{room_id}/lines", json={"lines": ["台词一", "台词二"]})
    assert state(client, room_id)["progress_epoch"] == 1


def test_event_requires_progress_epoch_field(client):
    room_id = make_room(client)
    # 缺少 progress_epoch 字段：请求体校验失败
    resp = client.post(f"/rooms/{room_id}/events", json={
        "participant_id": "actor-a", "seq": 1, "position": 0,
    })
    assert resp.status_code == 422


def test_stale_epoch_event_rejected_with_409(client):
    room_id = make_room(client, threshold=1)
    assert state(client, room_id)["progress_epoch"] == 1
    report(client, room_id, "actor-a", 1, 4)
    assert state(client, room_id)["public_position"] == 4

    # 导演前跳到别处，代际 1 → 2
    resp = put_cursor(client, room_id, 1, 2)
    assert resp.status_code == 200, resp.text
    assert resp.json()["changed"] is True
    assert resp.json()["progress_epoch"] == 2

    # 旧代际（1）的延迟/重试事件：409，不重新推进房间
    stale = report(client, room_id, "actor-a", 2, 4, progress_epoch=1)
    assert stale.status_code == 409, stale.text
    current = state(client, room_id)
    assert current["public_position"] == 2
    assert current["progress_epoch"] == 2
    # 旧事件没有写入参与者（参与者已在校准时清空，且旧事件被拒绝）
    assert current["active_participants"] == []

    # 携带未来代际同样拒绝
    future = report(client, room_id, "actor-a", 3, 4, progress_epoch=99)
    assert future.status_code == 409
    assert state(client, room_id)["public_position"] == 2


def test_director_can_jump_forward_and_roll_back(client):
    room_id = make_room(client, threshold=2)
    report(client, room_id, "actor-a", 1, 4)
    report(client, room_id, "actor-b", 1, 4)
    assert state(client, room_id)["public_position"] == 4

    # 回退到 1：公共位置允许被导演强制拉回，参与者清空，代际递增
    resp = put_cursor(client, room_id, 1, 1)
    body = resp.json()
    assert body["changed"] is True
    assert body["public_position"] == 1
    assert body["progress_epoch"] == 2
    assert body["state"]["public_position"] == 1
    assert body["state"]["active_participants"] == []
    current = state(client, room_id)
    assert current["current_line"] == {"position": 1, "text": "乙：来了？"}
    assert current["progress_epoch"] == 2

    # 新代际下重新上报：可再次自然推进，推进历史包含导演校准记录
    report(client, room_id, "actor-a", 1, 3)
    report(client, room_id, "actor-b", 1, 3)
    assert state(client, room_id)["public_position"] == 3

    # 继续前跳到 4
    resp = put_cursor(client, room_id, 2, 4)
    assert resp.json()["changed"] is True
    assert resp.json()["progress_epoch"] == 3

    history = client.get(f"/rooms/{room_id}/advancements").json()["advancements"]
    assert [a["position"] for a in history] == [4, 1, 3, 4]
    assert "导演校准" in history[1]["reason"]
    assert history[1]["contributors"] == []
    assert state(client, room_id)["last_advancement"]["position"] == 4


def test_cursor_clears_all_participant_progress(client):
    room_id = make_room(client, threshold=1, timeout=10)
    report(client, room_id, "actor-a", 1, 4)
    report(client, room_id, "actor-b", 1, 4)
    assert len(state(client, room_id)["active_participants"]) == 2

    put_cursor(client, room_id, 1, 2)
    current = state(client, room_id)
    assert current["active_participants"] == []

    # 清空是物理删除：即使等待超过超时窗口，也不会有“僵尸”参与者重新出现
    client.clock.advance(100)
    assert state(client, room_id)["active_participants"] == []

    # 旧参与者必须携带新代际并重新上报才会回来
    resp = report(client, room_id, "actor-a", 1, 2, progress_epoch=2)
    assert resp.status_code == 200
    assert [p["participant_id"] for p in state(client, room_id)["active_participants"]] == ["actor-a"]


def test_cursor_same_position_is_idempotent_no_epoch_bump(client):
    room_id = make_room(client, threshold=1)
    report(client, room_id, "actor-a", 1, 3)
    assert state(client, room_id)["public_position"] == 3

    # 目标与当前位置相同：幂等空操作，即使携带过期代际也成功，不增代际
    resp = put_cursor(client, room_id, 99, 3)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["changed"] is False
    assert body["public_position"] == 3
    assert body["progress_epoch"] == 1
    assert state(client, room_id)["progress_epoch"] == 1

    # 空操作不清空参与者、不写推进记录
    current = state(client, room_id)
    assert [p["participant_id"] for p in current["active_participants"]] == ["actor-a"]
    history = client.get(f"/rooms/{room_id}/advancements").json()["advancements"]
    assert [a["position"] for a in history] == [3]

    # 校准到新位置后再校准回同一位置：仍是幂等空操作，代际不再递增
    put_cursor(client, room_id, 1, 4)
    assert state(client, room_id)["progress_epoch"] == 2
    resp = put_cursor(client, room_id, 2, 4)
    assert resp.json()["changed"] is False
    assert resp.json()["progress_epoch"] == 2
    assert state(client, room_id)["progress_epoch"] == 2


def test_cursor_epoch_mismatch_returns_409_and_keeps_state(client):
    room_id = make_room(client, threshold=1)
    report(client, room_id, "actor-a", 1, 2)
    put_cursor(client, room_id, 1, 4)  # 代际 → 2，公共位置 4，参与者清空

    # 以过期代际回退：409，状态完全不变
    resp = put_cursor(client, room_id, 1, 1)
    assert resp.status_code == 409, resp.text
    current = state(client, room_id)
    assert current["public_position"] == 4
    assert current["progress_epoch"] == 2
    assert current["active_participants"] == []

    # 正确代际校准成功
    resp = put_cursor(client, room_id, 2, 1)
    assert resp.status_code == 200
    assert resp.json()["progress_epoch"] == 3
    assert state(client, room_id)["public_position"] == 1


def test_cursor_out_of_range_returns_422_and_keeps_state(client):
    room_id = make_room(client)  # 5 行台词，导入后代际为 1
    resp = put_cursor(client, room_id, 1, 5)
    assert resp.status_code == 422, resp.text
    current = state(client, room_id)
    assert current["public_position"] == -1
    assert current["progress_epoch"] == 1

    # 负位置由请求体校验拦截
    assert put_cursor(client, room_id, 1, -1).status_code == 422
    assert state(client, room_id)["progress_epoch"] == 1


def test_cursor_before_lines_imported_rejected(client):
    room_id = "room-" + uuid.uuid4().hex[:8]
    client.post("/rooms", json={
        "id": room_id, "name": "空房间",
        "advance_threshold": 1, "offline_timeout_seconds": 30,
    })
    resp = put_cursor(client, room_id, 0, 0)
    assert resp.status_code == 409
    assert state(client, room_id)["progress_epoch"] == 0


def test_cursor_unknown_room_returns_404(client):
    assert put_cursor(client, "nope", 0, 0).status_code == 404


def test_cursor_works_while_paused_and_does_not_touch_clock(client):
    room_id = make_room(client, threshold=1)
    report(client, room_id, "actor-a", 1, 4)
    pause(client, room_id)  # clock_version → 1，progress_epoch 仍为 1

    # 暂停期间导演仍可校准
    resp = put_cursor(client, room_id, 1, 2)
    assert resp.status_code == 200, resp.text
    assert resp.json()["progress_epoch"] == 2
    current = state(client, room_id)
    assert current["public_position"] == 2
    assert current["paused"] is True
    assert current["clock_version"] == 1
    assert current["active_participants"] == []

    # 暂停期间旧代际与新代际上报都仍被“暂停”规则拒绝
    assert report(client, room_id, "actor-a", 1, 2, progress_epoch=2).status_code == 409

    # 恢复时钟（clock_version 与 progress_epoch 互不影响）后携带新代际上报
    resume(client, room_id, expected=1)
    assert report(client, room_id, "actor-a", 1, 2, progress_epoch=2).status_code == 200
    assert state(client, room_id)["progress_epoch"] == 2


def test_reimport_lines_bumps_progress_epoch_and_fences_old_events(client):
    room_id = make_room(client, threshold=1)  # 导入 → revision 1，epoch 1
    report(client, room_id, "actor-a", 1, 4)

    # 整体重导台本：revision 与 progress_epoch 同时递增，参与者清空
    resp = client.post(f"/rooms/{room_id}/lines", json={"lines": ["新台词一", "新台词二"]})
    assert resp.json()["revision"] == 2
    assert resp.json()["progress_epoch"] == 2
    current = state(client, room_id)
    assert current["progress_epoch"] == 2
    assert current["active_participants"] == []
    assert current["public_position"] == -1

    # 重导前在途的旧代际事件失效
    stale = report(client, room_id, "actor-a", 2, 1, progress_epoch=1)
    assert stale.status_code == 409
    assert state(client, room_id)["active_participants"] == []
    assert state(client, room_id)["public_position"] == -1

    # 携带新代际上报恢复正常
    ok = report(client, room_id, "actor-a", 1, 1, progress_epoch=2)
    assert ok.status_code == 200
    assert ok.json()["applied"] is True
    assert state(client, room_id)["public_position"] == 1


def test_patch_lines_does_not_bump_progress_epoch(client):
    room_id = make_room(client, threshold=1)
    assert state(client, room_id)["progress_epoch"] == 1
    report(client, room_id, "actor-a", 1, 4)
    # 增量修订只重排位置、递增 revision，不开启新进度代际
    resp = patch_lines(client, room_id, 1, [
        {"op": "insert", "position": 0, "text": "导演：开机。"},
    ])
    assert resp.status_code == 200
    current = state(client, room_id)
    assert current["revision"] == 2
    assert current["progress_epoch"] == 1

    # 同一代际下旧参与者无需重新感知代际即可继续上报
    resp = report(client, room_id, "actor-a", 2, 4)
    assert resp.status_code == 200


def test_duplicate_event_under_current_epoch_still_idempotent(client):
    room_id = make_room(client, threshold=1)
    report(client, room_id, "actor-a", 5, 3)
    dup = report(client, room_id, "actor-a", 5, 3)
    assert dup.status_code == 200
    assert dup.json()["applied"] is False
    assert dup.json()["progress_epoch"] == 1

