"""请求体的 Pydantic 模型。"""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class RoomCreate(BaseModel):
    """创建房间：推进人数与离线超时在此设定。"""

    name: str = Field(min_length=1, max_length=200)
    advance_threshold: int = Field(ge=1, description="推进公共位置所需的活跃参与者人数")
    offline_timeout_seconds: int = Field(
        ge=1, description="超过该秒数未上报即视为离线，退出推进人数计算"
    )
    id: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=100,
        description="可选的房间 ID；缺省由服务端生成。重复创建同一 ID 返回已存在的房间",
    )


class LinesImport(BaseModel):
    """导入有序台词：列表顺序即台词顺序（位置从 0 开始）。"""

    lines: List[str] = Field(min_length=1)

    @field_validator("lines")
    @classmethod
    def _strip_and_require_non_empty(cls, value: List[str]) -> List[str]:
        cleaned = [line.strip() for line in value]
        if any(not line for line in cleaned):
            raise ValueError("台词不能为空白字符串")
        return cleaned


class EventReport(BaseModel):
    """演员上报：参与者 ID、单调递增事件序号、已到达的台词位置。

    ``progress_epoch`` 必须与房间当前进度代际一致；导演校准公共位置或台本
    整体重导会开启新代际，旧代际的迟到/重试上报一律返回 409，不能再推进
    房间。当前代际可通过 ``GET /rooms/{room_id}/state`` 查询。
    """

    participant_id: str = Field(min_length=1, max_length=100)
    seq: int = Field(ge=0, description="该参与者单调递增的事件序号")
    position: int = Field(ge=0, description="已到达的台词位置（从 0 开始）")
    progress_epoch: int = Field(
        ge=0, description="客户端已知的进度代际；与当前不一致时返回 409"
    )


class LineOperation(BaseModel):
    """单条台本修订操作；``position`` 一律指修订前（当前）行号。"""

    op: Literal["insert", "replace", "delete"]
    position: int = Field(
        ge=0,
        description="当前台本行号；insert 允许等于台词总数（追加到末尾）",
    )
    text: Optional[str] = Field(default=None, description="insert/replace 的台词文本")

    @model_validator(mode="after")
    def _check_text(self) -> "LineOperation":
        if self.op in ("insert", "replace"):
            if self.text is None or not self.text.strip():
                raise ValueError(f"{self.op} 操作必须提供非空台词文本 text")
            self.text = self.text.strip()
        elif self.text is not None:
            raise ValueError("delete 操作不接受 text 字段")
        return self


class LinesPatch(BaseModel):
    """台本增量修订：``base_revision`` 必须等于当前版本，操作整批原子生效。"""

    base_revision: int = Field(
        ge=0, description="客户端基于的台本版本；与当前版本不一致时返回 409"
    )
    operations: List[LineOperation] = Field(min_length=1)


class ClockUpdate(BaseModel):
    """排练暂停/恢复：携带客户端已知的时钟版本做乐观并发控制。"""

    expected_clock_version: int = Field(
        ge=0, description="客户端已知的 clock_version；与当前不一致时返回 409"
    )
    paused: bool = Field(description="目标状态：true 暂停排练，false 恢复排练")


class CursorUpdate(BaseModel):
    """导演校准公共位置：携带客户端已知的进度代际做乐观并发控制。

    允许前跳或回退；校验代际与目标范围后原子更新公共位置、递增
    ``progress_epoch`` 并清空全部参与者进度，使旧代际的迟到/重试上报失效。
    目标位置与当前一致时为幂等空操作，代际不递增。
    """

    expected_progress_epoch: int = Field(
        ge=0, description="客户端已知的 progress_epoch；与当前不一致时返回 409"
    )
    position: int = Field(
        ge=0, description="导演要校准到的目标台词位置（从 0 开始）"
    )
