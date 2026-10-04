"""命令受理与执行状态机。

控制台每次动作提交都落成一条命令单。幂等去重、当场拒绝/排队、进度展示、
长动作中断判定与审计全部围绕命令单的状态走：拒绝理由、进度和审计看到的是
同一份记录，不允许各自维护口径。

状态迁移::

    受理后  QUEUED ──(入场门禁满足，worker 领取)──▶ RUNNING
              │                                     ├─▶ SUCCEEDED
              │                                     ├─▶ PARTIAL_FAILED（走到一半失败/中断）
              │                                     └─▶ FAILED（一步都没落地就异常）
              ├─(reject 策略且门禁不满足)──────────▶ REJECTED
              └─(queue 策略且排队超时)─────────────▶ EXPIRED

只有 ``SUCCEEDED`` 才算走完；``PARTIAL_FAILED`` 是"动了设备但没走完"，必须
在现场和审计里一眼能看出来，不能混成成功。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
PARTIAL_FAILED = "partial_failed"
FAILED = "failed"
REJECTED = "rejected"
EXPIRED = "expired"

# 受理后仍可能变化的非终态；重启恢复时把这些一律判成中断。
ACTIVE_STATES = (QUEUED, RUNNING)
TERMINAL_STATES = (SUCCEEDED, PARTIAL_FAILED, FAILED, REJECTED, EXPIRED)

# 合法迁移表，登记之外的任何跳转都直接抛错，防止写出审计解释不了的状态。
_TRANSITIONS: dict[str, tuple[str, ...]] = {
    QUEUED: (RUNNING, REJECTED, EXPIRED),
    RUNNING: (SUCCEEDED, PARTIAL_FAILED, FAILED),
}

# reject / queue 两种入场策略。
REJECT = "reject"
QUEUE = "queue"


class IllegalTransition(RuntimeError):
    """命令状态迁移不在合法表内。"""


@dataclass
class Progress:
    """长动作的实时进度。

    ``stage`` 是当前卡在/跑到的步骤（对应编排返回的 ``stage``），``step_index``
    是已完成步骤序号，``steps_total`` 为预估总步数（未知时为 0），``tick`` 是
    循环类动作的当前迭代。``detail`` 放该步最近一次现场读数。
    """

    stage: str = ""
    step_index: int = 0
    steps_total: int = 0
    tick: int = 0
    detail: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        percent: float | None = None
        if self.steps_total > 0:
            percent = round(min(1.0, self.step_index / self.steps_total) * 100.0, 1)
        return {
            "stage": self.stage,
            "step_index": self.step_index,
            "steps_total": self.steps_total,
            "percent": percent,
            "tick": self.tick,
            "detail": dict(self.detail),
        }

    @classmethod
    def from_dict(cls, raw: dict | None) -> "Progress":
        raw = raw or {}
        return cls(
            stage=str(raw.get("stage", "")),
            step_index=int(raw.get("step_index", 0)),
            steps_total=int(raw.get("steps_total", 0)),
            tick=int(raw.get("tick", 0)),
            detail=dict(raw.get("detail", {})),
        )


@dataclass
class Command:
    """一条命令单：幂等键、动作、状态、门禁结论、进度、结果与审计同源。"""

    command_id: str
    request_id: str
    action: str
    params: dict
    policy: str = REJECT
    state: str = QUEUED
    origin: str = "console"
    created_ts: float = 0.0
    updated_ts: float = 0.0
    started_ts: float | None = None
    finished_ts: float | None = None
    queued_until_monotonic: float | None = None
    progress: Progress = field(default_factory=Progress)
    # 门禁/拒绝结论：{"step","current_state","message","code"}，受理与执行共用。
    rejection: dict | None = None
    result: dict | None = None
    error: dict | None = None
    steps: list = field(default_factory=list)
    attempts: int = 0
    replay_count: int = 0

    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def is_active(self) -> bool:
        return self.state in ACTIVE_STATES

    def transition(self, target: str) -> None:
        """按合法迁移表跳转，非法跳转直接抛错。"""

        allowed = _TRANSITIONS.get(self.state, ())
        if target not in allowed:
            raise IllegalTransition(f"命令 {self.command_id} 不能从 {self.state} 迁到 {target}")
        self.state = target

    def as_dict(self) -> dict:
        return {
            "command_id": self.command_id,
            "request_id": self.request_id,
            "action": self.action,
            "params": self.params,
            "policy": self.policy,
            "state": self.state,
            "origin": self.origin,
            "created_ts": round(self.created_ts, 3),
            "updated_ts": round(self.updated_ts, 3),
            "started_ts": None if self.started_ts is None else round(self.started_ts, 3),
            "finished_ts": None if self.finished_ts is None else round(self.finished_ts, 3),
            "progress": self.progress.as_dict(),
            "rejection": None if self.rejection is None else dict(self.rejection),
            "result": None if self.result is None else dict(self.result),
            "error": None if self.error is None else dict(self.error),
            "steps": list(self.steps),
            "attempts": self.attempts,
            "replay_count": self.replay_count,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Command":
        return cls(
            command_id=str(raw["command_id"]),
            request_id=str(raw["request_id"]),
            action=str(raw["action"]),
            params=dict(raw.get("params", {})),
            policy=str(raw.get("policy", REJECT)),
            state=str(raw.get("state", QUEUED)),
            origin=str(raw.get("origin", "console")),
            created_ts=float(raw.get("created_ts", 0.0)),
            updated_ts=float(raw.get("updated_ts", 0.0)),
            started_ts=raw.get("started_ts"),
            finished_ts=raw.get("finished_ts"),
            queued_until_monotonic=raw.get("queued_until_monotonic"),
            progress=Progress.from_dict(raw.get("progress")),
            rejection=None if raw.get("rejection") is None else dict(raw["rejection"]),
            result=None if raw.get("result") is None else dict(raw["result"]),
            error=None if raw.get("error") is None else dict(raw["error"]),
            steps=list(raw.get("steps", [])),
            attempts=int(raw.get("attempts", 0)),
            replay_count=int(raw.get("replay_count", 0)),
        )
