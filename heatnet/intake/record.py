"""受理记录。

每一次动作提交都落一条受理记录：标识、动作、参数指纹、状态机、已走的编排步骤、
终态结果与拒绝理由都在同一条记录里。控制台展示的进度、接口返回的拒绝理由、审计
流水里的迁移事件，全都从这条记录取数——三处看到的永远是同一份事实，不会出现
"接口说拒绝了、审计里却像执行过"的两张皮。
"""

from __future__ import annotations

from dataclasses import dataclass, field

# 状态机：accepted → running → succeeded / failed
#                ↘ queued（门控不满足且策略为排队）→ accepted（状态满足后放行）
#                ↘ rejected（门控不满足且策略为当场拒绝，终态）
# 进程重启时仍处于 accepted / queued / running 的记录一律转 interrupted（终态）。
ACCEPTED = "accepted"
QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
REJECTED = "rejected"
INTERRUPTED = "interrupted"

STATUSES = (ACCEPTED, QUEUED, RUNNING, SUCCEEDED, FAILED, REJECTED, INTERRUPTED)
TERMINAL = (SUCCEEDED, FAILED, REJECTED, INTERRUPTED)


@dataclass
class RequestRecord:
    """一条动作受理记录（内存形态与落盘形态同构）。"""

    request_id: str
    action: str
    params: dict
    fingerprint: str
    client_key: bool
    status: str = ACCEPTED
    stage: str = ""
    steps: list[dict] = field(default_factory=list)
    unmet: list[dict] = field(default_factory=list)
    result: dict | None = None
    error: str = ""
    attempts: int = 1
    submitted_ts: float = 0.0
    started_ts: float = 0.0
    finished_ts: float = 0.0
    history: list[dict] = field(default_factory=list)

    def terminal(self) -> bool:
        """是否已到终态（不再发生状态迁移）。"""

        return self.status in TERMINAL

    def transition(self, status: str, ts: float, note: str = "", unmet: list[dict] | None = None) -> None:
        """迁移状态并记入历史；``unmet`` 随拒绝／排队迁移更新。"""

        if status not in STATUSES:
            raise ValueError(f"未知受理状态: {status}")
        self.status = status
        if unmet is not None:
            self.unmet = [dict(item) for item in unmet]
        if status == RUNNING:
            self.started_ts = float(ts)
            self.unmet = []
        if status in TERMINAL:
            self.finished_ts = float(ts)
        self.history.append({"status": status, "ts": round(float(ts), 3), "note": str(note)})

    def note_step(self, step: dict) -> None:
        """追加一个执行中的编排步骤（进度）。"""

        self.steps.append(dict(step))

    def as_dict(self) -> dict:
        """转成字典形态，控制台响应、落盘与审计事件共用。"""

        return {
            "request_id": self.request_id,
            "action": self.action,
            "params": dict(self.params),
            "fingerprint": self.fingerprint,
            "client_key": self.client_key,
            "status": self.status,
            "stage": self.stage,
            "steps": [dict(step) for step in self.steps],
            "step_count": len(self.steps),
            "unmet": [dict(item) for item in self.unmet],
            "result": self.result,
            "error": self.error,
            "attempts": self.attempts,
            "submitted_ts": round(self.submitted_ts, 3),
            "started_ts": round(self.started_ts, 3),
            "finished_ts": round(self.finished_ts, 3),
            "history": [dict(item) for item in self.history],
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "RequestRecord":
        """从落盘字段还原一条记录。"""

        return cls(
            request_id=str(raw.get("request_id", "")),
            action=str(raw.get("action", "")),
            params=dict(raw.get("params", {})),
            fingerprint=str(raw.get("fingerprint", "")),
            client_key=bool(raw.get("client_key", False)),
            status=str(raw.get("status", ACCEPTED)),
            stage=str(raw.get("stage", "")),
            steps=[dict(item) for item in raw.get("steps", [])],
            unmet=[dict(item) for item in raw.get("unmet", [])],
            result=raw.get("result"),
            error=str(raw.get("error", "")),
            attempts=int(raw.get("attempts", 1)),
            submitted_ts=float(raw.get("submitted_ts", 0.0)),
            started_ts=float(raw.get("started_ts", 0.0)),
            finished_ts=float(raw.get("finished_ts", 0.0)),
            history=[dict(item) for item in raw.get("history", [])],
        )
