"""动作受理台。

控制台所有动作接口统一走"先受理、后执行"：提交即落一条受理记录并返回，执行
在后台单线程执行器里串行进行（设备动作本就不能并行，平台锁之外再加一道排队，
重复提交与并发提交在这里归并成一次执行）。这里定死四件事：

1. **幂等**：客户端每次提交带 ``request_id``，同一标识只执行一次；重复提交
   （含并发）拿到的是同一条记录与当前进度，``attempts`` 记下重复次数；同标识
   但动作或参数不同的，按冲突当场拒绝（409）。
2. **门控**：执行前评估动作门控（见 :mod:`~heatnet.intake.policy`），不满足时
   按动作策略当场拒绝（``rejected``）或排队等状态满足（``queued``，执行器在
   状态变化后重新评估、满足即放行）。
3. **进度**：长动作的每个编排步骤实时写进记录并落审计，控制台按标识查询；
   进程重启时仍处于进行中的记录一律标 ``interrupted``——中断就是部分失败，
   已走的步骤留在记录里，看得出没走完。
4. **一致**：拒绝理由、进度、终态结果与审计流水同源——每次状态迁移都发事件
   落审计，事件载荷就是记录本身的内容；记录同时按标识落盘，跨重启可查上一次
   跑到哪一步、结果是什么。
"""

from __future__ import annotations

import collections
import hashlib
import json
import threading
import uuid

from ..eventbus import Event
from ..errors import PlantError
from .actions import spec_for
from .policy import QUEUE, evaluate, on_block
from .record import (
    ACCEPTED,
    FAILED,
    INTERRUPTED,
    QUEUED,
    REJECTED,
    RUNNING,
    SUCCEEDED,
    RequestRecord,
)

_EVENT_ACTION = {
    ACCEPTED: "request_accepted",
    QUEUED: "request_queued",
    RUNNING: "request_started",
    SUCCEEDED: "request_succeeded",
    FAILED: "request_failed",
    REJECTED: "request_rejected",
    INTERRUPTED: "request_interrupted",
}

_SEVERITY = {
    ACCEPTED: "notice",
    QUEUED: "notice",
    RUNNING: "notice",
    SUCCEEDED: "notice",
    FAILED: "warning",
    REJECTED: "alarm",
    INTERRUPTED: "warning",
}

_QUEUED_POLL_S = 0.2


def _fingerprint(action: str, params: dict) -> str:
    """对动作与参数算指纹，同一标识换了内容要能查出来。"""

    canonical = json.dumps(
        {"action": action, "params": params}, ensure_ascii=False, sort_keys=True, default=str
    )
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()


class Intake:
    """动作受理台：幂等登记、门控、串行执行、进度落盘与恢复。"""

    def __init__(self, platform, memory_limit: int = 512) -> None:
        self._platform = platform
        self._records: dict[str, RequestRecord] = {}
        self._order: list[str] = []
        self._memory_limit = max(16, memory_limit)
        self._cond = threading.Condition(threading.RLock())
        self._pending: collections.deque[str] = collections.deque()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._restore()

    # ---- 提交 -------------------------------------------------------------

    def submit(self, action: str, params: dict, request_id: str | None = None) -> tuple[int, dict]:
        """受理一次动作提交，返回 ``(HTTP 状态码, 响应体)``。

        202 表示新受理（``accepted`` 待执行或 ``queued`` 排队等状态）；200 表示
        同一标识的重复提交，返回既有记录；409 表示门控当场拒绝或标识冲突。
        """

        spec = spec_for(action)
        clean = dict(params)
        if spec.validate is not None:
            spec.validate(self._platform, clean)
        client_key = request_id is not None and str(request_id).strip() != ""
        request_id = str(request_id).strip() if client_key else f"req-{uuid.uuid4().hex[:12]}"
        fingerprint = _fingerprint(action, clean)
        now = self._platform.clock.now()
        with self._cond:
            existing = self._records.get(request_id)
            if existing is not None:
                if existing.fingerprint != fingerprint:
                    return 409, {
                        "error": "request_id_conflict",
                        "request_id": request_id,
                        "message": "同一标识提交了不同的动作或参数，请换新标识",
                    }
                existing.attempts += 1
                self._persist(existing)
                self._publish(existing, "request_duplicate", note=f"第 {existing.attempts} 次提交")
                return 200, {"request": existing.as_dict(), "duplicate": True}
            record = RequestRecord(
                request_id=request_id,
                action=action,
                params=clean,
                fingerprint=fingerprint,
                client_key=client_key,
                submitted_ts=now,
            )
            unmet = evaluate(self._platform, action)
            if unmet and on_block(action) == QUEUE:
                self._register(record)
                self._transition(record, QUEUED, "门控不满足，排队等状态满足", unmet=unmet)
                # 排队的记录靠执行线程周期重评估放行，线程必须已经在跑。
                self._ensure_thread()
                self._cond.notify_all()
                return 202, {"request": record.as_dict(), "duplicate": False}
            if unmet:
                self._register(record)
                self._transition(record, REJECTED, "门控不满足，当场拒绝", unmet=unmet)
                return 409, {
                    "error": "interlock_rejected",
                    "request": record.as_dict(),
                    "duplicate": False,
                }
            self._register(record)
            self._transition(record, ACCEPTED, "已受理，等待执行")
            self._pending.append(request_id)
            self._ensure_thread()
            self._cond.notify_all()
            return 202, {"request": record.as_dict(), "duplicate": False}

    # ---- 查询 -------------------------------------------------------------

    def get(self, request_id: str) -> dict:
        """按标识取受理记录（含进度与结果），未登记即 ``KeyError``。"""

        with self._cond:
            record = self._records.get(str(request_id))
            if record is None:
                raise KeyError(f"未登记的请求标识: {request_id}")
            return record.as_dict()

    def list_requests(self, limit: int = 50, action: str | None = None, status: str | None = None) -> list[dict]:
        """列出最近的受理记录（新的在前），可按动作与状态过滤。"""

        with self._cond:
            selected: list[dict] = []
            for request_id in reversed(self._order):
                record = self._records[request_id]
                if action and record.action != action:
                    continue
                if status and record.status != status:
                    continue
                selected.append(record.as_dict())
                if len(selected) >= max(1, limit):
                    break
            return selected

    def summary(self) -> dict:
        """返回受理台汇总（各状态计数），供状态快照读取。"""

        with self._cond:
            by_status: dict[str, int] = {}
            for record in self._records.values():
                by_status[record.status] = by_status.get(record.status, 0) + 1
            return {
                "tracked": len(self._records),
                "by_status": by_status,
                "executor": "running" if self._thread is not None else "standby",
            }

    # ---- 进度 -------------------------------------------------------------

    def note_step(self, request_id: str, step: dict) -> None:
        """记录一个执行中的编排步骤并落盘（控制台进度与审计同源）。"""

        with self._cond:
            record = self._records.get(request_id)
            if record is None or record.terminal():
                return
            record.note_step(step)
            self._persist(record)

    # ---- 执行器 -----------------------------------------------------------

    def _ensure_thread(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run_loop, name="heatnet-intake", daemon=True)
        self._thread.start()

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            with self._cond:
                if not self._pending:
                    self._retry_queued()
                    self._cond.wait(timeout=_QUEUED_POLL_S)
                    continue
                request_id = self._pending.popleft()
            self._execute(request_id)

    def _execute(self, request_id: str) -> None:
        with self._cond:
            record = self._records[request_id]
            action = record.action
            params = dict(record.params)
        # 执行前再评估一次门控：排队等待期间状态可能又变了。
        unmet = evaluate(self._platform, action)
        if unmet:
            with self._cond:
                if on_block(action) == QUEUE:
                    self._transition(record, QUEUED, "执行前状态不再满足，回排队", unmet=unmet)
                else:
                    self._transition(record, REJECTED, "执行前状态不再满足，当场拒绝", unmet=unmet)
            return
        with self._cond:
            self._transition(record, RUNNING, "开始执行")
        spec = spec_for(action)
        report: dict
        error = ""
        try:
            outcome = spec.run(self._platform, params, request_id, self._step_callback(request_id))
            report = dict(outcome) if isinstance(outcome, dict) else {"value": outcome}
        except PlantError as exc:
            report = {"ok": False, "violations": [exc.message]}
            error = exc.message
        except Exception as exc:  # 执行中断属于部分失败，已走步骤留在记录里
            report = {"ok": False}
            error = f"{type(exc).__name__}: {exc}"
        ok = not error and bool(report.get("ok", True))
        result = {key: value for key, value in report.items() if key != "steps"}
        with self._cond:
            if not record.steps and isinstance(report.get("steps"), list):
                # 瞬时动作没有实时步骤回调，终态时把报告里的步骤补进记录，
                # 让"上一次跑了哪几步"对长动作与瞬时动作同样可查。
                for step in report["steps"]:
                    record.note_step(step)
            record.result = result
            record.error = error
            record.stage = str(report.get("stage", ""))
            if ok:
                self._transition(record, SUCCEEDED, "执行完成")
            else:
                violations = [str(item) for item in report.get("violations", [])]
                note = "；".join(violations) or error or "执行未达标"
                self._transition(record, FAILED, f"部分失败：{note}")

    def _step_callback(self, request_id: str):
        def _on_step(step: dict) -> None:
            self.note_step(request_id, step)

        return _on_step

    def _retry_queued(self) -> None:
        """重新评估排队中的记录，状态满足即放行（持锁调用）。"""

        for record in list(self._records.values()):
            if record.status != QUEUED:
                continue
            if evaluate(self._platform, record.action):
                continue
            self._transition(record, ACCEPTED, "状态满足，排队放行")
            self._pending.append(record.request_id)
            self._cond.notify_all()

    # ---- 状态迁移、审计与落盘 ---------------------------------------------

    def _register(self, record: RequestRecord) -> None:
        self._records[record.request_id] = record
        self._order.append(record.request_id)
        while len(self._order) > self._memory_limit:
            victim = next(
                (rid for rid in self._order if self._records[rid].terminal()), None
            )
            if victim is None:
                break
            self._order.remove(victim)
            del self._records[victim]

    def _transition(self, record: RequestRecord, status: str, note: str, unmet: list[dict] | None = None) -> None:
        record.transition(status, self._platform.clock.now(), note, unmet)
        self._persist(record)
        self._publish(record, _EVENT_ACTION[status], severity=_SEVERITY[status], note=note)
        self._cond.notify_all()

    def _publish(self, record: RequestRecord, event_action: str, severity: str = "notice", note: str = "") -> None:
        payload: dict = {
            "op_id": record.request_id,
            "operation": record.action,
            "status": record.status,
            "attempts": record.attempts,
            "step_count": len(record.steps),
        }
        if record.unmet:
            payload["unmet"] = [dict(item) for item in record.unmet]
        if record.error:
            payload["error"] = record.error
        self._platform.bus.publish(
            Event.of(
                "intake",
                "intake",
                event_action,
                severity=severity,
                message=note or event_action,
                **payload,
            )
        )

    def _persist(self, record: RequestRecord) -> None:
        self._platform.store.write(f"requests/{record.request_id}", record.as_dict())

    def _restore(self) -> None:
        """从落盘记录恢复；仍在进行中的一律按中断（部分失败）收尾。"""

        store = self._platform.store
        for doc_id in store.list_ids():
            if not doc_id.startswith("requests/"):
                continue
            raw = store.read(doc_id)
            if not isinstance(raw, dict):
                continue
            record = RequestRecord.from_dict(raw)
            self._records[record.request_id] = record
            self._order.append(record.request_id)
        self._order.sort(key=lambda rid: (self._records[rid].submitted_ts, rid))
        with self._cond:
            for record in list(self._records.values()):
                if record.terminal():
                    continue
                self._transition(record, INTERRUPTED, "进程重启时仍在进行中，按部分失败处理")

    # ---- 生命周期 -----------------------------------------------------------

    def shutdown(self) -> None:
        """停止后台执行线程（进程退出或测试收尾时调用）。"""

        self._stop.set()
        with self._cond:
            self._cond.notify_all()
        if self._thread is not None:
            self._thread.join(timeout=5)
