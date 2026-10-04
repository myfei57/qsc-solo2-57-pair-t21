"""命令中心：受理、排队、执行、进度与审计的唯一入口。

受理模型**定死为先受理后执行**：控制台调用 :meth:`CommandCenter.submit` 时同步
只做三件事——参数校验、幂等登记、入场门禁判断；随后立即返回命令单，真正动设备
全部交给一个后台 worker。短动作和长动作没有第二种受理语义，避免"有的接口同步
执行、有的异步"导致的重复提交与口径不一。

* 同一 ``request_id``：并发/重复提交只执行一次，后来者拿到同一张命令单
  （``replayed=true``）；键复用但动作或参数不一致，报幂等冲突当场拒绝。
* 门禁结论（卡在哪一步、当前什么状态、为什么）同时写进命令单与审计，控制台
  轮询 :class:`~heatnet.command.state.Progress` 看到的就是落审计的那份进度。
* ``queue`` 动作门禁不满足时挂成 QUEUED，worker 每轮重新看门禁，条件满足自动
  接着跑；排队超过 TTL 转 EXPIRED。
* 执行结果按"设备有没有被动过"区分 SUCCEEDED / PARTIAL_FAILED / FAILED。
"""

from __future__ import annotations

import threading
import time
from typing import Callable

from ..clock import Clock
from ..errors import PlantError
from ..eventbus import Event
from ..ids import IdFactory
from ..station.service import CancelToken
from ..store.jsonfile import JsonStore
from .ledger import (
    CommandLedger,
    IdempotencyConflict,
    InvalidRequestId,
    validate_request_id,
)
from .registry import ActionSpec, build_action_specs
from .state import (
    EXPIRED,
    FAILED,
    PARTIAL_FAILED,
    QUEUE,
    QUEUED,
    REJECT,
    REJECTED,
    RUNNING,
    SUCCEEDED,
    Command,
)

# 进度审计事件最小间隔（秒），防止长循环把审计刷爆；关键节点不受节流限制。
_PROGRESS_AUDIT_INTERVAL_S = 0.25


class Rejected(Exception):
    """受理阶段当场拒绝（缺参数/值非法）。"""

    def __init__(self, message: str, *, code: str = "bad_request", status: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.status = status


class CommandCenter:
    """受理命令并用单个 worker 串行执行。"""

    def __init__(
        self,
        platform,
        store: JsonStore,
        clock: Clock,
        queue_ttl_s: float = 120.0,
    ) -> None:
        self._platform = platform
        self._clock = clock
        self._ledger = CommandLedger(store, clock)
        self._specs = build_action_specs()
        self._ids = IdFactory("cmd")
        self._default_ttl_s = float(queue_ttl_s)
        self._wake = threading.Condition()
        self._stopping = threading.Event()
        self._worker = threading.Thread(target=self._run, name="heatnet-command", daemon=True)
        self._worker.start()
        # 启动时先看一眼有没有恢复后仍排队的单子。
        self._kick()

    # ---- 受理 ---------------------------------------------------------

    def specs(self) -> dict[str, ActionSpec]:
        return self._specs

    def submit(
        self,
        action: str,
        request_id: str,
        params: dict,
        origin: str = "console",
    ) -> tuple[Command, bool, int]:
        """受理一次提交。

        返回 ``(命令单, 是否重放, 建议 HTTP 状态码)``。状态码语义：首次受理 202，
        幂等重放 200，门禁当场拒绝 409。
        """

        spec = self._specs.get(action)
        if spec is None:
            raise Rejected(f"未知动作 {action}", code="unknown_action", status=404)

        try:
            request_id = validate_request_id(request_id)
        except InvalidRequestId as error:
            raise Rejected(str(error), code="missing_request_id", status=400) from error

        def same_payload(other_action: str, other_params: dict) -> bool:
            return other_action == action and _fingerprint(other_params) == _fingerprint(params)

        command_id = f"{action}-{request_id}"
        deadline = None
        if spec.policy == QUEUE:
            deadline = self._clock.monotonic() + (spec.queue_ttl_s or self._default_ttl_s)
        try:
            command, created = self._ledger.register(
                command_id,
                request_id,
                action,
                params,
                spec.policy,
                origin=origin,
                queue_deadline_monotonic=deadline,
                payload_fingerprint=same_payload,
            )
        except IdempotencyConflict as error:
            raise Rejected(
                f"幂等键 {request_id} 已用于动作 {error.command.action}，不能复用给 {action}",
                code="idempotency_conflict",
                status=409,
            ) from error

        if not created:
            return command, True, 200

        self._audit(
            command,
            "command_accepted",
            severity="notice",
            message=f"受理 {action}（{spec.policy}）",
        )

        # 入场门禁：reject 不满足当场拒绝；queue 不满足挂排队等条件。
        verdict = self._check_gate(spec, params)
        if verdict is not None:
            if spec.policy == QUEUE:
                from .state import Progress

                def queue_wait(c: Command, v=verdict) -> None:
                    c.progress = Progress(stage="queued_waiting_gate", detail={"gate": v})

                self._ledger.update(command.command_id, queue_wait)
                self._audit(
                    command,
                    "command_queued",
                    severity="notice",
                    message=f"{action} 排队等待：{verdict['message']}",
                    **_flat_verdict(verdict),
                )
                return command, False, 202
            self._reject_now(command, verdict)
            return command, False, 409

        # 门禁直接满足：唤醒 worker 执行。
        self._kick()
        return command, False, 202

    def _reject_now(self, command: Command, verdict: dict) -> None:
        def mutate(c: Command) -> None:
            c.transition(REJECTED)
            c.rejection = dict(verdict)
            c.progress.stage = verdict.get("step", "")
            c.progress.detail = {"gate": verdict}
            c.finished_ts = self._clock.now()

        self._ledger.update(command.command_id, mutate)
        self._audit(
            command,
            "command_rejected",
            severity="alarm",
            message=f"{command.action} 被拒绝（{verdict.get('step')}）：{verdict.get('message')}",
            **_flat_verdict(verdict),
        )

    def _check_gate(self, spec: ActionSpec, params: dict) -> dict | None:
        """门禁在平台锁内判断，保证看到的是一致现场快照。"""

        with self._platform._lock:  # noqa: SLF001 - 命令中心是平台内组件
            return spec.gate(self._platform, params, self._busy_long_actions())

    # ---- 执行循环 -----------------------------------------------------

    def _run(self) -> None:
        while not self._stopping.is_set():
            picked: Command | None = None
            with self._wake:
                while not self._stopping.is_set():
                    self._expire_locked()
                    picked = self._pick_next_locked()
                    if picked is not None:
                        break
                    self._wake.wait(timeout=0.2)
            if picked is None:
                continue
            self._execute(picked)

    def _pick_next_locked(self) -> Command | None:
        """严格 FIFO：只看队首，队首门禁不满足时不跳过它（避免后来者插队）。"""

        queued = self._ledger.queued()
        if not queued:
            return None
        command = queued[0]
        spec = self._specs[command.action]
        busy = self._busy_long_actions()
        with self._platform._lock:  # noqa: SLF001 - 重判门禁要看一致现场
            verdict = spec.gate(self._platform, command.params, busy)
        return None if verdict is not None else command

    def _busy_long_actions(self) -> set[str]:
        """当前正在执行的长动作集合，供互斥门禁判断。"""

        return {
            command.action
            for command in self._ledger.running()
            if self._specs.get(command.action) and self._specs[command.action].long_running
        }

    def _expire_locked(self) -> None:
        now_mono = self._clock.monotonic()
        for command in self._ledger.queued():
            deadline = command.queued_until_monotonic
            if deadline is not None and now_mono >= deadline:
                verdict = {
                    "step": command.progress.stage or "queue",
                    "current_state": "queued",
                    "code": "queue_timeout",
                    "message": f"排队等待超过 {self._specs[command.action].queue_ttl_s:.0f}s，条件仍未满足",
                }

                def mutate(c: Command, v=verdict) -> None:
                    c.transition(EXPIRED)
                    c.rejection = dict(v)
                    c.finished_ts = self._clock.now()

                self._ledger.update(command.command_id, mutate)
                self._audit(
                    command,
                    "command_expired",
                    severity="warning",
                    message=f"{command.action} 排队超时放弃：{verdict['message']}",
                    **_flat_verdict(verdict),
                )

    def _execute(self, command: Command) -> None:
        spec = self._specs[command.action]

        def start(c: Command) -> None:
            c.transition(RUNNING)
            c.started_ts = self._clock.now()
            c.attempts += 1

        self._ledger.update(command.command_id, start)
        self._audit(command, "command_started", severity="notice", message=f"开始执行 {command.action}")

        progressed = [0.0]
        # 长动作被中止后，收尾段（停泵等）的进度不能覆盖真正卡住的阶段。
        aborted = [False]

        def on_progress(stage: str, done: int, total: int, tick: int, detail: dict) -> None:
            if "abort" in stage:
                aborted[0] = True
            if aborted[0] and stage in ("pump_coasting_down", "pump_idle"):
                return
            def mutate(c: Command) -> None:
                c.progress.stage = stage
                c.progress.step_index = int(done)
                c.progress.steps_total = int(total)
                c.progress.tick = int(tick)
                c.progress.detail = dict(detail)

            self._ledger.update(command.command_id, mutate)
            now = self._clock.monotonic()
            if now - progressed[0] >= _PROGRESS_AUDIT_INTERVAL_S or stage in (
                "done",
                "settle",
                "pump_idle",
            ) or stage.endswith("abort_overtemperature"):
                progressed[0] = now
                self._audit(
                    command,
                    "command_progress",
                    severity="notice",
                    message=f"{command.action} 进度：{stage}",
                    stage=stage,
                    step_index=int(done),
                    steps_total=int(total),
                    tick=int(tick),
                    **{f"p_{k}": v for k, v in detail.items() if not isinstance(v, (dict, list))},
                )

        try:
            report = self._invoke(command.action, command.params, on_progress)
        except Rejected as error:
            # 执行前仍可能有参数衍生校验失败：按拒绝处理。
            self._finish_failure(command, FAILED, {"code": error.code, "message": error.message})
            return
        except PlantError as error:
            # 联锁/现场异常，且此前可能已有步骤落地：用 attempts/进度判断部分失败。
            record = error.as_record()
            state = PARTIAL_FAILED if command.progress.step_index > 0 or command.progress.tick > 0 else FAILED
            self._finish_failure(command, state, record)
            return
        except Exception as error:  # noqa: BLE001 - 任何异常都要让命令单走到终态
            state = PARTIAL_FAILED if command.progress.step_index > 0 or command.progress.tick > 0 else FAILED
            self._finish_failure(
                command,
                state,
                {"code": type(error).__name__, "message": str(error)},
            )
            return

        steps = list(report.get("steps", [])) if isinstance(report, dict) else []
        violations = list(report.get("violations", [])) if isinstance(report, dict) else []

        def finish(c: Command) -> None:
            c.steps = steps
            c.result = report
            c.progress.stage = str(report.get("stage") or c.progress.stage or "done")
            if isinstance(report, dict) and report.get("flow_t_h") is not None:
                c.progress.detail = {"flow_t_h": report.get("flow_t_h")}
            c.finished_ts = self._clock.now()
            if violations:
                c.transition(PARTIAL_FAILED)
                c.error = {
                    "code": "interlock_violation",
                    "message": "动作未走完：" + ", ".join(str(v) for v in violations),
                    "violations": violations,
                }
            else:
                c.transition(SUCCEEDED)

        self._ledger.update(command.command_id, finish)
        if violations:
            self._audit(
                command,
                "command_partial_failed",
                severity="warning",
                message=f"{command.action} 未走完，卡在 {command.progress.stage}：{', '.join(map(str, violations))}",
                stage=command.progress.stage,
                violations=violations,
            )
        else:
            self._audit(
                command,
                "command_succeeded",
                severity="notice",
                message=f"{command.action} 执行完成",
                stage=command.progress.stage,
            )
        self._kick()

    def _finish_failure(self, command: Command, state: str, error: dict) -> None:
        def mutate(c: Command) -> None:
            c.transition(state)
            c.error = dict(error)
            c.finished_ts = self._clock.now()

        self._ledger.update(command.command_id, mutate)
        self._audit(
            command,
            f"command_{state}",
            severity="warning" if state == PARTIAL_FAILED else "alarm",
            message=f"{command.action} {state}：{error.get('message', '')}",
            stage=command.progress.stage,
        )
        self._kick()

    # ---- 动作 -> 平台方法 ---------------------------------------------

    def _invoke(self, action: str, p: dict, on_progress: Callable) -> dict:
        platform = self._platform

        def num(key: str, default: float) -> float:
            try:
                return float(p.get(key, default))
            except (TypeError, ValueError) as error:
                raise Rejected(f"参数 {key!r} 必须是数值", code="bad_request", status=400) from error

        if action == "weather":
            return platform.push_weather(num("outdoor_c", 0.0), str(p.get("station", "console")))
        if action == "adjust":
            return platform.adjust_source(
                num("temperature_c", platform.source.temperature_c()),
                num("flow_t_h", platform.network.baseline_t_h()),
                str(p.get("origin", "console")),
            )
        if action == "startup":
            return platform.startup(num("outdoor_c", 0.0), num("demand_pct", 60.0), str(p.get("station", "console")), on_progress)
        if action == "shutdown":
            return platform.shutdown(str(p.get("reason", "console-shutdown")))
        if action == "dispatch":
            zones = p.get("zones")
            selected = None if not zones else [str(item) for item in zones]
            cancel = CancelToken(str(p.get("cancel_reason", "console-cancel"))) if p.get("cancel") else None
            return platform.dispatch(num("demand_pct", 60.0), num("target_c", platform.config.station.setpoint_c), selected, cancel)
        if action == "cycle":
            return platform.cycle(num("dt", 1.0))
        if action == "calibrate":
            return platform.calibrate_flow(num("measured_t_h", platform.network.baseline_t_h()))
        if action == "retrofit":
            return platform.retrofit_baseline(num("baseline_kpa", platform.pressure.baseline.value_kpa()))
        if action == "secondary_cycle":
            return platform.secondary_cycle(num("dt", 1.0), on_progress)
        if action == "meter":
            return platform.collect_heat(
                str(p.get("meter", platform.meter_names()[0])), num("delta_mwh", 1.0)
            )
        if action == "settle":
            return platform.settle_quota()
        if action == "zone_split":
            stations = p.get("stations") or []
            return platform.split_zone(
                str(p.get("source_zone", "")), [str(item) for item in stations], str(p.get("new_zone", ""))
            )
        if action == "valve_replay":
            stations = p.get("stations")
            selected = None if not stations else [str(item) for item in stations]
            return platform.replay_valves(selected)
        if action == "pump_start":
            return platform.start_pump(num("speed_pct", 85.0))
        if action == "pump_stop":
            return platform.stop_pump()
        if action == "disturb":
            return platform.disturb_secondary(num("heat_fraction", 0.5))
        if action == "trip":
            return platform.trip(str(p.get("reason", "overtemperature")))
        if action == "reset":
            return platform.reset_source(str(p.get("operator", "shift-lead")))
        raise Rejected(f"未知动作 {action}", code="unknown_action", status=404)

    # ---- 查询与生命周期 ----------------------------------------------

    def get(self, command_id: str) -> Command | None:
        return self._ledger.get(command_id)

    def by_request(self, request_id: str) -> Command | None:
        return self._ledger.by_request(request_id)

    def recent(self, action: str | None = None, limit: int = 50) -> list[Command]:
        return self._ledger.recent(action=action, limit=limit)

    def last_terminal(self, action: str) -> Command | None:
        return self._ledger.last_terminal(action)

    def describe(self, command: Command) -> dict:
        """控制台展示结构：状态、门禁、进度、结果同源。"""

        payload = command.as_dict()
        payload["status_href"] = f"/api/commands/{command.command_id}"
        return payload

    def _audit(self, command: Command, action_event: str, *, severity: str, message: str, **flat: object) -> None:
        event = Event.of(
            "console",
            "console",
            action_event,
            asset=command.action,
            op_id=command.command_id,
            severity=severity,
            message=message,
            request_id=command.request_id,
            command_id=command.command_id,
            command_state=command.state,
            **flat,
        )
        self._platform.bus.publish(event)

    def _kick(self) -> None:
        with self._wake:
            self._wake.notify_all()

    def stop(self, timeout: float = 2.0) -> None:
        """停 worker。非终态命令单不在内存里改判——由下次启动的恢复逻辑处理。"""

        self._stopping.set()
        self._kick()
        self._worker.join(timeout=timeout)


def _fingerprint(params: dict) -> tuple:
    """参数指纹：只比较影响执行结果的键，忽略来源/幂等键本身。"""

    ignored = {"request_id", "origin", "station", "operator"}
    return tuple(sorted((k, repr(v)) for k, v in params.items() if k not in ignored))


def _flat_verdict(verdict: dict) -> dict:
    return {
        "gate_step": verdict.get("step"),
        "gate_current_state": verdict.get("current_state"),
        "gate_code": verdict.get("code"),
    }
