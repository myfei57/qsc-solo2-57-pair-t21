"""二次网循环编排。

一个循环里先启泵、再推进流量与回水，任何一步越限都直接返回失败；不管走的是正常
路径还是失败路径，离开循环之前泵都必须停稳——收尾放在 ``finally`` 里，异常
短路和提前返回都不会把它绕过去。泵没停稳就返回，循环会一直空转，把二次网抽空。
"""

from __future__ import annotations

from typing import Callable

from ..pump.pump import Pump
from ..store.state import DurableState
from ..valve.valve import Valve
from .loop import SecondaryLoop

MAX_STEPS = 600

# 进度回调：(阶段, 已完成步数, 预估总步, 当前迭代, 现场读数)
ProgressCallback = Callable[[str, int, int, int, dict], None]


def run_cycle(
    loop: SecondaryLoop,
    pump: Pump,
    valve: Valve,
    durable: DurableState,
    dt: float = 1.0,
    speed_pct: float = 85.0,
    on_progress: ProgressCallback | None = None,
) -> dict:
    """跑完一个二次网循环，并保证泵在返回前停稳。"""

    if dt <= 0:
        raise ValueError("步长必须为正")

    def report(stage: str, tick: int, detail: dict) -> None:
        if on_progress is not None:
            # 预热升速 + 收尾停泵两段，取升速段做百分比，预估上限 MAX_STEPS。
            on_progress(stage, 0, MAX_STEPS, tick, detail)

    def circulate() -> dict:
        steps: list[dict] = []
        violations: list[str] = []
        last: dict = {}
        durable.save(f"secondary/{loop.limits.makeup_target_kpa:.0f}", {"running": True})
        pump.start(speed_pct)
        steps.append(
            {
                "component": "pump",
                "action": "start",
                "detail": {"target_flow_t_h": round(pump.target_flow_t_h(), 2)},
            }
        )
        report("pump_start", 0, {"target_flow_t_h": round(pump.target_flow_t_h(), 2)})
        pump.set_speed(speed_pct)
        for tick in range(MAX_STEPS):
            flow = pump.step(dt)
            last = loop.step(dt, flow)
            if tick % 20 == 0 or last["overheat"]:
                report(
                    "circulate",
                    tick,
                    {
                        "flow_t_h": round(flow, 2),
                        "return_c": round(loop.return_c(), 2),
                    },
                )
            if last["overheat"]:
                violations.append("return_overtemperature")
                steps.append(
                    {
                        "component": "secondary",
                        "action": "abort",
                        "detail": {
                            "return_c": round(loop.return_c(), 2),
                            "ceiling_c": round(loop.return_ceiling_c(), 2),
                        },
                    }
                )
                report(
                    "abort_overtemperature",
                    tick,
                    {"return_c": round(loop.return_c(), 2), "ceiling_c": round(loop.return_ceiling_c(), 2)},
                )
                return {
                    "ok": False,
                    "steps": steps,
                    "violations": violations,
                    "stage": "abort_overtemperature",
                    "flow_t_h": round(flow, 2),
                }
            if pump.target_flow_t_h() > 0 and abs(pump.flow_t_h() - pump.target_flow_t_h()) < 1e-6:
                break
        steps.append(
            {
                "component": "secondary",
                "action": "settle",
                "detail": {"flow_t_h": round(pump.flow_t_h(), 2), "return_c": round(loop.return_c(), 2)},
            }
        )
        report(
            "settle",
            MAX_STEPS,
            {"flow_t_h": round(pump.flow_t_h(), 2), "return_c": round(loop.return_c(), 2)},
        )
        valve.set_target(valve.position_pct())
        return {
            "ok": not violations,
            "steps": steps,
            "violations": violations,
            "flow_t_h": round(pump.flow_t_h(), 2),
            "balance": last,
        }

    try:
        outcome = circulate()
    finally:
        pump.stop()
        for tick in range(MAX_STEPS):
            pump.step(dt)
            if tick % 20 == 0:
                report("pump_coasting_down", tick, {"flow_t_h": round(pump.flow_t_h(), 2)})
            if pump.idle():
                break
        report("pump_idle", MAX_STEPS, {"flow_t_h": round(pump.flow_t_h(), 2)})
        durable.save(
            f"secondary/{loop.limits.makeup_target_kpa:.0f}",
            {"running": False, "flow_t_h": round(pump.flow_t_h(), 4)},
        )
    outcome["pump_idle"] = pump.idle()
    return outcome
