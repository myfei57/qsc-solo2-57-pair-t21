"""二次网循环编排。

一个循环里先启泵、再推进流量与回水，任何一步越限都直接返回失败；不管走的是正常
路径还是失败路径，离开循环之前泵都必须停稳——收尾放在 ``finally`` 里，异常
短路和提前返回都不会把它绕过去。泵没停稳就返回，循环会一直空转，把二次网抽空。

``progress`` 回调让受理台在循环推进过程中就能看到每一步（启泵、越限中止、稳态），
而不是等整个循环跑完才知道干了什么。
"""

from __future__ import annotations

from typing import Callable

from ..pump.pump import Pump
from ..store.state import DurableState
from ..valve.valve import Valve
from .loop import SecondaryLoop

MAX_STEPS = 600


def run_cycle(
    loop: SecondaryLoop,
    pump: Pump,
    valve: Valve,
    durable: DurableState,
    dt: float = 1.0,
    speed_pct: float = 85.0,
    progress: Callable[[dict], None] | None = None,
) -> dict:
    """跑完一个二次网循环，并保证泵在返回前停稳。"""

    if dt <= 0:
        raise ValueError("步长必须为正")

    def circulate() -> dict:
        steps: list[dict] = []
        violations: list[str] = []
        last: dict = {}

        def emit(step: dict) -> None:
            steps.append(step)
            if progress is not None:
                progress(step)

        durable.save(f"secondary/{loop.limits.makeup_target_kpa:.0f}", {"running": True})
        pump.start(speed_pct)
        emit(
            {
                "component": "pump",
                "action": "start",
                "detail": {"target_flow_t_h": round(pump.target_flow_t_h(), 2)},
            }
        )
        pump.set_speed(speed_pct)
        for _ in range(MAX_STEPS):
            flow = pump.step(dt)
            last = loop.step(dt, flow)
            if last["overheat"]:
                violations.append("return_overtemperature")
                emit(
                    {
                        "component": "secondary",
                        "action": "abort",
                        "detail": {
                            "return_c": round(loop.return_c(), 2),
                            "ceiling_c": round(loop.return_ceiling_c(), 2),
                        },
                    }
                )
                return {"ok": False, "steps": steps, "violations": violations, "flow_t_h": round(flow, 2)}
            if pump.target_flow_t_h() > 0 and abs(pump.flow_t_h() - pump.target_flow_t_h()) < 1e-6:
                break
        emit(
            {
                "component": "secondary",
                "action": "settle",
                "detail": {"flow_t_h": round(pump.flow_t_h(), 2), "return_c": round(loop.return_c(), 2)},
            }
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
        for _ in range(MAX_STEPS):
            pump.step(dt)
            if pump.idle():
                break
        durable.save(
            f"secondary/{loop.limits.makeup_target_kpa:.0f}",
            {"running": False, "flow_t_h": round(pump.flow_t_h(), 4)},
        )
    outcome["pump_idle"] = pump.idle()
    return outcome
