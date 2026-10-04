"""动作声明：门禁条件与"当场拒绝 / 排队等待"策略都在这里定死，每个动作一行，
不允许散落在接口处理器里临时判断。

门禁返回 ``None`` 表示当下允许；否则返回一条结构化结论，写清**卡在哪一步**
（``step``）、**当前是什么状态**（``current_state``）以及原因。受理阶段据此
当场拒绝（``reject`` 策略），或把命令单挂成排队（``queue`` 策略），由 worker
在每次空闲时重新看门禁、条件满足后接着跑。

门禁第三个参数是"当前正在执行的长动作集合"，用于互斥：投运和二次网循环都要
独占循环泵，任何一方在跑，另一方（以及停运）都不能进来。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from ..app import Platform

# 门禁结论：None=放行；否则 {"step","current_state","code","message"}
GateVerdict = dict | None
Gate = Callable[["Platform", dict, "set[str]"], GateVerdict]


def _blocked(step: str, current_state: str, code: str, message: str) -> dict:
    return {"step": step, "current_state": current_state, "code": code, "message": message}


# 需要独占循环泵的长动作。
_PUMP_LONG = {"startup", "secondary_cycle", "shutdown", "pump_stop"}


def _pump_busy(busy: set[str], step: str = "pump") -> GateVerdict:
    running = busy & _PUMP_LONG
    if not running:
        return None
    return _blocked(
        step,
        "long_action_running:" + ",".join(sorted(running)),
        "pump_busy",
        f"循环泵正被长动作 {sorted(running)} 占用，本动作排队等其收尾后执行",
    )


# ---- 各动作门禁 ---------------------------------------------------------


def _gate_startup(platform: "Platform", params: dict, busy: set[str]) -> GateVerdict:
    pump = _pump_busy(busy, step="cycle")
    if pump is not None:
        return pump
    state = platform.source.state()
    if state == "tripped":
        return _blocked(
            "weather",
            state,
            "source_tripped",
            f"热源处于停机状态（{platform.source.trip_reason()}），须先 /api/reset 复位才能投运",
        )
    if state == "online":
        return _blocked("weather", state, "already_online", "热源已带负荷在线，投运不重复执行")
    # idle / quality / quantity 都允许继续投运（可能是中途气象已推、调质进行中）。
    return None


def _gate_shutdown(platform: "Platform", params: dict, busy: set[str]) -> GateVerdict:
    state = platform.source.state()
    if state == "tripped":
        return _blocked("pump", state, "already_tripped", "热源已停机，停运动作不重复执行")
    if state == "idle" and platform.pump.idle():
        return _blocked("pump", state, "already_idle", "平台已在停运态，无需再次停运")
    return _pump_busy(busy)


def _gate_secondary_cycle(platform: "Platform", params: dict, busy: set[str]) -> GateVerdict:
    if not platform.pump.idle():
        return _blocked(
            "pump",
            "pump_running",
            "pump_busy",
            "循环泵仍在运行（上一个循环尚未收尾），本动作排队等泵停稳后自动执行",
        )
    return _pump_busy(busy)


def _gate_reset(platform: "Platform", params: dict, busy: set[str]) -> GateVerdict:
    state = platform.source.state()
    if state != "tripped":
        return _blocked("source", state, "not_tripped", f"热源当前为 {state}，只有停机(tripped)后才能复位")
    return None


def _gate_adjust(platform: "Platform", params: dict, busy: set[str]) -> GateVerdict:
    state = platform.source.state()
    if state == "tripped":
        return _blocked(
            "source", state, "source_tripped", f"热源已停机（{platform.source.trip_reason()}），禁止调质调量"
        )
    return None


def _gate_pump_start(platform: "Platform", params: dict, busy: set[str]) -> GateVerdict:
    if platform.pump.running():
        return _blocked("pump", "pump_running", "pump_running", "循环泵已在运行，启动不重复执行")
    return None


def _gate_pump_stop(platform: "Platform", params: dict, busy: set[str]) -> GateVerdict:
    if not platform.pump.running():
        return _blocked("pump", "pump_idle", "pump_idle", "循环泵未运行，无需停止")
    return None


def _always_allow(platform: "Platform", params: dict, busy: set[str]) -> GateVerdict:
    return None


@dataclass(frozen=True)
class ActionSpec:
    """一个动作的受理策略声明。"""

    action: str
    gate: Gate
    policy: str  # reject / queue
    long_running: bool = False
    # 排队最长等待秒数（仅 queue 策略）；超时转 expired。
    queue_ttl_s: float = 120.0


def build_action_specs() -> dict[str, ActionSpec]:
    """构造动作 -> 声明 的对照表。

    长动作里：投运内含二次网循环，同样排队，避免和单独的循环抢同一台泵；
    二次网循环排队，满足"条件满足自动接着跑"。其余动作一律当场拒绝或直接受理。
    """

    specs = [
        ActionSpec("weather", _always_allow, "reject"),
        ActionSpec("adjust", _gate_adjust, "reject"),
        ActionSpec("startup", _gate_startup, "queue", long_running=True, queue_ttl_s=600.0),
        ActionSpec("shutdown", _gate_shutdown, "reject", long_running=True),
        ActionSpec("dispatch", _always_allow, "reject"),
        ActionSpec("cycle", _always_allow, "reject", long_running=True),
        ActionSpec("calibrate", _always_allow, "reject"),
        ActionSpec("retrofit", _always_allow, "reject"),
        ActionSpec("secondary_cycle", _gate_secondary_cycle, "queue", long_running=True),
        ActionSpec("meter", _always_allow, "reject"),
        ActionSpec("settle", _always_allow, "reject"),
        ActionSpec("zone_split", _always_allow, "reject"),
        ActionSpec("valve_replay", _always_allow, "reject"),
        ActionSpec("pump_start", _gate_pump_start, "reject"),
        ActionSpec("pump_stop", _gate_pump_stop, "reject", long_running=True),
        ActionSpec("disturb", _always_allow, "reject"),
        ActionSpec("trip", _always_allow, "reject"),
        ActionSpec("reset", _gate_reset, "reject"),
    ]
    return {spec.action: spec for spec in specs}
