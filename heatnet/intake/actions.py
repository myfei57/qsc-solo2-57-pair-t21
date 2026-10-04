"""动作注册表：参数校验与执行分派。

受理台只认这里登记的动作。``validate`` 在提交时同步跑——参数越界、引用的对象
不存在这类"不碰设备也能判定"的错误当场报（400/404/409），不进队列；``run``
由执行器在后台线程里调，``op_id`` 取请求标识，让审计流水能按标识把一次请求的
全部步骤与汇总串起来。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..errors import NamespaceError
from ..station.service import CancelToken

Runner = Callable[[Any, dict, str, Callable[[dict], None] | None], dict]
Validator = Callable[[Any, dict], None]


@dataclass(frozen=True)
class ActionSpec:
    """一个可受理动作：同步校验 + 后台执行体。"""

    name: str
    run: Runner
    validate: Validator | None = None


# ---- 提交时校验 ---------------------------------------------------------


def _validate_dt(platform: Any, params: dict) -> None:
    if params["dt"] <= 0:
        raise ValueError("步长必须为正")


def _validate_pump(platform: Any, params: dict) -> None:
    if params.get("pump_action", "cycle") == "cycle":
        _validate_dt(platform, params)


def _validate_rounds(platform: Any, params: dict) -> None:
    if params["rounds"] < 1:
        raise ValueError("探测轮数必须为正")


def _validate_meter(platform: Any, params: dict) -> None:
    if params["meter"] not in platform.meter_names():
        raise KeyError(f"未登记热量表: {params['meter']}")


def _validate_split(platform: Any, params: dict) -> None:
    zones = platform.zones
    source = zones.get(params["source_zone"])
    new_zone = params["new_zone"]
    if new_zone == params["source_zone"]:
        raise NamespaceError("拆分后的分区名不能与原分区相同", component="ns")
    if new_zone in zones.names():
        raise NamespaceError(f"分区已存在: {new_zone}", component="ns")
    if not any(station in source.stations for station in params["stations"]):
        raise NamespaceError("拆分必须至少包含一个属于原分区的站", component="ns")


# ---- 执行体 -------------------------------------------------------------


def _run_weather(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.push_weather(params["outdoor_c"], params["station"], op_id=op_id)


def _run_adjust(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.adjust_source(params["temperature_c"], params["flow_t_h"], params["origin"])


def _run_startup(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.startup(
        params["outdoor_c"], params["demand_pct"], params["station"], op_id=op_id, on_step=on_step
    )


def _run_shutdown(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.shutdown(params["reason"], op_id=op_id, on_step=on_step)


def _run_dispatch(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    cancel = None
    if params.get("cancel"):
        # 控制台要求走取消路径：令牌先置位再下发，批量调节在第一站前中止。
        cancel = CancelToken(params["cancel_reason"])
        cancel.cancel()
    return platform.dispatch(
        params["demand_pct"], params["target_c"], params["zones"], cancel, op_id=op_id, on_step=on_step
    )


def _run_cycle(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.cycle(params["dt"])


def _run_secondary_cycle(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.secondary_cycle(params["dt"], op_id=op_id, on_step=on_step)


def _run_calibrate(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.calibrate_flow(params["measured_t_h"])


def _run_retrofit(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.retrofit_baseline(params["baseline_kpa"])


def _run_meter(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.collect_heat(params["meter"], params["delta_mwh"])


def _run_settle(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.settle_quota()


def _run_split(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.split_zone(params["source_zone"], params["stations"], params["new_zone"])


def _run_replay(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.replay_valves(params["stations"])


def _run_pump(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    kind = params.get("pump_action", "cycle")
    if kind == "start":
        return platform.start_pump(params["speed_pct"])
    if kind == "stop":
        return platform.stop_pump()
    return platform.secondary_cycle(params["dt"], op_id=op_id, on_step=on_step)


def _run_disturb(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.disturb_secondary(params["heat_fraction"])


def _run_trip(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.trip(params["reason"])


def _run_reset(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.reset_source(params["operator"])


def _run_concurrency(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return platform.concurrency_probe(rounds=params["rounds"])


def _run_reset_state(platform: Any, params: dict, op_id: str, on_step: Callable | None) -> dict:
    return {"removed": platform.reset_state()}


ACTIONS: dict[str, ActionSpec] = {
    "weather": ActionSpec("weather", _run_weather),
    "adjust": ActionSpec("adjust", _run_adjust),
    "startup": ActionSpec("startup", _run_startup),
    "shutdown": ActionSpec("shutdown", _run_shutdown),
    "dispatch": ActionSpec("dispatch", _run_dispatch),
    "cycle": ActionSpec("cycle", _run_cycle, _validate_dt),
    "secondary_cycle": ActionSpec("secondary_cycle", _run_secondary_cycle, _validate_dt),
    "calibrate": ActionSpec("calibrate", _run_calibrate),
    "retrofit": ActionSpec("retrofit", _run_retrofit),
    "meter": ActionSpec("meter", _run_meter, _validate_meter),
    "settle": ActionSpec("settle", _run_settle),
    "split": ActionSpec("split", _run_split, _validate_split),
    "replay": ActionSpec("replay", _run_replay),
    "pump": ActionSpec("pump", _run_pump, _validate_pump),
    "disturb": ActionSpec("disturb", _run_disturb),
    "trip": ActionSpec("trip", _run_trip),
    "reset": ActionSpec("reset", _run_reset),
    "concurrency": ActionSpec("concurrency", _run_concurrency, _validate_rounds),
    "reset_state": ActionSpec("reset_state", _run_reset_state),
}


def spec_for(action: str) -> ActionSpec:
    """按名取动作注册项，未登记即 ``KeyError``。"""

    try:
        return ACTIONS[action]
    except KeyError as error:
        raise KeyError(f"未登记动作: {action}") from error
