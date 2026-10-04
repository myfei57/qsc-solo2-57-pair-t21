"""动作门控策略。

每个动作的前置条件（门控）与"状态不满足时怎么办"在这里按动作定死：

- ``reject``：当场拒绝，受理记录直接进 ``rejected`` 终态，接口返回 409，理由
  说清卡在哪个条件、当前值是什么——瞬时动作（调温、复位、投运）走这条路，
  条件不满足必须当场暴露，不能悄悄排队；
- ``queue``：先受理为 ``queued`` 排队，执行器在状态变化后重新评估，条件满足
  自动放行执行——依赖前序动作到位的编排动作（调度下发等热源在线、二次网循环
  等泵空闲且回水不超温）走这条路。

门控评估只读平台状态，不触碰设备；评估结果（``unmet`` 明细）同时进受理记录、
接口响应与审计流水，三处一致。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

REJECT = "reject"
QUEUE = "queue"


@dataclass(frozen=True)
class Gate:
    """一条前置条件：``check`` 满足返回 True；不满足时用 ``message`` 与当前值说明卡在哪。"""

    name: str
    message: str
    check: Callable[[Any], bool]
    current: Callable[[Any], str]


@dataclass(frozen=True)
class ActionPolicy:
    """一个动作的门控策略：不满足时是当场拒绝还是排队等状态满足。"""

    on_block: str = REJECT
    gates: tuple[Gate, ...] = field(default_factory=tuple)


def _source_current(platform: Any) -> str:
    reason = platform.source.trip_reason() or "无"
    return f"热源状态={platform.source.state()} 停机原因={reason}"


GATE_SOURCE_NOT_TRIPPED = Gate(
    "source_not_tripped",
    "热源处于停机保护，需先复位再操作",
    lambda platform: platform.source.state() != "tripped",
    _source_current,
)
GATE_SOURCE_TRIPPED = Gate(
    "source_tripped",
    "热源未处于停机保护，无需复位",
    lambda platform: platform.source.state() == "tripped",
    _source_current,
)
GATE_SOURCE_ONLINE = Gate(
    "source_online",
    "热源未在线（调质调量未完成），调度下发等投运完成",
    lambda platform: platform.source.state() == "online",
    _source_current,
)
GATE_PUMP_IDLE = Gate(
    "pump_idle",
    "循环泵未停稳，等泵空闲后再起循环",
    lambda platform: platform.pump.idle(),
    lambda platform: f"泵运行={platform.pump.running()} 流量={round(platform.pump.flow_t_h(), 2)}t/h",
)
GATE_RETURN_NOT_OVERHEATED = Gate(
    "return_not_overheated",
    "二次网回水超温，等回水回落后再起循环",
    lambda platform: not platform.secondary.overheat(),
    lambda platform: (
        f"回水={round(platform.secondary.return_c(), 2)}℃ "
        f"上限={round(platform.secondary.return_ceiling_c(), 2)}℃"
    ),
)

# 按动作定死的策略表：状态不允许时是当场拒绝还是排队等状态满足，只看这张表。
POLICIES: dict[str, ActionPolicy] = {
    "weather": ActionPolicy(REJECT, (GATE_SOURCE_NOT_TRIPPED,)),
    "adjust": ActionPolicy(REJECT, (GATE_SOURCE_NOT_TRIPPED,)),
    "startup": ActionPolicy(REJECT, (GATE_SOURCE_NOT_TRIPPED,)),
    "shutdown": ActionPolicy(REJECT, ()),
    "dispatch": ActionPolicy(QUEUE, (GATE_SOURCE_ONLINE,)),
    "cycle": ActionPolicy(REJECT, ()),
    "secondary_cycle": ActionPolicy(QUEUE, (GATE_PUMP_IDLE, GATE_RETURN_NOT_OVERHEATED)),
    "calibrate": ActionPolicy(REJECT, ()),
    "retrofit": ActionPolicy(REJECT, ()),
    "meter": ActionPolicy(REJECT, ()),
    "settle": ActionPolicy(REJECT, ()),
    "split": ActionPolicy(REJECT, ()),
    "replay": ActionPolicy(REJECT, ()),
    "pump": ActionPolicy(REJECT, ()),
    "disturb": ActionPolicy(REJECT, ()),
    "trip": ActionPolicy(REJECT, ()),
    "reset": ActionPolicy(REJECT, (GATE_SOURCE_TRIPPED,)),
    "concurrency": ActionPolicy(REJECT, ()),
    "reset_state": ActionPolicy(REJECT, ()),
}


def evaluate(platform: Any, action: str) -> list[dict]:
    """评估一个动作的全部门控，返回未满足条件的明细列表（空表示放行）。"""

    unmet: list[dict] = []
    for gate in POLICIES[action].gates:
        if gate.check(platform):
            continue
        unmet.append({"gate": gate.name, "message": gate.message, "current": gate.current(platform)})
    return unmet


def on_block(action: str) -> str:
    """返回动作在门控不满足时的策略（``reject`` / ``queue``）。"""

    return POLICIES[action].on_block
