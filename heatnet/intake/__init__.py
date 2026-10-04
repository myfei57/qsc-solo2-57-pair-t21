"""动作受理台：幂等提交、门控、排队、进度与审计一致性。

控制台动作统一"先受理、后执行"：同一 ``request_id`` 只执行一次，状态不允许时
按动作策略当场拒绝或排队等状态满足，长动作的进度实时落记录与审计，进程重启时
未走完的记录按部分失败（``interrupted``）收尾。
"""

from __future__ import annotations

from .actions import ACTIONS, ActionSpec, spec_for
from .intake import Intake
from .policy import POLICIES, ActionPolicy, Gate, evaluate, on_block
from .record import RequestRecord

__all__ = [
    "ACTIONS",
    "POLICIES",
    "ActionPolicy",
    "ActionSpec",
    "Gate",
    "Intake",
    "RequestRecord",
    "evaluate",
    "on_block",
    "spec_for",
]
