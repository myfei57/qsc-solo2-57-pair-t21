"""命令受理、幂等、排队、进度与审计。"""

from __future__ import annotations

from .center import CommandCenter, Rejected
from .ledger import CommandLedger, IdempotencyConflict, InvalidRequestId
from .registry import ActionSpec, build_action_specs
from .state import (
    EXPIRED,
    FAILED,
    PARTIAL_FAILED,
    QUEUED,
    REJECTED,
    RUNNING,
    SUCCEEDED,
    ACTIVE_STATES,
    TERMINAL_STATES,
    Command,
    Progress,
)

__all__ = [
    "CommandCenter",
    "Rejected",
    "CommandLedger",
    "IdempotencyConflict",
    "InvalidRequestId",
    "ActionSpec",
    "build_action_specs",
    "Command",
    "Progress",
    "QUEUED",
    "RUNNING",
    "SUCCEEDED",
    "PARTIAL_FAILED",
    "FAILED",
    "REJECTED",
    "EXPIRED",
    "ACTIVE_STATES",
    "TERMINAL_STATES",
]
