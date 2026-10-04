"""命令台账：幂等登记与状态落盘。

每条命令单是 ``commands/<command_id>`` 下的一份原子 JSON 文档。登记时用
:meth:`JsonStore.create_if_absent` 做"文件不存在才创建"——同一个 ``request_id``
即便被多线程、多进程并发提交，也只有一方创建成功并拿到执行权，其余都回读同一
份文档。控制台轮询到的进度、拒绝理由和审计事件全部取自这份记录，口径天然一致。

内存里保留近期命令单供快速查询；终态文档不会删，跨重启靠扫描目录恢复。重启时
还停在非终态（``queued``/``running``）的命令单一律改判 ``partial_failed``：
进程都不在了，长动作不可能走完，必须明确告诉现场"没走完"，而不是假装还在跑
或偷偷重放一遍。
"""

from __future__ import annotations

import threading
from typing import Callable

from ..clock import Clock
from ..store.jsonfile import JsonStore
from .state import (
    PARTIAL_FAILED,
    QUEUED,
    RUNNING,
    Command,
)

PREFIX = "commands"
# request_id 允许的字符集：防路径穿越，同时够放 UUID 与业务单号。
_ALLOWED_REQUEST_ID = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
_MAX_REQUEST_ID_LEN = 128


class InvalidRequestId(ValueError):
    """幂等键缺失、过长或含非法字符。"""


class IdempotencyConflict(Exception):
    """同一幂等键被不同动作/参数复用。"""

    def __init__(self, command: Command) -> None:
        super().__init__(f"幂等键 {command.request_id} 已用于 {command.action}")
        self.command = command


def validate_request_id(request_id: str) -> str:
    """校验幂等键形态并返回归一化后的值。"""

    value = str(request_id or "").strip()
    if not value:
        raise InvalidRequestId("缺少幂等键 request_id（请用 X-Request-Id 头或 request_id 字段）")
    if len(value) > _MAX_REQUEST_ID_LEN or any(ch not in _ALLOWED_REQUEST_ID for ch in value):
        raise InvalidRequestId(
            f"幂等键只能由字母数字及 -_. 组成且不超过 {_MAX_REQUEST_ID_LEN} 个字符"
        )
    return value


class CommandLedger:
    """命令单的内存索引 + 原子落盘。"""

    def __init__(self, store: JsonStore, clock: Clock, prefix: str = PREFIX) -> None:
        self._store = store
        self._clock = clock
        self._prefix = prefix.strip("/")
        self._lock = threading.RLock()
        self._by_id: dict[str, Command] = {}
        self._requests: dict[str, str] = {}
        self._recover()

    # ---- 落盘映射 -----------------------------------------------------

    def doc_id(self, command_id: str) -> str:
        return f"{self._prefix}/{command_id}"

    def _persist(self, command: Command) -> None:
        command.updated_ts = self._clock.now()
        self._store.write(self.doc_id(command.command_id), command.as_dict())

    # ---- 登记 ---------------------------------------------------------

    def register(
        self,
        command_id: str,
        request_id: str,
        action: str,
        params: dict,
        policy: str,
        origin: str = "console",
        queue_deadline_monotonic: float | None = None,
        payload_fingerprint: Callable[[str, dict], bool] | None = None,
    ) -> tuple[Command, bool]:
        """幂等登记一条命令单。

        返回 ``(命令单, 是否首次创建)``。键已存在时做一致性检查：动作或关键参数
        不一致抛 :class:`IdempotencyConflict`；一致则回读原单并给 ``replay_count``
        加一，调用方据此把原结果/在途进度回给客户端，绝不二次执行。
        """

        request_id = validate_request_id(request_id)
        with self._lock:
            existing_id = self._requests.get(request_id)
            if existing_id is not None:
                existing = self._by_id[existing_id]
                if payload_fingerprint is not None and not payload_fingerprint(
                    existing.action, existing.params
                ):
                    raise IdempotencyConflict(existing)
                existing.replay_count += 1
                return existing, False

            command = Command(
                command_id=command_id,
                request_id=request_id,
                action=action,
                params=dict(params),
                policy=policy,
                origin=origin,
                created_ts=self._clock.now(),
                updated_ts=self._clock.now(),
                queued_until_monotonic=queue_deadline_monotonic,
            )
            payload = command.as_dict()
            created, _path = self._store.create_if_absent(self.doc_id(command_id), payload)
            if not created:
                # 并发跨进程：文档已被别人先建，回读它。
                raw = self._store.read(self.doc_id(command_id))
                command = Command.from_dict(raw)
                self._index(command)
                if action != command.action or (
                    payload_fingerprint is not None
                    and not payload_fingerprint(command.action, command.params)
                ):
                    raise IdempotencyConflict(command)
                command.replay_count += 1
                return command, False
            self._index(command)
            return command, True

    def _index(self, command: Command) -> None:
        self._by_id[command.command_id] = command
        self._requests[command.request_id] = command.command_id

    # ---- 状态更新 -----------------------------------------------------

    def update(self, command_id: str, mutate: Callable[[Command], None]) -> Command:
        """在台账锁内改一条命令单并立即落盘。"""

        with self._lock:
            command = self._by_id[command_id]
            mutate(command)
            self._persist(command)
            return command

    def get(self, command_id: str) -> Command | None:
        with self._lock:
            return self._by_id.get(command_id)

    def by_request(self, request_id: str) -> Command | None:
        with self._lock:
            command_id = self._requests.get(request_id)
            return None if command_id is None else self._by_id.get(command_id)

    def recent(self, action: str | None = None, limit: int = 50) -> list[Command]:
        """按创建顺序返回最近的命令单（旧的在前）。"""

        with self._lock:
            commands = [
                item for item in self._by_id.values() if action is None or item.action == action
            ]
        commands.sort(key=lambda item: item.created_ts)
        return commands[-limit:]

    def last_terminal(self, action: str) -> Command | None:
        """某动作最近一次走到终态的命令单，供控制台展示"上一次结果"。"""

        candidates = [
            item
            for item in self.recent(action, limit=1_000_000)
            if item.action == action and item.is_terminal()
        ]
        return candidates[-1] if candidates else None

    def queued(self) -> list[Command]:
        """返回当前排队中的命令单（按创建时间，FIFO）。"""

        with self._lock:
            queued = [item for item in self._by_id.values() if item.state == QUEUED]
        queued.sort(key=lambda item: item.created_ts)
        return queued

    def running(self) -> list[Command]:
        """返回正在执行的命令单。"""

        with self._lock:
            return [item for item in self._by_id.values() if item.state == RUNNING]

    # ---- 恢复 ---------------------------------------------------------

    def _recover(self) -> None:
        """扫描命令文档重建索引，并把非终态单改判为中断。"""

        for doc_id in self._store.list_ids():
            if not doc_id.startswith(f"{self._prefix}/"):
                continue
            raw = self._store.read(doc_id)
            if not isinstance(raw, dict):
                continue
            command = Command.from_dict(raw)
            self._index(command)
            if command.is_active():
                command.state = PARTIAL_FAILED
                command.finished_ts = self._clock.now()
                command.updated_ts = command.finished_ts
                command.error = {
                    "code": "interrupted_by_restart",
                    "message": "进程重启前命令未走到终态，按部分失败处理，需人工确认后重新提交",
                }
                command.progress.stage = command.progress.stage or "interrupted"
                self._persist(command)
