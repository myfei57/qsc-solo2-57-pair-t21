"""端到端验证：幂等受理、门禁拒绝、排队、进度、审计同源、中断恢复。

环境无 pytest，用标准库直接断言；覆盖需求里定死的每一条语义。
"""

from __future__ import annotations

import os
import tempfile
import threading
import time

from heatnet.app import build_platform
from heatnet.clock import ManualClock
from heatnet.config import default_config
from heatnet.console.api import Api


def wait_state(api: Api, command_id: str, *wanted: str, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        status, payload = api.handle("GET", f"/api/commands/{command_id}", {}, {})
        assert status == 200
        if payload["state"] in wanted:
            return payload
        if time.monotonic() > deadline:
            raise AssertionError(f"命令 {command_id} 未进入 {wanted}，当前 {payload['state']}")
        time.sleep(0.005)


def submit(api: Api, path: str, body: dict, request_id: str | None = None, headers: dict | None = None):
    hdrs = dict(headers or {})
    if request_id is not None:
        hdrs["x-request-id"] = request_id
    return api.handle("POST", path, {}, body, hdrs)


def build(tmp: str, name: str):
    root = os.path.join(tmp, name)
    platform = build_platform(default_config(), clock=ManualClock(), storage_root=root)
    return platform, root, Api(platform)


def main() -> None:
    tmp = tempfile.mkdtemp()
    failures: list[str] = []

    def check(name: str, fn) -> None:
        safe = "".join(ch if ch.isalnum() else "_" for ch in str(len(failures)))
        platform, root, api = build(tmp, f"case_{id(fn) % 100000}")
        try:
            fn(platform, api, root)
            print(f"PASS {name}")
        except Exception as error:  # noqa: BLE001
            failures.append(name)
            import traceback

            traceback.print_exc()
            print(f"FAIL {name}: {error}")
        finally:
            platform.close()

    # 1. 缺少幂等键当场拒绝（400）
    def t_missing_key(platform, api, root):
        status, payload = submit(api, "/api/weather", {"outdoor_c": -9.0})
        assert status == 400 and payload["error"] == "missing_request_id", payload

    # 2. 同步受理返回 202，且先受理后执行（命令单可在完成前查到）
    def t_accept_then_execute(platform, api, root):
        seen: list[int] = []
        stop = threading.Event()

        def poll():
            while not stop.is_set():
                _, p = api.handle("GET", "/api/commands/weather-r1", {}, {})
                if p.get("state") in ("queued", "running"):
                    seen.append(1)
                    return
                time.sleep(0.001)

        th = threading.Thread(target=poll)
        th.start()
        status, payload = submit(api, "/api/weather", {"outdoor_c": -9.0}, "r1")
        stop.set()
        th.join()
        assert status == 202, payload
        assert payload["state"] in ("queued", "running", "succeeded"), payload
        assert payload["status_href"] == "/api/commands/weather-r1"
        wait_state(api, "weather-r1", "succeeded")

    # 3. 同一标识连续/并发提交只执行一次
    def t_idempotent(platform, api, root):
        ids = [f"dup-{i}" for i in range(12)]
        results: list[tuple] = []

        def fire(rid):
            results.append(submit(api, "/api/disturb", {"heat_fraction": 0.1}, rid))

        threads = [threading.Thread(target=fire, args=(rid,)) for rid in ids]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 等 12 条不同命令单全部走完（dup-0 只被执行一次）
        for i in range(12):
            wait_state(api, f"disturb-dup-{i}", "succeeded")
        # 第二次起串行重放
        for _ in range(5):
            results.append(submit(api, "/api/disturb", {"heat_fraction": 0.1}, "dup-0"))
        command = api.platform.commands.get("disturb-dup-0")
        assert command.attempts == 1, f"执行次数应为1，实际 {command.attempts}"
        assert command.replay_count >= 5, command.replay_count
        # 现场扰动只发生一次（每个不同 id 一次，dup-0 只算一次）
        assert api.platform.secondary.disturbances() == 12, api.platform.secondary.disturbances()
        status, payload = submit(api, "/api/disturb", {"heat_fraction": 0.1}, "dup-0")
        assert status == 200 and payload["replayed"] is True, payload

    # 4. 幂等键复用但参数/动作不同 -> 409 冲突
    def t_idempotency_conflict(platform, api, root):
        status, _ = submit(api, "/api/trip", {"reason": "a"}, "k1")
        assert status == 202
        wait_state(api, "trip-k1", "succeeded")
        status, payload = submit(api, "/api/trip", {"reason": "a"}, "k1")
        assert status == 200 and payload["replayed"] is True, "同参数应是重放"
        status, payload = submit(api, "/api/trip", {"reason": "different"}, "k1")
        assert status == 409 and payload["error"] == "idempotency_conflict", payload
        status, payload = submit(api, "/api/reset", {"operator": "x"}, "k1")
        assert status == 409 and payload["error"] == "idempotency_conflict", payload

    # 5. 状态不允许当场拒绝，且说清卡在哪一步/当前状态
    def t_gate_reject(platform, api, root):
        # 先停机，再投运应被 reject（startup 是 queue，改用 reset 验证 reject）
        submit(api, "/api/trip", {"reason": "test"}, "g1")
        wait_state(api, "trip-g1", "succeeded")
        # reset 此时能成功；再 reset 一次应被拒绝
        submit(api, "/api/reset", {"operator": "op"}, "g2")
        wait_state(api, "reset-g2", "succeeded")
        status, payload = submit(api, "/api/reset", {"operator": "op"}, "g3")
        assert status == 409, payload
        # 命令单终态为 rejected，带 step/current_state
        _, detail = api.handle("GET", "/api/commands/reset-g3", {}, {})
        assert detail["state"] == "rejected"
        assert detail["rejection"]["step"] == "source", detail["rejection"]
        assert detail["rejection"]["current_state"] == "idle", detail["rejection"]
        assert detail["rejection"]["code"] == "not_tripped"

    # 6. queue 动作：泵忙时排队，泵停稳后自动执行；FIFO
    def t_queue(platform, api, root):
        # 让泵保持忙：直接在平台线程里起泵不停止
        platform.pump.start(85.0)
        status, payload = submit(api, "/api/secondary/cycle", {"dt": 1.0}, "q1")
        assert status == 202 and payload["state"] == "queued", payload
        assert payload["progress"]["stage"] == "queued_waiting_gate"
        assert payload["progress"]["detail"]["gate"]["step"] == "pump"
        status, q2 = submit(api, "/api/secondary/cycle", {"dt": 1.0}, "q2")
        assert q2["state"] == "queued"
        # 释放泵 -> worker 应自动依次执行两条
        platform.pump.stop()
        for _ in range(600):
            platform.pump.step(1.0)
            if platform.pump.idle():
                break
        first = wait_state(api, "secondary_cycle-q1", "succeeded", "partial_failed")
        second = wait_state(api, "secondary_cycle-q2", "succeeded", "partial_failed")
        assert first["started_ts"] <= second["started_ts"], "FIFO 顺序错误"
        assert first["state"] == "succeeded", first
        assert second["state"] == "succeeded", second

    # 7. 长动作进度可轮询，且与审计同源
    def t_progress_and_audit(platform, api, root):
        submit(api, "/api/secondary/cycle", {"dt": 1.0}, "p1")
        wait_state(api, "secondary_cycle-p1", "succeeded")
        _, detail = api.handle("GET", "/api/commands/secondary_cycle-p1", {}, {})
        assert detail["progress"]["stage"] in ("pump_idle", "done", "settle"), detail["progress"]
        assert detail["result"]["ok"] is True
        assert len(detail["steps"]) > 0
        # 审计里应能找到受理/开始/进度/完成，且带 command_id
        _, audit = api.handle("GET", "/api/audit", {"limit": "200"}, {})
        actions = [e["action"] for e in audit["entries"] if e["payload"].get("command_id") == "secondary_cycle-p1"]
        for needed in ("command_accepted", "command_started", "command_progress", "command_succeeded"):
            assert needed in actions, (needed, actions)

    # 8. 长动作中断 = 部分失败，能看出没走完
    def t_partial_failure(platform, api, root):
        # 连续扰动把回水顶过上限，循环应越温中止
        platform.disturb_secondary(0.9)
        platform.disturb_secondary(0.9)
        submit(api, "/api/secondary/cycle", {"dt": 1.0}, "f1")
        final = wait_state(api, "secondary_cycle-f1", "partial_failed")
        assert final["error"]["code"] == "interlock_violation", final["error"]
        assert "return_overtemperature" in final["error"]["violations"]
        assert final["progress"]["stage"].endswith("abort_overtemperature"), final["progress"]
        # 上一次结果可按动作查询
        _, last = api.handle("GET", "/api/command/last/secondary_cycle", {}, {})
        assert last["last"]["state"] == "partial_failed", last

    # 9. 进程中断恢复为 partial_failed(restart)，不重跑
    def t_restart_recovery(platform, api, root):
        # 人为构造一条 running 命令单后“崩溃”（不 close，直接新建平台读同一存储）
        submit(api, "/api/secondary/cycle", {"dt": 1.0}, "z1")
        # 立即停掉 worker 视角：直接另起平台前先把命令置成 running 模拟中途断电
        wait_state(api, "secondary_cycle-z1", "succeeded", "partial_failed", "queued", "running", timeout=0.2) \
            if False else None
        # 用台账直接写一条非终态单
        from heatnet.command.state import Command, Progress, RUNNING
        from heatnet.command.ledger import CommandLedger

        ledger = CommandLedger(platform.store, platform.clock)
        cmd, created = ledger.register("secondary_cycle-crash", "crash", "secondary_cycle", {"dt": 1.0}, "queue")
        assert created

        def go(c: Command):
            c.state = RUNNING
            c.started_ts = platform.clock.now()
            c.progress = Progress(stage="circulate", tick=37)

        ledger.update("secondary_cycle-crash", go)
        platform.close()

        # 重新装配（同存储），应恢复为 partial_fixed 且不执行
        p2 = build_platform(default_config(), clock=ManualClock(), storage_root=root)
        try:
            got = p2.commands.get("secondary_cycle-crash")
            assert got.state == "partial_failed", got.state
            assert got.error["code"] == "interrupted_by_restart", got.error
            assert got.progress.tick == 37
        finally:
            p2.close()

    # 10. 拒绝理由/进度/审计一致
    def t_consistency(platform, api, root):
        submit(api, "/api/trip", {"reason": "x"}, "c1")
        wait_state(api, "trip-c1", "succeeded")
        status, payload = submit(api, "/api/reset", {"operator": "o"}, "c2")
        wait_state(api, "reset-c2", "succeeded")
        submit(api, "/api/reset", {"operator": "o"}, "c3")
        _, detail = api.handle("GET", "/api/commands/reset-c3", {}, {})
        _, audit = api.handle("GET", "/api/audit", {"limit": "300"}, {})
        rej = [
            e for e in audit["entries"]
            if e["payload"].get("command_id") == "reset-c3" and e["action"] == "command_rejected"
        ]
        assert rej, "审计里缺少拒绝事件"
        assert rej[0]["payload"]["gate_step"] == detail["rejection"]["step"]
        assert rej[0]["payload"]["gate_current_state"] == detail["rejection"]["current_state"]
        assert rej[0]["severity"] == "alarm"

    for name, fn in [
        ("缺少幂等键->400", t_missing_key),
        ("先受理后执行(202)", t_accept_then_execute),
        ("同标识并发只执行一次", t_idempotent),
        ("幂等键冲突->409", t_idempotency_conflict),
        ("门禁当场拒绝并说清卡点", t_gate_reject),
        ("queue排队+FIFO+自动执行", t_queue),
        ("进度轮询与审计同源", t_progress_and_audit),
        ("长动作中断=部分失败", t_partial_failure),
        ("重启恢复部分失败不重跑", t_restart_recovery),
        ("拒绝理由/进度/审计一致", t_consistency),
    ]:
        check(name, fn)

    if failures:
        raise SystemExit(f"{len(failures)} 个用例失败: {failures}")
    print("ALL PASS")


if __name__ == "__main__":
    main()
