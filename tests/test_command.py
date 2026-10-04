"""命令中心：排队、长动作进度、部分失败、中断恢复与审计同源。"""

from __future__ import annotations

import time

from heatnet.app import build_platform
from heatnet.clock import ManualClock
from heatnet.command.state import PARTIAL_FAILED, QUEUED, RUNNING, SUCCEEDED, Command, Progress
from heatnet.config import default_config
from heatnet.console.api import Api
from heatnet.command.ledger import CommandLedger


def submit(api, path, body, request_id):
    return api.handle("POST", path, {}, body, {"x-request-id": request_id})


def wait_for(api, command_id, *states, timeout=5.0):
    deadline = time.monotonic() + timeout
    while True:
        status, payload = api.handle("GET", f"/api/commands/{command_id}", {}, {})
        assert status == 200
        if payload["state"] in states:
            return payload
        assert time.monotonic() < deadline, f"{command_id} 卡在 {payload['state']}"
        time.sleep(0.005)


def test_secondary_cycle_queues_when_pump_busy_then_runs(platform):
    api = Api(platform)
    platform.pump.start(85.0)  # 泵忙
    status, first = submit(api, "/api/secondary/cycle", {"dt": 1.0}, "q1")
    assert status == 202
    assert first["state"] == QUEUED
    assert first["progress"]["detail"]["gate"]["step"] == "pump"

    status, second = submit(api, "/api/secondary/cycle", {"dt": 1.0}, "q2")
    assert second["state"] == QUEUED

    platform.pump.stop()
    for _ in range(600):
        platform.pump.step(1.0)
        if platform.pump.idle():
            break

    done1 = wait_for(api, "secondary_cycle-q1", SUCCEEDED)
    done2 = wait_for(api, "secondary_cycle-q2", SUCCEEDED)
    assert done1["started_ts"] <= done2["started_ts"]  # FIFO
    assert done1["result"]["pump_idle"] is True


def test_progress_is_visible_and_audited(platform):
    api = Api(platform)
    submit(api, "/api/secondary/cycle", {"dt": 1.0}, "p1")
    done = wait_for(api, "secondary_cycle-p1", SUCCEEDED)
    assert done["progress"]["stage"] == "pump_idle"
    assert done["result"]["ok"] is True
    _, audit = api.handle("GET", "/api/audit", {"limit": "300"}, {})
    actions = {
        e["action"]
        for e in audit["entries"]
        if e["payload"].get("command_id") == "secondary_cycle-p1"
    }
    assert {"command_accepted", "command_started", "command_progress", "command_succeeded"} <= actions


def test_overheated_cycle_is_partial_failure(platform):
    api = Api(platform)
    platform.disturb_secondary(0.9)
    platform.disturb_secondary(0.9)
    submit(api, "/api/secondary/cycle", {"dt": 1.0}, "f1")
    done = wait_for(api, "secondary_cycle-f1", PARTIAL_FAILED)
    assert "return_overtemperature" in done["error"]["violations"]
    assert done["progress"]["stage"] == "abort_overtemperature"
    status, last = api.handle("GET", "/api/command/last/secondary_cycle", {}, {})
    assert status == 200
    assert last["last"]["state"] == PARTIAL_FAILED


def test_startup_long_action_reports_stages(platform):
    api = Api(platform)
    platform.push_weather(-8.0)
    submit(api, "/api/startup", {"outdoor_c": -8.0, "demand_pct": 60.0}, "u1")
    done = wait_for(api, "startup-u1", SUCCEEDED, PARTIAL_FAILED)
    # 投运要走完 weather/quality/quantity/makeup/cycle 五段。
    assert done["result"]["stage"] == "done"
    assert done["progress"]["stage"] in ("done", "cycle.done")
    assert len(done["steps"]) > 5


def test_recovered_nonterminal_command_marked_interrupted(tmp_path):
    root = str(tmp_path / "state")
    clock = ManualClock()
    platform = build_platform(default_config(), clock=clock, storage_root=root)
    ledger = CommandLedger(platform.store, clock)
    command, created = ledger.register("secondary_cycle-crash", "crash", "secondary_cycle", {"dt": 1.0}, "queue")
    assert created

    def to_running(c: Command) -> None:
        c.state = RUNNING
        c.started_ts = clock.now()
        c.progress = Progress(stage="circulate", tick=37)

    ledger.update("secondary_cycle-crash", to_running)
    platform.close()

    rebuilt = build_platform(default_config(), clock=clock, storage_root=root)
    try:
        recovered = rebuilt.commands.get("secondary_cycle-crash")
        assert recovered.state == PARTIAL_FAILED
        assert recovered.error["code"] == "interrupted_by_restart"
        assert recovered.progress.tick == 37
    finally:
        rebuilt.close()
