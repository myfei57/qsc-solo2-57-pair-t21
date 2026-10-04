"""受理台：幂等、并发去重、门控拒绝与排队、进度与审计一致、部分失败与中断恢复。"""

from __future__ import annotations

import threading

from heatnet.app import build_platform
from heatnet.config import default_config
from heatnet.console.api import Api


def _audit_for(platform, request_id: str) -> list[dict]:
    """取出与某请求标识相关的全部审计记录（按时间升序）。"""

    return [
        entry
        for entry in platform.audit.tail(500)
        if entry["payload"].get("op_id") == request_id
    ]


def test_same_request_id_executes_only_once(platform, wait_terminal):
    api = Api(platform)
    body = {"meter": "st01.heat-meter", "delta_mwh": 4.0, "request_id": "m-1"}
    status, first = api.handle("POST", "/api/meter", {}, dict(body))
    assert status == 202
    assert first["duplicate"] is False
    done = wait_terminal(platform, "m-1")
    assert done["status"] == "succeeded"
    assert done["result"]["total_mwh"] == 4.0

    # 网络抖动重试：同一标识返回既有记录，不再执行。
    status, second = api.handle("POST", "/api/meter", {}, dict(body))
    assert status == 200
    assert second["duplicate"] is True
    assert second["request"]["attempts"] == 2
    assert platform.meters.total_mwh() == 4.0
    # 重复提交本身也落审计，现场能看到抖动发生了几次。
    duplicates = [e for e in _audit_for(platform, "m-1") if e["action"] == "request_duplicate"]
    assert duplicates and duplicates[-1]["payload"]["attempts"] == 2


def test_concurrent_same_id_runs_exactly_once(platform, wait_terminal):
    api = Api(platform)
    barrier = threading.Barrier(8)
    outcomes: list[tuple[int, dict]] = []
    lock = threading.Lock()

    def submit() -> None:
        barrier.wait()
        outcome = api.handle(
            "POST", "/api/meter", {}, {"meter": "st02.heat-meter", "delta_mwh": 2.0, "request_id": "c-1"}
        )
        with lock:
            outcomes.append(outcome)

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert [status for status, _ in outcomes].count(202) == 1
    assert [status for status, _ in outcomes].count(200) == 7
    done = wait_terminal(platform, "c-1")
    assert done["status"] == "succeeded"
    assert done["attempts"] == 8
    assert platform.meters.get("st02.heat-meter").total_mwh() == 2.0


def test_same_id_with_different_params_conflicts(platform, wait_terminal):
    api = Api(platform)
    status, _ = api.handle(
        "POST", "/api/meter", {}, {"meter": "st01.heat-meter", "delta_mwh": 1.0, "request_id": "x-1"}
    )
    assert status == 202
    wait_terminal(platform, "x-1")
    status, payload = api.handle(
        "POST", "/api/meter", {}, {"meter": "st01.heat-meter", "delta_mwh": 9.0, "request_id": "x-1"}
    )
    assert status == 409
    assert payload["error"] == "request_id_conflict"


def test_server_generated_id_when_client_omits(platform, wait_terminal):
    api = Api(platform)
    status, payload = api.handle("POST", "/api/meter", {}, {"meter": "st01.heat-meter", "delta_mwh": 1.0})
    assert status == 202
    request_id = payload["request"]["request_id"]
    assert request_id.startswith("req-")
    assert payload["request"]["client_key"] is False
    assert wait_terminal(platform, request_id)["status"] == "succeeded"


def test_instant_action_record_carries_steps(platform, wait_terminal):
    api = Api(platform)
    status, payload = api.handle("POST", "/api/weather", {}, {"outdoor_c": -9.0, "request_id": "w-1"})
    assert status == 202
    done = wait_terminal(platform, "w-1")
    assert done["status"] == "succeeded"
    assert [step["action"] for step in done["steps"]] == ["apply_forecast", "fire_forecast"]


def test_gate_rejects_immediately_with_reason(platform):
    api = Api(platform)
    platform.trip("overtemperature")
    status, payload = api.handle(
        "POST", "/api/adjust", {}, {"temperature_c": 90.0, "flow_t_h": 1500.0, "request_id": "g-1"}
    )
    assert status == 409
    assert payload["error"] == "interlock_rejected"
    request = payload["request"]
    assert request["status"] == "rejected"
    assert request["unmet"][0]["gate"] == "source_not_tripped"
    assert "tripped" in request["unmet"][0]["current"]
    # 拒绝理由：接口响应、受理记录、审计流水三处一致。
    stored = platform.intake.get("g-1")
    assert stored["unmet"] == request["unmet"]
    rejected = [e for e in _audit_for(platform, "g-1") if e["action"] == "request_rejected"]
    assert rejected and rejected[0]["payload"]["unmet"] == request["unmet"]
    assert rejected[0]["severity"] == "alarm"
    # 当场拒绝，设备没有被碰。
    assert platform.source.temperature_c() != 90.0


def test_reset_requires_tripped_source(platform):
    api = Api(platform)
    status, payload = api.handle("POST", "/api/reset", {}, {"request_id": "r-1"})
    assert status == 409
    assert payload["request"]["unmet"][0]["gate"] == "source_tripped"


def test_dispatch_queues_until_source_online(platform, wait_terminal):
    api = Api(platform)
    status, queued = api.handle(
        "POST", "/api/dispatch", {}, {"demand_pct": 55.0, "request_id": "d-1"}
    )
    assert status == 202
    assert queued["request"]["status"] == "queued"
    assert queued["request"]["unmet"][0]["gate"] == "source_online"

    # 投运完成后热源在线，排队的调度被自动放行执行。
    status, accepted = api.handle(
        "POST", "/api/startup", {}, {"outdoor_c": -8.0, "request_id": "s-1"}
    )
    assert status == 202
    assert wait_terminal(platform, "s-1")["status"] == "succeeded"
    dispatched = wait_terminal(platform, "d-1")
    assert dispatched["status"] == "succeeded"
    assert [item["status"] for item in dispatched["history"]] == [
        "queued",
        "accepted",
        "running",
        "succeeded",
    ]
    assert platform.network.dispatches() == 1


def test_secondary_cycle_waits_for_idle_pump(online, wait_terminal):
    api = Api(online)
    online.start_pump(80.0)
    status, queued = api.handle("POST", "/api/secondary/cycle", {}, {"request_id": "sc-1"})
    assert status == 202
    assert queued["request"]["status"] == "queued"
    assert queued["request"]["unmet"][0]["gate"] == "pump_idle"
    online.stop_pump()
    done = wait_terminal(online, "sc-1")
    assert done["status"] == "succeeded"
    assert done["result"]["pump_idle"] is True


def test_progress_steps_match_audit_trail(online, wait_terminal):
    api = Api(online)
    status, accepted = api.handle("POST", "/api/secondary/cycle", {}, {"request_id": "p-1"})
    assert status == 202
    done = wait_terminal(online, "p-1")
    assert done["status"] == "succeeded"
    assert [step["action"] for step in done["steps"]] == ["start", "settle"]
    # 进度与审计同源：审计里同一标识的步骤事件与记录里的步骤一一对应。
    steps_from_audit = [
        (entry["component"], entry["action"])
        for entry in _audit_for(online, "p-1")
        if entry["component"] not in ("intake", "audit")
    ]
    steps_from_record = [(step["component"], step["action"]) for step in done["steps"]]
    assert steps_from_audit == steps_from_record
    # 受理台自身的迁移事件也带同一标识。
    intake_events = [
        entry["action"] for entry in _audit_for(online, "p-1") if entry["component"] == "intake"
    ]
    assert intake_events == ["request_accepted", "request_started", "request_succeeded"]


def test_partial_failure_keeps_finished_steps(online, wait_terminal):
    api = Api(online)
    status, accepted = api.handle(
        "POST",
        "/api/dispatch",
        {},
        {"demand_pct": 55.0, "cancel": True, "cancel_reason": "console-cancel", "request_id": "f-1"},
    )
    assert status == 202
    failed = wait_terminal(online, "f-1")
    assert failed["status"] == "failed"
    # 部分失败：流量分配已做、逐站调节没走完，记录里看得出停在哪。
    assert failed["step_count"] > 0
    assert not any(step["action"] == "adjust" for step in failed["steps"])
    assert "cancelled:console-cancel" in failed["result"]["violations"]
    events = [e for e in _audit_for(online, "f-1") if e["action"] == "request_failed"]
    assert events and events[0]["severity"] == "warning"


def test_interrupted_records_surface_after_restart(platform, clock):
    api = Api(platform)
    status, queued = api.handle(
        "POST", "/api/dispatch", {}, {"demand_pct": 55.0, "request_id": "q-1"}
    )
    assert status == 202
    assert queued["request"]["status"] == "queued"

    # 进程重启：仍在排队的记录按中断（部分失败）收尾，跨重启能查到上一次跑到哪。
    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    try:
        record = restarted.intake.get("q-1")
        assert record["status"] == "interrupted"
        assert record["history"][-1]["status"] == "interrupted"
        interrupted = [e for e in _audit_for(restarted, "q-1") if e["action"] == "request_interrupted"]
        assert interrupted and interrupted[0]["severity"] == "warning"
    finally:
        restarted.close()


def test_terminal_record_survives_restart(platform, clock, wait_terminal):
    api = Api(platform)
    status, accepted = api.handle(
        "POST", "/api/meter", {}, {"meter": "st01.heat-meter", "delta_mwh": 3.0, "request_id": "t-1"}
    )
    assert status == 202
    assert wait_terminal(platform, "t-1")["status"] == "succeeded"

    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    try:
        record = restarted.intake.get("t-1")
        assert record["status"] == "succeeded"
        assert record["result"]["total_mwh"] == 3.0
        # 重启后同一标识再提交仍然只算重复，不会重执行（热量表是内存态，重启归零）。
        status, again = Api(restarted).handle(
            "POST", "/api/meter", {}, {"meter": "st01.heat-meter", "delta_mwh": 3.0, "request_id": "t-1"}
        )
        assert status == 200
        assert again["duplicate"] is True
        assert restarted.meters.total_mwh() == 0.0
    finally:
        restarted.close()


def test_requests_listing_and_filters(platform, wait_terminal):
    api = Api(platform)
    api.handle("POST", "/api/meter", {}, {"meter": "st01.heat-meter", "delta_mwh": 1.0, "request_id": "l-1"})
    api.handle("POST", "/api/trip", {}, {"reason": "test", "request_id": "l-2"})
    wait_terminal(platform, "l-1")
    wait_terminal(platform, "l-2")

    status, payload = api.handle("GET", "/api/requests", {}, {})
    assert status == 200
    ids = [item["request_id"] for item in payload["requests"]]
    assert ids[0] == "l-2" and ids[1] == "l-1"
    status, payload = api.handle("GET", "/api/requests", {"action": "meter"}, {})
    assert [item["request_id"] for item in payload["requests"]] == ["l-1"]
    status, payload = api.handle("GET", "/api/requests", {"status": "succeeded"}, {})
    assert {item["request_id"] for item in payload["requests"]} == {"l-1", "l-2"}
    status, payload = api.handle("GET", "/api/request/l-1", {}, {})
    assert status == 200
    assert payload["request"]["result"]["total_mwh"] == 1.0
    summary = platform.status()["intake"]
    assert summary["by_status"]["succeeded"] == 2
