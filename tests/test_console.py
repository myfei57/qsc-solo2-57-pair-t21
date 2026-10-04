"""控制台接口与 HTTP 服务。

动作类 POST 的契约已改为"先受理后执行"：返回 202 与命令单，执行结果靠
``/api/commands/{id}`` 轮询。本文件按新契约验证受理、幂等与查询。
"""

from __future__ import annotations

import threading
import time

from heatnet.console.api import Api
from heatnet.console.server import ConsoleServer, probe_endpoints


def submit(api: Api, path: str, body: dict, request_id: str, headers: dict | None = None):
    hdrs = {"x-request-id": request_id, **(headers or {})}
    return api.handle("POST", path, {}, body, hdrs)


def wait_for(api: Api, command_id: str, *states: str, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        status, payload = api.handle("GET", f"/api/commands/{command_id}", {}, {})
        assert status == 200
        if payload["state"] in states:
            return payload
        if time.monotonic() > deadline:
            raise AssertionError(f"{command_id} 未进入 {states}，当前 {payload['state']}")
        time.sleep(0.005)


def test_api_returns_status_and_routes(platform):
    api = Api(platform)
    status, payload = api.handle("GET", "/api/status", {}, {})
    assert status == 200
    assert payload["platform"] == platform.config.name
    paths = [route["path"] for route in api.routes()]
    assert "/api/dispatch" in paths
    assert "/api/station/{name}" in paths
    assert "/api/zone/{name}" in paths
    assert "/api/commands/{command_id}" in paths


def test_api_unknown_path_and_station_are_404(platform):
    api = Api(platform)
    status, payload = api.handle("GET", "/api/does-not-exist", {}, {})
    assert status == 404
    assert payload["error"] == "not_found"
    status, payload = api.handle("GET", "/api/station/nope", {}, {})
    assert status == 404
    assert payload["error"] == "not_found"


def test_api_station_and_zone_details(platform):
    api = Api(platform)
    status, payload = api.handle("GET", "/api/station/st01", {}, {})
    assert status == 200
    assert payload["station"]["name"] == "st01"
    assert payload["zone"] == "north"
    status, zone = api.handle("GET", "/api/zone/north", {}, {})
    assert status == 200
    assert [item["name"] for item in zone["stations"]] == ["st01", "st02", "st03"]


def test_api_missing_request_id_is_400(platform):
    api = Api(platform)
    status, payload = api.handle("POST", "/api/weather", {}, {"outdoor_c": -9.0})
    assert status == 400
    assert payload["error"] == "missing_request_id"
    # 同步非动作接口的非法参数仍是当场 400。
    status, payload = api.handle("POST", "/api/concurrency", {}, {"rounds": 0})
    assert status == 400
    assert payload["error"] == "bad_request"


def test_api_action_accepted_async_and_succeeds(platform):
    api = Api(platform)
    status, accepted = submit(api, "/api/weather", {"outdoor_c": -9.0}, "w1")
    assert status == 202
    assert accepted["command_id"] == "weather-w1"
    assert accepted["status_href"] == "/api/commands/weather-w1"
    done = wait_for(api, "weather-w1", "succeeded")
    assert done["result"]["ok"] is True
    status, adjusted = submit(api, "/api/adjust", {"temperature_c": 88.0, "flow_t_h": 1500.0}, "a1")
    assert status == 202
    wait_for(api, "adjust-a1", "succeeded")
    status, ready = api.handle("GET", "/api/readiness", {}, {})
    assert status == 200
    assert ready["checks"]["curve_fresh"] is True


def test_api_idempotent_replay_returns_same_command(platform):
    api = Api(platform)
    status, first = submit(api, "/api/disturb", {"heat_fraction": 0.2}, "d1")
    assert status == 202
    wait_for(api, "disturb-d1", "succeeded")
    status, replay = submit(api, "/api/disturb", {"heat_fraction": 0.2}, "d1")
    assert status == 200
    assert replay["replayed"] is True
    assert replay["command_id"] == "disturb-d1"
    command = platform.commands.get("disturb-d1")
    assert command.attempts == 1
    assert platform.secondary.disturbances() == 1


def test_api_idempotency_conflict_is_409(platform):
    api = Api(platform)
    submit(api, "/api/trip", {"reason": "a"}, "k1")
    wait_for(api, "trip-k1", "succeeded")
    # 同键不同参数 -> 冲突
    status, payload = submit(api, "/api/trip", {"reason": "b"}, "k1")
    assert status == 409
    assert payload["error"] == "idempotency_conflict"
    # 同键不同动作 -> 冲突
    status, payload = submit(api, "/api/reset", {"operator": "x"}, "k1")
    assert status == 409
    assert payload["error"] == "idempotency_conflict"


def test_api_gate_rejection_says_where_it_blocks(platform):
    api = Api(platform)
    submit(api, "/api/trip", {"reason": "x"}, "g1")
    wait_for(api, "trip-g1", "succeeded")
    submit(api, "/api/reset", {"operator": "op"}, "g2")
    wait_for(api, "reset-g2", "succeeded")
    # 复位只能在 tripped 态做；idle 态当场拒绝，且说清卡点与当前状态。
    status, rejected = submit(api, "/api/reset", {"operator": "op"}, "g3")
    assert status == 409
    assert rejected["state"] == "rejected"
    assert rejected["rejection"]["step"] == "source"
    assert rejected["rejection"]["current_state"] == "idle"
    assert rejected["rejection"]["code"] == "not_tripped"


def test_api_forecast_curve_and_reports(platform):
    api = Api(platform)
    status, curve = api.handle("GET", "/api/curve", {}, {})
    assert status == 200
    assert len(curve["table"]) == 3
    status, forecast = api.handle("GET", "/api/forecast", {"outdoor_c": "-10", "steps": "3"}, {})
    assert status == 200
    assert forecast["count"] == 3
    status, pressure = api.handle("GET", "/api/pressure", {}, {})
    assert status == 200
    assert pressure["baseline_kpa"] > 0.0
    status, quota = api.handle("GET", "/api/quota", {}, {})
    assert status == 200
    assert quota["accumulator"]["count"] == 6


def test_api_timeline_journal_and_store(platform):
    api = Api(platform)
    platform.start()
    platform.push_weather(-8.0)
    status, timeline = api.handle("GET", "/api/timeline", {"asset": "st01"}, {})
    assert status == 200
    assert timeline["timeline"] == []
    status, journal = api.handle("GET", "/api/journal", {}, {})
    assert status == 200
    assert journal["count"] > 0
    status, store = api.handle("GET", "/api/store", {}, {})
    assert status == 200
    assert isinstance(store["documents"], list)


def test_api_actions_cover_queue_and_meter(platform):
    api = Api(platform)
    status, queued = submit(api, "/api/dispatch", {"demand_pct": 55.0, "zones": ["north"]}, "q1")
    assert status == 202
    assert wait_for(api, "dispatch-q1", "succeeded")["result"]["ok"] is True
    status, meter = submit(api, "/api/meter", {"meter": "st01.heat-meter", "delta_mwh": 4.0}, "m1")
    assert wait_for(api, "meter-m1", "succeeded")["result"]["total_mwh"] > 0.0
    status, settled = submit(api, "/api/settle", {}, "s1")
    assert wait_for(api, "settle-s1", "succeeded")["result"]["period"] == 1
    status, pump = submit(api, "/api/pump", {"action": "start", "speed_pct": 70.0}, "pu1")
    assert wait_for(api, "pump_start-pu1", "succeeded")["result"]["running"] is True
    status, stopped = submit(api, "/api/pump", {"action": "stop"}, "pu2")
    assert wait_for(api, "pump_stop-pu2", "succeeded")["result"]["running"] is False
    status, disturbed = submit(api, "/api/disturb", {"heat_fraction": 0.2}, "d2")
    assert wait_for(api, "disturb-d2", "succeeded")["result"]["disturbances"] == 1


def test_api_concurrency_trip_and_reset(platform):
    api = Api(platform)
    status, concurrency = api.handle("POST", "/api/concurrency", {}, {"rounds": 12})
    assert status == 200
    assert concurrency["ok"] is True
    status, trip = submit(api, "/api/trip", {"reason": "console"}, "t1")
    assert wait_for(api, "trip-t1", "succeeded")["result"]["state"] == "tripped"
    status, reset = submit(api, "/api/reset", {"operator": "console"}, "r1")
    assert wait_for(api, "reset-r1", "succeeded")["result"]["state"] == "idle"


def test_api_command_last_terminal(platform):
    api = Api(platform)
    submit(api, "/api/disturb", {"heat_fraction": 0.2}, "l1")
    wait_for(api, "disturb-l1", "succeeded")
    status, payload = api.handle("GET", "/api/command/last/disturb", {}, {})
    assert status == 200
    assert payload["last"]["command_id"] == "disturb-l1"
    assert payload["last"]["state"] == "succeeded"


def test_api_concurrent_same_request_id_runs_once(platform):
    api = Api(platform)
    barrier = threading.Barrier(8)
    outcomes: list[tuple] = []

    def fire() -> None:
        barrier.wait()
        outcomes.append(submit(api, "/api/disturb", {"heat_fraction": 0.1}, "same"))

    threads = [threading.Thread(target=fire) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    wait_for(api, "disturb-same", "succeeded")
    assert platform.commands.get("disturb-same").attempts == 1
    assert platform.secondary.disturbances() == 1
    assert sum(1 for _, payload in outcomes if payload.get("replayed")) == 7


def test_server_serves_api_over_http(platform):
    platform.start()
    platform.push_weather(-8.0)
    server = ConsoleServer(platform, host="127.0.0.1", port=0)
    base_url = server.start()
    try:
        probe = probe_endpoints(
            base_url,
            ["/api/status", "/api/readiness", "/api/topology", "/api/stations", "/api/nope"],
        )
    finally:
        server.stop()
    assert probe["/api/status"]["ok"] is True
    assert probe["/api/readiness"]["ok"] is True
    assert probe["/api/topology"]["ok"] is True
    assert probe["/api/stations"]["ok"] is True
    assert probe["/api/nope"]["ok"] is False
    assert server.port == 0
