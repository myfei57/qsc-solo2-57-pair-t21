"""控制台接口处理器。

查询类接口直接返回平台快照；**所有动作类 POST 都走命令中心，受理语义定死为
"先受理后执行"**：接口同步只做幂等登记和入场门禁，返回 ``202`` 与命令单
（含状态查询地址），设备动作在后台 worker 执行。同一幂等键（``X-Request-Id``
头或 body 里的 ``request_id``）只执行一次，后来者拿到同一张命令单；门禁不放行
时按动作策略当场拒绝（409，带卡在哪一步）或排队等待。

短动作与长动作没有第二种受理方式，控制台轮询 ``/api/commands/{id}`` 即可拿到
拒绝理由、实时进度与最终结果——这些字段与审计流水同源。
"""

from __future__ import annotations

from ..app import Platform
from ..audit.timeline import build_timeline
from ..command.center import Rejected
from ..errors import PlantError
from .router import Router

# POST 路径 -> 命令动作名。动作策略（门禁、reject/queue）由命令中心的声明表定。
_ACTION_PATHS: dict[str, str] = {
    "/api/weather": "weather",
    "/api/adjust": "adjust",
    "/api/startup": "startup",
    "/api/shutdown": "shutdown",
    "/api/dispatch": "dispatch",
    "/api/cycle": "cycle",
    "/api/calibrate": "calibrate",
    "/api/retrofit": "retrofit",
    "/api/secondary/cycle": "secondary_cycle",
    "/api/meter": "meter",
    "/api/settle": "settle",
    "/api/zone/split": "zone_split",
    "/api/valve/replay": "valve_replay",
    "/api/pump": "pump_dispatch",
    "/api/disturb": "disturb",
    "/api/trip": "trip",
    "/api/reset": "reset",
}


class Api:
    """把 HTTP 请求翻译成平台调用。"""

    def __init__(self, platform: Platform) -> None:
        self._platform = platform
        self._router = Router()
        self._register()

    @property
    def platform(self) -> Platform:
        return self._platform

    def _register(self) -> None:
        add = self._router.add
        add("GET", "/api/status", lambda params, body, headers: self._platform.status())
        add("GET", "/api/snapshot", lambda params, body, headers: self._platform.snapshot())
        add("GET", "/api/topology", self._topology)
        add("GET", "/api/namespaces", self._namespaces)
        add("GET", "/api/zones", lambda params, body, headers: self._platform.zones.describe())
        add("GET", "/api/zone/{name}", self._zone)
        add("GET", "/api/stations", self._stations)
        add("GET", "/api/station/{name}", self._station)
        add("GET", "/api/valves", self._valves)
        add("GET", "/api/curve", self._curve)
        add("GET", "/api/forecast", self._forecast)
        add("GET", "/api/pressure", self._pressure)
        add("GET", "/api/quota", self._quota)
        add("GET", "/api/billing", self._billing)
        add("GET", "/api/trend", self._trend)
        add("GET", "/api/events", self._events)
        add("GET", "/api/audit", self._audit)
        add("GET", "/api/alarms", self._alarms)
        add("GET", "/api/timeline", self._timeline)
        add("GET", "/api/journal", self._journal)
        add("GET", "/api/store", self._store)
        add("GET", "/api/readiness", self._readiness)
        add("GET", "/api/commands", self._command_list)
        add("GET", "/api/commands/{command_id}", self._command_detail)
        add("GET", "/api/command/last/{action}", self._command_last)
        # 动作类 POST 统一由命令中心受理，不再各自直接调平台。
        for path in _ACTION_PATHS:
            add("POST", path, self._make_submit(path))
        add("POST", "/api/concurrency", self._concurrency)
        add("POST", "/api/plant/reset-state", lambda params, body, headers: {"removed": self._platform.reset_state()})

    def routes(self) -> list[dict]:
        """返回接口清单，供自检与文档核对。"""

        return [route.as_dict() for route in self._router.routes()]

    def handle(
        self,
        method: str,
        path: str,
        query: dict,
        body: dict,
        headers: dict | None = None,
    ) -> tuple[int, dict]:
        """处理一次请求，返回状态码与响应体。"""

        matched = self._router.match(method, path)
        if matched is None:
            return 404, {"error": "not_found", "path": path}
        handler, params = matched
        merged = {**query, **params}
        try:
            payload = handler(merged, body, headers or {})
        except Rejected as error:
            return error.status, {
                "error": error.code,
                "message": error.message,
                "component": "console",
            }
        except PlantError as error:
            self._platform.note_error(error)
            return 409, {"error": type(error).__name__, **error.as_record()}
        except KeyError as error:
            return 404, {"error": "not_found", "message": str(error)}
        except (TypeError, ValueError) as error:
            return 400, {"error": "bad_request", "message": str(error)}
        # 受理处理器把 200/202/409 放在内部字段里，取走后不暴露给客户端。
        status = 200
        if isinstance(payload, dict) and "__http_status__" in payload:
            status = int(payload.pop("__http_status__"))
        return status, payload

    # ---- 命令受理与查询 ----------------------------------------------

    def _make_submit(self, path: str):
        """生成绑定了具体路径的受理处理器。"""

        def submit(params: dict, body: dict, headers: dict) -> dict:
            action = _ACTION_PATHS.get(path, "")
            request_id = str(headers.get("x-request-id") or body.get("request_id") or "").strip()
            origin = str(body.get("origin") or body.get("station") or "console")

            # /api/pump 是个复用入口：按 action 字段拆成两个独立动作，互不复用。
            if path == "/api/pump":
                pump_action = str(body.get("action", "cycle"))
                if pump_action == "start":
                    action = "pump_start"
                elif pump_action == "stop":
                    action = "pump_stop"
                else:
                    action = "secondary_cycle"

            command, replayed, http_status = self._platform.commands.submit(
                action, request_id, dict(body), origin=origin
            )
            payload = self._platform.commands.describe(command)
            payload["replayed"] = replayed
            # 受理结果用 __http_status__ 告诉 HTTP 层语义（200/202/409）。
            payload["__http_status__"] = http_status
            return payload

        return submit

    def _command_detail(self, params: dict, body: dict, headers: dict) -> dict:
        command = self._platform.commands.get(params["command_id"])
        if command is None:
            raise KeyError(f"命令不存在: {params['command_id']}")
        return self._platform.commands.describe(command)

    def _command_last(self, params: dict, body: dict, headers: dict) -> dict:
        action = params["action"]
        command = self._platform.commands.last_terminal(action)
        if command is None:
            return {"action": action, "last": None}
        return {"action": action, "last": self._platform.commands.describe(command)}

    def _command_list(self, params: dict, body: dict, headers: dict) -> dict:
        action = params.get("action")
        limit = int(params.get("limit", 50))
        commands = self._platform.commands.recent(action=action, limit=limit)
        return {
            "count": len(commands),
            "commands": [self._platform.commands.describe(item) for item in commands],
        }

    # ---- 查询 ---------------------------------------------------------

    def _topology(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        topology = self._platform.topology
        payload = topology.summary()
        payload["downstream"] = {stage: topology.downstream(stage) for stage in topology.order()}
        payload["verify"] = topology.verify(set())
        return payload

    def _namespaces(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        registry = self._platform.namespaces
        payload = registry.describe()
        payload["by_kind"] = {
            kind: [device.address for device in registry.by_kind(kind)] for kind in registry.kinds()
        }
        return payload

    def _zone(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        name = params.get("name", "")
        zone = self._platform.zones.get(name)
        return {
            "zone": zone.as_dict(),
            "stations": [
                self._platform.service.get(station).snapshot().as_dict()
                for station in self._platform.zones.stations(name)
            ],
        }

    def _stations(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        zone = params.get("zone")
        names = self._platform.service.names()
        if zone:
            names = [name for name in names if self._platform.service.get(name).zone() == zone]
        return {
            "count": len(names),
            "stations": [self._platform.service.get(name).snapshot().as_dict() for name in names],
        }

    def _station(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        name = params.get("name", "")
        station = self._platform.service.get(name)
        valve = self._platform.service.valve(name)
        return {
            "station": station.snapshot().as_dict(),
            "valve": valve.snapshot().as_dict(),
            "zone": self._platform.mapping.zone_of(name),
        }

    def _valves(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        names = self._platform.service.names()
        return {
            "count": len(names),
            "valves": [self._platform.service.valve(name).snapshot().as_dict() for name in names],
        }

    def _curve(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        payload = self._platform.curve_report()
        payload["table"] = self._platform.curve.table.points()
        return payload

    def _forecast(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        outside = params.get("outdoor_c")
        horizon = int(params.get("steps", 5))
        return self._platform.outdoor_forecast(
            None if outside is None else float(outside), steps=horizon
        )

    def _pressure(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        return self._platform.pressure_report()

    def _quota(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        return self._platform.quota_report()

    def _billing(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        quota = float(params.get("quota_mwh", self._platform.config.quota.meter_wrap_mwh / 40.0))
        return {"lines": self._platform.billing(quota)}

    def _trend(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        signal = params.get("signal", "supply_c")
        samples = self._platform.trend.series(signal, limit=int(params.get("limit", 60)))
        filled = self._platform.trend.fill_gaps(signal, samples)
        return {
            "signal": signal,
            "period_s": self._platform.trend.period_s,
            "signals": self._platform.trend.signals(),
            "samples": [sample.as_dict() for sample in filled],
        }

    def _events(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        topic = params.get("topic")
        return {"events": [event.as_dict() for event in self._platform.bus.history(topic)]}

    def _audit(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        limit = int(params.get("limit", 100))
        return {"entries": self._platform.audit.entries(limit=limit)}

    def _alarms(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        limit = int(params.get("limit", 20))
        return {
            "recent": self._platform.audit.alarms()[-limit:],
            "latch": self._platform.source.snapshot().as_dict(),
        }

    def _timeline(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        asset = params.get("asset")
        records = self._platform.audit.records(limit=int(params.get("limit", 200)))
        return {"timeline": build_timeline(records, asset=asset)}

    def _journal(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        journal = self._platform.audit.journal
        payload: dict = {
            "topic": journal.topic,
            "count": journal.count(),
            "active_segment": journal.active_segment(),
            "segments": journal.segments(),
        }
        segment = params.get("segment")
        if segment is not None:
            payload["records"] = self._platform.audit.replay_segment(int(segment))
        return payload

    def _store(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        return {
            "root": self._platform.store.root,
            "documents": self._platform.store.list_ids(),
            "metrics": self._platform.metrics.summary(),
            "platform_state": self._platform.store.read("platform_state"),
        }

    def _readiness(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        platform = self._platform
        checks = {
            "curve_fresh": not platform.curve.is_stale(platform.clock.now()),
            "source_online": platform.source.state() == "online",
            "flow_above_threshold": not platform.network.low_flow(),
            "pressure_in_band": not platform.secondary.needs_makeup()
            and not platform.secondary.overpressure(),
        }
        info = {
            "source_state": platform.source.state(),
            "supply_c": round(platform.source.temperature_c(), 2),
            "flow_t_h": round(platform.network.flow_t_h(), 2),
            "last_audit_ts": platform.audit.tail(1)[-1]["ts"] if platform.audit.tail(1) else None,
        }
        return {"ready": all(checks.values()), "checks": checks, "info": info}

    # ---- 非动作类管理接口 --------------------------------------------

    def _concurrency(self, params: dict, body: dict, headers: dict | None = None) -> dict:
        return self._platform.concurrency_probe(rounds=int(body.get("rounds", 24)))
