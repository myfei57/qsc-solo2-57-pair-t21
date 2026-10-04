"""控制台接口处理器。

查询类接口直接读平台快照。动作类接口统一走受理台（``platform.intake``）：
每次提交带 ``request_id``（请求体或查询串，缺省时服务端生成），同一标识只执行
一次；受理结果定死为三种——``202`` 已受理（待执行或排队等状态满足）、``200``
同一标识的重复提交（返回既有记录与当前进度）、``409`` 门控当场拒绝或标识冲突。
进度与结果用 ``GET /api/request/{request_id}`` 查询，拒绝理由、进度与审计流水
同源。参数缺失或取值非法仍当场返回 ``400``，平台联锁异常统一转成 ``409``。
"""

from __future__ import annotations

from ..app import Platform
from ..audit.timeline import build_timeline
from ..errors import PlantError
from .router import Router


def _as_float(body: dict, key: str, default: float) -> float:
    value = body.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError) as error:
        raise PlantError(f"参数 {key!r} 必须是数值", component="console") from error


def _request_id(params: dict, body: dict) -> str | None:
    """取客户端幂等标识：请求体优先，查询串次之；都没有则由受理台生成。"""

    value = body.get("request_id") or params.get("request_id")
    if value is None or str(value).strip() == "":
        return None
    return str(value)


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
        add("GET", "/api/status", lambda params, body: self._platform.status())
        add("GET", "/api/snapshot", lambda params, body: self._platform.snapshot())
        add("GET", "/api/topology", self._topology)
        add("GET", "/api/namespaces", self._namespaces)
        add("GET", "/api/zones", lambda params, body: self._platform.zones.describe())
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
        add("GET", "/api/requests", self._requests)
        add("GET", "/api/request/{request_id}", self._request)
        add("POST", "/api/weather", self._weather)
        add("POST", "/api/adjust", self._adjust)
        add("POST", "/api/startup", self._startup)
        add("POST", "/api/shutdown", self._shutdown)
        add("POST", "/api/dispatch", self._dispatch)
        add("POST", "/api/cycle", self._cycle)
        add("POST", "/api/calibrate", self._calibrate)
        add("POST", "/api/retrofit", self._retrofit)
        add("POST", "/api/secondary/cycle", self._secondary_cycle)
        add("POST", "/api/meter", self._meter)
        add("POST", "/api/settle", self._settle)
        add("POST", "/api/zone/split", self._split)
        add("POST", "/api/valve/replay", self._replay)
        add("POST", "/api/pump", self._pump)
        add("POST", "/api/disturb", self._disturb)
        add("POST", "/api/trip", self._trip)
        add("POST", "/api/reset", self._reset)
        add("POST", "/api/concurrency", self._concurrency)
        add("POST", "/api/plant/reset-state", self._reset_state)

    def routes(self) -> list[dict]:
        """返回接口清单，供自检与文档核对。"""

        return [route.as_dict() for route in self._router.routes()]

    def handle(self, method: str, path: str, query: dict, body: dict) -> tuple[int, dict]:
        """处理一次请求，返回状态码与响应体。"""

        matched = self._router.match(method, path)
        if matched is None:
            return 404, {"error": "not_found", "path": path}
        handler, params = matched
        merged = {**query, **params}
        try:
            payload = handler(merged, body)
        except PlantError as error:
            self._platform.note_error(error)
            return 409, {"error": type(error).__name__, **error.as_record()}
        except KeyError as error:
            return 404, {"error": "not_found", "message": str(error)}
        except (TypeError, ValueError) as error:
            return 400, {"error": "bad_request", "message": str(error)}
        if isinstance(payload, tuple):
            return payload
        return 200, payload

    # ---- 查询 ---------------------------------------------------------

    def _topology(self, params: dict, body: dict) -> dict:
        topology = self._platform.topology
        payload = topology.summary()
        payload["downstream"] = {stage: topology.downstream(stage) for stage in topology.order()}
        payload["verify"] = topology.verify(set())
        return payload

    def _namespaces(self, params: dict, body: dict) -> dict:
        registry = self._platform.namespaces
        payload = registry.describe()
        payload["by_kind"] = {
            kind: [device.address for device in registry.by_kind(kind)] for kind in registry.kinds()
        }
        return payload

    def _zone(self, params: dict, body: dict) -> dict:
        name = params.get("name", "")
        zone = self._platform.zones.get(name)
        return {
            "zone": zone.as_dict(),
            "stations": [
                self._platform.service.get(station).snapshot().as_dict()
                for station in self._platform.zones.stations(name)
            ],
        }

    def _stations(self, params: dict, body: dict) -> dict:
        zone = params.get("zone")
        names = self._platform.service.names()
        if zone:
            names = [name for name in names if self._platform.service.get(name).zone() == zone]
        return {
            "count": len(names),
            "stations": [self._platform.service.get(name).snapshot().as_dict() for name in names],
        }

    def _station(self, params: dict, body: dict) -> dict:
        name = params.get("name", "")
        station = self._platform.service.get(name)
        valve = self._platform.service.valve(name)
        return {
            "station": station.snapshot().as_dict(),
            "valve": valve.snapshot().as_dict(),
            "zone": self._platform.mapping.zone_of(name),
        }

    def _valves(self, params: dict, body: dict) -> dict:
        names = self._platform.service.names()
        return {
            "count": len(names),
            "valves": [self._platform.service.valve(name).snapshot().as_dict() for name in names],
        }

    def _curve(self, params: dict, body: dict) -> dict:
        payload = self._platform.curve_report()
        payload["table"] = self._platform.curve.table.points()
        return payload

    def _forecast(self, params: dict, body: dict) -> dict:
        outside = params.get("outdoor_c")
        horizon = int(params.get("steps", 5))
        return self._platform.outdoor_forecast(
            None if outside is None else float(outside), steps=horizon
        )

    def _pressure(self, params: dict, body: dict) -> dict:
        return self._platform.pressure_report()

    def _quota(self, params: dict, body: dict) -> dict:
        return self._platform.quota_report()

    def _billing(self, params: dict, body: dict) -> dict:
        quota = float(params.get("quota_mwh", self._platform.config.quota.meter_wrap_mwh / 40.0))
        return {"lines": self._platform.billing(quota)}

    def _trend(self, params: dict, body: dict) -> dict:
        signal = params.get("signal", "supply_c")
        samples = self._platform.trend.series(signal, limit=int(params.get("limit", 60)))
        filled = self._platform.trend.fill_gaps(signal, samples)
        return {
            "signal": signal,
            "period_s": self._platform.trend.period_s,
            "signals": self._platform.trend.signals(),
            "samples": [sample.as_dict() for sample in filled],
        }

    def _events(self, params: dict, body: dict) -> dict:
        topic = params.get("topic")
        return {"events": [event.as_dict() for event in self._platform.bus.history(topic)]}

    def _audit(self, params: dict, body: dict) -> dict:
        limit = int(params.get("limit", 100))
        return {"entries": self._platform.audit.entries(limit=limit)}

    def _alarms(self, params: dict, body: dict) -> dict:
        limit = int(params.get("limit", 20))
        return {
            "recent": self._platform.audit.alarms()[-limit:],
            "latch": self._platform.source.snapshot().as_dict(),
        }

    def _timeline(self, params: dict, body: dict) -> dict:
        asset = params.get("asset")
        records = self._platform.audit.records(limit=int(params.get("limit", 200)))
        return {"timeline": build_timeline(records, asset=asset)}

    def _journal(self, params: dict, body: dict) -> dict:
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

    def _store(self, params: dict, body: dict) -> dict:
        return {
            "root": self._platform.store.root,
            "documents": self._platform.store.list_ids(),
            "metrics": self._platform.metrics.summary(),
            "platform_state": self._platform.store.read("platform_state"),
        }

    def _readiness(self, params: dict, body: dict) -> dict:
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

    def _requests(self, params: dict, body: dict) -> dict:
        return {
            "requests": self._platform.intake.list_requests(
                limit=int(params.get("limit", 50)),
                action=params.get("action") or None,
                status=params.get("status") or None,
            )
        }

    def _request(self, params: dict, body: dict) -> dict:
        return {"request": self._platform.intake.get(params.get("request_id", ""))}

    # ---- 动作 ---------------------------------------------------------

    def _submit(self, action: str, params: dict, body: dict, query: dict) -> tuple[int, dict]:
        """把一次动作提交给受理台，返回 ``(状态码, 响应体)``。"""

        return self._platform.intake.submit(action, params, request_id=_request_id(query, body))

    def _weather(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit(
            "weather",
            {
                "outdoor_c": _as_float(body, "outdoor_c", 0.0),
                "station": str(body.get("station", "console")),
            },
            body,
            params,
        )

    def _adjust(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit(
            "adjust",
            {
                "temperature_c": _as_float(body, "temperature_c", self._platform.source.temperature_c()),
                "flow_t_h": _as_float(body, "flow_t_h", self._platform.network.baseline_t_h()),
                "origin": str(body.get("origin", "console")),
            },
            body,
            params,
        )

    def _startup(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit(
            "startup",
            {
                "outdoor_c": _as_float(body, "outdoor_c", 0.0),
                "demand_pct": _as_float(body, "demand_pct", 60.0),
                "station": str(body.get("station", "console")),
            },
            body,
            params,
        )

    def _shutdown(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit(
            "shutdown", {"reason": str(body.get("reason", "console-shutdown"))}, body, params
        )

    def _dispatch(self, params: dict, body: dict) -> tuple[int, dict]:
        zones = body.get("zones")
        return self._submit(
            "dispatch",
            {
                "demand_pct": _as_float(body, "demand_pct", 60.0),
                "target_c": _as_float(body, "target_c", self._platform.config.station.setpoint_c),
                "zones": None if not zones else [str(item) for item in zones],
                "cancel": bool(body.get("cancel")),
                "cancel_reason": str(body.get("cancel_reason", "console-cancel")),
            },
            body,
            params,
        )

    def _cycle(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("cycle", {"dt": _as_float(body, "dt", 1.0)}, body, params)

    def _calibrate(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit(
            "calibrate",
            {"measured_t_h": _as_float(body, "measured_t_h", self._platform.network.baseline_t_h())},
            body,
            params,
        )

    def _retrofit(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit(
            "retrofit",
            {"baseline_kpa": _as_float(body, "baseline_kpa", self._platform.pressure.baseline.value_kpa())},
            body,
            params,
        )

    def _secondary_cycle(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("secondary_cycle", {"dt": _as_float(body, "dt", 1.0)}, body, params)

    def _meter(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit(
            "meter",
            {
                "meter": str(body.get("meter", self._platform.meter_names()[0])),
                "delta_mwh": _as_float(body, "delta_mwh", 1.0),
            },
            body,
            params,
        )

    def _settle(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("settle", {}, body, params)

    def _split(self, params: dict, body: dict) -> tuple[int, dict]:
        stations = body.get("stations") or []
        return self._submit(
            "split",
            {
                "source_zone": str(body.get("source_zone", "")),
                "stations": [str(item) for item in stations],
                "new_zone": str(body.get("new_zone", "")),
            },
            body,
            params,
        )

    def _replay(self, params: dict, body: dict) -> tuple[int, dict]:
        stations = body.get("stations")
        return self._submit(
            "replay",
            {"stations": None if not stations else [str(item) for item in stations]},
            body,
            params,
        )

    def _pump(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit(
            "pump",
            {
                "pump_action": str(body.get("action", "cycle")),
                "speed_pct": _as_float(body, "speed_pct", 85.0),
                "dt": _as_float(body, "dt", 1.0),
            },
            body,
            params,
        )

    def _disturb(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit(
            "disturb", {"heat_fraction": _as_float(body, "heat_fraction", 0.5)}, body, params
        )

    def _trip(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("trip", {"reason": str(body.get("reason", "overtemperature"))}, body, params)

    def _reset(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("reset", {"operator": str(body.get("operator", "shift-lead"))}, body, params)

    def _concurrency(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("concurrency", {"rounds": int(body.get("rounds", 24))}, body, params)

    def _reset_state(self, params: dict, body: dict) -> tuple[int, dict]:
        return self._submit("reset_state", {}, body, params)
