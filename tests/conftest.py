"""测试夹具：所有用例都从干净存储和可推进时钟起步。"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from heatnet.app import Platform, build_platform  # noqa: E402
from heatnet.clock import ManualClock  # noqa: E402
from heatnet.config import default_config  # noqa: E402
from heatnet.intake.record import TERMINAL  # noqa: E402


@pytest.fixture()
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture()
def platform(tmp_path, clock) -> Platform:
    instance = build_platform(default_config(), clock=clock, storage_root=str(tmp_path / "state"))
    yield instance
    instance.close()


@pytest.fixture()
def online(platform) -> Platform:
    """已下发气象、已投运、已带负荷的平台。"""

    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    platform.dispatch(60.0, 60.0)
    return platform


@pytest.fixture()
def wait_terminal():
    """轮询受理台直到请求到达终态，返回终态记录。"""

    def _wait(platform: Platform, request_id: str, timeout: float = 10.0) -> dict:
        deadline = time.monotonic() + timeout
        record = platform.intake.get(request_id)
        while record["status"] not in TERMINAL and time.monotonic() < deadline:
            time.sleep(0.01)
            record = platform.intake.get(request_id)
        if record["status"] not in TERMINAL:
            raise AssertionError(f"请求 {request_id} 在 {timeout}s 内未到达终态: {record['status']}")
        return record

    return _wait
