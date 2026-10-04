"""极简 pytest 替身：实现 fixture 标记并运行 test_*.py 里的 test_ 函数。

仅用于本机没有 pytest 的环境做等价验证；不追求覆盖 pytest 全部能力。
"""

from __future__ import annotations

import contextlib
import io
import pathlib
import sys
import tempfile
import types
import traceback
import importlib.util
import inspect

ROOT = pathlib.Path("/workspace")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import contextlib

_pytest = types.ModuleType("pytest")


class _Raises:
    def __init__(self, expected):
        self.expected = expected

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            raise AssertionError(f"期望抛出 {self.expected}，但未抛出")
        return issubclass(exc_type, self.expected)


def raises(expected):
    return _Raises(expected)


class _Approx:
    def __init__(self, expected, rel=1e-6, abs=1e-9):
        self.expected = expected
        self.rel = rel
        self.abs = abs

    def __eq__(self, other):
        return abs(other - self.expected) <= max(self.rel * abs(self.expected), self.abs)

    def __repr__(self):
        return f"approx({self.expected})"


def approx(expected, rel=1e-6, abs=1e-9):
    return _Approx(expected, rel, abs)


class _Capture:
    def __init__(self, out, err):
        self._out, self._err = out, err

    def readouterr(self):
        class R:
            pass
        r = R()
        r.out = self._out.getvalue()
        r.err = self._err.getvalue()
        self._out.seek(0)
        self._out.truncate(0)
        self._err.seek(0)
        self._err.truncate(0)
        return r


def make_capsys():
    out, err = io.StringIO(), io.StringIO()
    return _Capture(out, err), out, err


class _FixtureMarker:
    def __init__(self, func, **kwargs):
        self.func = func
        self.kwargs = kwargs


def fixture(*args, **kwargs):
    if args and callable(args[0]):
        return _FixtureMarker(args[0])
    def wrap(func):
        return _FixtureMarker(func, **kwargs)
    return wrap


_pytest.fixture = fixture
_pytest.raises = raises
_pytest.approx = approx
sys.modules["pytest"] = _pytest

# 复用真实夹具定义
sys.path.insert(0, str(ROOT / "tests"))
import conftest  # noqa: E402


def load_module(path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_fixtures(tmp_path: pathlib.Path):
    root = tmp_path / "state"
    clock = conftest.clock.func()
    platform = conftest.platform.func(tmp_path, clock)
    online = conftest.online.func(platform)
    return {"clock": clock, "platform": platform, "online": online, "tmp_path": tmp_path}

# conftest 的 platform 夹具期望 tmp_path 直接是存储目录，单独给它一个目录。
def make_platform_fixtures(tmp_path: pathlib.Path):
    clock = conftest.clock.func()
    platform = conftest.platform.func(tmp_path, clock)
    online = conftest.online.func(platform)
    return {"clock": clock, "platform": platform, "online": online}


def run_file(path: pathlib.Path) -> tuple[int, int]:
    module = load_module(path)
    funcs = [(n, f) for n, f in inspect.getmembers(module, inspect.isfunction) if n.startswith("test_")]
    passed = failed = 0
    for name, func in funcs:
        params = inspect.signature(func).parameters
        case_tmp = pathlib.Path(tempfile.mkdtemp(prefix=f"{name}_"))
        fixtures: dict = {"tmp_path": case_tmp}
        platform = None
        if "clock" in params and "platform" not in params:
            fixtures["clock"] = conftest.clock.func()
        if "platform" in params or "online" in params:
            clock = conftest.clock.func()
            platform = conftest.platform.func(case_tmp, clock)
            fixtures["clock"] = clock
            fixtures["platform"] = platform
            if "online" in params:
                fixtures["online"] = conftest.online.func(platform)
        capsys = None
        cap_out = cap_err = None
        if "capsys" in params:
            capsys, cap_out, cap_err = make_capsys()
            fixtures["capsys"] = capsys
        kwargs = {pname: fixtures[pname] for pname in params if pname in fixtures}
        try:
            if capsys is not None:
                with contextlib.redirect_stdout(cap_out), contextlib.redirect_stderr(cap_err):
                    func(**kwargs)
            else:
                func(**kwargs)
            passed += 1
            print(f"PASS {path.name}::{name}")
        except Exception:  # noqa: BLE001
            failed += 1
            print(f"FAIL {path.name}::{name}")
            traceback.print_exc()
        finally:
            if platform is not None:
                platform.close()
    return passed, failed


def main() -> None:
    targets = [pathlib.Path(a) for a in sys.argv[1:]] or list((ROOT / "tests").glob("test_*.py"))
    total_p = total_f = 0
    for path in sorted(targets):
        p, f = run_file(path)
        total_p += p
        total_f += f
    print(f"\n{total_p} passed, {total_f} failed")
    sys.exit(1 if total_f else 0)


if __name__ == "__main__":
    main()
