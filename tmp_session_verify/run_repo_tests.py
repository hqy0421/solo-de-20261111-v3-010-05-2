"""Minimal pytest shim to execute test_session_partitions.py without pytest installed."""
import importlib.util
import sys
import traceback
from contextlib import contextmanager


class _PytestShim:
    @staticmethod
    @contextmanager
    def raises(exc_type):
        try:
            yield
        except exc_type:
            return
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(
                f"expected {exc_type.__name__}, got {type(exc).__name__}: {exc}"
            )
        raise AssertionError(f"expected {exc_type.__name__} to be raised")


sys.modules["pytest"] = _PytestShim  # type: ignore[assignment]

spec = importlib.util.spec_from_file_location(
    "test_session_partitions",
    "/workspace/python_modules/dagster/dagster_tests/definitions_tests/test_session_partitions.py",
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

failures = []
tests = sorted(name for name in vars(module) if name.startswith("test_"))
for name in tests:
    try:
        getattr(module, name)()
        print(f"PASS {name}")
    except Exception:  # noqa: BLE001
        failures.append(name)
        print(f"FAIL {name}")
        traceback.print_exc()

print()
print(f"{len(tests) - len(failures)}/{len(tests)} passed")
if failures:
    print("FAILURES:", failures)
    raise SystemExit(1)
print("ALL TESTS PASSED")
