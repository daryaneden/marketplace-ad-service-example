import os
import signal
import subprocess
import sys
import time

import pytest

WORKER = "outbox"
STUB = """
import os
import signal
import sys
import time
from pathlib import Path

role = sys.argv[1]
events = Path(os.environ["EVENTS"])
def record(action):
    with events.open("a") as stream:
        print(f"{role}:{action}", file=stream)
if role == "migrate":
    record("start")
    time.sleep(0.05)
    record("end")
    sys.exit(7 if os.getenv("FAIL") == role else 0)
def stop(signum, frame):
    record("stopped")
    sys.exit(0)
if os.getenv("IGNORE_TERM") != role:
    signal.signal(signal.SIGTERM, stop)
record(str(os.getpid()))
if os.getenv("FAIL") == role:
    deadline = time.monotonic() + 3
    other = "api" if role != "api" else os.environ["WORKER"]
    while not any(
        line.startswith(other + ":") for line in events.read_text().splitlines()
    ):
        if time.monotonic() >= deadline:
            sys.exit(9)
        time.sleep(0.01)
    sys.exit(int(os.getenv("FAIL_CODE", "9")))
while True:
    time.sleep(0.1)
"""


@pytest.mark.parametrize(
    ("failure", "fail_code", "ignore_term", "expected"),
    [
        ("migrate", 7, "", 7),
        ("api", 9, "", 9),
        (WORKER, 9, "", 9),
        (WORKER, 0, "", 1),
        ("", 0, "", 0),
        ("api", 9, WORKER, 9),
    ],
    ids=[
        "migration-failure",
        "api-failure",
        "worker-failure",
        "unexpected-zero",
        "sigterm-both",
        "forced-shutdown",
    ],
)
def test_container_process_lifecycle(
    tmp_path, pytestconfig, failure, fail_code, ignore_term, expected
):
    events = tmp_path / "events"
    stub = tmp_path / "stub.py"
    stub.write_text(STUB)
    launcher = tmp_path / "launch.py"
    launcher.write_text(
        "import sys\nfrom bin.container import supervise\n"
        "def cmd(role): return [sys.executable, sys.argv[1], role]\n"
        "sys.exit(supervise(cmd('migrate'), cmd('api'), cmd(sys.argv[2]), grace=0.3))\n"
    )
    root = pytestconfig.rootpath
    env = {
        **os.environ,
        "PYTHONPATH": str(root),
        "PYTHONDONTWRITEBYTECODE": "1",
        "EVENTS": str(events),
        "FAIL": failure,
        "WORKER": WORKER,
        "FAIL_CODE": str(fail_code),
        "IGNORE_TERM": ignore_term,
    }
    with subprocess.Popen(
        [sys.executable, str(launcher), str(stub), WORKER],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            if not failure:
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline and process.poll() is None:
                    if events.exists() and len(events.read_text().splitlines()) == 4:
                        process.send_signal(signal.SIGTERM)
                        break
                    time.sleep(0.01)
            _, stderr = process.communicate(timeout=5)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
    assert process.returncode == expected, stderr
    lines = events.read_text().splitlines()
    assert lines[:2] == ["migrate:start", "migrate:end"]
    if failure == "migrate":
        assert lines == ["migrate:start", "migrate:end"]
        return
    assert {line.split(":")[0] for line in lines[2:4]} == {"api", WORKER}
    for line in lines[2:4]:
        role, pid = line.split(":")
        with pytest.raises(ProcessLookupError):
            os.kill(int(pid), 0)
        if role not in (failure, ignore_term):
            assert f"{role}:stopped" in lines
