import signal
import subprocess
import sys
import time
from collections.abc import Sequence

POLL_INTERVAL = 0.05


def _stop(children: list[subprocess.Popen], grace: float) -> bool:
    for child in children:
        if child.poll() is None:
            child.terminate()

    deadline = time.monotonic() + grace
    for child in children:
        try:
            child.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            break

    remaining = [child for child in children if child.poll() is None]
    for child in remaining:
        child.kill()
    for child in remaining:
        try:
            child.wait(timeout=1)
        except subprocess.TimeoutExpired:
            print("Unable to reap child after SIGKILL", file=sys.stderr)
    return not remaining


def _exit_code(returncode: int) -> int:
    return 128 - returncode if returncode < 0 else returncode


def supervise(
    migration: Sequence[str],
    api: Sequence[str],
    worker: Sequence[str],
    *,
    grace: float = 10,
) -> int:
    stopping = False
    children = []

    def request_stop(signum: int, frame: object) -> None:
        nonlocal stopping
        stopping = True

    previous = {
        sig: signal.signal(sig, request_stop) for sig in (signal.SIGTERM, signal.SIGINT)
    }

    def run() -> int:
        if stopping:
            return 0
        migrator = subprocess.Popen(migration)
        children.append(migrator)
        while not stopping and migrator.poll() is None:
            time.sleep(POLL_INTERVAL)
        if stopping:
            return 0
        if migrator.returncode:
            return _exit_code(migrator.returncode)

        for command in (api, worker):
            if stopping:
                break
            children.append(subprocess.Popen(command))

        while not stopping:
            for child in children[1:]:
                code = child.poll()
                if code is not None:
                    print(f"Child process exited with status {code}", file=sys.stderr)
                    return _exit_code(code) or 1
            time.sleep(POLL_INTERVAL)
        return 0

    try:
        result = run()
    finally:
        try:
            graceful = _stop(children, grace)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
    return result or (0 if graceful else 1)


if __name__ == "__main__":
    python = sys.executable
    sys.exit(
        supervise(
            [python, "-m", "alembic", "upgrade", "head"],
            [python, "-m", "bin.api"],
            [python, "-m", "bin.outbox"],
        )
    )
