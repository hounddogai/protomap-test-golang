"""Start, stop, restart, or inspect the local fixture without starting a second instance."""

import argparse
import fcntl
import json
import os
import signal
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / ".cache"
BINARY = CACHE / "fixture"
STATE = CACHE / "runtime.json"
PID = CACHE / "fixture.pid"
COMMAND = [str(BINARY), "serve", "--state", str(STATE)]


def running():
    if not PID.exists():
        return None
    try:
        pid = int(PID.read_text())
        result = subprocess.run(["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip() == " ".join(COMMAND):
            return pid
    except (OSError, ValueError):
        pass
    return None


def stop():
    pid = running()
    if pid is not None:
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 10
        while running() is not None and time.monotonic() < deadline:
            time.sleep(0.1)
        if running() == pid:
            os.kill(pid, signal.SIGKILL)
    PID.unlink(missing_ok=True)
    STATE.unlink(missing_ok=True)
    print("Fixture stopped")


def start():
    if running() is not None:
        print("Fixture is already running; use restart after changes")
        return
    CACHE.mkdir(exist_ok=True)
    STATE.unlink(missing_ok=True)
    subprocess.run([str(ROOT / "shell/scripts/go"), "build", "-o", str(BINARY), "./cmd/fixture"],
                   cwd=ROOT, check=True)
    with (CACHE / "runtime.log").open("w") as log:
        process = subprocess.Popen(COMMAND, cwd=ROOT, stdin=subprocess.DEVNULL, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
    PID.write_text(str(process.pid) + "\n")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError((CACHE / "runtime.log").read_text())
        if STATE.exists():
            state = json.loads(STATE.read_text())
            if state["pid"] == process.pid:
                print(f"Ready: {state['services']} services, {len(state['addresses'])} domain listeners, PID {process.pid}")
                return
        time.sleep(0.1)
    stop()
    raise RuntimeError("Fixture did not become ready; inspect .cache/runtime.log")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["start", "stop", "restart", "status"])
    args = parser.parse_args()
    try:
        CACHE.mkdir(exist_ok=True)
        # Keep the lock file so every invocation locks the same inode, even across stop and restart.
        with (CACHE / "fixture.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if args.command == "stop":
                stop()
            elif args.command == "restart":
                stop()
                start()
            elif args.command == "start":
                start()
            elif running() is not None and STATE.exists():
                print(STATE.read_text().strip())
            else:
                parser.exit(1, "Fixture is not running\n")
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"Development command failed: {error}\n")


if __name__ == "__main__":
    main()
