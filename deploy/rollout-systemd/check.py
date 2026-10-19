#!/usr/bin/env python3
"""P0: real Linux/systemd handoff of a non-root identity probe.

The container is isolated, has no host filesystem mounts, and is removed on
completion. Its privileged mode is required for a real systemd/cgroup test.
This does not certify business-state handoff or the default installer.
"""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent.parent
UNIT = "gateway-probe.service"


def run(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def output(args):
    return subprocess.check_output(args, text=True).strip()


def wait(fn, timeout=25):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            value = fn()
            if value:
                return value
        except (OSError, ValueError, http.client.HTTPException, subprocess.CalledProcessError) as exc:
            last = exc
        time.sleep(0.04)
    raise AssertionError(f"condition timed out: {last}")


def request(port, path="/", method="GET"):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        conn.request(method, path)
        res = conn.getresponse()
        body = res.read()
        assert res.status < 300, (res.status, body)
        return json.loads(body) if body else None
    finally:
        conn.close()


def prop(name):
    return output(["systemctl", "show", UNIT, "--property=" + name, "--value"])


def pids():
    group = prop("ControlGroup")
    if not group:
        return []
    path = Path("/sys/fs/cgroup" + group) / "cgroup.procs"
    return [int(x) for x in path.read_text().split()] if path.exists() else []


def stable(version, previous=None):
    def ready():
        state = request(18080)
        if state["version"] != version or state["busy"] or state["pid"] == previous:
            return False
        assert state["credential"], "read-only credential was lost"
        assert state["decision"]["cold_start_release"] == state["identity"]["release"]
        if int(prop("MainPID")) != state["pid"] or pids() != [state["pid"]]:
            return False
        return state
    return wait(ready)


def hold():
    conn = http.client.HTTPConnection("127.0.0.1", 18080, timeout=15)
    conn.request("GET", "/hold")
    res = conn.getresponse()
    assert res.status == 200
    first = res.readline()
    assert first.startswith(b"data: ")
    return conn, res


def inside(after_reboot=False):
    releases = json.loads(Path("/test/releases.json").read_text())
    if after_reboot:
        state = stable("A")
        print(f"PASS container reboot selects committed A, PID {state['pid']}", flush=True)
        return
    run(["systemctl", "daemon-reload"])
    run(["systemctl", "enable", "--now", UNIT])
    a = stable("A")
    assert output(["stat", "-c", "%u", f"/proc/{a['pid']}"]) == "987", "probe ran with wrong identity"
    restarts = prop("NRestarts")
    stream, response = hold()
    request(19090, "/upgrade?release=" + releases["B"], "POST")
    candidate = wait(lambda: (s if (s := request(18080))["version"] == "B" else False))
    assert len(pids()) == 2 and int(prop("MainPID")) == a["pid"], "old stream did not hold old owner"
    assert b"data: done" in response.read(), "old SSE broke during listener transfer"
    stream.close()
    b = stable("B")
    assert b["pid"] == candidate["pid"], "systemd restarted promoted candidate"
    assert prop("NRestarts") == restarts, "normal handoff restarted service"
    print("PASS A -> B, listener sharing, old SSE drain, 1 -> 2 -> 1, MainPID", flush=True)

    request(19090, "/upgrade?release=" + releases["C"], "POST")
    c = stable("C")
    assert prop("NRestarts") == restarts
    # The inherited public control socket must still be reachable after A/B exit.
    import socket
    sock = socket.socket(socket.AF_UNIX)
    sock.settimeout(2)
    sock.connect("/run/gateway-probe/control.sock")
    sock.sendall(b"GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
    chunks = []
    while part := sock.recv(4096):
        chunks.append(part)
    sock.close()
    assert b'"version":"C"' in b"".join(chunks)
    print("PASS B -> C and inherited control socket", flush=True)

    stream, response = hold()
    request(19090, "/upgrade?release=" + releases["A"], "POST")
    candidate = wait(lambda: (s if (s := request(18080))["version"] == "A" else False))
    assert candidate["pid"] in pids() and candidate["pid"] != c["pid"]
    os.kill(candidate["pid"], signal.SIGKILL)
    response.read()
    stream.close()
    c = stable("C", previous=c["pid"])
    print("PASS candidate crash before commit cold-starts C", flush=True)

    request(19090, "/upgrade?release=" + releases["A"] + "&failpoint=after-commit", "POST")
    a = stable("A")
    assert a["decision"]["phase"] == "STABLE", "expected a cold restart after injected crash"
    print("PASS crash after COMMITTED before MainPID cold-starts A", flush=True)
    os.kill(a["pid"], signal.SIGKILL)
    a = stable("A", previous=a["pid"])
    print("PASS active main process crash restarts selected artifact", flush=True)

    stream, response = hold()
    request(19090, "/upgrade?release=" + releases["B"], "POST")
    wait(lambda: request(18080)["version"] == "B" and len(pids()) == 2)
    run(["systemctl", "stop", UNIT])
    response.read()
    stream.close()
    assert pids() == [] and prop("MainPID") == "0", "stop left live service processes"
    run(["systemctl", "start", UNIT])
    stable("A")
    print("PASS stop during two-process phase cleans entire cgroup; start selects A", flush=True)


def host():
    arch = output(["docker", "info", "--format", "{{.Architecture}}"])
    goarch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}[arch]
    run(["docker", "build", "-t", "aiagent-gateway-systemd-test:bookworm", str(ROOT / "deploy/rollout-systemd")])
    name = "gateway-systemd-p0-" + uuid.uuid4().hex[:10]
    with tempfile.TemporaryDirectory(prefix="gateway-systemd-p0-") as tmp:
        stage = Path(tmp)
        releases = {}
        for version in ("A", "B", "C"):
            binary = stage / ("probe-" + version)
            env = dict(os.environ, GOOS="linux", GOARCH=goarch, CGO_ENABLED="0")
            run(["go", "build", "-ldflags=-X main.version=" + version, "-o", str(binary), "./deploy/rollout-systemd/probe"], cwd=ROOT, env=env)
            digest = hashlib.sha256(binary.read_bytes()).hexdigest()
            releases[version] = digest
            target = stage / "releases" / digest / "gateway"
            target.parent.mkdir(parents=True)
            shutil.copy2(binary, target)
        (stage / "releases.json").write_text(json.dumps(releases))
        (stage / "probe.conf").write_text("probe-credential\n")
        (stage / "probe.conf").chmod(0o600)
        (stage / UNIT).write_text(f"""[Unit]
Description=Gateway P0 handoff test only
StartLimitIntervalSec=60
StartLimitBurst=10
[Service]
Type=notify
NotifyAccess=main
User=gateway-probe
Group=gateway-probe
LoadCredential=probe.conf:/etc/gateway-probe.conf
ExecStart=/opt/gateway-probe/launcher launch --initial={releases['A']} --config=%d/probe.conf
Restart=on-failure
RestartSec=1
KillMode=mixed
TimeoutStartSec=20
TimeoutStopSec=15
RuntimeDirectory=gateway-probe
RuntimeDirectoryMode=0700
StateDirectory=gateway-probe
StateDirectoryMode=0700
UMask=0077
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/gateway-probe /run/gateway-probe
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
[Install]
WantedBy=multi-user.target
""")
        try:
            run(["docker", "run", "--detach", "--name", name, "--label", "aiagent-gateway.test=p0",
                 "--privileged", "--cgroupns=private", "--tmpfs", "/run", "--tmpfs", "/run/lock", "--tmpfs", "/tmp",
                 "aiagent-gateway-systemd-test:bookworm"], stdout=subprocess.DEVNULL)
            wait(lambda: output(["docker", "exec", name, "systemctl", "show", "--property=Version", "--value"]), timeout=30)
            run(["docker", "exec", name, "mkdir", "-p", "/opt/gateway-probe", "/test"])
            for src, dst in ((stage / "releases", "/opt/gateway-probe/releases"),
                             (stage / "probe-A", "/opt/gateway-probe/launcher"),
                             (stage / UNIT, "/etc/systemd/system/" + UNIT),
                             (stage / "probe.conf", "/etc/gateway-probe.conf"),
                             (stage / "releases.json", "/test/releases.json"),
                             (Path(__file__), "/test/check.py")):
                run(["docker", "cp", str(src), name + ":" + dst])
            run(["docker", "exec", name, "chown", "-R", "0:0", "/opt/gateway-probe"])
            run(["docker", "exec", name, "chmod", "-R", "go-w", "/opt/gateway-probe"])
            run(["docker", "exec", name, "python3", "/test/check.py", "--inside"])
            run(["docker", "restart", name], stdout=subprocess.DEVNULL)
            wait(lambda: output(["docker", "exec", name, "systemctl", "show", "--property=Version", "--value"]), timeout=30)
            run(["docker", "exec", name, "python3", "/test/check.py", "--after-reboot"])
        except BaseException:
            subprocess.run(["docker", "logs", "--tail=60", name], check=False)
            subprocess.run(["docker", "exec", name, "journalctl", "-u", UNIT, "--no-pager", "-n", "80"], check=False)
            raise
        finally:
            subprocess.run(["docker", "rm", "--force", name], stdout=subprocess.DEVNULL, check=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inside", action="store_true")
    parser.add_argument("--after-reboot", action="store_true")
    args = parser.parse_args()
    if args.inside or args.after_reboot:
        inside(args.after_reboot)
    else:
        host()
