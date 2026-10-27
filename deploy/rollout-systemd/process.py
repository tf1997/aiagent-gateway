#!/usr/bin/env python3
"""Complete gateway lifecycle on real systemd in an isolated Linux container.

No host mounts or published ports. Builds three different artifacts, exercises
the real owner/controller and removes the test container on every exit path.
"""
import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent.parent
UNIT = "aiagent-gateway.service"


def run(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def output(args):
    return subprocess.check_output(args, text=True).strip()


def wait(fn, timeout=45):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            value = fn()
            if value:
                return value
        except (OSError, ValueError, http.client.HTTPException, subprocess.CalledProcessError) as exc:
            last = exc
        time.sleep(.05)
    raise AssertionError(f"condition timed out: {last}")


class UnixHTTP(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect("/run/aiagent-gateway-control/controller.sock")


def request(port, path, token="", method="GET", body=None, session=""):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        conn.request(method, path, None if body is None else json.dumps(body),
                     {"Authorization": "Bearer " + token, "Content-Type": "application/json", "X-Session-ID": session})
        res = conn.getresponse()
        return res.status, res.read()
    finally:
        conn.close()


def prop(name):
    return output(["systemctl", "show", UNIT, "--property=" + name, "--value"])


def pids():
    group = prop("ControlGroup")
    path = Path("/sys/fs/cgroup" + group) / "cgroup.procs"
    result = []
    for item in path.read_text().split() if group and path.exists() else []:
        try:
            # Short-lived systemctl queries also belong to the cgroup. The
            # topology budget counts serving gateway executables only.
            if os.readlink(f"/proc/{item}/exe").startswith("/opt/aiagent-gateway/releases/"):
                result.append(int(item))
        except FileNotFoundError:
            pass
    return result


def inside(after_reboot=False, cycles=0):
    cfg = json.loads(Path("/etc/aiagent-gateway/config.yaml").read_text())
    artifacts = json.loads(Path("/test/releases.json").read_text())
    policy = json.loads(Path("/test/policy.json").read_text())
    caller = "systemd-test-caller"

    def control(path="/control/v1/rollout", method="GET", body=None, expected=200):
        conn = UnixHTTP("localhost", timeout=10)
        try:
            conn.request(method, path, None if body is None else json.dumps(body), {"Authorization": "Bearer " + cfg["admin_token"]})
            res = conn.getresponse()
            raw = res.read()
            assert res.status == expected, (res.status, raw)
            return json.loads(raw)
        finally:
            conn.close()

    def stable(artifact, previous=None):
        def check():
            report = control()
            identity = control("/control/v1/identity")
            if report["phase"] not in ("COMPLETED", "ROLLED_BACK") or report["stable"]["artifact"] != artifact or not identity["ready"]:
                return False
            pid = identity["pid"]
            if pid == previous or int(prop("MainPID")) != pid or pids() != [pid]:
                return False
            assert identity["release"] == Path(artifact).parent.name
            return identity
        return wait(check)

    if after_reboot:
        stable(artifacts[2])
        assert request(9090, "/admin/v1/routes", cfg["admin_token"])[0] == 200
        print("PASS complete gateway container reboot selects committed C and restores configuration", flush=True)
        return

    metrics = {"systemd": output(["systemctl", "--version"]).splitlines()[0], "release_windows_ms": [],
               "snapshot_bytes": [], "request_ms": [], "handoff_request_ms": [], "queue_timeouts": 0,
               "queue_rejected": 0, "peak_rss_kib": 0, "peak_fds": 0, "peak_service_pids": 0, "request_failures": 0}
    sampling = threading.Event()
    def sample():
        while not sampling.wait(.1):
            try:
                active = pids()
                rss, fds = 0, 0
                for pid in active:
                    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                        if line.startswith("VmRSS:"):
                            rss += int(line.split()[1])
                    fds += len(list(Path(f"/proc/{pid}/fd").iterdir()))
                metrics["peak_service_pids"] = max(metrics["peak_service_pids"], len(active))
                metrics["peak_rss_kib"] = max(metrics["peak_rss_kib"], rss)
                metrics["peak_fds"] = max(metrics["peak_fds"], fds)
            except (OSError, subprocess.CalledProcessError):
                pass
    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    held = threading.Event()
    calls = []

    class Agent(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            calls.append(self.path)
            self.send_response(503 if self.path == "/fail" else 200)
            self.send_header("Content-Type", "text/event-stream" if self.path == "/hold" else "application/json")
            self.end_headers()
            if self.path == "/hold":
                self.wfile.write(b'data: {"started":true}\n\n')
                self.wfile.flush()
                held.wait(60)
                self.wfile.write(b"data: [DONE]\n\n")
            else:
                self.wfile.write(b'{"ok":true}')

    agent = http.server.ThreadingHTTPServer(("127.0.0.1", 18090), Agent)
    threading.Thread(target=agent.serve_forever, daemon=True).start()
    run([artifacts[0], "install"])
    manifest = json.loads(Path("/etc/aiagent-gateway/deployment.json").read_text())
    assert set(manifest) == {"initial_release", "policy"}
    manifest["policy"] = policy
    Path("/etc/aiagent-gateway/deployment.json").write_text(json.dumps(manifest))
    a = stable(artifacts[0])
    assert output(["stat", "-c", "%u", f"/proc/{a['pid']}"]) != "0"
    assert prop("NotifyAccess") == "main" and prop("KillMode") == "mixed"
    print("PASS complete gateway non-root startup, credentials, one serving PID and READY barrier", flush=True)
    status, body = request(9090, "/admin/v1/routes", cfg["admin_token"], "POST", {
        "key": "probe", "protocol": "proxy", "consumers": [{"id": "alice", "token": caller}],
        "endpoints": [{"url": "http://127.0.0.1:18090"}]})
    assert status == 201, (status, body)

    def begin(index):
        return control(method="POST", body={"artifact": artifacts[index], "policy": policy})

    def pump(phases, failing=False):
        end = time.monotonic() + 60
        while time.monotonic() < end:
            report = wait(control)
            assert len(pids()) <= 2, "third service process"
            if report["phase"] in phases:
                if report["phase"] == "COMPLETED":
                    metrics["release_windows_ms"].append(report["handoff_milliseconds"])
                    metrics["snapshot_bytes"].append(report["snapshot_bytes"])
                    for key in ("state_status", "state_queue", "allocation_queue"):
                        metrics["queue_timeouts"] += report.get(key, {}).get("Timeouts", 0)
                        metrics["queue_rejected"] += report.get(key, {}).get("Rejected", 0)
                return report
            assert report["phase"] not in ("RECOVERY_REQUIRED", "ROLLED_BACK", "FAILED"), report
            started = time.monotonic()
            status, body = request(8080, "/agents/probe/" + ("fail" if failing else "ok"), caller, session=secrets.token_hex(8))
            elapsed = (time.monotonic() - started) * 1000
            metrics["request_ms"].append(elapsed)
            if report["phase"] == "HANDOFF":
                metrics["handoff_request_ms"].append(elapsed)
            metrics["request_failures"] += status != (503 if failing else 200)
            assert status == (503 if failing else 200), (status, body, report)
            time.sleep(.025)
        raise AssertionError(report)

    restarts = prop("NRestarts")
    stream = http.client.HTTPConnection("127.0.0.1", 8080, timeout=60)
    stream.request("GET", "/agents/probe/hold", headers={"Authorization": "Bearer " + caller, "X-Session-ID": "old-stream"})
    response = stream.getresponse()
    assert response.status == 200, (response.status, response.read())
    assert response.readline().startswith(b"data:")
    with Path("/test/install.log").open("w+") as log:
        cli = subprocess.Popen([artifacts[1], "install"], stdout=log, stderr=log)
        try:
            task = wait(lambda: r if (r := control())["phase"] == "VERIFYING" else None)
        finally:
            cli.terminate()
            cli.wait(timeout=10)
    assert begin(1)["rollout_id"] == task["rollout_id"]
    pump({"DRAINING"})
    assert len(pids()) == 2 and int(prop("MainPID")) == a["pid"]
    held.set()
    assert b"[DONE]" in response.read()
    stream.close()
    result = pump({"COMPLETED"})
    b = stable(artifacts[1])
    assert result["committed"] and result["statistics"]["direct_ingress_counts"]["http"]["success"] > 0
    assert prop("NRestarts") == restarts and calls.count("/hold") == 1
    print("PASS complete A -> B, old SSE drain, direct ingress, durable state and MainPID takeover", flush=True)
    print("PASS install CLI disconnect leaves the background rollout running", flush=True)
    begin(2)
    pump({"COMPLETED"})
    c = stable(artifacts[2])
    assert prop("NRestarts") == restarts
    print("PASS complete B -> C without service restart; inherited control socket remains reachable", flush=True)
    begin(0)
    pump({"ROLLED_BACK"}, True)
    assert int(prop("MainPID")) == c["pid"] and pids() == [c["pid"]]
    print("PASS complete failed candidate rolls back under systemd", flush=True)
    os.kill(c["pid"], signal.SIGKILL)
    c = stable(artifacts[2], c["pid"])
    assert request(8080, "/agents/probe/ok", caller, session="after-crash")[0] == 200
    print("PASS active gateway SIGKILL restarts durable C and restores business state", flush=True)
    begin(0)
    wait(lambda: control()["phase"] == "VERIFYING" and len(pids()) == 2)
    run(["systemctl", "stop", UNIT])
    assert not pids() and prop("MainPID") == "0"
    run(["systemctl", "start", UNIT])
    # A precommit restart reports rollback; its cold-start choice must still be C.
    identity = wait(lambda: control("/control/v1/identity"))
    assert identity["release"] == Path(artifacts[2]).parent.name and pids() == [identity["pid"]]
    assert control()["phase"] == "ROLLED_BACK"
    print("PASS systemd stop cleans both complete processes; start rejects uncommitted candidate", flush=True)
    # Restore a completed terminal record before testing machine reboot.
    begin(1)
    pump({"COMPLETED"})
    begin(2)
    pump({"COMPLETED"})
    stable(artifacts[2])
    policy.update(canary_percent=50, verify_min_seconds=0, observe_seconds=1)
    for index in range(cycles):
        target = 1 if index % 2 == 0 else 2
        begin(target)
        pump({"COMPLETED"})
        stable(artifacts[target])
    if cycles % 2:
        begin(2)
        pump({"COMPLETED"})
    sampling.set()
    sampler.join(timeout=5)
    def quantiles(values):
        ordered = sorted(values)
        return {"samples": len(ordered), **{name: round(ordered[min(len(ordered)-1, int((len(ordered)-1)*q))], 3)
                for name, q in (("p50", .5), ("p95", .95), ("p99", .99))}} if ordered else {"samples": 0}
    for key in ("release_windows_ms", "request_ms", "handoff_request_ms"):
        metrics[key] = quantiles(metrics[key])
    metrics["passed"] = True
    metrics["extra_release_cycles"] = cycles
    metrics["scope"] = "small-state serial HTTP workload; not a production capacity ceiling"
    Path("/test/metrics.json").write_text(json.dumps(metrics, indent=2))
    print("PASS resource/latency measurements:", json.dumps(metrics), flush=True)
    config_before = Path("/etc/aiagent-gateway/config.yaml").read_bytes()
    data_before = Path("/var/lib/aiagent-gateway/routes.json").read_bytes()
    run([artifacts[2], "uninstall"])
    assert not pids()
    assert Path("/var/lib/aiagent-gateway/owner.lock").exists()
    run([artifacts[2], "install"])
    stable(artifacts[2])
    assert Path("/etc/aiagent-gateway/config.yaml").read_bytes() == config_before
    assert Path("/var/lib/aiagent-gateway/routes.json").read_bytes() == data_before
    print("PASS packaged installer, uninstall retains state, reinstall resumes committed C", flush=True)
    agent.shutdown()
    agent.server_close()


def host(reuse_image=False, cycles=0, report="/tmp/gateway-systemd-process.json", image="aiagent-gateway-systemd-test:bookworm"):
    arch = output(["docker", "info", "--format", "{{.Architecture}}"])
    goarch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}[arch]
    if not reuse_image:
        run(["docker", "build", "-t", image, str(ROOT / "deploy/rollout-systemd")])
    name = "gateway-systemd-process-" + uuid.uuid4().hex[:10]
    with tempfile.TemporaryDirectory(prefix="gateway-systemd-process-") as tmp:
        stage = Path(tmp)
        artifacts = []
        for version in ("A", "B", "C"):
            binary = stage / ("gateway-" + version)
            run(["go", "build", "-ldflags=-buildid=systemd-process-" + version, "-o", str(binary), "./cmd/gateway"],
                cwd=ROOT, env=dict(os.environ, GOOS="linux", GOARCH=goarch, CGO_ENABLED="0"))
            digest = hashlib.sha256(binary.read_bytes()).hexdigest()
            target = stage / "releases" / digest / "gateway"
            target.parent.mkdir(parents=True)
            shutil.copy2(binary, target)
            artifacts.append("/opt/aiagent-gateway/releases/" + digest + "/gateway")
        (stage / "releases.json").write_text(json.dumps(artifacts))
        (stage / "config.yaml").write_text(json.dumps({"admin_token": secrets.token_hex(24), "data_dir": "/var/lib/aiagent-gateway"}))
        (stage / "config.yaml").chmod(0o600)
        policy = {"auto_profiles": False, "canary_percent": 5, "readiness_timeout_seconds": 10,
                  "verify_min_seconds": 1, "verify_timeout_seconds": 15, "min_success_per_profile": 1,
                  "max_effective_failures": 0, "required_profiles": ["http"], "observe_seconds": 2,
                  "observe_min_success_per_profile": 1, "drain_timeout_seconds": 15}
        (stage / "policy.json").write_text(json.dumps(policy))
        metrics = {"passed": False}
        try:
            run(["docker", "run", "--detach", "--name", name, "--label", "aiagent-gateway.test=complete-process",
                 "--privileged", "--cgroupns=private", "--tmpfs", "/run", "--tmpfs", "/run/lock", "--tmpfs", "/tmp", image], stdout=subprocess.DEVNULL)
            wait(lambda: output(["docker", "exec", name, "systemctl", "show", "--property=Version", "--value"]))
            run(["docker", "exec", name, "mkdir", "-p", "/opt/aiagent-gateway", "/etc/aiagent-gateway", "/test"])
            for source, dest in [(stage / "releases", "/opt/aiagent-gateway/releases"), (stage / "gateway-A", "/opt/aiagent-gateway/launcher"),
                                 (stage / "config.yaml", "/etc/aiagent-gateway/config.yaml"), (stage / "policy.json", "/test/policy.json"),
                                 (stage / "releases.json", "/test/releases.json"),
                                 (ROOT / "deploy/rollout-process/systemd_agent.py", "/test/systemd_agent.py"),
                                 (Path(__file__), "/test/process.py")]:
                run(["docker", "cp", str(source), name + ":" + dest])
            run(["docker", "exec", name, "chown", "-R", "0:0", "/opt/aiagent-gateway"])
            run(["docker", "exec", name, "chmod", "-R", "go-w", "/opt/aiagent-gateway"])
            run(["docker", "exec", name, "python3", "/test/process.py", "--inside", "--cycles", str(cycles)])
            run(["docker", "cp", name + ":/test/metrics.json", report])
            metrics = json.loads(Path(report).read_text())
            def cleanup_check(action, **kwargs):
                result = run(["docker", "exec", "-i", name, "python3", "/test/systemd_agent.py", "--guest"],
                             input=json.dumps(dict(action=action, **kwargs)), capture_output=True)
                return json.loads(result.stdout)
            fixture = cleanup_check("cleanup_fixture", binary=artifacts[2])
            run(["docker", "restart", name], stdout=subprocess.DEVNULL)
            wait(lambda: output(["docker", "exec", name, "systemctl", "show", "--property=Version", "--value"]))
            run(["docker", "exec", name, "python3", "/test/process.py", "--after-reboot"])
            metrics = json.loads(Path(report).read_text())
            metrics["cleanup"] = cleanup_check("cleanup_verify", fixture=fixture)
            metrics["passed"] = True
            metrics["source_commit"] = output(["git", "-C", str(ROOT), "rev-parse", "HEAD"])
            metrics["source_modified"] = bool(output(["git", "-C", str(ROOT), "status", "--porcelain"]))
            Path(report).write_text(json.dumps(metrics, indent=2) + "\n")
        except BaseException as exc:
            metrics["passed"] = False
            metrics["error_type"] = type(exc).__name__
            Path(report).write_text(json.dumps(metrics, indent=2) + "\n")
            subprocess.run(["docker", "exec", name, "journalctl", "-u", UNIT, "--no-pager", "-n", "100"], check=False)
            raise
        finally:
            subprocess.run(["docker", "rm", "--force", name], check=False, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inside", action="store_true")
    parser.add_argument("--after-reboot", action="store_true")
    parser.add_argument("--reuse-image", action="store_true", help="use the existing local systemd test image without rebuilding")
    parser.add_argument("--image", default="aiagent-gateway-systemd-test:bookworm", help="prebuilt systemd image; use with --reuse-image")
    parser.add_argument("--cycles", type=int, default=0)
    parser.add_argument("--report", default="/tmp/gateway-systemd-process.json")
    args = parser.parse_args()
    if not 0 <= args.cycles <= 100:
        parser.error("cycles must be between 0 and 100")
    inside(args.after_reboot, args.cycles) if args.inside or args.after_reboot else host(args.reuse_image, args.cycles, args.report, args.image)
