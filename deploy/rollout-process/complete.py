#!/usr/bin/env python3
"""Complete-process A -> B -> C, direct ingress, live SSE and rollback.

Uses isolated paths/ports and mock upstreams. No installed service is modified.
"""
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

from check import port, request


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=10)
        self.path = str(path)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def main(binary):
    repo = Path(__file__).resolve().parents[2]
    releases = {"/source-stream": threading.Event(), "/candidate-stream": threading.Event()}
    calls = []

    class Upstream(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            calls.append(self.path)
            self.send_response(503 if self.path == "/fail" else 200)
            if self.path in releases:
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                self.wfile.write(b'data: {"started":true}\n\n')
                self.wfile.flush()
                if releases[self.path].wait(90):
                    self.wfile.write(b"data: [DONE]\n\n")
            else:
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok":true}')

    upstream = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    children = []
    streams = []
    with tempfile.TemporaryDirectory(prefix="gw-full-", dir="/tmp") as tmp:
        root = Path(tmp)
        log = (root / "process.log").open("w+")
        data, admin = port(), port()
        token, caller = secrets.token_hex(24), secrets.token_hex(24)
        artifacts = []
        for name in ("a", "b", "c"):
            source = Path(binary)
            if name != "a":
                source = root / ("build-" + name)
                subprocess.run(["go", "build", *(["-race"] if "--race" in sys.argv else []), "-ldflags=-buildid=complete-process-" + name,
                                "-o", str(source), "./cmd/gateway"], cwd=repo, check=True)
            raw = source.read_bytes()
            artifact = root / "releases" / hashlib.sha256(raw).hexdigest() / "gateway"
            artifact.parent.mkdir(parents=True)
            artifact.write_bytes(raw)
            artifact.chmod(0o755)
            artifacts.append(str(artifact))
        cfg = root / "config.yaml"
        cfg.write_text(json.dumps({"admin_token": token, "data_dir": str(root / "data"),
                                   "state_socket": str(root / "state.sock"),
                                   "control_socket": str(root / "control.sock"),
                                   "port": str(data), "admin_port": str(admin)}))
        cfg.chmod(0o600)
        policy = {"auto_profiles": False, "canary_percent": 5,
                  "readiness_timeout_seconds": 10, "verify_min_seconds": 1,
                  "verify_timeout_seconds": 15, "min_success_per_profile": 1,
                  "max_effective_failures": 0, "required_profiles": ["http"],
                  "observe_seconds": 3, "observe_min_success_per_profile": 1,
                  "drain_timeout_seconds": 20}
        manifest = root / "deployment.json"
        manifest.write_text(json.dumps({"initial_release": artifacts[0], "policy": policy}))

        def control(path="/control/v1/rollout", method="GET", value=None, expected=200):
            conn = UnixHTTP(root / "control.sock")
            try:
                conn.request(method, path, None if value is None else json.dumps(value),
                             {"Authorization": "Bearer " + token})
                res = conn.getresponse()
                body = json.loads(res.read())
                assert res.status == expected, (res.status, body)
                return body
            finally:
                conn.close()

        def wait(check, timeout=30):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    value = check()
                    if value:
                        return value
                except (OSError, http.client.HTTPException, ValueError):
                    pass
                time.sleep(.05)
            raise AssertionError("complete process condition timed out")

        def live():
            rows = subprocess.check_output(["ps", "-axo", "pid=,stat=,command="], text=True)
            return {int(parts[0]) for line in rows.splitlines()
                    if len(parts := line.strip().split(None, 2)) == 3
                    and str(root / "releases") in parts[2] and not parts[1].startswith("Z")}

        def begin(index, active_policy=policy):
            return control(method="POST", value={"artifact": artifacts[index], "policy": active_policy})

        def pump(phases, fail=False, timeout=40):
            deadline = time.monotonic() + timeout
            report = {}
            while time.monotonic() < deadline:
                report = wait(control, 15)
                assert len(live()) <= 2, "third gateway process created"
                if report["phase"] in phases:
                    return report
                assert report["phase"] not in ("FAILED", "RECOVERY_REQUIRED", "ROLLED_BACK"), report
                status, body = request(data, "/agents/probe/" + ("fail" if fail else "ok"), caller,
                                       session=secrets.token_hex(8))
                assert status == (503 if fail else 200), (status, body, report)
                time.sleep(.025)
            raise AssertionError(report)

        def stream(path):
            conn = http.client.HTTPConnection("127.0.0.1", data, timeout=60)
            conn.request("GET", "/agents/probe" + path,
                         headers={"Authorization": "Bearer " + caller, "X-Session-ID": path})
            res = conn.getresponse()
            assert res.status == 200 and res.readline().startswith(b"data:")
            streams.append(conn)
            return res

        try:
            child = subprocess.Popen([artifacts[0], "process", "--config", str(cfg),
                                      "--deployment", str(manifest), "--releases-dir", str(root / "releases")],
                                     stdout=log, stderr=log)
            children.append(child)
            wait(lambda: request(data, "/readyz")[0] == 200)
            wait(lambda: control()["phase"] == "COMPLETED")
            assert live() == {child.pid}, live()
            status, body = request(admin, "/admin/v1/routes", token, "POST", {
                "key": "probe", "protocol": "proxy", "consumers": [{"id": "alice", "token": caller}],
                "endpoints": [{"url": f"http://127.0.0.1:{upstream.server_port}"}],
            })
            assert status == 201, (status, body)
            old_stream = stream("/source-stream")
            started = begin(1)
            assert begin(1)["rollout_id"] == started["rollout_id"]
            pump({"OBSERVING"})
            new_stream = stream("/candidate-stream")
            pump({"DRAINING"})
            assert len(live()) == 2 and child.poll() is None
            releases["/source-stream"].set()
            assert b"[DONE]" in old_stream.read()
            result = pump({"COMPLETED"})
            assert child.wait(timeout=10) == 0
            assert result["stable"]["artifact"] == artifacts[1]
            assert result["statistics"]["direct_ingress_counts"]["http"]["success"] >= 1, result
            current = control("/control/v1/identity")["pid"]
            assert live() == {current}, live()
            releases["/candidate-stream"].set()
            assert b"[DONE]" in new_stream.read()
            assert calls.count("/source-stream") == calls.count("/candidate-stream") == 1
            print("PASS 1 -> 2 -> 1, automatic canary, direct ingress samples, live SSE across owner commit", flush=True)

            begin(2)
            result = pump({"COMPLETED"})
            newest = control("/control/v1/identity")["pid"]
            assert newest != current and live() == {newest}, live()
            assert result["stable"]["artifact"] == artifacts[2]
            assert request(admin, "/admin/v1/routes", token)[0] == 200
            print("PASS independent B -> C with inherited public/control listeners and no third process", flush=True)

            begin(0)
            result = pump({"ROLLED_BACK"}, fail=True)
            assert result["stable"]["artifact"] == artifacts[2]
            assert control("/control/v1/identity")["pid"] == newest and live() == {newest}
            status, body = request(data, "/agents/probe/ok", caller, session="after-rollback")
            assert status == 200, (status, body, control("/control/v1/identity"), result)
            print("PASS failed candidate rolls back, source listeners resume and candidate exits", flush=True)

            releases["/source-stream"].clear()
            draining = stream("/source-stream")
            begin(0, dict(policy, drain_timeout_seconds=1))
            result = pump({"RECOVERY_REQUIRED"})
            assert result["resume_phase"] == "DRAINING" and len(live()) == 2, result
            rejected = control(method="POST", value={"artifact": artifacts[1], "policy": policy}, expected=409)
            assert rejected["error"] == "ROLLOUT_IN_PROGRESS" and len(live()) == 2
            releases["/source-stream"].set()
            assert b"[DONE]" in draining.read()
            control("/control/v1/rollout/resume", "POST")
            result = pump({"COMPLETED"})
            newest = control("/control/v1/identity")["pid"]
            assert result["stable"]["artifact"] == artifacts[0] and live() == {newest}
            rejected = control("/control/v1/rollout/rollback", "POST", expected=409)
            assert rejected["error"] == "ROLLBACK_AFTER_COMMIT"
            print("PASS drain timeout retains both processes, rejects third candidate, resumes to successful release", flush=True)

            short = dict(policy, verify_timeout_seconds=2, verify_min_seconds=0)
            begin(1, short)
            wait(lambda: control()["phase"] == "ROLLED_BACK", 15)
            assert live() == {newest}
            print("PASS missing samples never promote; background controller rolls back without CLI", flush=True)

            os.kill(newest, signal.SIGTERM)
            wait(lambda: not live(), 25)
            launcher = subprocess.Popen([artifacts[1], "launch", "--config", str(cfg),
                                         "--deployment", str(manifest), "--releases-dir", str(root / "releases"),
                                         "--unit="], stdout=log, stderr=log)
            children.append(launcher)
            identity = wait(lambda: control("/control/v1/identity"))
            assert identity["pid"] == launcher.pid and identity["release"] == Path(artifacts[0]).parent.name, identity
            assert live() == {launcher.pid}
            assert request(data, "/agents/probe/ok", caller, session="after-rollback")[0] == 200
            print("PASS read-only launch exec keeps PID, selects durable release and restores existing session", flush=True)
            log.flush();log.seek(0)
            assert "WARNING: DATA RACE" not in log.read(), "race detector reported a concurrency bug"
        except BaseException:
            log.flush();log.seek(0)
            print(log.read(), file=sys.stderr)
            raise
        finally:
            for event in releases.values():
                event.set()
            for conn in streams:
                conn.close()
            for pid in live():
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            try:
                wait(lambda: not live(), 25)
            except AssertionError:
                for pid in live():
                    os.kill(pid, signal.SIGKILL)
            for child in children:
                child.wait(timeout=10)
            log.close()
            upstream.shutdown()
            upstream.server_close()


if __name__ == "__main__":
    main(str(Path(sys.argv[1]).resolve()))
