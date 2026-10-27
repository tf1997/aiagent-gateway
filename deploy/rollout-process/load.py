#!/usr/bin/env python3
"""Concurrent HTTP and SSE through complete gateway upgrades on Linux.

Builds three real artifacts and runs in an isolated, non-root Docker container
with loopback networking only. Uses a synthetic upstream, no model calls.
"""
import argparse
from collections import Counter
import concurrent.futures
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import platform
import secrets
import signal
import socket
import subprocess
import tempfile
import threading
import time
import uuid


def quantiles(values):
    ordered = sorted(values)
    return {"samples": len(ordered), **{
        name: round(ordered[int((len(ordered) - 1) * q)], 3)
        for name, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))
    }} if ordered else {"samples": 0}


def wait(fn, timeout=60):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            value = fn()
            if value:
                return value
        except (OSError, http.client.HTTPException):
            pass
        time.sleep(.05)
    raise AssertionError("condition timed out")


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path, timeout=10):
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def inside(args):
    report = {"passed": False, "concurrency": args.concurrency, "stages": [],
              "scope": "closed-loop clients, synthetic upstream, Linux container; not production capacity",
              "environment": {"system": platform.platform(), "cpus": os.cpu_count(), "uid": os.geteuid()},
              "cycles": args.cycles, "profiles": args.profiles, "sessions_per_client": args.sessions_per_client,
              "session_policy": "new session per request" if not args.sessions_per_client else
                                "persistent client pools; separate pool per release during VERIFYING to obtain candidate samples",
              "baseline_seconds": args.baseline_seconds,
              "observe_seconds": args.observe_seconds,
              "upstream_http_ms": args.http_ms, "upstream_sse_ms": args.sse_ms}
    wall_started, mono_started = time.time(), time.monotonic()
    calls, active_upstream, peak_upstream = Counter(), 0, 0
    call_lock = threading.Lock()

    class Upstream(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            nonlocal active_upstream, peak_upstream
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            with call_lock:
                calls[self.headers["X-Load-Request"]] += 1
                active_upstream += 1
                peak_upstream = max(peak_upstream, active_upstream)
            try:
                sse = self.path == "/sse"
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream" if sse else "application/json")
                self.end_headers()
                if sse:
                    self.wfile.write(b'data: {"started":true}\n\n')
                    self.wfile.flush()
                    time.sleep(args.sse_ms / 1000)
                    self.wfile.write(b"data: [DONE]\n\n")
                else:
                    time.sleep(args.http_ms / 1000)
                    self.wfile.write(b'{"ok":true}')
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with call_lock:
                    active_upstream -= 1

    class Server(http.server.ThreadingHTTPServer):
        request_queue_size = 256
        daemon_threads = True

    upstream = Server(("127.0.0.1", 0), Upstream)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    with tempfile.TemporaryDirectory(prefix="gw-load-", dir="/tmp") as tmp:
        root = Path(tmp)
        artifacts = []
        for source in args.artifacts:
            raw = Path(source).read_bytes()
            artifact = root / "releases" / hashlib.sha256(raw).hexdigest() / "gateway"
            artifact.parent.mkdir(parents=True)
            artifact.write_bytes(raw)
            artifact.chmod(0o755)
            artifacts.append(str(artifact))
        report["artifact_sha256"] = [Path(path).parent.name for path in artifacts]

        def free_port():
            with socket.socket() as s:
                s.bind(("127.0.0.1", 0))
                return s.getsockname()[1]
        data, admin = free_port(), free_port()
        token, caller = secrets.token_hex(24), secrets.token_hex(24)
        config = root / "config.yaml"
        config.write_text(json.dumps({"admin_token": token, "data_dir": str(root / "data"),
                                     "state_socket": str(root / "state.sock"), "control_socket": str(root / "control.sock"),
                                     "port": str(data), "admin_port": str(admin)}))
        config.chmod(0o600)
        policy = {"auto_profiles": False, "canary_percent": 5, "readiness_timeout_seconds": 15,
                  "verify_min_seconds": 2, "verify_timeout_seconds": 30, "min_success_per_profile": 5,
                  "max_effective_failures": 0, "required_profiles": ["http"], "observe_seconds": args.observe_seconds,
                  "observe_min_success_per_profile": 5, "drain_timeout_seconds": 30}
        manifest = root / "deployment.json"
        manifest.write_text(json.dumps({"initial_release": artifacts[0], "policy": policy}))

        def control(path="/control/v1/rollout", method="GET", value=None):
            conn = UnixHTTP(str(root / "control.sock"), timeout=10)
            try:
                conn.request(method, path, None if value is None else json.dumps(value),
                             {"Authorization": "Bearer " + token})
                response = conn.getresponse()
                result = json.loads(response.read())
                assert response.status == 200, result
                return result
            finally:
                conn.close()

        def pids():
            found = []
            for path in Path("/proc").iterdir():
                if path.name.isdigit():
                    try:
                        if os.readlink(path / "exe").startswith(str(root / "releases") + "/"):
                            found.append(int(path.name))
                    except OSError:
                        pass
            return found

        stop_sampling = threading.Event()
        metrics = {"peak_rss_kib": 0, "peak_fds": 0, "peak_service_pids": 0, "cpu_seconds": 0., "peak_cpu_percent": 0.}
        def sample():
            ticks, previous = {}, time.monotonic()
            hz = os.sysconf("SC_CLK_TCK")
            while not stop_sampling.wait(.1):
                now, rss, fds, used = time.monotonic(), 0, 0, 0.
                active = pids()
                for pid in active:
                    try:
                        proc = Path("/proc") / str(pid)
                        fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
                        cpu = (int(fields[11]) + int(fields[12])) / hz
                        used += max(0, cpu - ticks.get(pid, 0))
                        ticks[pid] = cpu
                        for line in (proc / "status").read_text().splitlines():
                            if line.startswith("VmRSS:"):
                                rss += int(line.split()[1])
                        fds += len(list((proc / "fd").iterdir()))
                    except (OSError, ValueError):
                        pass
                metrics["cpu_seconds"] += used
                metrics["peak_cpu_percent"] = max(metrics["peak_cpu_percent"], used / (now - previous) * 100)
                metrics["peak_rss_kib"] = max(metrics["peak_rss_kib"], rss)
                metrics["peak_fds"] = max(metrics["peak_fds"], fds)
                metrics["peak_service_pids"] = max(metrics["peak_service_pids"], len(active))
                previous = now
        sampler = threading.Thread(target=sample, daemon=True)
        log = (root / "gateway.log").open("w+")
        child = None
        try:
            child = subprocess.Popen([artifacts[0], "process", "--config", str(config), "--deployment", str(manifest),
                                      "--releases-dir", str(root / "releases")], stdout=log, stderr=log)
            wait(lambda: control()["phase"] == "COMPLETED")
            conn = http.client.HTTPConnection("127.0.0.1", admin, timeout=10)
            conn.request("POST", "/admin/v1/routes", json.dumps({"key": "probe", "protocol": "proxy",
                         "consumers": [{"id": "load", "token": caller}],
                         "endpoints": [{"url": f"http://127.0.0.1:{upstream.server_port}"}]}),
                         {"Authorization": "Bearer " + token})
            response = conn.getresponse()
            assert response.status == 201, response.read()
            response.read()
            conn.close()
            sampler.start()
            for index, profile in enumerate(args.profiles * args.cycles, 1):
                stop = threading.Event()
                lock = threading.Lock()
                gate = threading.Barrier(args.concurrency + 1)
                latencies, first_bytes, handoff_latencies = [], [], []
                profile_latencies = {name: [] for name in ("http", "sse")}
                statuses, errors, samples = Counter(), [], 0
                current_phase = "BASELINE"
                phases = {}
                began = time.monotonic()
                def worker(number):
                    nonlocal samples
                    gate.wait()
                    sequence = 0
                    request_profile = ("http" if number % 2 == 0 else "sse") if profile == "mixed" else profile
                    while not stop.is_set():
                        sequence += 1
                        rid = f"{index}-{request_profile}-{number}-{sequence}"
                        pool = f"verify-{index}" if current_phase == "VERIFYING" else "pool"
                        session = (f"{pool}-{request_profile}-{number}-{(sequence - 1) % args.sessions_per_client}"
                                   if args.sessions_per_client else rid)
                        phase, start, first, status, error = current_phase, time.monotonic(), None, 0, None
                        conn = http.client.HTTPConnection("127.0.0.1", data, timeout=30)
                        try:
                            conn.request("POST", "/agents/probe/" + ("sse" if request_profile == "sse" else "ok"), b"{}",
                                         {"Authorization": "Bearer " + caller, "X-Session-ID": session, "X-Load-Request": rid})
                            response = conn.getresponse()
                            status = response.status
                            prefix = response.readline() if request_profile == "sse" and status == 200 else b""
                            first = (time.monotonic() - start) * 1000
                            body = prefix + response.read()
                            if status != 200 or (request_profile == "sse" and b"data: [DONE]" not in body) or (request_profile == "http" and body != b'{"ok":true}'):
                                error = body[:250].decode("utf-8", "replace")
                        except (OSError, http.client.HTTPException) as exc:
                            error = type(exc).__name__ + ": " + str(exc)
                        finally:
                            conn.close()
                        elapsed = (time.monotonic() - start) * 1000
                        with lock:
                            latencies.append(elapsed)
                            profile_latencies[request_profile].append(elapsed)
                            if first is not None:
                                first_bytes.append(first)
                            if phase == "HANDOFF":
                                handoff_latencies.append(elapsed)
                            statuses[str(status)] += 1
                            if error is not None:
                                samples += 1
                                if len(errors) < 10:
                                    errors.append({"phase": phase, "status": status, "message": error})
                with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                    futures = [pool.submit(worker, number) for number in range(args.concurrency)]
                    gate.wait()
                    try:
                        wait(lambda: len(latencies) >= args.concurrency, 45)
                        time.sleep(args.baseline_seconds)
                        required = ["http", "sse"] if profile == "mixed" else [profile]
                        rollout = control(method="POST", value={"artifact": artifacts[index % len(artifacts)],
                                          "policy": dict(policy, required_profiles=required)})
                        deadline = time.monotonic() + 90
                        while time.monotonic() < deadline:
                            rollout = control()
                            current_phase = rollout["phase"]
                            phases.setdefault(current_phase, time.monotonic() - began)
                            if current_phase in ("COMPLETED", "RECOVERY_REQUIRED", "ROLLED_BACK", "FAILED"):
                                break
                            time.sleep(.05)
                        time.sleep(args.baseline_seconds)
                    finally:
                        stop.set()
                        for future in futures:
                            future.result()
                stage = {"index": index, "profile": profile, "requests": len(latencies), "failures": samples, "statuses": dict(statuses),
                         "duration_seconds": round(time.monotonic() - began, 3), "latency_ms": quantiles(latencies),
                         "profile_latency_ms": {name: quantiles(values) for name, values in profile_latencies.items() if values},
                         "first_response_ms": quantiles(first_bytes), "handoff_request_ms": quantiles(handoff_latencies),
                         "errors": errors, "phase_times_seconds": phases,
                         "rollout": {key: rollout.get(key) for key in ("phase", "reason", "stable", "statistics", "snapshot_bytes", "handoff_milliseconds", "state_status", "state_queue", "allocation_queue")}}
                stage["requests_per_second"] = round(stage["requests"] / stage["duration_seconds"], 3)
                stage["candidate_pause_ms"] = {
                    name: {"last": rollout.get(name, {}).get("LastPauseMicros", 0) / 1000,
                           "max": rollout.get(name, {}).get("MaxPauseMicros", 0) / 1000,
                           "count": rollout.get(name, {}).get("PauseCount", 0)}
                    for name in ("state_queue", "allocation_queue")}
                report["stages"].append(stage)
                print(json.dumps(stage), flush=True)
                assert rollout["phase"] == "COMPLETED", "rollout did not complete under load"
                assert samples == 0, "requests failed under load"
                for name in ("state_status", "state_queue", "allocation_queue"):
                    assert not rollout[name]["Timeouts"] and not rollout[name]["Rejected"], name + " exhausted its wait budget"
                wait(lambda: len(pids()) == 1)
                stable_pid = pids()[0]
                stage["stable_resources"] = {
                    "rss_kib": int(next(line.split()[1] for line in Path(f"/proc/{stable_pid}/status").read_text().splitlines()
                                        if line.startswith("VmRSS:"))),
                    "fds": len(list(Path(f"/proc/{stable_pid}/fd").iterdir())),
                }
            report["passed"] = True
        except BaseException as exc:
            report["error"] = type(exc).__name__ + ": " + str(exc)
            raise
        finally:
            stop_sampling.set()
            if sampler.ident is not None:
                sampler.join(timeout=5)
            report["resources"] = {k: round(v, 3) if isinstance(v, float) else v for k, v in metrics.items()}
            wall, elapsed = time.time() - wall_started, time.monotonic() - mono_started
            report["timing"] = {"wall_seconds": round(wall, 3), "monotonic_seconds": round(elapsed, 3),
                                "clock_difference_seconds": round(wall - elapsed, 3)}
            with call_lock:
                report["upstream_requests"] = sum(calls.values())
                report["duplicate_upstream_requests"] = sum(n - 1 for n in calls.values())
                report["peak_upstream_requests"] = peak_upstream
            if (metrics["peak_service_pids"] > 2 or report["duplicate_upstream_requests"]
                    or report["upstream_requests"] != sum(stage["requests"] for stage in report["stages"])):
                report["passed"] = False
            Path(args.report).write_text(json.dumps(report, indent=2) + "\n")
            for pid in pids():
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            try:
                wait(lambda: not pids(), 35)
            except AssertionError:
                for pid in pids():
                    os.kill(pid, signal.SIGKILL)
            if child is not None:
                child.wait(timeout=10)
            log.close()
            upstream.shutdown()
            upstream.server_close()
        assert report["passed"], "process budget or upstream execution count check failed"


def host(args):
    repo = Path(__file__).resolve().parents[2]
    arch = subprocess.check_output(["docker", "info", "--format", "{{.Architecture}}"], text=True).strip()
    goarch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}[arch]
    name = "gateway-load-" + uuid.uuid4().hex[:10]
    wall_started, mono_started = time.time(), time.monotonic()
    with tempfile.TemporaryDirectory(prefix="gateway-load-build-") as tmp:
        stage = Path(tmp)
        for version in ("a", "b", "c"):
            subprocess.run(["go", "build", "-ldflags=-buildid=load-" + version, "-o", str(stage / version), "./cmd/gateway"],
                           cwd=repo, env=dict(os.environ, GOOS="linux", GOARCH=goarch, CGO_ENABLED="0"), check=True)
        try:
            subprocess.run(["docker", "create", "--name", name, "--init", "--network", "none", "--user", "65534:65534",
                            "--entrypoint", "python3", args.image, "/load.py", "--inside", "--artifacts", "/a", "/b", "/c",
                            "--concurrency", str(args.concurrency), "--baseline-seconds", str(args.baseline_seconds),
                            "--observe-seconds", str(args.observe_seconds),
                            "--cycles", str(args.cycles), "--profiles", *args.profiles,
                            "--http-ms", str(args.http_ms), "--sse-ms", str(args.sse_ms),
                            "--sessions-per-client", str(args.sessions_per_client),
                            "--report", "/tmp/report.json"], check=True, stdout=subprocess.DEVNULL)
            for path in [Path(__file__), *(stage / name for name in ("a", "b", "c"))]:
                subprocess.run(["docker", "cp", str(path), name + ":/" + path.name], check=True)
            result = subprocess.run(["docker", "start", "-a", name])
            subprocess.run(["docker", "cp", name + ":/tmp/report.json", args.report], check=True)
            report = json.loads(Path(args.report).read_text())
            wall, elapsed = time.time() - wall_started, time.monotonic() - mono_started
            report["host_timing"] = {"wall_seconds": round(wall, 3), "monotonic_seconds": round(elapsed, 3),
                                     "clock_difference_seconds": round(wall - elapsed, 3)}
            report["source_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
            report["source_modified"] = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True).strip())
            Path(args.report).write_text(json.dumps(report, indent=2) + "\n")
            result.check_returncode()
        finally:
            subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--baseline-seconds", type=float, default=2)
    parser.add_argument("--observe-seconds", type=int, default=3, help="Observation window; allow full stream responses to finish")
    parser.add_argument("--cycles", type=int, default=1, help="Repeat the selected profile sequence on the same state directory")
    parser.add_argument("--profiles", choices=("http", "sse", "mixed"), nargs="+", default=["http", "sse"],
                        help="Mixed assigns alternating clients to HTTP/SSE at the same time")
    parser.add_argument("--http-ms", type=float, default=50)
    parser.add_argument("--sse-ms", type=float, default=250)
    parser.add_argument("--sessions-per-client", type=int, default=0,
                        help="Reuse a pool across releases, plus a per-release verification pool; 0 uses new sessions per request")
    parser.add_argument("--report", default="/tmp/gateway-concurrent-rollout.json")
    parser.add_argument("--image", default="aiagent-gateway-systemd-test:bookworm")
    parser.add_argument("--inside", action="store_true")
    parser.add_argument("--artifacts", nargs=3)
    args = parser.parse_args()
    if args.concurrency < 1 or args.cycles < 1 or args.observe_seconds < 1 or args.sessions_per_client < 0 or min(args.baseline_seconds, args.http_ms, args.sse_ms) < 0:
        parser.error("concurrency/cycles must be positive; durations/session pool must be nonnegative")
    if "mixed" in args.profiles and args.concurrency < 2:
        parser.error("mixed requires at least two clients")
    if args.inside and not args.artifacts:
        parser.error("--inside requires three --artifacts")
    (inside if args.inside else host)(args)
