#!/usr/bin/env python3
"""Real Hermes HTTP/SSE/A2A across a complete gateway rollout.

Uses the existing local model configuration for short synthetic conversations.
Creates two disposable Hermes containers with fresh data and credentials, and
an isolated gateway. Never starts or changes the existing local deployment.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import uuid

from complete import UnixHTTP
from check import port
from systemd_agent import LinuxGateway

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "deploy/hermes-local"))
import check_gateway as interop
from gateway import require
from manage import read_env, write_private

IMAGE = "nousresearch/hermes-agent@sha256:fca358f12efd65bfaaca05884166f15c0e2788375ca30d77061ac1ebc96452b7"


def wait(check, seconds=90):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        try:
            value = check()
            if value:
                return value
        except (OSError, RuntimeError):
            pass
        time.sleep(.2)
    raise RuntimeError("acceptance condition timed out")


def main(binary, report_path, systemd_image=None):
    containers, children = [], []
    endpoints, host_endpoints = [], []
    wall_started, mono_started = time.time(), time.monotonic()
    report = {"passed": False, "agent": IMAGE, "checks": {},
              "started_at": datetime.now(timezone.utc).isoformat(),
              "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "source_modified": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip())}

    def save():
        write_private(Path(report_path), json.dumps(report, indent=2) + "\n")

    def check(name, action):
        report["active_check"] = name
        save()
        print("Checking " + name, flush=True)
        result = action()
        report["checks"][name] = result
        save()
        print("PASS " + name, flush=True)
        return result

    with tempfile.TemporaryDirectory(prefix="gw-agent-", dir="/tmp") as tmp:
        root = Path(tmp)
        linux = LinuxGateway(root, systemd_image) if systemd_image else None
        log = (root / "gateway.log").open("w+")
        env = read_env()
        artifacts = []
        try:
            report["active_check"] = "setup"
            if linux:
                linux.start()
                artifacts = linux.build()
            for index in range(0 if linux else 2):
                source = Path(binary)
                if index:
                    source = root / "gateway-b"
                    subprocess.run(["go", "build", "-ldflags=-buildid=real-agent-b", "-o", str(source), "./cmd/gateway"], cwd=ROOT, check=True)
                raw = source.read_bytes()
                artifact = root / "releases" / hashlib.sha256(raw).hexdigest() / "gateway"
                artifact.parent.mkdir(parents=True)
                artifact.write_bytes(raw)
                artifact.chmod(0o755)
                artifacts.append(str(artifact))
            report["artifact_sha256"] = [Path(path).parent.name for path in artifacts]
            for index in range(2):
                name = "gateway-agent-test-" + uuid.uuid4().hex[:10]
                http_key, a2a_key = secrets.token_hex(24), secrets.token_hex(24)
                settings = dict(env, API_SERVER_KEY=http_key, A2A_PEER_TOKENS="gateway:" + a2a_key,
                                HERMES_UID="10000", HERMES_GID="10000", API_SERVER_ENABLED="true",
                                API_SERVER_HOST="0.0.0.0", API_SERVER_PORT="8642", A2A_HOST="0.0.0.0", A2A_PORT="9900",
                                A2A_PUBLIC_URL="http://127.0.0.1:9900/", TERMINAL_ENV="local", TERMINAL_CWD="/opt/data/workspace")
                private = root / ("agent-" + str(index) + ".env")
                write_private(private, "".join(k + "=" + v + "\n" for k, v in settings.items()))
                subprocess.run(["docker", "create", "--name", name, *(["--network", linux.network] if linux else []),
                                "--label", "aiagent-gateway.test=real-agent",
                                "--publish", "127.0.0.1::8642", "--publish", "127.0.0.1::9900", "--env-file", str(private),
                                IMAGE, "gateway", "run"], check=True, stdout=subprocess.DEVNULL)
                containers.append(name)
                subprocess.run(["docker", "cp", str(ROOT / "deploy/hermes-local/.runtime/home/config.yaml"), name + ":/opt/data/config.yaml"], check=True)
                subprocess.run(["docker", "start", name], check=True, stdout=subprocess.DEVNULL)
                ports = json.loads(subprocess.check_output(["docker", "inspect", "--format", "{{json .NetworkSettings.Ports}}", name], text=True))
                http_url = "http://127.0.0.1:" + ports["8642/tcp"][0]["HostPort"]
                a2a_url = "http://127.0.0.1:" + ports["9900/tcp"][0]["HostPort"]
                wait(lambda: require(http_url + "/health", timeout=3), 180)
                wait(lambda: require(a2a_url + "/.well-known/agent-card.json", a2a_key, timeout=3), 180)
                host_endpoints.append({"name": "isolated-" + str(index), "interfaces": {
                    "proxy": {"url": http_url, "token": http_key, "health_url": http_url + "/health"},
                    "a2a": {"url": a2a_url, "token": a2a_key, "protocol_version": "1.0", "health_url": a2a_url + "/health"}}})
                endpoint = json.loads(json.dumps(host_endpoints[-1]))
                if linux:
                    for profile, number in [("proxy", 8642), ("a2a", 9900)]:
                        endpoint["interfaces"][profile]["url"] = f"http://{name}:{number}"
                        endpoint["interfaces"][profile]["health_url"] = f"http://{name}:{number}/health"
                endpoints.append(endpoint)
            print("PASS two isolated real Hermes instances are ready", flush=True)
            data, admin = (linux.data_port, linux.admin_port) if linux else (port(), port())
            interop.DATA_URL, interop.ADMIN_URL = f"http://127.0.0.1:{data}", f"http://127.0.0.1:{admin}"
            creds = {"admin_token": secrets.token_hex(24), "callers": [{"id": "alice", "token": secrets.token_hex(24)}, {"id": "bob", "token": secrets.token_hex(24)}]}
            cfg = root / "config.yaml"
            settings = {"admin_token": creds["admin_token"], "data_dir": str(root / "data"), "port": data,
                        "admin_port": admin, "public_url": interop.DATA_URL,
                        "state_socket": str(root / "state.sock"), "control_socket": str(root / "control.sock")}
            if linux:
                settings.update(data_dir="/var/lib/aiagent-gateway", port=8080, admin_port=9090,
                                state_socket="/run/aiagent-gateway/state.sock",
                                control_socket="/run/aiagent-gateway-control/controller.sock")
            write_private(cfg, json.dumps(settings))
            policy = {"auto_profiles": False, "canary_percent": 50, "readiness_timeout_seconds": 15, "verify_min_seconds": 0,
                      "verify_timeout_seconds": 300, "min_success_per_profile": 1, "max_effective_failures": 0,
                      "required_profiles": ["http", "sse", "a2a", "a2a_sse"], "observe_seconds": 2,
                      "observe_min_success_per_profile": 1, "drain_timeout_seconds": 60}
            if linux:
                linux.install(cfg, artifacts, policy)
            else:
                manifest = root / "deployment.json"
                write_private(manifest, json.dumps({"initial_release": artifacts[0], "policy": policy}))
                child = subprocess.Popen([artifacts[0], "launch", "--config", str(cfg), "--deployment", str(manifest),
                                          "--releases-dir", str(root / "releases"), "--unit="], stdout=log, stderr=log)
                children.append(child)

            def control(method="GET", body=None, path="/control/v1/rollout"):
                if linux:
                    return linux.control(method, body, path)
                conn = UnixHTTP(root / "control.sock")
                try:
                    conn.request(method, path, None if body is None else json.dumps(body), {"Authorization": "Bearer " + creds["admin_token"]})
                    response = conn.getresponse()
                    if response.status != 200:
                        raise RuntimeError("control HTTP " + str(response.status))
                    return json.loads(response.read())
                finally:
                    conn.close()

            wait(lambda: control()["phase"] == "COMPLETED")
            if linux:
                report["checks"]["systemd_before"] = before_systemd = linux.stable(artifacts[0])
            a2a = host_endpoints[0]["interfaces"]["a2a"]
            card = require(a2a["url"] + "/.well-known/agent-card.json", a2a["token"])
            requested = {"key": "hermes-local", "agent_type": "hermes", "expose": ["proxy", "a2a"],
                         "consumers": creds["callers"], "endpoints": endpoints,
                         "agent_card": {k: card[k] for k in ("name", "description", "version", "skills", "defaultInputModes", "defaultOutputModes")},
                         "timeout": {"response_header_ms": 180000, "stream_idle_ms": 120000, "max_duration_ms": 300000}}
            requested["agent_card"]["capabilities"] = {"streaming": True}
            route = require(interop.ADMIN_URL + "/admin/v1/routes", creds["admin_token"], requested)
            check("preflight", lambda: interop.preflight(creds))
            before_http = check("before_http", lambda: interop.http_sessions(creds, uuid.uuid4().hex[:12]))
            # Record only protocol structure when a live upstream yields an
            # empty completed task; never include model text or credentials.
            original_rpc = interop.rpc
            def checked_rpc(token, method, params):
                result = original_rpc(token, method, params)
                if method == "SendMessage" and result.get("task") and not interop.task_text(result["task"]):
                    task = result["task"]
                    report["empty_task"] = {"keys": list(task), "status_keys": list(task.get("status", {})),
                        "artifact_count": len(task.get("artifacts", [])), "history_count": len(task.get("history", []))}
                return result
            interop.rpc = checked_rpc
            before_a2a = check("before_a2a", lambda: interop.a2a_context(creds))
            print("PASS real HTTP/SSE/A2A, conversation memory, pagination and caller isolation before rollout", flush=True)
            report["active_check"] = "missing_samples_rollback"
            control("POST", {"artifact": artifacts[1], "policy": dict(policy, verify_timeout_seconds=2)})
            rejected = wait(lambda: r if (r := control())["phase"] == "ROLLED_BACK" else None, 30)
            assert rejected["stable"]["artifact"] == artifacts[0] and not rejected["committed"]
            if linux:
                after_rollback = linux.stable(artifacts[0], restarts=before_systemd["restarts"])
                assert after_rollback["main_pid"] == before_systemd["main_pid"]
            else:
                assert child.poll() is None
            report["checks"]["missing_samples_rollback"] = {"phase": rejected["phase"], "reason": rejected["reason"],
                                                           "source_preserved": True, "committed": False}
            save()
            print("PASS missing samples roll back before retrying the real-agent rollout", flush=True)
            control("POST", {"artifact": artifacts[1], "policy": policy})
            report["active_check"] = "rollout_start"
            wait(lambda: control()["phase"] == "VERIFYING")
            for index in range(12):
                current = control()
                report["last_rollout"] = {key: current.get(key) for key in ("phase", "committed", "statistics")}
                if current["phase"] == "COMPLETED":
                    break
                assert current["phase"] not in ("FAILED", "ROLLED_BACK", "ROLLING_BACK"), current["phase"]
                if current["phase"] == "RECOVERY_REQUIRED":
                    assert current["resume_phase"] == "OBSERVING"
                    # A paused controller publishes its last persisted sample;
                    # it does not refresh counts until Resume. Collect all four
                    # profiles on direct ingress once, then resume explicitly.
                    check(f"direct_http_{index}", lambda: interop.http_sessions(creds, uuid.uuid4().hex[:12]))
                    check(f"direct_a2a_{index}", lambda: interop.a2a_context(creds))
                    control("POST", path="/control/v1/rollout/resume")
                    report["observation_resumes"] = report.get("observation_resumes", 0) + 1
                    current = wait(lambda: r if (r := control())["phase"] in
                                   ("COMPLETED", "RECOVERY_REQUIRED", "ROLLED_BACK", "FAILED") else None)
                    assert current["phase"] == "COMPLETED", current["phase"]
                    break
                statistics = current.get("statistics", {})
                counts = statistics.get("direct_ingress_counts" if current["phase"] == "OBSERVING" else "counts", {})
                def missing(profile):
                    return counts.get(profile, {}).get("success", 0) < 1
                if missing("http") or missing("sse"):
                    check(f"rollout_http_{index}", lambda: interop.http_sessions(creds, uuid.uuid4().hex[:12]))
                elif missing("a2a") or missing("a2a_sse"):
                    check(f"rollout_a2a_{index}", lambda: interop.a2a_context(creds))
                else:
                    time.sleep(1)
            report["active_check"] = "rollout_completion"
            current = wait(lambda: r if (r := control())["phase"] == "COMPLETED" else None)
            if linux:
                report["checks"]["systemd_after"] = linux.stable(artifacts[1], previous=before_systemd["main_pid"],
                                                                 restarts=before_systemd["restarts"])
            else:
                assert child.wait(timeout=10) == 0
            assert current["stable"]["artifact"] == artifacts[1] and current["committed"]
            report["last_rollout"] = {key: current.get(key) for key in ("phase", "committed", "statistics")}
            report["checks"]["rollout"] = {"statistics": current["statistics"], "snapshot_bytes": current["snapshot_bytes"], "handoff_milliseconds": current["handoff_milliseconds"]}
            report["active_check"] = "existing_resources_after_handoff"
            token, other = [c["token"] for c in creds["callers"]]
            session = before_http["session_id"]
            history = require(interop.DATA_URL + "/agents/hermes-local/api/sessions/" + session + "/messages?limit=100&order=latest", token,
                              headers={"X-Hermes-Session-Id": session})
            assert len(history["data"]) == before_http["history_messages"]
            for task in before_a2a["task_ids"]:
                existing = interop.rpc(token, "GetTask", {"id": task})
                assert existing["id"] == task and existing["contextId"] == before_a2a["context_id"]
            assert interop.rpc(other, "ListTasks", {"contextId": before_a2a["context_id"]})["tasks"] == []
            for ep in route["endpoints"]:
                detail = require(interop.ADMIN_URL + "/admin/v1/routes/" + route["id"] + "/endpoints/" + ep["id"], creds["admin_token"])
                assert detail["runtime"]["http_inflight"] == 0
            report["checks"]["after"] = {"http_history_preserved": True, "a2a_identity_preserved": True, "caller_isolation": True, "inflight_zero": True}
            if linux:
                report["active_check"] = "systemd_reboot"
                fixture = linux.guest("cleanup_fixture", binary=artifacts[1])
                linux.reboot()
                report["checks"]["systemd_reboot"] = linux.stable(artifacts[1])
                report["checks"]["cleanup"] = linux.guest("cleanup_verify", fixture=fixture)
                history = require(interop.DATA_URL + "/agents/hermes-local/api/sessions/" + session + "/messages?limit=100&order=latest", token,
                                  headers={"X-Hermes-Session-Id": session})
                assert len(history["data"]) == before_http["history_messages"]
                for task in before_a2a["task_ids"]:
                    existing = interop.rpc(token, "GetTask", {"id": task})
                    assert existing["id"] == task and existing["contextId"] == before_a2a["context_id"]
                assert interop.rpc(other, "ListTasks", {"contextId": before_a2a["context_id"]})["tasks"] == []
                report["checks"]["after_reboot"] = {"http_history_preserved": True, "a2a_identity_preserved": True, "caller_isolation": True}
            report["passed"] = True
            print("PASS real-agent rollout: all four profiles and direct ingress samples; existing resources survive owner handoff", flush=True)
        except BaseException as exc:
            report["error_type"] = type(exc).__name__
            report["failed_check"] = report.get("active_check", "setup")
            if isinstance(exc, interop.CheckFailure):
                report["check_error"] = str(exc)
                report["diagnostics"] = exc.diagnostics
                if "a2a" in report["failed_check"]:
                    report["upstream_task_diagnostics"] = []
                    for ep in host_endpoints:
                        iface = ep["interfaces"]["a2a"]
                        diagnostic = {"endpoint": ep["name"]}
                        try:
                            result = require(iface["url"], iface["token"], {
                                "jsonrpc": "2.0", "id": uuid.uuid4().hex, "method": "ListTasks",
                                "params": {"pageSize": 20, "includeArtifacts": True}}, timeout=5)
                            diagnostic["tasks"] = [interop.task_summary(task) for task in result.get("result", {}).get("tasks", [])]
                        except Exception as diagnostic_error:
                            diagnostic["error_type"] = type(diagnostic_error).__name__
                        report["upstream_task_diagnostics"].append(diagnostic)
            # Provider responses and gateway logs can contain conversation data.
            # Preserve only the exception type in the portable report.
            raise
        finally:
            wall, elapsed = time.time() - wall_started, time.monotonic() - mono_started
            report["timing"] = {"wall_seconds": round(wall, 3), "monotonic_seconds": round(elapsed, 3),
                                "clock_difference_seconds": round(wall - elapsed, 3)}
            report["finished_at"] = datetime.now(timezone.utc).isoformat()
            report.pop("active_check", None)
            save()
            def owned_pids():
                rows = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
                return [int(parts[0]) for row in rows.splitlines()
                        if len(parts := row.strip().split(None, 1)) == 2
                        and parts[1].startswith(str(root / "releases") + "/")]
            for pid in owned_pids():
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            end = time.monotonic() + 30
            while owned_pids() and time.monotonic() < end:
                time.sleep(.1)
            # A committed candidate is no longer our direct subprocess. Recheck
            # its unique executable path before escalation and directory cleanup.
            for pid in owned_pids():
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            for child in children:
                try:
                    child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=10)
            for name in containers:
                subprocess.run(["docker", "rm", "--force", name], check=False, stdout=subprocess.DEVNULL)
            if linux:
                linux.close()
            log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", nargs="?", help="local gateway binary; omitted with --systemd")
    parser.add_argument("--systemd", action="store_true", help="build and install the gateway in an isolated Linux/systemd container")
    parser.add_argument("--systemd-image", default="aiagent-gateway-systemd-test:bookworm")
    parser.add_argument("--report", default="/tmp/gateway-real-agent-rollout.json")
    args = parser.parse_args()
    if not args.systemd and not args.binary:
        parser.error("binary is required unless --systemd is selected")
    main(str(Path(args.binary).resolve()) if args.binary else None, args.report, args.systemd_image if args.systemd else None)
