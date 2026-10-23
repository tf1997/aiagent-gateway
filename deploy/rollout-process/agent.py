#!/usr/bin/env python3
"""Real Hermes HTTP/SSE/A2A across a complete gateway rollout.

Uses the existing local model configuration for short synthetic conversations.
Creates two disposable Hermes containers with fresh data and credentials, and
an isolated gateway. Never starts or changes the existing local deployment.
"""
import argparse
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


def main(binary, report_path):
    containers, children = [], []
    report = {"passed": False, "agent": IMAGE, "checks": {}}
    with tempfile.TemporaryDirectory(prefix="gw-agent-", dir="/tmp") as tmp:
        root = Path(tmp)
        log = (root / "gateway.log").open("w+")
        env = read_env()
        artifacts = []
        try:
            for index in range(2):
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
            endpoints = []
            for index in range(2):
                name = "gateway-agent-test-" + uuid.uuid4().hex[:10]
                http_key, a2a_key = secrets.token_hex(24), secrets.token_hex(24)
                settings = dict(env, API_SERVER_KEY=http_key, A2A_PEER_TOKENS="gateway:" + a2a_key,
                                HERMES_UID="10000", HERMES_GID="10000", API_SERVER_ENABLED="true",
                                API_SERVER_HOST="0.0.0.0", API_SERVER_PORT="8642", A2A_HOST="0.0.0.0", A2A_PORT="9900",
                                A2A_PUBLIC_URL="http://127.0.0.1:9900/", TERMINAL_ENV="local", TERMINAL_CWD="/opt/data/workspace")
                private = root / ("agent-" + str(index) + ".env")
                write_private(private, "".join(k + "=" + v + "\n" for k, v in settings.items()))
                subprocess.run(["docker", "create", "--name", name, "--label", "aiagent-gateway.test=real-agent",
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
                endpoints.append({"name": "isolated-" + str(index), "interfaces": {
                    "proxy": {"url": http_url, "token": http_key, "health_url": http_url + "/health"},
                    "a2a": {"url": a2a_url, "token": a2a_key, "protocol_version": "1.0", "health_url": a2a_url + "/health"}}})
            print("PASS two isolated real Hermes instances are ready", flush=True)
            data, admin = port(), port()
            interop.DATA_URL, interop.ADMIN_URL = f"http://127.0.0.1:{data}", f"http://127.0.0.1:{admin}"
            creds = {"admin_token": secrets.token_hex(24), "callers": [{"id": "alice", "token": secrets.token_hex(24)}, {"id": "bob", "token": secrets.token_hex(24)}]}
            cfg = root / "config.yaml"
            write_private(cfg, json.dumps({"admin_token": creds["admin_token"], "data_dir": str(root / "data"), "port": data,
                "admin_port": admin, "public_url": interop.DATA_URL, "state_socket": str(root / "state.sock"), "control_socket": str(root / "control.sock")}))
            policy = {"auto_profiles": False, "canary_percent": 50, "readiness_timeout_seconds": 15, "verify_min_seconds": 0,
                      "verify_timeout_seconds": 300, "min_success_per_profile": 1, "max_effective_failures": 0,
                      "required_profiles": ["http", "sse", "a2a", "a2a_sse"], "observe_seconds": 2,
                      "observe_min_success_per_profile": 1, "drain_timeout_seconds": 60}
            manifest = root / "deployment.json"
            write_private(manifest, json.dumps({"initial_release": artifacts[0], "policy": policy}))
            child = subprocess.Popen([artifacts[0], "launch", "--config", str(cfg), "--deployment", str(manifest),
                                      "--releases-dir", str(root / "releases"), "--unit="], stdout=log, stderr=log)
            children.append(child)

            def control(method="GET", body=None, path="/control/v1/rollout"):
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
            a2a = endpoints[0]["interfaces"]["a2a"]
            card = require(a2a["url"] + "/.well-known/agent-card.json", a2a["token"])
            requested = {"key": "hermes-local", "agent_type": "hermes", "expose": ["proxy", "a2a"],
                         "consumers": creds["callers"], "endpoints": endpoints,
                         "agent_card": {k: card[k] for k in ("name", "description", "version", "skills", "defaultInputModes", "defaultOutputModes")},
                         "timeout": {"response_header_ms": 180000, "stream_idle_ms": 120000, "max_duration_ms": 300000}}
            requested["agent_card"]["capabilities"] = {"streaming": True}
            route = require(interop.ADMIN_URL + "/admin/v1/routes", creds["admin_token"], requested)
            report["checks"]["preflight"] = interop.preflight(creds)
            before_http = interop.http_sessions(creds, uuid.uuid4().hex[:12])
            report["checks"]["before_http"] = before_http
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
            before_a2a = interop.a2a_context(creds)
            report["checks"]["before_a2a"] = before_a2a
            print("PASS real HTTP/SSE/A2A, conversation memory, pagination and caller isolation before rollout", flush=True)
            control("POST", {"artifact": artifacts[1], "policy": policy})
            wait(lambda: control()["phase"] == "VERIFYING")
            for index in range(12):
                current = control()
                if current["phase"] == "COMPLETED":
                    break
                assert current["phase"] not in ("FAILED", "ROLLED_BACK", "ROLLING_BACK"), current["phase"]
                statistics = current.get("statistics", {})
                counts = statistics.get("direct_ingress_counts" if current["phase"] in ("OBSERVING", "RECOVERY_REQUIRED") else "counts", {})
                def missing(profile):
                    return counts.get(profile, {}).get("success", 0) < 1
                if missing("http") or missing("sse"):
                    interop.http_sessions(creds, uuid.uuid4().hex[:12])
                elif missing("a2a") or missing("a2a_sse"):
                    interop.a2a_context(creds)
                elif current["phase"] == "RECOVERY_REQUIRED":
                    assert current["resume_phase"] == "OBSERVING"
                    control("POST", path="/control/v1/rollout/resume")
                else:
                    time.sleep(1)
            current = wait(lambda: r if (r := control())["phase"] == "COMPLETED" else None)
            assert child.wait(timeout=10) == 0
            assert current["stable"]["artifact"] == artifacts[1] and current["committed"]
            report["checks"]["rollout"] = {"statistics": current["statistics"], "snapshot_bytes": current["snapshot_bytes"], "handoff_milliseconds": current["handoff_milliseconds"]}
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
            report["passed"] = True
            print("PASS real-agent rollout: all four profiles and direct ingress samples; existing resources survive owner handoff", flush=True)
        except BaseException as exc:
            report["error_type"] = type(exc).__name__
            # Provider responses and gateway logs can contain conversation data.
            # Preserve only the exception type in the portable report.
            raise
        finally:
            write_private(Path(report_path), json.dumps(report, indent=2) + "\n")
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
            log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--report", default="/tmp/gateway-real-agent-rollout.json")
    args = parser.parse_args()
    main(str(Path(args.binary).resolve()), args.report)
