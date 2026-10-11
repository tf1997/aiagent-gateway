#!/usr/bin/env python3
"""Run an isolated native gateway against the existing local Hermes container."""
import argparse
import fcntl
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

from manage import ROOT, RUNTIME, NoRedirect, write_private

REPO = ROOT.parent.parent
STATE = RUNTIME / "gateway"
BINARY = STATE / "gateway"
DATA_URL = "http://127.0.0.1:18080"
ADMIN_URL = "http://127.0.0.1:19090"
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect)


def load(path):
    return json.loads(path.read_text())


def api(url, token="", payload=None, method=None, headers=None, timeout=180):
    h = {"Accept": "application/json", **(headers or {})}
    if token:
        h["Authorization"] = "Bearer " + token
    data = None
    if payload is not None:
        data = json.dumps(payload).encode()
        h["Content-Type"] = "application/json"
        if isinstance(payload, dict) and payload.get("jsonrpc") == "2.0":
            h["A2A-Version"] = "1.0"
    request = urllib.request.Request(url, data=data, headers=h, method=method)
    try:
        response = HTTP.open(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        raw = response.read((4 << 20) + 1)
        if len(raw) > 4 << 20:
            raise RuntimeError("Response exceeds 4 MiB")
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeError):
            # A proxy may return HTML/plaintext errors. Keep its status, not its body.
            if 200 <= response.status < 300:
                raise RuntimeError(f"Invalid JSON response (HTTP {response.status}): {url}") from None
            body = None
        return response.status, body, response.headers


def require(url, token="", payload=None, method=None, headers=None, timeout=180):
    status, body, _ = api(url, token, payload, method, headers, timeout)
    if status < 200 or status >= 300:
        raise RuntimeError(f"HTTP {status}: {url}")
    return body


def credentials():
    return load(STATE / "credentials.json")


def upstream_keys():
    result = {}
    for line in (RUNTIME / "gateway.env").read_text().splitlines():
        if line and not line.startswith("#"):
            k, v = line.split("=", 1)
            if k in ("HERMES_LOCAL_HTTP_KEY", "HERMES_LOCAL_A2A_KEY", "HERMES_REPLICA_HTTP_KEY", "HERMES_REPLICA_A2A_KEY"):
                result[k] = v
    if not all(result.get(k) for k in ("HERMES_LOCAL_HTTP_KEY", "HERMES_LOCAL_A2A_KEY")):
        raise RuntimeError("Missing Hermes gateway credentials; run manage.py up first")
    return result


def write_config(creds):
    # JSON is a YAML subset; avoid a Python YAML dependency in the helper.
    config = {"admin_token": creds["admin_token"],
              "credentials": upstream_keys(), "data_dir": str(STATE / "data"),
              "listen": "127.0.0.1:18080", "admin_listen": "127.0.0.1:19090",
              "public_url": DATA_URL}
    path = STATE / "gateway.yaml"
    write_private(path, json.dumps(config, indent=2) + "\n")
    return str(path)


def saved_pid():
    path = STATE / "process.json"
    if not path.exists():
        return None
    info = load(path)
    pid = int(info["pid"])
    if pid <= 1:
        raise RuntimeError("Invalid gateway PID")
    # Refuse to signal or reuse a recycled PID belonging to another executable.
    result = subprocess.run(["ps", "-p", str(pid), "-o", "comm="], capture_output=True, text=True)
    if result.returncode != 0:
        return None
    if result.stdout.strip() != str(BINARY):
        raise RuntimeError("Saved PID belongs to another executable; inspect process.json")
    return pid


def register():
    creds = credentials()
    requested = load(RUNTIME / "gateway-route.json")
    requested["agent_type"] = "hermes"
    requested["consumers"] = [{"id": c["id"], "token": c["token"]} for c in creds["callers"]]
    if requested["key"] != "hermes-local":
        raise RuntimeError("This isolated runner expects route key hermes-local")
    a2a = requested["endpoints"][0]["interfaces"]["a2a"]
    card = require(a2a["url"].rstrip("/") + "/.well-known/agent-card.json",
                   upstream_keys()[a2a["credential_ref"]])
    if not any(i.get("protocolVersion") == "1.0" and i.get("protocolBinding") == "JSONRPC"
               for i in card.get("supportedInterfaces", [])):
        raise RuntimeError("Hermes Card does not advertise A2A 1.0 JSONRPC")
    requested["agent_card"] = {k: card[k] for k in
                               ("name", "description", "version", "skills", "defaultInputModes", "defaultOutputModes")}
    requested["agent_card"]["capabilities"] = {"streaming": card.get("capabilities", {}).get("streaming", False)}
    # Blocking model calls can exceed the generic route's 10-second header timeout.
    requested["timeout"] = {"response_header_ms": 180000, "stream_idle_ms": 120000, "max_duration_ms": 300000}
    routes = require(ADMIN_URL + "/admin/v1/routes", creds["admin_token"])["items"]
    route = next((r for r in routes if r["key"] == requested["key"]), None)
    if route is None:
        route = require(ADMIN_URL + "/admin/v1/routes", creds["admin_token"], requested)
    else:
        if (route["agent_type"] not in ("generic", "hermes") or set(route["expose"]) != {"proxy", "a2a"}
                or not route["enabled"]):
            raise RuntimeError("Existing route type/exposure/state differs; reconcile explicitly in the management API")
        current_interfaces = [e["interfaces"] for e in route["endpoints"]]
        if current_interfaces != [e["interfaces"] for e in requested["endpoints"][:len(current_interfaces)]]:
            raise RuntimeError("Existing route endpoints differ; reconcile explicitly in the management API")
        for endpoint in requested["endpoints"][len(current_interfaces):]:
            require(ADMIN_URL + "/admin/v1/routes/" + route["id"] + "/endpoints", creds["admin_token"], endpoint)
        route = require(ADMIN_URL + "/admin/v1/routes/" + route["id"], creds["admin_token"])
        patch = {k: requested[k] for k in ("agent_type", "agent_card", "timeout") if requested[k] != route.get(k)}
        if patch:
            route = require(ADMIN_URL + "/admin/v1/routes/" + route["id"], creds["admin_token"],
                            patch, "PATCH", {"If-Match": '"' + str(route["version"]) + '"'})
    # Register/rotate access dynamically; plaintext tokens are never in the response.
    route = require(ADMIN_URL + "/admin/v1/routes", creds["admin_token"],
                    {"key": requested["key"], "consumers": requested["consumers"]})
    write_private(STATE / "registered-route.json", json.dumps(route, indent=2) + "\n")
    print(f"Route ready: {DATA_URL}/agents/hermes-local (HTTP + A2A)", flush=True)


def start():
    upstream_keys()  # Fail before starting if the Hermes deployment has not been configured.
    if saved_pid():
        require(ADMIN_URL + "/admin/v1/config/status", credentials()["admin_token"], timeout=5)
        register()
        print("Existing local gateway reused.", flush=True)
        return
    for port in (18080, 19090):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                raise RuntimeError(f"Port {port} is already occupied; no existing service was modified")
        except ConnectionRefusedError:
            pass
    path = STATE / "credentials.json"
    if not path.exists():
        if (STATE / "data/config.json").exists():
            raise RuntimeError("Existing data has no saved credentials; refusing to replace credentials")
        c = {"admin_token": secrets.token_urlsafe(32), "callers": [
            {"id": name, "token": secrets.token_urlsafe(32), "routes": ["hermes-local"]}
            for name in ("local-client", "isolation-check")]}
        write_private(path, json.dumps(c, indent=2) + "\n")
        write_private(STATE / "client.env", f"GATEWAY_ADMIN_TOKEN={c['admin_token']}\nGATEWAY_CALLER_TOKEN={c['callers'][0]['token']}\n")
    config_path = write_config(credentials())
    build_env = os.environ.copy()
    build_env["GOCACHE"] = "/private/tmp/aiagent-gateway-go-cache" if sys.platform == "darwin" else "/tmp/aiagent-gateway-go-cache"
    subprocess.run(["go", "build", "-o", str(BINARY), "./cmd/gateway"], cwd=REPO, env=build_env, check=True)
    subprocess.run([str(BINARY), "--config", config_path, "--init-only"], cwd=REPO, check=True)
    fd = os.open(STATE / "gateway.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, "ab", buffering=0) as log:
        process = subprocess.Popen([str(BINARY), "--config", config_path], cwd=REPO, stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=log, start_new_session=True)
    write_private(STATE / "process.json", json.dumps({"pid": process.pid, "binary": str(BINARY)}) + "\n")
    for _ in range(40):
        if process.poll() is not None:
            raise RuntimeError("Gateway exited; inspect .runtime/gateway/gateway.log")
        try:
            require(ADMIN_URL + "/admin/v1/config/status", credentials()["admin_token"], timeout=1)
            require(DATA_URL + "/readyz", timeout=1)
            register()
            return
        except (urllib.error.URLError, TimeoutError):
            time.sleep(0.25)
    raise RuntimeError("Gateway readiness timeout; inspect .runtime/gateway/gateway.log")


def stop():
    pid = saved_pid()
    if pid is None:
        print("Local gateway is not running.")
        return
    os.kill(pid, signal.SIGTERM)
    for _ in range(100):
        if saved_pid() is None:
            (STATE / "process.json").unlink(missing_ok=True)
            print("Local gateway stopped; persisted configuration retained.")
            return
        time.sleep(0.25)
    raise RuntimeError("Gateway is still shutting down; no forced kill was issued")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["up", "connect", "status", "stop", "restart"], nargs="?", default="up")
    args = parser.parse_args()
    STATE.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (STATE / "control.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.action in ("stop", "restart"):
            stop()
        if args.action in ("up", "restart"):
            start()
        elif args.action == "connect":
            register()
        elif args.action == "status":
            pid = saved_pid()
            if not pid:
                raise RuntimeError("Local gateway is not running")
            require(ADMIN_URL + "/admin/v1/config/status", credentials()["admin_token"], timeout=5)
            print(f"Gateway running: PID {pid}; data {DATA_URL}; admin {ADMIN_URL}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"未完成：{exc}", file=sys.stderr)
        sys.exit(1)
