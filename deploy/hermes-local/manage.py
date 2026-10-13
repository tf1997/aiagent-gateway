#!/usr/bin/env python3
"""Isolated Hermes Docker environment. Uses only the Python standard library."""
import argparse
import getpass
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / ".runtime"
MODEL_BASE = "http://www.tf1997.store:9091/v1"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never forward a model or local API credential to a redirected origin.
        return None


HTTP = urllib.request.build_opener(NoRedirect)


def request(url, key="", payload=None, method=None, timeout=30):
    headers = {"Accept": "application/json"}
    if key:
        headers["Authorization"] = "Bearer " + key
    body = None
    if payload is not None:
        body = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
        if isinstance(payload, dict) and payload.get("jsonrpc") == "2.0":
            headers["A2A-Version"] = "1.0"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with HTTP.open(req, timeout=timeout) as response:
            raw = response.read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise RuntimeError("HTTP response exceeds 4 MiB")
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        # Upstream error bodies may echo credentials; report status only.
        raise RuntimeError(f"HTTP {exc.code} from {url}") from None
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Cannot connect to {url}: {type(exc.reason).__name__}") from None


def write_private(path, content):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink():
        raise RuntimeError(f"Refusing symlink: {path}")
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".write-")
    try:
        with os.fdopen(fd, "w") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def read_env():
    env = {}
    for line in (RUNTIME / "hermes.env").read_text().splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            env[key] = value
    return env


def compose(*args):
    return ["docker", "compose", "--project-directory", str(ROOT),
            "--env-file", str(RUNTIME / "compose.env"),
            "-f", str(ROOT / "compose.yaml"), *args]


def run(*args):
    subprocess.run(compose(*args), check=True)


def configure(args):
    marker = RUNTIME / "settings.json"
    if marker.exists():
        if args.model or args.api_key_file:
            raise RuntimeError("Existing configuration preserved. Update .runtime files explicitly before restarting.")
        if not all((RUNTIME / p).is_file() for p in ("hermes.env", "home/config.yaml", "compose.env")):
            raise RuntimeError("Saved configuration is incomplete; review .runtime before restarting.")
        return json.loads(marker.read_text())
    # Never overwrite a partially configured or user-managed Hermes home.
    if any((RUNTIME / p).exists() for p in ("hermes.env", "home/config.yaml", "compose.env")):
        raise RuntimeError("Partial configuration exists under .runtime; review it before configuring again.")
    key = os.environ.get("HERMES_MODEL_API_KEY", "")
    if args.api_key_file:
        key = Path(args.api_key_file).expanduser().read_text().strip()
    if not key:
        if not sys.stdin.isatty():
            raise RuntimeError("Set HERMES_MODEL_API_KEY or pass --api-key-file; a model API Key is required.")
        key = getpass.getpass("模型 API Key（隐藏输入）: ").strip()
    if not key or any(ord(c) < 32 or ord(c) == 127 for c in key):
        raise RuntimeError("Invalid model API Key")
    result = request(MODEL_BASE + "/models", key)
    models = sorted({m["id"] for m in result.get("data", [])
                     if isinstance(m, dict) and isinstance(m.get("id"), str)})
    model = args.model or os.environ.get("HERMES_MODEL_ID", "")
    if not model:
        if not sys.stdin.isatty():
            raise RuntimeError("Set HERMES_MODEL_ID or pass --model after selecting a model from /v1/models.")
        print("可用模型：")
        for item in models:
            print("  " + item)
        model = input("模型 ID: ").strip()
    if not model or any(ord(c) < 32 for c in model):
        raise RuntimeError("Invalid model ID")
    if models and model not in models:
        raise RuntimeError("Selected model is absent from /v1/models; check the exact model ID.")
    for port in (args.http_port, args.a2a_port):
        if not 1024 <= port <= 65535:
            raise RuntimeError("Ports must be between 1024 and 65535")
    if args.http_port == args.a2a_port:
        raise RuntimeError("HTTP and A2A need distinct host ports")
    if RUNTIME.is_symlink():
        raise RuntimeError("Refusing a symlinked .runtime directory")
    # A small tool-free request confirms the configured OpenAI-compatible endpoint.
    probe = request(MODEL_BASE + "/chat/completions", key, {
        "model": model, "messages": [{"role": "user", "content": "Reply only HERMES_READY."}],
        "stream": False,
    }, timeout=120)
    if not probe.get("choices"):
        raise RuntimeError("Model probe did not return chat completion choices")
    RUNTIME.mkdir(mode=0o700, parents=True, exist_ok=True)
    (RUNTIME / "home/workspace").mkdir(mode=0o700, parents=True, exist_ok=True)
    api_key, a2a_key = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    env = {"HERMES_MODEL_API_KEY": key, "OPENAI_API_KEY": key,
           "OPENAI_BASE_URL": MODEL_BASE, "API_SERVER_KEY": api_key,
           "A2A_PEER_TOKENS": "gateway:" + a2a_key, "A2A_TRUSTED_PEERS": "gateway"}
    write_private(RUNTIME / "hermes.env", "".join(f"{k}={v}\n" for k, v in env.items()))
    # JSON is valid YAML. Hermes expands ${...} only at runtime, keeping secrets out of this file.
    config = {
        "model": {"provider": "custom", "default": model, "base_url": MODEL_BASE,
                  "api_key": "${HERMES_MODEL_API_KEY}", "api_mode": "chat_completions"},
        "terminal": {"backend": "local", "cwd": "/opt/data/workspace"},
        "gateway": {"platforms": {"api_server": {"enabled": True},
                                   "a2a": {"enabled": True, "extra": {"port": 9900}}}},
    }
    write_private(RUNTIME / "home/config.yaml", json.dumps(config, indent=2) + "\n")
    write_private(RUNTIME / "compose.env", (
        f"HERMES_LOCAL_UID={os.getuid()}\nHERMES_LOCAL_GID={os.getgid()}\n"
        f"HERMES_HTTP_PORT={args.http_port}\nHERMES_A2A_PORT={args.a2a_port}\n"))
    # Dedicated gateway credentials: distinct from the model provider credential.
    settings = {"model_base": MODEL_BASE, "model": model,
                "http_port": args.http_port, "a2a_port": args.a2a_port}
    route = {
        "key": "hermes-local", "agent_type": "hermes", "expose": ["proxy", "a2a"],
        "endpoints": [{"name": "local-default-profile", "interfaces": {
            "proxy": {"url": f"http://127.0.0.1:{args.http_port}", "token": api_key,
                      "health_url": f"http://127.0.0.1:{args.http_port}/health"},
            "a2a": {"url": f"http://127.0.0.1:{args.a2a_port}/", "protocol_version": "1.0",
                    "token": a2a_key, "health_url": f"http://127.0.0.1:{args.a2a_port}/health"},
        }}],
    }
    write_private(RUNTIME / "gateway-route.json", json.dumps(route, indent=2) + "\n")
    write_private(marker, json.dumps(settings, indent=2) + "\n")
    print("模型连通性通过；配置与凭据已写入 deploy/hermes-local/.runtime（文件权限 0600）。")
    return settings


def smoke(settings):
    env = read_env()
    base = f"http://127.0.0.1:{settings['http_port']}"
    request(base + "/health")
    capabilities = request(base + "/v1/capabilities", env["API_SERVER_KEY"])
    if not isinstance(capabilities, dict):
        raise RuntimeError("Unexpected capabilities response")
    request(base + "/api/sessions", env["API_SERVER_KEY"])
    reply = request(base + "/v1/chat/completions", env["API_SERVER_KEY"], {
        "model": "hermes-agent", "messages": [{"role": "user", "content": "只回复 HERMES_READY，不要调用工具。"}],
        "stream": False,
    }, timeout=180)
    if not reply.get("choices"):
        raise RuntimeError("Hermes chat probe returned no choices")
    a2a_url = f"http://127.0.0.1:{settings['a2a_port']}"
    a2a_key = env["A2A_PEER_TOKENS"].split(":", 1)[1]
    card = request(a2a_url + "/.well-known/agent-card.json", a2a_key)
    interfaces = card.get("supportedInterfaces", [])
    if not any(i.get("protocolVersion") == "1.0" for i in interfaces):
        raise RuntimeError("A2A Card does not advertise protocol 1.0")
    rpc = request(a2a_url + "/", a2a_key, {
        "jsonrpc": "2.0", "id": "deployment-check", "method": "GetTask",
        "params": {"id": "deployment-check-nonexistent-task"},
    })
    if rpc.get("error", {}).get("code") != -32001:
        raise RuntimeError("A2A GetTask did not return the expected TaskNotFound code")
    print("通过：HTTP health、认证 capabilities/sessions、Hermes 模型调用、A2A 1.0 Card/GetTask。")
    print(f"HTTP endpoint: {base}")
    print(f"A2A endpoint:  {a2a_url}/")
    print("网关路由配置：deploy/hermes-local/.runtime/gateway-route.json（请勿提交 Git）。")
    write_private(RUNTIME / "smoke-result.json", json.dumps({
        "passed": True, "model": settings["model"], "http_endpoint": base, "a2a_endpoint": a2a_url + "/",
        "checks": ["health", "capabilities", "sessions_list", "chat_completion", "a2a_card", "a2a_get_task"],
    }, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["configure", "up", "check", "status", "logs", "down"], nargs="?", default="up")
    parser.add_argument("--model")
    parser.add_argument("--api-key-file")
    parser.add_argument("--http-port", type=int, default=18642)
    parser.add_argument("--a2a-port", type=int, default=19900)
    args = parser.parse_args()
    if args.action in ("up", "configure"):
        settings = configure(args)
        if args.action == "configure":
            return
        run("config", "--quiet")
        run("pull")
        run("up", "-d", "--wait", "--wait-timeout", "240")
        smoke(settings)
    elif args.action == "check":
        smoke(json.loads((RUNTIME / "settings.json").read_text()))
    elif args.action == "status":
        run("ps")
    elif args.action == "logs":
        run("logs", "--tail", "100")
    else:
        run("down")  # Bind-mounted data is preserved.


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"未完成：{exc}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("已取消；已有数据保留。", file=sys.stderr)
        sys.exit(130)
