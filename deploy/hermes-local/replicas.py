#!/usr/bin/env python3
"""Add a second isolated Hermes instance for the same logical agent."""
import argparse
import json
import secrets
import subprocess
from manage import ROOT, RUNTIME, read_env, write_private


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["up", "status", "stop"], default="up", nargs="?")
    args = parser.parse_args()
    command = ["docker", "compose", "--project-directory", str(ROOT), "--env-file", str(RUNTIME / "compose.env"),
               "-f", str(ROOT / "compose.yaml"), "-f", str(ROOT / "compose.replicas.yaml")]
    if args.action == "up":
        marker = RUNTIME / "replica-settings.json"
        if not marker.exists():
            if (RUNTIME / "home-2").exists() or (RUNTIME / "hermes-2.env").exists():
                raise RuntimeError("Partial replica configuration exists; inspect it before retrying")
            upstream = read_env()
            http_key, a2a_key = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
            upstream.update(API_SERVER_KEY=http_key, A2A_PEER_TOKENS="gateway:" + a2a_key)
            write_private(RUNTIME / "hermes-2.env", "".join(f"{k}={v}\n" for k, v in upstream.items()))
            write_private(RUNTIME / "home-2/config.yaml", (RUNTIME / "home/config.yaml").read_text())
            (RUNTIME / "home-2/workspace").mkdir(mode=0o700)
            write_private(marker, json.dumps({"http_port": 18643, "a2a_port": 19901,
                                             "http_key": http_key, "a2a_key": a2a_key}, indent=2) + "\n")
        settings = json.loads(marker.read_text())
        route_path = RUNTIME / "gateway-route.json"
        route = json.loads(route_path.read_text())
        replica = {"name": "local-replica-profile", "interfaces": {
            "proxy": {"url": "http://127.0.0.1:18643", "token": settings["http_key"], "health_url": "http://127.0.0.1:18643/health"},
            "a2a": {"url": "http://127.0.0.1:19901/", "protocol_version": "1.0", "token": settings["a2a_key"], "health_url": "http://127.0.0.1:19901/health"}}}
        matches = [e for e in route["endpoints"] if e.get("name") == replica["name"]]
        if matches and matches != [replica]:
            raise RuntimeError("Replica endpoint configuration differs; reconcile explicitly")
        if not matches:
            route["endpoints"].append(replica)
        route["agent_type"] = "hermes"
        write_private(route_path, json.dumps(route, indent=2) + "\n")
        subprocess.run(command + ["config", "--quiet"], check=True)
        subprocess.run(command + ["up", "-d", "--wait", "--wait-timeout", "240", "hermes2"], check=True)
        print("Replica ready: HTTP 18643; A2A 19901. Run gateway.py restart to register its route.")
    else:
        subprocess.run(command + (["ps"] if args.action == "status" else ["stop", "hermes2"]), check=True)


if __name__ == "__main__":
    main()
