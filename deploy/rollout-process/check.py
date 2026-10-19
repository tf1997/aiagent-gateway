#!/usr/bin/env python3
"""P1: one complete gateway PID, real HTTP/SSE, drain and durable restart.

Usage: python3 deploy/rollout-process/check.py /path/to/gateway
This exercises the development `process` entry, not whole-gateway rollout.
All configuration, ports and processes belong to this temporary test.
"""

import http.client
import http.server
import json
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(number, path, token="", method="GET", value=None, session=""):
    conn = http.client.HTTPConnection("127.0.0.1", number, timeout=3)
    try:
        headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
        if session:
            headers["X-Session-ID"] = session
        conn.request(method, path, None if value is None else json.dumps(value), headers)
        response = conn.getresponse()
        return response.status, response.read()
    finally:
        conn.close()


def main(binary):
    stream_finish = threading.Event()
    calls = []

    class Upstream(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            self.do_POST()

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            calls.append(self.path)
            self.send_response(200)
            if self.path == "/stream":
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                self.wfile.write(b'data: {"text":"hello"}\n\n')
                self.wfile.flush()
                assert stream_finish.wait(15), "test never released stream"
                self.wfile.write(b"data: [DONE]\n\n")
            else:
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok":true}')

    upstream = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
    thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    thread.start()
    proc = None
    try:
        with tempfile.TemporaryDirectory(prefix="gateway-process-") as temporary:
            root = Path(temporary)
            data, admin = port(), port()
            token, caller = secrets.token_hex(24), secrets.token_hex(24)
            config = root / "config.yaml"
            config.write_text(json.dumps({"admin_token": token, "data_dir": str(root / "data"),
                                         "port": str(data), "admin_port": str(admin),
                                         "state_socket": str(root / "state.sock")}))
            config.chmod(0o600)
            with (root / "process.log").open("w+") as log:
                def start():
                    child = subprocess.Popen([binary, "process", "--config=" + str(config)], stdout=log, stderr=log)
                    deadline = time.monotonic() + 15
                    try:
                        while time.monotonic() < deadline and child.poll() is None:
                            try:
                                if request(data, "/readyz")[0] == 200:
                                    rows = subprocess.check_output(["ps", "-A", "-o", "pid=,ppid="], text=True).splitlines()
                                    assert not any(int(row.split()[1]) == child.pid for row in rows), "gateway started child service processes"
                                    return child
                            except OSError:
                                pass
                            time.sleep(0.05)
                        log.flush()
                        log.seek(0)
                        raise AssertionError("process did not become ready: " + log.read())
                    except BaseException:
                        child.kill()
                        child.wait()
                        raise

                proc = start()
                status, body = request(admin, "/admin/v1/routes", token, "POST", {
                    "key": "test", "protocol": "proxy", "consumers": [{"id": "alice", "token": caller}],
                    "endpoints": [{"url": f"http://127.0.0.1:{upstream.server_port}"}],
                })
                assert status == 201, (status, body)
                assert request(data, "/agents/test/once", caller, "POST", {"input": "hello"}, "session")[0] == 200
                assert calls == ["/once"], calls
                assert request(data, "/agents/test/denied", "wrong-token")[0] == 401
                assert calls == ["/once"], "unauthorized request reached upstream"
                print("PASS complete gateway serves HTTP in one PID, authorization, one upstream execution", flush=True)

                conn = http.client.HTTPConnection("127.0.0.1", data, timeout=15)
                conn.request("GET", "/agents/test/stream", headers={"Authorization": "Bearer " + caller, "X-Session-ID": "stream"})
                response = conn.getresponse()
                assert response.status == 200, (response.status, response.read())
                first = response.readline()
                assert first.startswith(b"data:"), first
                proc.send_signal(signal.SIGTERM)
                time.sleep(0.1)
                assert proc.poll() is None, "gateway exited while SSE was active"
                stream_finish.set()
                assert b"[DONE]" in response.read(), "shutdown truncated stream"
                conn.close()
                assert proc.wait(timeout=20) == 0
                print("PASS graceful shutdown drains an active SSE before releasing ownership", flush=True)

                previous = proc.pid
                proc = start()
                assert proc.pid != previous
                assert request(data, "/agents/test/recovered", caller, "POST", {}, "session")[0] == 200
                assert calls == ["/once", "/stream", "/recovered"], calls
                proc.send_signal(signal.SIGTERM)
                assert proc.wait(timeout=20) == 0
                print("PASS restart restores configuration and serves existing session with one PID", flush=True)
    finally:
        stream_finish.set()
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait()
        upstream.shutdown()
        upstream.server_close()


if __name__ == "__main__":
    main(str(Path(sys.argv[1]).resolve()))
