#!/usr/bin/env python3
"""Exercise real gateway processes/TCP on temporary ports, without systemd.

Usage: go build -o /tmp/rollout-gateway ./cmd/gateway
       python3 deploy/rollout-local/check.py /tmp/rollout-gateway
The production controller state machine is covered separately by Go tests.
"""
import http.client
import http.server
import json
import os
import pathlib
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=5)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def main():
    binary = str(pathlib.Path(sys.argv[1]).resolve())
    admin, caller = secrets.token_hex(24), secrets.token_hex(24)
    calls = []
    lock = threading.Lock()
    servers, processes, logs = [], [], []
    with tempfile.TemporaryDirectory(prefix='gw-rollout-', dir='/tmp') as temp:
        root = pathlib.Path(temp)
        admin_port, public_port = port(), port()
        config_path = root / 'config.yaml'
        config_path.write_text(json.dumps({
            'admin_token': admin,
            'data_dir': str(root / 'data'),
            'state_socket': str(root / 'state.sock'),
            'entry_socket': str(root / 'entry.sock'),
            'public_url': f'http://127.0.0.1:{public_port}',
            'admin_port': admin_port,
            'port': public_port,
        }))
        config_path.chmod(0o600)

        def request(target, method='GET', value=None, token=admin, session=None):
            body = None if value is None else json.dumps(value).encode()
            headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}
            if session:
                headers['X-Session-ID'] = session
            if target.startswith('/'):
                connection = UnixHTTP(str(root / 'entry.sock'))
                try:
                    connection.request(method, target, body, headers)
                    response = connection.getresponse()
                    data = response.read()
                    if response.status >= 300:
                        raise RuntimeError((response.status, data.decode()))
                    return json.loads(data)
                finally:
                    connection.close()
            query = urllib.request.Request(target, body, headers, method=method)
            try:
                with urllib.request.urlopen(query, timeout=5) as response:
                    return response.status, json.loads(response.read())
            except urllib.error.HTTPError as error:
                return error.code, json.loads(error.read())

        def wait(call):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    result = call()
                    if result:
                        return result
                except (OSError, ValueError, http.client.HTTPException):
                    pass
                time.sleep(.1)
            raise RuntimeError('gateway process did not become ready')

        def start(name, *args):
            log = open(root / (name + '.log'), 'w+')
            logs.append(log)
            process = subprocess.Popen([binary, *args, "--config", str(config_path)], stdout=log, stderr=log)
            processes.append(process)
            return process

        def upstream(index):
            class Handler(http.server.BaseHTTPRequestHandler):
                def do_GET(self):
                    self.handle_request()

                def do_POST(self):
                    self.rfile.read(int(self.headers.get('Content-Length', '0')))
                    self.handle_request()

                def handle_request(self):
                    with lock:
                        calls.append((index, self.path))
                    data = json.dumps({'backend': index, 'path': self.path}).encode()
                    self.send_response(502 if self.path == '/fail' else 200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)

                def log_message(self, *args):
                    pass
            server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            servers.append(server)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            return 'http://127.0.0.1:' + str(server.server_port)

        try:
            subprocess.run([binary, '--config', str(config_path), '--init-only'], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            start('state', 'state-server')
            admin_url = f'http://127.0.0.1:{admin_port}'
            wait(lambda: request(admin_url + '/admin/v1/config/status')[0] == 200)
            status, _ = request(admin_url + '/admin/v1/routes', 'POST', {
                'key': 'probe', 'consumers': [{'id': 'probe', 'token': caller}], 'agent_type': 'generic', 'expose': ['proxy'],
                'endpoints': [{'interfaces': {'proxy': {'url': upstream(i)}}} for i in (1, 2)]})
            assert status == 201
            targets = []
            for slot in ('a', 'b'):
                data_port, control_port = port(), port()
                start(slot, 'worker', '--slot=' + slot, '--release=probe-' + slot,
                      f'--listen=127.0.0.1:{data_port}', f'--admin=127.0.0.1:{control_port}')
                control_url = f'http://127.0.0.1:{control_port}'
                identity = wait(lambda: request(control_url + '/control/v1/identity')[1])
                assert identity['ready']
                targets.append({'instance': slot, 'release': identity['release'],
                                'incarnation': identity['incarnation'],
                                'url': f'http://127.0.0.1:{data_port}', 'admin_url': control_url})
            start('entry', 'entry')
            wait(lambda: (root / 'entry.sock').exists())
            old, candidate = targets
            traffic = {'revision': 1, 'rollout_id': 'process-probe', 'stable': old,
                       'candidate_percent': 0}
            request('/control/v1/traffic', 'PUT', {'Expected': 0, 'Traffic': traffic})
            url = f'http://127.0.0.1:{public_port}/agents/probe'
            status, original = request(url + '/ok', token=caller, session='existing')
            assert status == 200
            traffic.update(revision=2, candidate=candidate, candidate_percent=5)
            request('/control/v1/traffic', 'PUT', {'Expected': 1, 'Traffic': traffic})
            for index in range(100):
                status, _ = request(url + '/ok', token=caller, session=f'new-{index}')
                assert status == 200
            stats = request('/control/v1/stats?since=2000-01-01T00:00:00Z&worker=' + candidate['incarnation'])
            assert stats['counts']['http']['success'] == 5, stats
            traffic.update(revision=3, candidate_percent=100)
            request('/control/v1/traffic', 'PUT', {'Expected': 2, 'Traffic': traffic})
            status, continued = request(url + '/ok', token=caller, session='existing')
            assert status == 200 and continued['backend'] == original['backend']
            before = len(calls)
            status, _ = request(url + '/fail', 'POST', {}, token=caller, session='existing')
            assert status == 502 and len(calls) == before + 1
            stats = request('/control/v1/stats?since=2000-01-01T00:00:00Z&worker=' + candidate['incarnation'])
            assert stats['failures'] == 1, stats
            traffic.update(revision=4, candidate_percent=0)
            request('/control/v1/traffic', 'PUT', {'Expected': 3, 'Traffic': traffic})
            status, restored = request(url + '/ok', token=caller, session='existing')
            assert status == 200 and restored['backend'] == original['backend']
            new_caller = secrets.token_hex(24)
            status, _ = request(admin_url + '/admin/v1/routes', 'POST', {
                'key': 'probe', 'consumers': [{'id': 'probe', 'token': new_caller}]})
            assert status == 200
            assert request(url + '/ok', token=caller, session='existing')[0] == 401
            status, rotated = request(url + '/ok', token=new_caller, session='existing')
            assert status == 200 and rotated['backend'] == original['backend']
            status, _ = request(admin_url + '/admin/v1/routes', 'POST', {'key': 'probe', 'consumers': []})
            assert status == 200
            assert request(url + '/ok', token=new_caller, session='existing')[0] == 401
            print('PASS dynamic consumer token rotation/revocation and original session ownership')
            print('PASS real state/entry/two-worker processes, 5% allocation, full switch,')
            print('     session ownership, failed POST without replay, and traffic rollback')
        finally:
            for process in reversed(processes):
                process.terminate()
            for process in reversed(processes):
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for server in servers:
                server.shutdown()
                server.server_close()
            for log in logs:
                log.close()


if __name__ == '__main__':
    main()
