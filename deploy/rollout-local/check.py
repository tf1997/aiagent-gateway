#!/usr/bin/env python3
"""Real host/worker lifecycle checks on temporary ports; no systemd or Agent.

Usage: python3 deploy/rollout-local/check.py /tmp/gateway
Builds two distinct copies from this checkout to exercise A -> B -> C.
"""
import hashlib
import http.client
import http.server
import json
import os
import pathlib
import secrets
import signal
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
    source = pathlib.Path(sys.argv[1]).resolve()
    repo = pathlib.Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix='gw-host-', dir='/tmp') as temp:
        root = pathlib.Path(temp)
        token, caller = secrets.token_hex(24), secrets.token_hex(24)
        public_port, admin_port = port(), port()
        public = f'http://127.0.0.1:{public_port}'
        admin = f'http://127.0.0.1:{admin_port}'
        cfg = root / 'config.yaml'
        cfg.write_text(json.dumps({
            'admin_token': token, 'data_dir': str(root / 'data'),
            'state_socket': str(root / 'state.sock'),
            'entry_socket': str(root / 'entry.sock'),
            'control_socket': str(root / 'controller.sock'),
            'port': public_port, 'admin_port': admin_port, 'public_url': public,
        }))
        cfg.chmod(0o600)
        artifacts = []
        for name in ('a', 'b', 'c'):
            binary = source
            if name != 'a':
                binary = root / ('build-' + name)
                subprocess.run(['go', 'build', '-ldflags=-buildid=host-check-' + name,
                                '-o', str(binary), './cmd/gateway'], cwd=repo, check=True)
            data = binary.read_bytes()
            artifact = root / 'releases' / hashlib.sha256(data).hexdigest() / 'gateway'
            artifact.parent.mkdir(parents=True, mode=0o755)
            artifact.write_bytes(data)
            artifact.chmod(0o755)
            artifacts.append(str(artifact))
        policy = {'auto_profiles': False, 'canary_percent': 5,
                  'readiness_timeout_seconds': 10, 'verify_min_seconds': 1,
                  'verify_timeout_seconds': 20, 'min_success_per_profile': 1,
                  'max_effective_failures': 0, 'required_profiles': ['http'],
                  'observe_seconds': 3, 'observe_min_success_per_profile': 1,
                  'drain_timeout_seconds': 2}
        manifest = root / 'deployment.json'
        manifest.write_text(json.dumps({'infrastructure': str(source),
                                        'initial_release': artifacts[0], 'policy': policy}))
        manifest.chmod(0o600)
        processes, logs, upstreams = [], [], []
        stream_started, stream_finish = threading.Event(), threading.Event()
        calls, call_lock = [], threading.Lock()
        sequence = 0

        def request(url, method='GET', value=None, auth=token, session=None, sock=None, extra=None):
            body = None if value is None else json.dumps(value).encode()
            headers = {'Authorization': 'Bearer ' + auth, 'Content-Type': 'application/json'}
            if session:
                headers['X-Session-ID'] = session
            headers.update(extra or {})
            if sock:
                connection = UnixHTTP(str(root / sock))
                try:
                    connection.request(method, url, body, headers)
                    response = connection.getresponse()
                    return response.status, json.loads(response.read())
                finally:
                    connection.close()
            try:
                response = urllib.request.urlopen(urllib.request.Request(url, body, headers, method=method), timeout=5)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                data = response.read()
                try:
                    data = json.loads(data)
                except ValueError:
                    pass
                return response.status, data

        def control(path='/control/v1/rollout', method='GET', value=None):
            status, data = request(path, method, value, sock='controller.sock')
            assert status == 200, (status, data)
            return data

        def wait(check, timeout=20):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    result = check()
                    if result:
                        return result
                except (OSError, ValueError, http.client.HTTPException):
                    pass
                time.sleep(.1)
            raise AssertionError('timed out waiting for host condition')

        def identity(target):
            status, data = request(target['admin_url'] + '/control/v1/identity')
            assert status == 200 and data['ready'], (status, data)
            return data

        def children(host):
            result = subprocess.run(['ps', '-axo', 'pid=,ppid=,stat='], capture_output=True, text=True, check=True)
            return {int(pid): state for pid, parent, state in
                    (line.split() for line in result.stdout.splitlines()) if int(parent) == host.pid}

        def alive(pid):
            try:
                os.kill(pid, 0)
                # An orphan zombie is not running; init reaps it asynchronously.
                result = subprocess.run(['ps', '-p', str(pid), '-o', 'stat='], capture_output=True, text=True)
                return result.returncode == 0 and not result.stdout.strip().startswith('Z')
            except ProcessLookupError:
                return False

        def start():
            log = open(root / f'host-{len(logs)}.log', 'w+')
            logs.append(log)
            process = subprocess.Popen([str(source), 'host', '--config', str(cfg),
                                        '--deployment', str(manifest)], stdout=log, stderr=log)
            processes.append(process)
            return process

        def begin(artifact):
            return control(method='POST', value={'Artifact': artifact, 'Policy': policy})

        def pump(host, phases, fail=False):
            nonlocal sequence
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline:
                report = control()
                if report['phase'] in phases:
                    return report
                assert host.poll() is None
                assert len(children(host)) <= 2, children(host)
                assert report['phase'] not in ('FAILED', 'RECOVERY_REQUIRED'), report
                sequence += 1
                suffix = '/fail' if fail else '/ok'
                status, _ = request(public + '/agents/probe' + suffix, auth=caller,
                                    session='new-' + str(sequence))
                assert status == (502 if fail else 200), status
                time.sleep(.03)
            raise AssertionError(control())

        def epoch():
            status, data = request('/control/v1/state', 'POST',
                                   {'action': 'status', 'worker': 'probe', 'id': secrets.token_hex(8)}, sock='state.sock')
            assert status == 200, data
            return data['Epoch']

        def upstream(index):
            class Handler(http.server.BaseHTTPRequestHandler):
                def do_GET(self):
                    self.handle_request()

                def do_POST(self):
                    self.rfile.read(int(self.headers.get('Content-Length', '0')))
                    self.handle_request()

                def handle_request(self):
                    with call_lock:
                        calls.append((index, self.path))
                    if self.path == '/stream':
                        self.send_response(200)
                        self.send_header('Content-Type', 'text/event-stream')
                        self.end_headers()
                        self.wfile.write(b'data: first\n\n')
                        self.wfile.flush()
                        stream_started.set()
                        stream_finish.wait(45)
                        self.wfile.write(b'data: last\n\n')
                        self.wfile.flush()
                        return
                    data = json.dumps({'backend': index}).encode()
                    self.send_response(502 if self.path == '/fail' else 200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)

                def log_message(self, *args):
                    pass
            server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            upstreams.append(server)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            return f'http://127.0.0.1:{server.server_port}'

        try:
            host = start()
            report = wait(lambda: (r if (r := control())['phase'] == 'COMPLETED' else None))
            initial_pid = host.pid
            initial_epoch = epoch()
            old_pid = identity(report['stable'])['pid']
            assert set(children(host)) == {old_pid}
            status, route = request(admin + '/admin/v1/routes', 'POST', {
                'key': 'probe', 'consumers': [{'id': 'probe', 'token': caller}],
                'agent_type': 'generic', 'expose': ['proxy'],
                'endpoints': [{'interfaces': {'proxy': {'url': upstream(i)}}} for i in (1, 2)]},
                extra={'Idempotency-Key': 'route-create'})
            assert status == 201, route
            status, original = request(public + '/agents/probe/ok', auth=caller, session='existing')
            assert status == 200
            connection = http.client.HTTPConnection('127.0.0.1', public_port, timeout=45)
            connection.request('GET', '/agents/probe/stream', headers={'Authorization': 'Bearer ' + caller,
                                                                      'X-Session-ID': 'stream'})
            stream = connection.getresponse()
            assert stream.status == 200 and stream.readline() == b'data: first\n'
            assert stream_started.is_set()

            submitted = begin(artifacts[1])
            assert begin(artifacts[1])['rollout_id'] == submitted['rollout_id']
            report = pump(host, {'VERIFYING'})
            assert len(children(host)) == 2
            # Normal business mutation is frozen while both versions participate.
            assert request(admin + '/admin/v1/routes', 'POST', {'key': 'blocked'})[0] == 409
            report = pump(host, {'RECOVERY_REQUIRED'})
            assert report['reason'] == 'old_worker_drain_timeout', report
            assert alive(old_pid) and len(children(host)) == 2
            assert request('/control/v1/rollout', 'POST', {'Artifact': artifacts[2], 'Policy': policy},
                           sock='controller.sock')[0] == 409
            stream_finish.set()
            assert b'data: last' in stream.read()
            connection.close()
            control('/control/v1/rollout/resume', 'POST')
            report = pump(host, {'COMPLETED'})
            assert report['stable']['artifact'] == artifacts[1]
            assert not alive(old_pid) and len(children(host)) == 1
            assert host.pid == initial_pid and epoch() == initial_epoch
            assert request(public + '/agents/probe/ok', auth=caller, session='existing')[1] == original
            assert request(admin + '/admin/v1/routes', 'POST', {'key': 'probe'})[0] == 200
            print('PASS 2 -> 3 -> 2, drain timeout/resume, SSE, stable host PID/epoch and Agent binding', flush=True)

            begin(artifacts[2])
            report = pump(host, {'COMPLETED'})
            assert report['stable']['artifact'] == artifacts[2] and len(children(host)) == 1
            assert host.pid == initial_pid and epoch() == initial_epoch
            stable = report['stable']
            stable_pid = identity(stable)['pid']
            print('PASS consecutive A -> B -> C promotion', flush=True)

            begin(artifacts[0])
            report = pump(host, {'ROLLED_BACK'}, fail=True)
            assert report['stable'] == stable and set(children(host)) == {stable_pid}
            before = len([c for c in calls if c[1] == '/fail'])
            assert request(public + '/agents/probe/fail', 'POST', {}, auth=caller, session='existing')[0] == 502
            assert len([c for c in calls if c[1] == '/fail']) == before + 1
            print('PASS failed candidate rollback and no replay of failed POST', flush=True)

            # An abruptly dead stable child is replaced without replacing host/state.
            os.kill(stable_pid, signal.SIGKILL)
            report = wait(lambda: (r if (r := control())['phase'] == 'COMPLETED' and
                                  r['stable']['incarnation'] != stable['incarnation'] else None))
            assert len(children(host)) == 1 and epoch() == initial_epoch
            print('PASS stable worker crash recovery/reaping', flush=True)

            begin(artifacts[0])
            pump(host, {'VERIFYING'})
            child_pids = list(children(host))
            host.kill()
            host.wait(timeout=10)
            wait(lambda: all(not alive(pid) for pid in child_pids))
            host = start()
            report = wait(lambda: (r if (r := control())['phase'] == 'ROLLED_BACK' else None))
            assert report['stable']['artifact'] == artifacts[2] and len(children(host)) == 1
            assert epoch() != initial_epoch
            assert request(public + '/agents/probe/ok', auth=caller, session='existing')[1] == original
            assert request(admin + '/admin/v1/routes', 'POST', {'key': 'probe'})[0] == 200
            child_pids = list(children(host))
            host.terminate()
            host.wait(timeout=25)
            assert all(not alive(pid) for pid in child_pids)
            print('PASS parent death, host restart rollback, durable binding and full shutdown cleanup', flush=True)
        except BaseException:
            for log in logs:
                log.flush()
                log.seek(0)
                print(log.read()[-12000:], file=sys.stderr)
            raise
        finally:
            stream_finish.set()
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=25)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            for server in upstreams:
                server.shutdown()
                server.server_close()
            for log in logs:
                log.close()


if __name__ == '__main__':
    main()
