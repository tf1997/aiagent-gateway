"""Disposable Linux/systemd gateway used by the real-agent acceptance script."""
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent.parent
UNIT = "aiagent-gateway.service"


def wait(action, seconds=90):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        try:
            value = action()
            if value:
                return value
        except (OSError, RuntimeError, subprocess.CalledProcessError):
            pass
        time.sleep(.2)
    raise RuntimeError("Linux gateway acceptance condition timed out")


class LinuxGateway:
    def __init__(self, root, image):
        self.root, self.image = root, image
        self.name = "gateway-agent-systemd-" + uuid.uuid4().hex[:10]
        self.network = self.name + "-network"
        self.created_network = self.created_container = False

    def start(self):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            data_port = reservation.getsockname()[1]
        subprocess.run(["docker", "network", "create", self.network], check=True, stdout=subprocess.DEVNULL)
        self.created_network = True
        subprocess.run(["docker", "create", "--name", self.name, "--network", self.network,
                        "--label", "aiagent-gateway.test=systemd-real-agent", "--privileged", "--cgroupns=private",
                        "--tmpfs", "/run", "--tmpfs", "/run/lock", "--tmpfs", "/tmp",
                        "--publish", f"127.0.0.1:{data_port}:14030", self.image],
                       check=True, stdout=subprocess.DEVNULL)
        self.created_container = True
        subprocess.run(["docker", "start", self.name], check=True, stdout=subprocess.DEVNULL)
        wait(lambda: self.output("systemctl", "show", "--property=Version", "--value"))
        self.output("mkdir", "-p", "/test", "/opt/aiagent-gateway", "/etc/aiagent-gateway")
        self.copy(Path(__file__), "/test/systemd_agent.py")
        ports = json.loads(subprocess.check_output(["docker", "inspect", "--format", "{{json .NetworkSettings.Ports}}", self.name], text=True))
        self.data_port = int(ports["14030/tcp"][0]["HostPort"])

    def output(self, *args):
        return subprocess.check_output(["docker", "exec", self.name, *args], text=True).strip()

    def copy(self, source, target):
        subprocess.run(["docker", "cp", str(source), self.name + ":" + target], check=True)

    def build(self):
        arch = subprocess.check_output(["docker", "info", "--format", "{{.Architecture}}"], text=True).strip()
        goarch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}[arch]
        artifacts = []
        for version in ("a", "b"):
            source = self.root / ("gateway-" + version)
            subprocess.run(["go", "build", "-ldflags=-buildid=systemd-real-agent-" + version,
                            "-o", str(source), "./cmd/gateway"], cwd=ROOT,
                           env=dict(os.environ, GOOS="linux", GOARCH=goarch, CGO_ENABLED="0"), check=True)
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            artifact = "/opt/aiagent-gateway/releases/" + digest + "/gateway"
            self.output("mkdir", "-p", str(Path(artifact).parent))
            self.copy(source, artifact)
            artifacts.append(artifact)
        self.output("chown", "-R", "0:0", "/opt/aiagent-gateway")
        self.output("chmod", "-R", "go-w", "/opt/aiagent-gateway")
        return artifacts

    def install(self, config, artifacts, policy):
        self.copy(config, "/etc/aiagent-gateway/config.yaml")
        self.output("chown", "0:0", "/etc/aiagent-gateway/config.yaml")
        self.output("chmod", "600", "/etc/aiagent-gateway/config.yaml")
        self.output(artifacts[0], "install")
        manifest = self.root / "deployment.json"
        manifest.write_text(json.dumps({"initial_release": artifacts[0], "policy": policy}))
        manifest.chmod(0o600)
        self.copy(manifest, "/etc/aiagent-gateway/deployment.json")
        self.output("chown", "0:0", "/etc/aiagent-gateway/deployment.json")

    def guest(self, action, **kwargs):
        result = subprocess.run(["docker", "exec", "-i", self.name, "python3", "/test/systemd_agent.py", "--guest"],
                                input=json.dumps(dict(action=action, **kwargs)), text=True, capture_output=True, timeout=20)
        if result.returncode:
            raise RuntimeError("Linux gateway helper failed")
        return json.loads(result.stdout)

    def control(self, method="GET", body=None, path="/control/v1/rollout"):
        return self.guest("control", method=method, body=body, path=path)

    def inspect(self):
        return self.guest("inspect")

    def stable(self, artifact, previous=None, restarts=None):
        def check():
            state = self.inspect()
            identity = self.control(path="/control/v1/identity")
            if not identity["ready"] or identity["release"] != Path(artifact).parent.name:
                return False
            pid = identity["pid"]
            if state["main_pid"] != pid or state["pids"] != [pid] or pid == previous:
                return False
            assert state["uids"] == [state["service_uid"]] and state["service_uid"] != 0
            assert state["tcp_listen_ports"] == [14030], state["tcp_listen_ports"]
            assert state["notify_access"] == "main" and state["kill_mode"] == "mixed"
            if restarts is not None:
                assert state["restarts"] == restarts
            return state
        return wait(check)

    def reboot(self):
        subprocess.run(["docker", "restart", self.name], check=True, stdout=subprocess.DEVNULL)
        wait(lambda: self.output("systemctl", "show", "--property=Version", "--value"))

    def close(self):
        if self.created_container:
            subprocess.run(["docker", "rm", "--force", self.name], check=False, stdout=subprocess.DEVNULL)
        if self.created_network:
            subprocess.run(["docker", "network", "rm", self.network], check=False, stdout=subprocess.DEVNULL)


def guest():
    request = json.load(sys.stdin)
    if request["action"] == "control":
        class UnixHTTP(http.client.HTTPConnection):
            def connect(self):
                self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.sock.settimeout(self.timeout)
                self.sock.connect("/run/aiagent-gateway-control/controller.sock")
        config = json.loads(Path("/etc/aiagent-gateway/config.yaml").read_text())
        conn = UnixHTTP("localhost", timeout=10)
        try:
            conn.request(request["method"], request["path"],
                         None if request["body"] is None else json.dumps(request["body"]),
                         {"Authorization": "Bearer " + config["admin_token"]})
            response = conn.getresponse()
            if response.status != 200:
                raise RuntimeError("control HTTP " + str(response.status))
            result = json.loads(response.read())
        finally:
            conn.close()
    elif request["action"] == "inspect":
        def prop(name):
            return subprocess.check_output(["systemctl", "show", UNIT, "--property=" + name, "--value"], text=True).strip()
        group = prop("ControlGroup")
        pids, uids = [], []
        path = Path("/sys/fs/cgroup" + group) / "cgroup.procs"
        for item in path.read_text().split() if group and path.exists() else []:
            try:
                if os.readlink(f"/proc/{item}/exe").startswith("/opt/aiagent-gateway/releases/"):
                    pids.append(int(item))
                    uids.append(Path(f"/proc/{item}").stat().st_uid)
            except FileNotFoundError:
                pass
        listen_ports = set()
        for pid in pids:
            inodes = set()
            for fd in Path(f"/proc/{pid}/fd").iterdir():
                try:
                    link = os.readlink(fd)
                except FileNotFoundError:
                    continue
                if link.startswith("socket:["):
                    inodes.add(link[8:-1])
            for family in ("tcp", "tcp6"):
                for line in Path(f"/proc/{pid}/net/{family}").read_text().splitlines()[1:]:
                    fields = line.split()
                    if fields[3] == "0A" and fields[9] in inodes:
                        listen_ports.add(int(fields[1].split(":")[-1], 16))
        result = {"main_pid": int(prop("MainPID")), "pids": pids, "uids": uids,
                  "tcp_listen_ports": sorted(listen_ports),
                  "service_uid": int(subprocess.check_output(["id", "-u", "aiagent-gateway"], text=True)),
                  "restarts": int(prop("NRestarts")), "notify_access": prop("NotifyAccess"), "kill_mode": prop("KillMode"),
                  "systemd": subprocess.check_output(["systemctl", "--version"], text=True).splitlines()[0],
                  "kernel": os.uname().release, "arch": os.uname().machine,
                  "filesystem": subprocess.check_output(["stat", "-f", "-c", "%T", "/var/lib/aiagent-gateway"], text=True).strip()}
    elif request["action"] == "cleanup_fixture":
        releases = []
        for index in range(4):
            raw = ("unused-cleanup-test-" + str(index) + "-" + uuid.uuid4().hex).encode()
            digest = hashlib.sha256(raw).hexdigest()
            path = Path("/opt/aiagent-gateway/releases") / digest / "gateway"
            path.parent.mkdir()
            path.write_bytes(raw)
            path.chmod(0o755)
            stamp = time.time() + index
            os.utime(path.parent, (stamp, stamp))
            releases.append(str(path))
        orphan = Path("/var/lib/aiagent-gateway/handoff/snapshots/.incoming-cleanup-test")
        orphan.write_bytes(b"unreferenced")
        rejected = subprocess.run([request["binary"], "cleanup"], capture_output=True, text=True)
        assert rejected.returncode != 0 and all(Path(path).exists() for path in releases) and orphan.exists()
        result = {"obsolete": releases[:2], "recent": releases[2:], "orphan": str(orphan), "active_cleanup_refused": True}
    elif request["action"] == "cleanup_verify":
        fixture = request["fixture"]
        assert all(not Path(path).exists() for path in fixture["obsolete"])
        assert all(Path(path).exists() for path in fixture["recent"])
        assert not Path(fixture["orphan"]).exists()
        result = {"active_cleanup_refused": fixture["active_cleanup_refused"],
                  "obsolete_releases_removed": 2, "recent_releases_retained": 2, "orphan_removed": True}
    else:
        raise RuntimeError("unknown guest action")
    print(json.dumps(result))


if __name__ == "__main__" and sys.argv[1:] == ["--guest"]:
    guest()
