#!/usr/bin/env python3
"""Run recovery and cleanup tests on private ext4/XFS images in Linux.

Formats only newly created image files in a disposable container. No host
mounts, physical disks, model calls, load generation, or power-cut claims.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent.parent
PACKAGES = ("state", "handoff", "entry", "process", "service")
IMAGE = "aiagent-gateway-storage-test:bookworm"


def inside(report_path):
    started = time.monotonic()
    report = {"passed": False, "started_at": datetime.now(timezone.utc).isoformat(),
              "kernel": os.uname().release, "arch": os.uname().machine,
              "uid": 65534, "physical_power_loss": False, "filesystems": []}
    try:
        Path("/storage").mkdir()
        for filesystem in ("ext4", "xfs"):
            image = Path("/storage") / (filesystem + ".img")
            with image.open("xb") as file:
                file.truncate(512 << 20)
            subprocess.run(["mkfs." + filesystem, "-q", "-F" if filesystem == "ext4" else "-f", str(image)], check=True)
            mount = Path("/storage") / filesystem
            mount.mkdir()
            subprocess.run(["mount", "-t", filesystem, "-o", "loop", str(image), str(mount)], check=True)
            item = {"requested": filesystem, "mount": subprocess.check_output(
                ["findmnt", "-n", "-o", "FSTYPE,OPTIONS", "--target", str(mount)], text=True).strip(), "packages": []}
            report["filesystems"].append(item)
            try:
                cases = mount / "cases"
                cases.mkdir()
                os.chown(cases, 65534, 65534)
                for package in PACKAGES:
                    began = time.monotonic()
                    command = ["runuser", "-u", "nobody", "--", "env", "TMPDIR=" + str(cases),
                               "/tests/" + package + ".test", "-test.count=1", "-test.timeout=180s"]
                    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    item["packages"].append({"package": package, "exit_code": result.returncode,
                                             "seconds": round(time.monotonic() - began, 3), "output": result.stdout})
                    print(filesystem, package, result.returncode, flush=True)
                    if result.returncode:
                        raise RuntimeError(filesystem + " " + package + " tests failed")
            finally:
                subprocess.run(["umount", str(mount)], check=True)
            check = (["e2fsck", "-f", "-n", str(image)] if filesystem == "ext4"
                     else ["xfs_repair", "-n", str(image)])
            result = subprocess.run(check, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            item["filesystem_check"] = {"command": check, "exit_code": result.returncode, "output": result.stdout}
            if result.returncode:
                raise RuntimeError(filesystem + " consistency check failed")
            image.unlink()
        report["passed"] = True
    except BaseException as exc:
        report["error"] = type(exc).__name__ + ": " + str(exc)
        raise
    finally:
        report["seconds"] = round(time.monotonic() - started, 3)
        Path(report_path).write_text(json.dumps(report, indent=2) + "\n")


def host(args):
    arch = subprocess.check_output(["docker", "info", "--format", "{{.Architecture}}"], text=True).strip()
    goarch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}[arch]
    if not args.reuse_image:
        subprocess.run(["docker", "build", "-t", IMAGE, str(Path(__file__).parent)], check=True)
    name = "gateway-storage-" + uuid.uuid4().hex[:10]
    with tempfile.TemporaryDirectory(prefix="gateway-storage-") as tmp:
        stage = Path(tmp)
        stage.chmod(0o755)
        for package in PACKAGES:
            subprocess.run(["go", "test", "-c", "-o", str(stage / (package + ".test")), "./internal/" + package],
                           cwd=ROOT, env=dict(os.environ, GOOS="linux", GOARCH=goarch, CGO_ENABLED="0"), check=True)
        try:
            subprocess.run(["docker", "create", "--name", name, "--label", "aiagent-gateway.test=storage",
                            "--privileged", "--network", "none", "--entrypoint", "python3",
                            IMAGE, "/check.py", "--inside", "--report", "/report.json"], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["docker", "cp", str(stage), name + ":/tests"], check=True)
            subprocess.run(["docker", "cp", str(Path(__file__)), name + ":/check.py"], check=True)
            result = subprocess.run(["docker", "start", "-a", name])
            subprocess.run(["docker", "cp", name + ":/report.json", args.report], check=True)
            report = json.loads(Path(args.report).read_text())
            report["source_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
            report["source_modified"] = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip())
            Path(args.report).write_text(json.dumps(report, indent=2) + "\n")
            result.check_returncode()
        finally:
            subprocess.run(["docker", "rm", "--force", name], check=False, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inside", action="store_true")
    parser.add_argument("--reuse-image", action="store_true")
    parser.add_argument("--report", default="/tmp/gateway-storage-matrix.json")
    args = parser.parse_args()
    inside(args.report) if args.inside else host(args)
