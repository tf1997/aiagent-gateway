#!/usr/bin/env python3
"""Run the complete gateway rollout regression with isolated ports and state."""
import argparse
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "deploy/rollout-process"))
from complete import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", nargs="?")
    args = parser.parse_args()
    if args.binary:
        main(str(Path(args.binary).resolve()))
    else:
        with tempfile.TemporaryDirectory(prefix="gateway-rollout-") as directory:
            binary = str(Path(directory) / "gateway")
            subprocess.run(["go", "build", "-o", binary, "./cmd/gateway"], cwd=ROOT, check=True)
            main(binary)
