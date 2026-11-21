#!/usr/bin/env python3
"""Launch the Rust engine.

The scored path replaces this process with the binary. The binary writes
Parquet itself, so NumPy and PyArrow stay off the clock. `--suite` still
checks fixtures inside one engine process.
"""
import os
import sys


def find_bin():
    env = os.environ.get("SHOPPING_BIN")
    if env:
        return env
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)
    candidates = [
        "/app/shopping",
        os.path.join(root, "target", "release", "shopping"),
        os.path.join(root, "target", "x86_64-unknown-linux-musl", "release", "shopping"),
    ]
    for path in candidates:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    raise SystemExit("shopping binary not found; set SHOPPING_BIN")


def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "--suite":
        if len(sys.argv) != 3:
            raise SystemExit("usage: pipeline.py --suite FIXTURES_DIR")
        import shutil
        import subprocess
        import tempfile

        scratch = os.environ.get("SCRATCH_DIR")
        cleanup = scratch is None
        if cleanup:
            scratch = tempfile.mkdtemp(prefix="shopping-suite-")
        else:
            os.makedirs(scratch, exist_ok=True)
        rc = subprocess.call([find_bin(), "--suite", sys.argv[2], scratch])
        if cleanup:
            shutil.rmtree(scratch, ignore_errors=True)
        if rc != 0:
            raise SystemExit(rc)
        return

    if len(sys.argv) == 3:
        data, out = sys.argv[1], sys.argv[2]
    else:
        data = os.environ.get("DATA_DIR", "/data")
        out = os.environ.get("OUTPUT_DIR", "/output")
    scratch = os.environ.get("SCRATCH_DIR", "/scratch")
    os.makedirs(scratch, exist_ok=True)
    os.makedirs(out, exist_ok=True)
    binary = find_bin()
    os.execv(binary, [binary, data, scratch, out])


if __name__ == "__main__":
    main()
