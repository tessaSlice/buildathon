#!/usr/bin/env python3
"""Run the Rust engine, then write the two Parquet tables."""
import os
import subprocess
import sys
import tempfile
import time

np = None
pa = None
pq = None


def _arrow():
    """Load NumPy and PyArrow only for the scoring path, which writes Parquet."""
    global np, pa, pq
    if pa is not None:
        return
    import numpy
    import pyarrow
    import pyarrow.parquet

    np = numpy
    pa = pyarrow
    pq = pyarrow.parquet


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


def reset(folder):
    os.makedirs(folder, exist_ok=True)
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if (
            name.endswith(".parquet")
            and not name.startswith((".", "_"))
            and os.path.isfile(path)
            and not os.path.islink(path)
        ):
            os.remove(path)


def take(data, off, n):
    end = off + n
    if end > len(data):
        raise SystemExit("truncated engine output")
    return data[off:end], end


def i32s(buf):
    _arrow()
    return np.frombuffer(buf, dtype="<i4")


def i64s(buf):
    return np.frombuffer(buf, dtype="<i8")


def u32s(buf):
    return np.frombuffer(buf, dtype="<u4")


def utf8_array(blob, lengths, nulls=None):
    n = int(lengths.shape[0])
    offsets = np.zeros(n + 1, dtype=np.int32)
    if n:
        np.cumsum(lengths, out=offsets[1:], dtype=np.int64)
        if int(offsets[-1]) != int(lengths.sum()):
            raise SystemExit("string blob does not match lengths")
    validity = None
    if nulls is not None:
        validity = pa.py_buffer(np.packbits(nulls.astype(np.uint8), bitorder="little"))
    return pa.Array.from_buffers(
        pa.string(),
        n,
        [validity, pa.py_buffer(offsets), pa.py_buffer(blob)],
    )


def read_user(path):
    _arrow()
    data = open(path, "rb").read()
    if data[:4] != b"SSU1":
        raise SystemExit("bad user.bin magic")
    n = int.from_bytes(data[4:8], "little")
    off = 8
    if n == 0:
        return pa.table(
            {
                "user_id": pa.array([], type=pa.string()),
                "day": pa.array([], type=pa.date32()),
                "sessions": pa.array([], type=pa.int64()),
                "events": pa.array([], type=pa.int64()),
                "revenue_cents": pa.array([], type=pa.int64()),
                "top_category": pa.array([], type=pa.string()),
            }
        )
    days, off = take(data, off, 4 * n)
    sessions, off = take(data, off, 8 * n)
    events, off = take(data, off, 8 * n)
    revenue, off = take(data, off, 8 * n)
    present, off = take(data, off, n)
    user_len, off = take(data, off, 4 * n)
    cat_len, off = take(data, off, 4 * n)
    user_lengths = u32s(user_len)
    cat_lengths = u32s(cat_len)
    user_blob, off = take(data, off, int(user_lengths.sum()))
    cat_blob, off = take(data, off, int(cat_lengths.sum()))
    if off != len(data):
        raise SystemExit("user.bin has trailing bytes")
    present_arr = np.frombuffer(present, dtype=np.uint8)
    return pa.table(
        {
            "user_id": utf8_array(user_blob, user_lengths),
            "day": pa.Array.from_buffers(pa.date32(), n, [None, pa.py_buffer(days)]),
            "sessions": pa.Array.from_buffers(pa.int64(), n, [None, pa.py_buffer(sessions)]),
            "events": pa.Array.from_buffers(pa.int64(), n, [None, pa.py_buffer(events)]),
            "revenue_cents": pa.Array.from_buffers(pa.int64(), n, [None, pa.py_buffer(revenue)]),
            "top_category": utf8_array(cat_blob, cat_lengths, present_arr),
        }
    )


def read_campaign(path):
    _arrow()
    data = open(path, "rb").read()
    if data[:4] != b"SSC1":
        raise SystemExit("bad campaign.bin magic")
    n = int.from_bytes(data[4:8], "little")
    off = 8
    if n == 0:
        return pa.table(
            {
                "campaign_id": pa.array([], type=pa.string()),
                "channel": pa.array([], type=pa.string()),
                "day": pa.array([], type=pa.date32()),
                "sessions": pa.array([], type=pa.int64()),
                "purchasing_sessions": pa.array([], type=pa.int64()),
                "revenue_cents": pa.array([], type=pa.int64()),
                "assisted_sessions": pa.array([], type=pa.int64()),
            }
        )
    days, off = take(data, off, 4 * n)
    sessions, off = take(data, off, 8 * n)
    purchasing, off = take(data, off, 8 * n)
    revenue, off = take(data, off, 8 * n)
    assisted, off = take(data, off, 8 * n)
    camp_len, off = take(data, off, 4 * n)
    chan_len, off = take(data, off, 4 * n)
    camp_lengths = u32s(camp_len)
    chan_lengths = u32s(chan_len)
    camp_blob, off = take(data, off, int(camp_lengths.sum()))
    chan_blob, off = take(data, off, int(chan_lengths.sum()))
    if off != len(data):
        raise SystemExit("campaign.bin has trailing bytes")
    return pa.table(
        {
            "campaign_id": utf8_array(camp_blob, camp_lengths),
            "channel": utf8_array(chan_blob, chan_lengths),
            "day": pa.Array.from_buffers(pa.date32(), n, [None, pa.py_buffer(days)]),
            "sessions": pa.Array.from_buffers(pa.int64(), n, [None, pa.py_buffer(sessions)]),
            "purchasing_sessions": pa.Array.from_buffers(
                pa.int64(), n, [None, pa.py_buffer(purchasing)]
            ),
            "revenue_cents": pa.Array.from_buffers(pa.int64(), n, [None, pa.py_buffer(revenue)]),
            "assisted_sessions": pa.Array.from_buffers(
                pa.int64(), n, [None, pa.py_buffer(assisted)]
            ),
        }
    )


def write_table(folder, table):
    _arrow()
    reset(folder)
    pq.write_table(table, os.path.join(folder, "part-0.parquet"), compression="snappy")


def run_suite(fixtures):
    """Run every fixture in one production engine process and check both tables."""
    import shutil

    scratch = os.environ.get("SCRATCH_DIR")
    cleanup = scratch is None
    if cleanup:
        scratch = tempfile.mkdtemp(prefix="shopping-suite-")
    else:
        os.makedirs(scratch, exist_ok=True)
    rc = subprocess.call([find_bin(), "--suite", fixtures, scratch])
    if cleanup:
        shutil.rmtree(scratch, ignore_errors=True)
    if rc != 0:
        raise SystemExit(rc)


def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "--suite":
        if len(sys.argv) != 3:
            raise SystemExit("usage: pipeline.py --suite FIXTURES_DIR")
        run_suite(sys.argv[2])
        return
    _arrow()
    if len(sys.argv) == 3:
        data, out = sys.argv[1], sys.argv[2]
    else:
        data = os.environ.get("DATA_DIR", "/data")
        out = os.environ.get("OUTPUT_DIR", "/output")
    scratch = os.environ.get("SCRATCH_DIR")
    cleanup = False
    if not scratch:
        scratch = tempfile.mkdtemp(prefix="shopping-")
        cleanup = True
    os.makedirs(scratch, exist_ok=True)
    started = time.perf_counter()
    subprocess.check_call([find_bin(), data, scratch])
    t0 = time.perf_counter()
    write_table(os.path.join(out, "user_daily"), read_user(os.path.join(scratch, "user.bin")))
    write_table(
        os.path.join(out, "campaign_daily"),
        read_campaign(os.path.join(scratch, "campaign.bin")),
    )
    print(f"parquet: {time.perf_counter() - t0:.3f}s", file=sys.stderr)
    print(f"total: {time.perf_counter() - started:.3f}s", file=sys.stderr)
    if cleanup:
        for name in ("user.bin", "campaign.bin"):
            try:
                os.remove(os.path.join(scratch, name))
            except OSError:
                pass


if __name__ == "__main__":
    main()
