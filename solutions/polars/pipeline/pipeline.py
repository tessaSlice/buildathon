#!/usr/bin/env python3
"""Shopping Sessions pipeline (Polars)."""
import glob
import json
import os
import sys
import time
from datetime import datetime, timezone

NCPU = os.environ.get("NCPU")
if NCPU:
    # Must be set before polars is imported.
    os.environ.setdefault("POLARS_MAX_THREADS", NCPU)

import polars as pl  # noqa: E402

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
if len(sys.argv) == 3:
    DATA = sys.argv[1]
    OUT = sys.argv[2]
if os.path.isdir(SCRATCH):
    os.environ.setdefault("POLARS_TEMP_DIR", SCRATCH)
    os.environ.setdefault("TMPDIR", SCRATCH)

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 604_800_000
LATE_MS = 86_400_000
PRICE_MAX = 10_000_000

# ASCII digits only. Polars' strptime accepts second 60 as a leap second and
# rolls it into the next minute, so HMS is range-checked before the parse.
F1_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
F2_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}$"
F3_RE = r"^[0-9]+$"
QUOTED_RE = r'^"[^"]*"$'
PLAIN_RE = r'^[^"]*$'

EVENT_FIELDS = [
    "event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents",
]
CLICK_FIELDS = ["click_id", "user_id", "campaign_id", "ts"]
LINK_FIELDS = ["user_id_a", "user_id_b"]

USER_DAILY = {
    "user_id": pl.String,
    "day": pl.Date,
    "sessions": pl.Int64,
    "events": pl.Int64,
    "revenue_cents": pl.Int64,
    "top_category": pl.String,
}
CAMPAIGN_DAILY = {
    "campaign_id": pl.String,
    "channel": pl.String,
    "day": pl.Date,
    "sessions": pl.Int64,
    "purchasing_sessions": pl.Int64,
    "revenue_cents": pl.Int64,
    "assisted_sessions": pl.Int64,
}

EVENT_OUT = {
    "event_id": pl.String,
    "user_id": pl.String,
    "ts_ms": pl.Int64,
    "event_type": pl.String,
    "product_id": pl.String,
    "price": pl.Int64,
}
CLICK_OUT = {
    "click_id": pl.String,
    "user_id": pl.String,
    "campaign_id": pl.String,
    "ts_ms": pl.Int64,
}


def log(stage, started):
    print(f"{stage}: {time.perf_counter() - started:.3f}s", file=sys.stderr)


def f1_to_ms(text):
    dt = datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1000


def empty_frame(schema):
    return pl.DataFrame(schema=schema)


def list_csvs(subdir):
    return sorted(glob.glob(os.path.join(DATA, subdir, "*.csv")))


def contains_cr(path):
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                return False
            if b"\r" in chunk:
                return True


def read_cr_lines(path):
    """Split only on \\n so a \\r stays inside the field (E20)."""
    with open(path, "rb") as handle:
        data = handle.read()
    parts = data.split(b"\n")
    lines = []
    for raw in parts[1:]:
        if raw == b"":
            continue
        lines.append(raw.decode("utf-8"))
    return lines


def read_clean_lines(paths):
    frame = pl.read_csv(
        paths,
        has_header=True,
        separator="\x1e",
        quote_char=None,
        eol_char="\n",
        infer_schema=False,
        truncate_ragged_lines=False,
    )
    return frame.rename({frame.columns[0]: "line"})


def read_raw_lines(paths):
    if not paths:
        return empty_frame({"line": pl.String})
    clean, cr = [], []
    for path in paths:
        (cr if contains_cr(path) else clean).append(path)
    frames = []
    if clean:
        frames.append(read_clean_lines(clean))
    if cr:
        lines = []
        for path in cr:
            lines.extend(read_cr_lines(path))
        frames.append(pl.DataFrame({"line": pl.Series(lines, dtype=pl.String)}))
    if not frames:
        return empty_frame({"line": pl.String})
    return pl.concat(frames, how="vertical")


def parse_lines(lines, fields):
    schema = {name: pl.String for name in fields}
    if lines.height == 0:
        return empty_frame(schema)
    n = len(fields)
    df = lines.filter(pl.col("line").is_not_null() & (pl.col("line") != ""))
    df = df.filter(pl.col("line").str.count_matches(",", literal=True) == n - 1)
    if df.height == 0:
        return empty_frame(schema)
    df = df.with_columns(pl.col("line").str.split_exact(",", n - 1).alias("_p"))
    exprs = []
    ok = pl.lit(True)
    for i, name in enumerate(fields):
        raw = pl.col("_p").struct.field(f"field_{i}")
        quoted = raw.str.contains(QUOTED_RE).fill_null(False)
        plain = raw.str.contains(PLAIN_RE).fill_null(False)
        ok = ok & (quoted | plain)
        inner = raw.str.slice(1, raw.str.len_chars() - 2)
        val = pl.when(quoted).then(inner).otherwise(raw)
        val = pl.when(val == "").then(pl.lit(None, dtype=pl.String)).otherwise(val)
        exprs.append(val.alias(name))
    return df.select(exprs + [ok.alias("_ok")]).filter(pl.col("_ok")).drop("_ok")


def ts_ms_expr(col):
    s = pl.col(col)
    is_f1 = s.str.contains(F1_RE).fill_null(False)
    is_f2 = s.str.contains(F2_RE).fill_null(False)
    is_f3 = s.str.contains(F3_RE).fill_null(False)
    hour = s.str.slice(11, 2).cast(pl.Int64, strict=False)
    minute = s.str.slice(14, 2).cast(pl.Int64, strict=False)
    second = s.str.slice(17, 2).cast(pl.Int64, strict=False)
    hms_ok = (
        hour.is_between(0, 23) & minute.is_between(0, 59) & second.is_between(0, 59)
    ).fill_null(False)
    f1_ms = s.str.to_datetime(
        "%Y-%m-%dT%H:%M:%SZ", time_zone="UTC", strict=False
    ).dt.epoch("ms")
    naive_ms = s.str.slice(0, 19).str.to_datetime(
        "%Y-%m-%dT%H:%M:%S", time_zone="UTC", strict=False
    ).dt.epoch("ms")
    sign = pl.when(s.str.slice(19, 1) == "-").then(
        pl.lit(-1, dtype=pl.Int64)
    ).otherwise(pl.lit(1, dtype=pl.Int64))
    off_h = s.str.slice(20, 2).cast(pl.Int64, strict=False)
    off_m = s.str.slice(23, 2).cast(pl.Int64, strict=False)
    offset_ok = (off_h.is_between(0, 23) & off_m.is_between(0, 59)).fill_null(False)
    offset_ms = sign * (off_h * 3600 + off_m * 60) * 1000
    f2_ms = naive_ms - offset_ms
    stripped = s.str.replace(r"^0+", "")
    f3_ms = (
        pl.when(stripped == "")
        .then(pl.lit(0, dtype=pl.Int64))
        .when(stripped.str.len_chars() > 18)
        .then(pl.lit(None, dtype=pl.Int64))
        .otherwise(stripped.cast(pl.Int64, strict=False))
    )
    return (
        pl.when(is_f1 & hms_ok).then(f1_ms)
        .when(is_f2 & hms_ok & offset_ok).then(f2_ms)
        .when(is_f3).then(f3_ms)
        .otherwise(pl.lit(None, dtype=pl.Int64))
    )


def price_expr(col):
    s = pl.col(col)
    is_digits = s.str.contains(r"^[0-9]+$").fill_null(False)
    stripped = s.str.replace(r"^0+", "")
    value = (
        pl.when(stripped == "")
        .then(pl.lit(0, dtype=pl.Int64))
        .when(stripped.str.len_chars() > 8)
        .then(pl.lit(None, dtype=pl.Int64))
        .otherwise(stripped.cast(pl.Int64, strict=False))
    )
    return (
        pl.when(is_digits & value.is_between(1, PRICE_MAX, closed="both"))
        .then(value)
        .otherwise(pl.lit(None, dtype=pl.Int64))
    )


def ms_to_date(col):
    days = (pl.col(col) // DAY_MS).cast(pl.Int32)
    return (pl.date(1970, 1, 1) + pl.duration(days=days)).cast(pl.Date)


def union_find(links):
    """Person name is the bytewise-minimum id in the component (L2)."""
    schema = {"user_id": pl.String, "person": pl.String}
    if links.height == 0:
        return empty_frame(schema)
    parent = {}

    def find(node):
        seen = []
        while True:
            nxt = parent.setdefault(node, node)
            if nxt == node:
                root = node
                break
            seen.append(node)
            node = nxt
        for item in seen:
            parent[item] = root
        return root

    def union(left, right):
        ra, rb = find(left), find(right)
        if ra == rb:
            return
        if rb < ra:
            ra, rb = rb, ra
        parent[rb] = ra

    for left, right in links.iter_rows():
        if left is None or right is None:
            continue
        union(left, right)

    users, people = [], []
    for node in list(parent):
        root = find(node)
        if root != node:
            users.append(node)
            people.append(root)
    if not users:
        return empty_frame(schema)
    return pl.DataFrame({"user_id": users, "person": people})


def apply_person(df, person_map):
    if df.height == 0 or person_map.height == 0:
        return df.with_columns(pl.col("user_id").alias("person"))
    return df.join(person_map, on="user_id", how="left").with_columns(
        pl.coalesce(pl.col("person"), pl.col("user_id")).alias("person")
    )


def prepare_events(lines, window_start, window_end):
    parsed = parse_lines(lines, EVENT_FIELDS)
    parsed = parsed.filter(pl.col("event_id").is_not_null())
    if parsed.height == 0:
        return empty_frame(EVENT_OUT)
    # Dedup on event_id by smallest ingest_ts BEFORE validation (E5).
    # ingest_ts is F1, so the string order is the time order.
    parsed = parsed.sort("ingest_ts").unique(subset=["event_id"], keep="first")
    parsed = parsed.with_columns(
        ts_ms_expr("ts").alias("ts_ms"),
        ts_ms_expr("ingest_ts").alias("ingest_ms"),
        price_expr("price_cents").alias("price"),
    )
    parsed = parsed.with_columns(
        pl.when(pl.col("event_type") == "purchase")
        .then(pl.col("price"))
        .otherwise(pl.lit(None, dtype=pl.Int64))
        .alias("price")
    )
    is_purchase = pl.col("event_type") == "purchase"
    keep = (
        pl.col("user_id").is_not_null()
        & pl.col("product_id").is_not_null()
        & pl.col("event_type").is_in(["view", "cart", "purchase"])
        & pl.col("ts_ms").is_not_null()
        & (pl.col("ts_ms") >= window_start)
        & (pl.col("ts_ms") < window_end)
        & ((pl.col("ingest_ms") - pl.col("ts_ms")) <= LATE_MS)
        & (~is_purchase | pl.col("price").is_not_null())
    )
    return parsed.filter(keep.fill_null(False)).select(list(EVENT_OUT))


def prepare_clicks(lines, campaigns, click_start, window_end):
    parsed = parse_lines(lines, CLICK_FIELDS)
    parsed = parsed.filter(pl.col("click_id").is_not_null())
    if parsed.height == 0:
        return empty_frame(CLICK_OUT)
    parsed = parsed.unique(subset=["click_id"], keep="first")
    parsed = parsed.with_columns(ts_ms_expr("ts").alias("ts_ms"))
    keep = (
        pl.col("user_id").is_not_null()
        & pl.col("campaign_id").is_not_null()
        & pl.col("ts_ms").is_not_null()
        & (pl.col("ts_ms") >= click_start)
        & (pl.col("ts_ms") < window_end)
    )
    parsed = parsed.filter(keep.fill_null(False))
    parsed = parsed.join(campaigns.select("campaign_id"), on="campaign_id", how="semi")
    if parsed.height == 0:
        return empty_frame(CLICK_OUT)
    return parsed.select(list(CLICK_OUT))


def sessionize(events):
    """New session only when the gap is strictly greater than 30 minutes."""
    if events.height == 0:
        return events
    events = events.sort(["person", "ts_ms", "event_id"])
    events = events.with_columns(pl.col("ts_ms").shift(1).over("person").alias("_prev"))
    events = events.with_columns(
        (
            pl.col("_prev").is_null() | ((pl.col("ts_ms") - pl.col("_prev")) > GAP_MS)
        ).cast(pl.Int64).alias("_new")
    )
    events = events.with_columns(pl.col("_new").cum_sum().over("person").alias("_sess"))
    events = events.with_columns(
        pl.col("ts_ms").min().over(["person", "_sess"]).alias("start_ts")
    )
    events = events.with_columns(ms_to_date("start_ts").alias("day"))
    return events.drop("_prev", "_new", "_sess")


def top_categories(events, products):
    schema = {"person": pl.String, "day": pl.Date, "top_category": pl.String}
    purch = events.filter(pl.col("event_type") == "purchase")
    if purch.height == 0:
        return empty_frame(schema)
    purch = purch.join(products, on="product_id", how="left").with_columns(
        pl.col("category").fill_null("unknown")
    )
    cat = purch.group_by(["person", "day", "category"]).agg(
        pl.col("price").sum().alias("rev")
    )
    return (
        cat.sort(
            ["person", "day", "rev", "category"],
            descending=[False, False, True, False],
        )
        .group_by(["person", "day"], maintain_order=True)
        .agg(pl.col("category").first().alias("top_category"))
    )


def user_daily(sessions, top):
    if sessions.height == 0:
        return empty_frame(USER_DAILY)
    frame = sessions.group_by(["person", "day"]).agg(
        pl.len().cast(pl.Int64).alias("sessions"),
        pl.col("events").sum().cast(pl.Int64).alias("events"),
        pl.col("revenue_cents").sum().cast(pl.Int64).alias("revenue_cents"),
    )
    frame = frame.join(top, on=["person", "day"], how="left").rename({"person": "user_id"})
    return frame


def attribute(sessions, clicks, campaigns):
    assist_schema = {
        "campaign_id": pl.String,
        "day": pl.Date,
        "assisted_sessions": pl.Int64,
    }
    if clicks.height == 0:
        credited = sessions.with_columns(pl.lit("organic").alias("campaign_id"))
        assist = empty_frame(assist_schema)
    else:
        # Collapse same-timestamp clicks so asof keeps the smallest click_id.
        collapsed = (
            clicks.group_by(["person", "ts_ms"])
            .agg(pl.col("campaign_id").sort_by("click_id").first().alias("campaign_id"))
            .sort(["person", "ts_ms"])
        )
        credited = (
            sessions.sort(["person", "start_ts"])
            .join_asof(
                collapsed,
                left_on="start_ts",
                right_on="ts_ms",
                by="person",
                strategy="backward",
                check_sortedness=False,
            )
            .with_columns(
                pl.when(
                    (
                        pl.col("ts_ms").is_not_null()
                        & ((pl.col("start_ts") - pl.col("ts_ms")) <= WEEK_MS)
                    ).fill_null(False)
                )
                .then(pl.col("campaign_id"))
                .otherwise(pl.lit("organic"))
                .alias("campaign_id")
            )
        )
        pairs = clicks.select(["person", "campaign_id"]).unique()
        camp_clicks = (
            clicks.select(["person", "campaign_id", "ts_ms"])
            .unique()
            .sort(["person", "campaign_id", "ts_ms"])
        )
        left = (
            sessions.select(["person", "start_ts", "day"])
            .join(pairs, on="person", how="inner")
            .sort(["person", "campaign_id", "start_ts"])
        )
        if left.height == 0:
            assist = empty_frame(assist_schema)
        else:
            matched = left.join_asof(
                camp_clicks,
                left_on="start_ts",
                right_on="ts_ms",
                by=["person", "campaign_id"],
                strategy="backward",
                check_sortedness=False,
            ).filter(
                (
                    pl.col("ts_ms").is_not_null()
                    & ((pl.col("start_ts") - pl.col("ts_ms")) <= WEEK_MS)
                ).fill_null(False)
            )
            if matched.height == 0:
                assist = empty_frame(assist_schema)
            else:
                assist = matched.group_by(["campaign_id", "day"]).agg(
                    pl.len().cast(pl.Int64).alias("assisted_sessions")
                )
    cred = credited.group_by(["campaign_id", "day"]).agg(
        pl.len().cast(pl.Int64).alias("sessions"),
        pl.col("has_purchase").cast(pl.Int64).sum().alias("purchasing_sessions"),
        pl.col("revenue_cents").sum().cast(pl.Int64).alias("revenue_cents"),
    )
    camp = cred.join(assist, on=["campaign_id", "day"], how="full", coalesce=True)
    camp = camp.with_columns(
        pl.col("sessions").fill_null(0),
        pl.col("purchasing_sessions").fill_null(0),
        pl.col("revenue_cents").fill_null(0),
        pl.col("assisted_sessions").fill_null(0),
    ).filter((pl.col("sessions") > 0) | (pl.col("assisted_sessions") > 0))
    camp = camp.join(campaigns, on="campaign_id", how="left").with_columns(
        pl.when(pl.col("campaign_id") == "organic")
        .then(pl.lit("organic"))
        .otherwise(pl.col("channel"))
        .alias("channel")
    )
    return camp


def read_lookup(path, key, value):
    frame = pl.read_csv(path, infer_schema=False).select(
        pl.col(key).cast(pl.String),
        pl.col(value).cast(pl.String),
    )
    return frame.unique(subset=[key], keep="first")


def finalize(df, schema):
    if df.height == 0:
        return empty_frame(schema)
    missing = [name for name in schema if name not in df.columns]
    if missing:
        df = df.with_columns(
            [pl.lit(None, dtype=schema[name]).alias(name) for name in missing]
        )
    return df.select(list(schema)).cast(schema)


def write_table(name, df, schema):
    folder = os.path.join(OUT, name)
    os.makedirs(folder, exist_ok=True)
    for old in glob.glob(os.path.join(folder, "*.parquet")):
        if os.path.isfile(old) and not os.path.islink(old):
            os.remove(old)
    finalize(df, schema).write_parquet(os.path.join(folder, "part-0.parquet"))


def main():
    started = time.perf_counter()
    with open(os.path.join(DATA, "manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    window_start = f1_to_ms(manifest["window_start"])
    window_end = f1_to_ms(manifest["window_end"])
    click_start = window_start - WEEK_MS
    print(
        f"window {manifest['window_start']} .. {manifest['window_end']} "
        f"threads={pl.thread_pool_size()} mem_gb={os.environ.get('MEM_GB', '')}",
        file=sys.stderr,
    )

    t0 = time.perf_counter()
    products = read_lookup(os.path.join(DATA, "products.csv"), "product_id", "category")
    campaigns = read_lookup(os.path.join(DATA, "campaigns.csv"), "campaign_id", "channel")
    event_lines = read_raw_lines(list_csvs("events"))
    click_lines = read_raw_lines(list_csvs("clicks"))
    link_lines = read_raw_lines(list_csvs("links"))
    log("read", t0)

    t0 = time.perf_counter()
    links = parse_lines(link_lines, LINK_FIELDS)
    links = links.filter(
        pl.col("user_id_a").is_not_null() & pl.col("user_id_b").is_not_null()
    )
    person_map = union_find(links)
    log("links", t0)

    t0 = time.perf_counter()
    events = prepare_events(event_lines, window_start, window_end)
    events = apply_person(events, person_map)
    log("events", t0)

    t0 = time.perf_counter()
    events = sessionize(events)
    if events.height == 0:
        sessions = empty_frame({
            "person": pl.String,
            "start_ts": pl.Int64,
            "day": pl.Date,
            "events": pl.Int64,
            "revenue_cents": pl.Int64,
            "has_purchase": pl.Boolean,
        })
        top = empty_frame({"person": pl.String, "day": pl.Date, "top_category": pl.String})
    else:
        sessions = events.group_by(["person", "start_ts", "day"]).agg(
            pl.len().cast(pl.Int64).alias("events"),
            pl.col("price").fill_null(0).sum().cast(pl.Int64).alias("revenue_cents"),
            (pl.col("event_type") == "purchase").any().alias("has_purchase"),
        )
        top = top_categories(events, products)
    users = user_daily(sessions, top)
    log("sessions", t0)

    t0 = time.perf_counter()
    if sessions.height == 0:
        camps = empty_frame(CAMPAIGN_DAILY)
    else:
        clicks = prepare_clicks(click_lines, campaigns, click_start, window_end)
        clicks = apply_person(clicks, person_map)
        camps = attribute(sessions, clicks, campaigns)
    log("attribute", t0)

    t0 = time.perf_counter()
    write_table("user_daily", users, USER_DAILY)
    write_table("campaign_daily", camps, CAMPAIGN_DAILY)
    log("write", t0)
    log("total", started)


if __name__ == "__main__":
    main()
