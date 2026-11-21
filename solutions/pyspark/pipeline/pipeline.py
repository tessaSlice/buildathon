#!/usr/bin/env python3
"""Shopping Sessions pipeline (PySpark).

Heavy work stays in Spark SQL. Union-find for linked ids runs on the driver.
"""
import glob
import json
import os
import sys
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/tmp/shopping-scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 604_800_000
LATE_MS = 86_400_000
PRICE_MAX = 10_000_000

# Exact length plus a structural regex. `$` in Java can match before a trailing
# line terminator, so length (not `$` alone) rejects a trailing CR (E20).
F1_RE = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
F2_RE = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}$"

USER_COLS = ["user_id", "day", "sessions", "events", "revenue_cents", "top_category"]
CAMP_COLS = [
    "campaign_id", "channel", "day", "sessions",
    "purchasing_sessions", "revenue_cents", "assisted_sessions",
]


def log(msg):
    print(msg, file=sys.stderr)


class Timer:
    def __init__(self):
        self.t = time.perf_counter()

    def stage(self, name):
        now = time.perf_counter()
        log(f"[timing] {name}: {now - self.t:.3f}s")
        self.t = now


def _days_from_civil(y, m, d):
    """Howard Hinnant days since Unix epoch. y is adjusted for Jan/Feb."""
    y -= int(m <= 2)
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def _civil_ok(y, m, d, h, mi, s):
    if not (1 <= m <= 12 and 0 <= h <= 23 and 0 <= mi <= 59 and 0 <= s <= 59):
        return False
    leap = (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0)
    dim = [0, 31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m]
    return 1 <= d <= dim


def parse_f1(value):
    """Manifest timestamps are valid F1. Same calendar math as the SQL parser."""
    if len(value) != 20 or value[4] != "-" or value[7] != "-" or value[10] != "T" or value[13] != ":" or value[16] != ":" or value[19] != "Z":
        raise ValueError(f"bad manifest timestamp: {value!r}")
    if not (value[:4] + value[5:7] + value[8:10] + value[11:13] + value[14:16] + value[17:19]).isdigit():
        raise ValueError(f"bad manifest timestamp: {value!r}")
    y = int(value[0:4])
    m = int(value[5:7])
    d = int(value[8:10])
    h = int(value[11:13])
    mi = int(value[14:16])
    s = int(value[17:19])
    if not _civil_ok(y, m, d, h, mi, s):
        raise ValueError(f"bad manifest timestamp: {value!r}")
    return (_days_from_civil(y, m, d) * 86400 + h * 3600 + mi * 60 + s) * 1000


def _epoch_sql(y, mo, d, h, mi, s):
    """UTC epoch millis from BIGINT part expressions, or NULL if the calendar is impossible."""
    leap = f"((({y}) % 4 = 0 AND ({y}) % 100 <> 0) OR (({y}) % 400 = 0))"
    dim = (
        f"(CASE ({mo}) "
        f"WHEN 1 THEN 31 WHEN 2 THEN CASE WHEN {leap} THEN 29 ELSE 28 END "
        f"WHEN 3 THEN 31 WHEN 4 THEN 30 WHEN 5 THEN 31 WHEN 6 THEN 30 "
        f"WHEN 7 THEN 31 WHEN 8 THEN 31 WHEN 9 THEN 30 WHEN 10 THEN 31 "
        f"WHEN 11 THEN 30 WHEN 12 THEN 31 ELSE 0 END)"
    )
    valid = (
        f"(({mo}) BETWEEN 1 AND 12 AND ({d}) BETWEEN 1 AND {dim} "
        f"AND ({h}) BETWEEN 0 AND 23 AND ({mi}) BETWEEN 0 AND 59 AND ({s}) BETWEEN 0 AND 59)"
    )
    y2 = f"(({y}) - CASE WHEN ({mo}) <= 2 THEN 1 ELSE 0 END)"
    era = f"(CASE WHEN ({y2}) >= 0 THEN div(({y2}), CAST(400 AS BIGINT)) ELSE div(({y2}) - 399, CAST(400 AS BIGINT)) END)"
    yoe = f"(({y2}) - ({era}) * 400)"
    doy = f"(div(153 * (({mo}) + CASE WHEN ({mo}) > 2 THEN -3 ELSE 9 END) + 2, 5) + ({d}) - 1)"
    doe = f"(({yoe}) * 365 + div(({yoe}), 4) - div(({yoe}), 100) + ({doy}))"
    days = f"(({era}) * 146097 + ({doe}) - 719468)"
    epoch = f"((({days}) * 86400 + ({h}) * 3600 + ({mi}) * 60 + ({s})) * CAST(1000 AS BIGINT))"
    return f"(CASE WHEN {valid} THEN {epoch} ELSE CAST(NULL AS BIGINT) END)"


def _parts_sql(col):
    def sub(start, length):
        return f"CAST(substring({col}, {start}, {length}) AS BIGINT)"
    return sub(1, 4), sub(6, 2), sub(9, 2), sub(12, 2), sub(15, 2), sub(18, 2)


def parse_ts_sql(col):
    """F1 / F2 / F3 to epoch millis. NULL means a bad timestamp (caller applies the window)."""
    y, mo, d, h, mi, s = _parts_sql(col)
    f1 = _epoch_sql(y, mo, d, h, mi, s)
    off_h = f"CAST(substring({col}, 21, 2) AS BIGINT)"
    off_m = f"CAST(substring({col}, 24, 2) AS BIGINT)"
    sign = f"(CASE WHEN substring({col}, 20, 1) = '-' THEN CAST(-1 AS BIGINT) ELSE CAST(1 AS BIGINT) END)"
    # Naive clock is parsed as UTC, then the offset is subtracted (E7, E17).
    f2_inner = f"(({f1}) - ({sign}) * (({off_h}) * 3600 + ({off_m}) * 60) * CAST(1000 AS BIGINT))"
    f2 = (
        f"(CASE WHEN ({off_h}) BETWEEN 0 AND 23 AND ({off_m}) BETWEEN 0 AND 59 "
        f"THEN {f2_inner} ELSE CAST(NULL AS BIGINT) END)"
    )
    # ASCII digits only. More than 18 significant digits does not fit a positive int64:
    # treat as outside the window instead of casting (E18).
    digits_only = f"(length({col}) > 0 AND length(regexp_replace({col}, '[0-9]', '')) = 0)"
    sig = f"regexp_replace({col}, '^0+', '')"
    f3 = (
        f"(CASE WHEN length({sig}) > 18 THEN CAST(NULL AS BIGINT) "
        f"WHEN length({sig}) = 0 THEN CAST(0 AS BIGINT) "
        f"ELSE CAST({sig} AS BIGINT) END)"
    )
    return (
        f"(CASE "
        f"WHEN length({col}) = 20 AND {col} RLIKE '{F1_RE}' THEN {f1} "
        f"WHEN length({col}) = 25 AND {col} RLIKE '{F2_RE}' THEN {f2} "
        f"WHEN {digits_only} THEN {f3} "
        f"ELSE CAST(NULL AS BIGINT) END)"
    )


def parse_price_sql(col):
    """Purchase price: ASCII digits, value in 1..10_000_000. Leading zeros allowed (0250 = 250)."""
    digits_only = f"({col} IS NOT NULL AND length({col}) > 0 AND length(regexp_replace({col}, '[0-9]', '')) = 0)"
    sig = f"regexp_replace({col}, '^0+', '')"
    return (
        f"(CASE WHEN {digits_only} THEN "
        f"CASE WHEN length({sig}) BETWEEN 1 AND 8 "
        f"AND CAST({sig} AS BIGINT) BETWEEN 1 AND {PRICE_MAX} "
        f"THEN CAST({sig} AS BIGINT) ELSE CAST(NULL AS BIGINT) END "
        f"ELSE CAST(NULL AS BIGINT) END)"
    )


def day_sql(ts_col):
    # Integer days since the epoch. Avoid a timezone-sensitive timestamp cast.
    return f"date_add(DATE '1970-01-01', CAST(div({ts_col}, CAST({DAY_MS} AS BIGINT)) AS INT))"


def _unquote_sql(expr):
    return f"""CASE
        WHEN {expr} = '' OR {expr} = '""' THEN CAST(NULL AS STRING)
        WHEN substring({expr}, 1, 1) = '"' THEN substring({expr}, 2, length({expr}) - 2)
        ELSE {expr}
    END"""


def _field_ok_sql(expr):
    # A quote is legal only as a wrapper around the whole field, with no quote inside.
    return f"""(
        instr({expr}, '"') = 0
        OR (
            length({expr}) >= 2
            AND substring({expr}, 1, 1) = '"'
            AND substring({expr}, length({expr}), 1) = '"'
            AND instr(substring({expr}, 2, length({expr}) - 2), '"') = 0
        )
    )"""


def build_session(scratch):
    os.makedirs(scratch, exist_ok=True)
    os.makedirs(os.path.join(scratch, ".ivy2"), exist_ok=True)
    os.environ.setdefault("SPARK_LOCAL_DIRS", scratch)
    os.environ.setdefault("TMPDIR", scratch)
    driver_gb = max(1, int(MEM_GB * 0.75))
    spark = (
        SparkSession.builder.master(f"local[{NCPU}]")
        .appName("shopping-sessions")
        .config("spark.driver.memory", f"{driver_gb}g")
        .config("spark.local.dir", scratch)
        .config("spark.sql.shuffle.partitions", str(max(1, NCPU * 4)))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.ivy", os.path.join(scratch, ".ivy2"))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", str(256 * 1024 * 1024))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def csv_paths(subdir):
    return sorted(glob.glob(os.path.join(DATA, subdir, "*.csv")))


def read_body_lines(spark, paths, view):
    """Raw lines, header of each file removed. lineSep \\n keeps CR in the field (E20)."""
    if not paths:
        spark.createDataFrame([], T.StructType([T.StructField("line", T.StringType())])).createOrReplaceTempView(view)
        return
    raw = (
        spark.read.option("lineSep", "\n")
        .text(paths)
        .select(
            F.input_file_name().alias("file"),
            F.monotonically_increasing_id().alias("mid"),
            F.col("value").alias("line"),
        )
    )
    raw.createOrReplaceTempView(view + "_raw")
    # rn is computed before the empty-line filter so a blank line cannot become the header.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW {view} AS
        SELECT line
        FROM (
            SELECT line, row_number() OVER (PARTITION BY file ORDER BY mid) AS rn
            FROM {view}_raw
        ) t
        WHERE rn > 1 AND line IS NOT NULL AND line <> ''
    """)


def parse_fields(spark, src, dst, names):
    """Split on comma. Wrong field count or a non-wrapping quote drops the line. No trim."""
    cols = []
    oks = []
    for i, name in enumerate(names):
        ref = f"a[{i}]"
        cols.append(f"{_unquote_sql(ref)} AS {name}")
        oks.append(_field_ok_sql(ref))
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW {dst} AS
        SELECT {", ".join(cols)}
        FROM (
            SELECT split(line, ',', -1) AS a
            FROM {src}
        ) s
        WHERE size(a) = {len(names)} AND {" AND ".join(oks)}
    """)


def union_find(pairs):
    """Person name is the bytewise minimum id in the component, even if it has no events."""
    parent = {}
    rank = {}
    best = {}

    def add(x):
        if x not in parent:
            parent[x] = x
            rank[x] = 0
            best[x] = x

    def find(x):
        add(x)
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            nxt = parent[x]
            parent[x] = root
            x = nxt
        return root

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            ra, rb = rb, ra
        parent[rb] = ra
        if best[rb] < best[ra]:
            best[ra] = best[rb]
        if rank[ra] == rank[rb]:
            rank[ra] += 1

    for a, b in pairs:
        union(a, b)

    rows = []
    for node in list(parent):
        root = find(node)
        name = best[root]
        if name != node:
            rows.append((node, name))
    return rows


def write_table(spark, view, relpath, columns):
    path = os.path.join(OUT, relpath)
    df = spark.table(view).select(*[F.col(c) for c in columns])
    df.write.mode("overwrite").parquet(path)
    wrote = [
        name for name in os.listdir(path)
        if name.endswith(".parquet") and not name.startswith((".", "_"))
    ]
    if not wrote:
        spark.createDataFrame([], df.schema).write.mode("overwrite").parquet(path)


def main():
    timer = Timer()
    os.makedirs(SCRATCH, exist_ok=True)
    os.makedirs(OUT, exist_ok=True)
    spark = build_session(SCRATCH)
    try:
        with open(os.path.join(DATA, "manifest.json"), encoding="utf-8") as fh:
            manifest = json.load(fh)
        window_start = parse_f1(manifest["window_start"])
        window_end = parse_f1(manifest["window_end"])
        click_lo = window_start - WEEK_MS
        log(f"window_ms: [{window_start}, {window_end}) click_lo={click_lo}")

        read_body_lines(spark, csv_paths("events"), "event_lines")
        read_body_lines(spark, csv_paths("clicks"), "click_lines")
        read_body_lines(spark, csv_paths("links"), "link_lines")
        timer.stage("read")

        parse_fields(
            spark, "link_lines", "link_parsed", ["user_id_a", "user_id_b"],
        )
        pairs = [
            (row.user_id_a, row.user_id_b)
            for row in spark.sql(
                "SELECT user_id_a, user_id_b FROM link_parsed "
                "WHERE user_id_a IS NOT NULL AND user_id_b IS NOT NULL"
            ).collect()
        ]
        map_schema = T.StructType([
            T.StructField("user_id", T.StringType()),
            T.StructField("person", T.StringType()),
        ])
        spark.createDataFrame(union_find(pairs), map_schema).createOrReplaceTempView("person_map")

        str_pair = lambda a, b: T.StructType([
            T.StructField(a, T.StringType()),
            T.StructField(b, T.StringType()),
        ])
        (
            spark.read.option("header", True)
            .option("ignoreLeadingWhiteSpace", False)
            .option("ignoreTrailingWhiteSpace", False)
            .schema(str_pair("product_id", "category"))
            .csv(os.path.join(DATA, "products.csv"))
            .createOrReplaceTempView("products")
        )
        (
            spark.read.option("header", True)
            .option("ignoreLeadingWhiteSpace", False)
            .option("ignoreTrailingWhiteSpace", False)
            .schema(str_pair("campaign_id", "channel"))
            .csv(os.path.join(DATA, "campaigns.csv"))
            .createOrReplaceTempView("campaigns")
        )
        timer.stage(f"links ({len(pairs)} edges)")

        parse_fields(
            spark, "event_lines", "event_parsed",
            ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"],
        )
        # Dedup on event_id by smallest ingest_ts string (F1 sorts in time order) BEFORE validation.
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW events_dedup AS
            SELECT event_id, user_id, ts, ingest_ts, event_type, product_id, price_cents
            FROM (
                SELECT
                    event_id, user_id, ts, ingest_ts, event_type, product_id, price_cents,
                    row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts) AS rn
                FROM event_parsed
                WHERE event_id IS NOT NULL
            ) t
            WHERE rn = 1
        """)
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW events_kept AS
            SELECT /*+ BROADCAST(m), BROADCAST(p) */
                e.event_id,
                coalesce(m.person, e.user_id) AS person,
                e.ts_ms,
                e.event_type,
                coalesce(p.category, 'unknown') AS category,
                CASE
                    WHEN e.event_type = 'purchase' THEN e.price_cents
                    ELSE CAST(0 AS BIGINT)
                END AS price_cents
            FROM (
                SELECT
                    event_id,
                    user_id,
                    event_type,
                    product_id,
                    {parse_ts_sql("ts")} AS ts_ms,
                    {parse_ts_sql("ingest_ts")} AS ingest_ms,
                    {parse_price_sql("price_cents")} AS price_cents
                FROM events_dedup
            ) e
            LEFT JOIN person_map m ON e.user_id = m.user_id
            LEFT JOIN products p ON e.product_id = p.product_id
            WHERE e.user_id IS NOT NULL
              AND e.product_id IS NOT NULL
              AND e.event_type IN ('view', 'cart', 'purchase')
              AND e.ts_ms IS NOT NULL
              AND e.ts_ms >= {window_start} AND e.ts_ms < {window_end}
              AND e.ingest_ms IS NOT NULL
              AND e.ingest_ms - e.ts_ms <= {LATE_MS}
              AND (e.event_type <> 'purchase' OR e.price_cents IS NOT NULL)
        """)
        # New session only when the gap is strictly greater than 30 minutes.
        # Equal timestamps have gap 0, so they stay with the first of them (E16).
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW events_sess AS
            SELECT
                person, event_id, event_type, category, price_cents, session_idx, start_ts,
                {day_sql("start_ts")} AS day
            FROM (
                SELECT
                    person, event_id, event_type, category, price_cents, session_idx,
                    min(ts_ms) OVER (PARTITION BY person, session_idx) AS start_ts
                FROM (
                    SELECT
                        person, event_id, ts_ms, event_type, category, price_cents,
                        sum(CASE
                            WHEN prev_ts IS NULL OR ts_ms - prev_ts > {GAP_MS} THEN 1
                            ELSE 0
                        END) OVER (
                            PARTITION BY person
                            ORDER BY ts_ms, event_id
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS session_idx
                    FROM (
                        SELECT
                            person, event_id, ts_ms, event_type, category, price_cents,
                            lag(ts_ms) OVER (
                                PARTITION BY person ORDER BY ts_ms, event_id
                            ) AS prev_ts
                        FROM events_kept
                    ) a
                ) b
            ) c
        """)
        events_df = spark.table("events_sess").persist(StorageLevel.MEMORY_AND_DISK)
        events_df.createOrReplaceTempView("events_sess")
        n_events = events_df.count()
        timer.stage(f"events ({n_events} surviving)")

        spark.sql("""
            CREATE OR REPLACE TEMP VIEW sessions AS
            SELECT
                person,
                session_idx,
                start_ts,
                day,
                count(*) AS events,
                sum(price_cents) AS revenue_cents,
                max(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchase
            FROM events_sess
            GROUP BY person, session_idx, start_ts, day
        """)
        sessions_df = spark.table("sessions").persist(StorageLevel.MEMORY_AND_DISK)
        sessions_df.createOrReplaceTempView("sessions")
        n_sessions = sessions_df.count()
        timer.stage(f"sessions ({n_sessions})")

        parse_fields(
            spark, "click_lines", "click_parsed",
            ["click_id", "user_id", "campaign_id", "ts"],
        )
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW clicks_dedup AS
            SELECT click_id, user_id, campaign_id, ts
            FROM (
                SELECT
                    click_id, user_id, campaign_id, ts,
                    row_number() OVER (PARTITION BY click_id ORDER BY click_id) AS rn
                FROM click_parsed
                WHERE click_id IS NOT NULL
            ) t
            WHERE rn = 1
        """)
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW clicks_kept AS
            SELECT /*+ BROADCAST(m), BROADCAST(c) */
                k.click_id,
                coalesce(m.person, k.user_id) AS person,
                k.ts_ms,
                k.campaign_id
            FROM (
                SELECT
                    click_id,
                    user_id,
                    campaign_id,
                    {parse_ts_sql("ts")} AS ts_ms
                FROM clicks_dedup
            ) k
            INNER JOIN campaigns c ON k.campaign_id = c.campaign_id
            LEFT JOIN person_map m ON k.user_id = m.user_id
            WHERE k.user_id IS NOT NULL
              AND k.ts_ms IS NOT NULL
              AND k.ts_ms >= {click_lo} AND k.ts_ms < {window_end}
        """)
        clicks_df = spark.table("clicks_kept").persist(StorageLevel.MEMORY_AND_DISK)
        clicks_df.createOrReplaceTempView("clicks_kept")
        n_clicks = clicks_df.count()

        # One row per (person, ts): smallest click_id, and that click's campaign.
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW clicks_at AS
            SELECT person, ts_ms, min_by(campaign_id, click_id) AS campaign_id
            FROM clicks_kept
            GROUP BY person, ts_ms
        """)
        # kind 0 (click) sorts before kind 1 (session) at the same timestamp, so a click
        # at the session start is visible to the running max. Then apply the 7-day check.
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW credited AS
            SELECT
                person,
                session_idx,
                CASE
                    WHEN last_click IS NULL THEN 'organic'
                    WHEN start_ts - last_click.click_ts > {WEEK_MS} THEN 'organic'
                    ELSE last_click.campaign_id
                END AS campaign_id
            FROM (
                SELECT
                    person,
                    session_idx,
                    start_ts,
                    kind,
                    max(CASE
                        WHEN kind = 0 THEN named_struct('click_ts', ts_ms, 'campaign_id', campaign_id)
                    END) OVER (
                        PARTITION BY person
                        ORDER BY ts_ms ASC, kind ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS last_click
                FROM (
                    SELECT
                        person,
                        start_ts AS ts_ms,
                        CAST(NULL AS STRING) AS campaign_id,
                        CAST(1 AS INT) AS kind,
                        session_idx,
                        start_ts
                    FROM sessions
                    UNION ALL
                    SELECT
                        person,
                        ts_ms,
                        campaign_id,
                        CAST(0 AS INT) AS kind,
                        CAST(NULL AS BIGINT) AS session_idx,
                        CAST(NULL AS BIGINT) AS start_ts
                    FROM clicks_at
                ) u
            ) w
            WHERE kind = 1
        """)
        # Assists: distinct (person, campaign) joined to sessions, then the same
        # backward window per campaign. Do not inequality-join every click to every session.
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW assist_hits AS
            SELECT person, campaign_id, day
            FROM (
                SELECT
                    person, campaign_id, kind, day, ts_ms,
                    max(CASE WHEN kind = 0 THEN ts_ms END) OVER (
                        PARTITION BY person, campaign_id
                        ORDER BY ts_ms ASC, kind ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS last_click_ts
                FROM (
                    SELECT
                        s.person,
                        p.campaign_id,
                        s.start_ts AS ts_ms,
                        CAST(1 AS INT) AS kind,
                        s.day
                    FROM sessions s
                    INNER JOIN (
                        SELECT DISTINCT person, campaign_id FROM clicks_kept
                    ) p ON s.person = p.person
                    UNION ALL
                    SELECT
                        person,
                        campaign_id,
                        ts_ms,
                        CAST(0 AS INT) AS kind,
                        CAST(NULL AS DATE) AS day
                    FROM (
                        SELECT DISTINCT person, campaign_id, ts_ms FROM clicks_kept
                    ) d
                ) u
            ) w
            WHERE kind = 1
              AND last_click_ts IS NOT NULL
              AND ts_ms - last_click_ts <= {WEEK_MS}
        """)
        timer.stage(f"attribute ({n_clicks} clicks)")

        spark.sql("""
            CREATE OR REPLACE TEMP VIEW top_cat AS
            SELECT person, day, category AS top_category
            FROM (
                SELECT
                    person, day, category, rev,
                    row_number() OVER (
                        PARTITION BY person, day
                        ORDER BY rev DESC, category ASC
                    ) AS rn
                FROM (
                    SELECT person, day, category, sum(price_cents) AS rev
                    FROM events_sess
                    WHERE event_type = 'purchase'
                    GROUP BY person, day, category
                ) g
            ) r
            WHERE rn = 1
        """)
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW user_daily_v AS
            SELECT
                s.person AS user_id,
                s.day AS day,
                CAST(count(*) AS BIGINT) AS sessions,
                CAST(sum(s.events) AS BIGINT) AS events,
                CAST(sum(s.revenue_cents) AS BIGINT) AS revenue_cents,
                t.top_category AS top_category
            FROM sessions s
            LEFT JOIN top_cat t
              ON s.person = t.person AND s.day = t.day
            GROUP BY s.person, s.day, t.top_category
        """)
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW campaign_daily_v AS
            SELECT
                x.campaign_id AS campaign_id,
                CASE
                    WHEN x.campaign_id = 'organic' THEN 'organic'
                    ELSE camp.channel
                END AS channel,
                x.day AS day,
                CAST(x.sessions AS BIGINT) AS sessions,
                CAST(x.purchasing_sessions AS BIGINT) AS purchasing_sessions,
                CAST(x.revenue_cents AS BIGINT) AS revenue_cents,
                CAST(x.assisted_sessions AS BIGINT) AS assisted_sessions
            FROM (
                SELECT
                    coalesce(c.campaign_id, a.campaign_id) AS campaign_id,
                    coalesce(c.day, a.day) AS day,
                    coalesce(c.sessions, CAST(0 AS BIGINT)) AS sessions,
                    coalesce(c.purchasing_sessions, CAST(0 AS BIGINT)) AS purchasing_sessions,
                    coalesce(c.revenue_cents, CAST(0 AS BIGINT)) AS revenue_cents,
                    CASE
                        WHEN coalesce(c.campaign_id, a.campaign_id) = 'organic' THEN CAST(0 AS BIGINT)
                        ELSE coalesce(a.assisted_sessions, CAST(0 AS BIGINT))
                    END AS assisted_sessions
                FROM (
                    SELECT
                        cr.campaign_id AS campaign_id,
                        s.day AS day,
                        count(*) AS sessions,
                        sum(CAST(s.has_purchase AS BIGINT)) AS purchasing_sessions,
                        sum(s.revenue_cents) AS revenue_cents
                    FROM sessions s
                    JOIN credited cr
                      ON s.person = cr.person AND s.session_idx = cr.session_idx
                    GROUP BY cr.campaign_id, s.day
                ) c
                FULL OUTER JOIN (
                    SELECT campaign_id, day, count(*) AS assisted_sessions
                    FROM assist_hits
                    GROUP BY campaign_id, day
                ) a
                  ON c.campaign_id = a.campaign_id AND c.day = a.day
            ) x
            LEFT JOIN campaigns camp ON x.campaign_id = camp.campaign_id
            WHERE x.sessions <> 0 OR x.assisted_sessions <> 0
        """)
        write_table(spark, "user_daily_v", "user_daily", USER_COLS)
        write_table(spark, "campaign_daily_v", "campaign_daily", CAMP_COLS)
        timer.stage("write")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
