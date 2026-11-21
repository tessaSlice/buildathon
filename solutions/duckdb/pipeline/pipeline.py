#!/usr/bin/env python3
"""Shopping Sessions pipeline (DuckDB).

Heavy work runs in DuckDB. Python is used for union-find and for files that
contain a CR byte, which DuckDB's reader would treat as a newline.
"""
import json
import os
import sys
import time

import duckdb
import pyarrow as pa

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = max(1, int(os.environ.get("NCPU", os.cpu_count() or 1)))
MEM_GB = float(os.environ.get("MEM_GB", "8"))

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 604_800_000
LATE_MS = 86_400_000

# A whole line is valid when every field is unquoted without a quote, or quoted
# with the quotes only at the two ends. One match replaces per-field checks.
def _field_pat() -> str:
    return r'(?:[^",]*|"[^"]*")'


def line_pattern(n_fields: int) -> str:
    field = _field_pat()
    return "^" + ",".join([field] * n_fields) + "$"


EVENT_RE = line_pattern(7)
CLICK_RE = line_pattern(4)
LINK_RE = line_pattern(2)

# Raw lines: delim/quote never occur, so the reader does not unquote or repair.
READ_LINE_OPTS = """
    header = true,
    columns = {'line': 'VARCHAR'},
    delim = chr(30),
    quote = chr(1),
    escape = chr(1),
    auto_detect = false,
    strict_mode = false,
    ignore_errors = true,
    parallel = true,
    max_line_size = 10485760
"""


def log_stage(name, started):
    now = time.perf_counter()
    print(f"{name}: {now - started:.3f}s", file=sys.stderr, flush=True)
    return now


def sql_quote(value):
    return "'" + value.replace("'", "''") + "'"


def sql_file_list(paths):
    return "[" + ", ".join(sql_quote(path) for path in paths) + "]"


def list_csvs(subdir):
    folder = os.path.join(DATA, subdir)
    if not os.path.isdir(folder):
        return []
    found = []
    for name in sorted(os.listdir(folder)):
        if not name.endswith(".csv") or name.startswith("."):
            continue
        path = os.path.join(folder, name)
        if os.path.isfile(path):
            found.append(path)
    return found


def contains_cr(path):
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(8 * 1024 * 1024)
            if not chunk:
                return False
            if b"\r" in chunk:
                return True


def classify(paths):
    clean, dirty = [], []
    for path in paths:
        if contains_cr(path):
            dirty.append(path)
        else:
            clean.append(path)
    return clean, dirty


def python_data_lines(path):
    """Split on LF only. A CR is data, including a trailing CR before LF."""
    with open(path, "rb") as handle:
        parts = handle.read().split(b"\n")
    return [part.decode("utf-8") for part in parts[1:]]


def lines_sql(con, clean, dirty, name):
    parts = []
    if clean:
        parts.append(
            f"SELECT line FROM read_csv({sql_file_list(clean)}, {READ_LINE_OPTS})"
        )
    if dirty:
        table = f"{name}_cr_lines"
        con.execute(f"CREATE TEMP TABLE {table} (line VARCHAR)")
        rows = []
        for path in dirty:
            rows.extend(python_data_lines(path))
        if rows:
            con.execute(f"INSERT INTO {table} SELECT unnest(?)", [rows])
        parts.append(f"SELECT line FROM {table}")
    if not parts:
        return "(SELECT NULL::VARCHAR AS line WHERE false)"
    return "(" + " UNION ALL ".join(parts) + ")"


class UnionFind:
    """Parent pointers plus the bytewise-minimum id of each component."""

    def __init__(self):
        self.parent = {}
        self.rank = {}
        self.comp_min = {}

    def find(self, node):
        parent = self.parent
        if node not in parent:
            parent[node] = node
            self.rank[node] = 0
            self.comp_min[node] = node
            return node
        root = node
        while parent[root] != root:
            root = parent[root]
        while parent[node] != root:
            parent[node], node = root, parent[node]
        return root

    def union(self, left, right):
        ra = self.find(left)
        rb = self.find(right)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.comp_min[rb] < self.comp_min[ra]:
            self.comp_min[ra] = self.comp_min[rb]
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1

    def mapping(self):
        """Ids whose person name is not themselves. Identity ids are omitted."""
        ids = []
        persons = []
        parent = self.parent
        for node in list(parent):
            root = self.find(node)
            person = self.comp_min[root]
            if person != node:
                ids.append(node)
                persons.append(person)
        return ids, persons


def connect():
    con = duckdb.connect()
    con.execute(f"SET threads = {NCPU}")
    con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
    scratch_tmp = os.path.join(SCRATCH, "duckdb_tmp")
    try:
        os.makedirs(scratch_tmp, exist_ok=True)
    except OSError:
        pass
    con.execute(f"SET temp_directory = {sql_quote(scratch_tmp)}")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET enable_progress_bar = false")
    return con


def create_macros(con):
    # try_strptime accepts a trailing space, so length and regexp_full_match gate it.
    # AND does not short-circuit: every predicate must be non-throwing (try_cast).
    con.execute(
        r"""
        CREATE MACRO parse_ts(s) AS (
          CASE length(s)
            WHEN 20 THEN
              CASE
                WHEN regexp_full_match(s, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$')
                THEN epoch_ms(try_strptime(s, '%Y-%m-%dT%H:%M:%SZ'))::BIGINT
                ELSE NULL::BIGINT
              END
            WHEN 25 THEN
              CASE
                WHEN regexp_full_match(s, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}$')
                 AND try_cast(substr(s, 21, 2) AS INTEGER) BETWEEN 0 AND 23
                 AND try_cast(substr(s, 24, 2) AS INTEGER) BETWEEN 0 AND 59
                THEN epoch_ms(try_strptime(substr(s, 1, 19), '%Y-%m-%dT%H:%M:%S'))::BIGINT
                     - (CASE WHEN substr(s, 20, 1) = '-' THEN -1 ELSE 1 END)::BIGINT
                       * (
                           try_cast(substr(s, 21, 2) AS INTEGER)::BIGINT * 3600
                           + try_cast(substr(s, 24, 2) AS INTEGER)::BIGINT * 60
                         )
                       * 1000
                ELSE NULL::BIGINT
              END
            ELSE
              CASE
                WHEN regexp_full_match(s, '^[0-9]+$') AND length(ltrim(s, '0')) = 0 THEN 0::BIGINT
                WHEN regexp_full_match(s, '^[0-9]+$') AND length(ltrim(s, '0')) <= 18
                  THEN try_cast(ltrim(s, '0') AS BIGINT)
                ELSE NULL::BIGINT
              END
          END
        )
        """
    )
    con.execute(
        r"""
        CREATE MACRO purchase_cents(p) AS (
          CASE
            WHEN regexp_full_match(p, '^[0-9]+$')
             AND length(ltrim(p, '0')) BETWEEN 1 AND 8
             AND try_cast(ltrim(p, '0') AS BIGINT) BETWEEN 1 AND 10000000
            THEN try_cast(ltrim(p, '0') AS BIGINT)
            ELSE NULL::BIGINT
          END
        )
        """
    )
    con.execute(
        r"""
        CREATE MACRO field_ok(f) AS (
          CASE
            WHEN f IS NULL THEN false
            WHEN NOT contains(f, '"') THEN true
            WHEN length(f) >= 2
             AND starts_with(f, '"')
             AND ends_with(f, '"')
             AND NOT contains(substr(f, 2, length(f) - 2), '"')
            THEN true
            ELSE false
          END
        )
        """
    )
    con.execute(
        r"""
        -- Fields that reach unquote already match the line pattern, so a leading
        -- quote means a wrapping pair and nothing else.
        CREATE MACRO unquote(f) AS (
          CASE
            WHEN f IS NULL OR f = '' OR f = '""' THEN NULL::VARCHAR
            WHEN starts_with(f, '"') THEN NULLIF(substr(f, 2, length(f) - 2), '')
            ELSE f
          END
        )
        """
    )


def load_dimensions(con):
    con.execute(
        """
        CREATE TEMP TABLE products AS
        SELECT product_id, any_value(category) AS category
        FROM read_csv(?, header = true,
                      columns = {'product_id': 'VARCHAR', 'category': 'VARCHAR'},
                      auto_detect = false)
        GROUP BY product_id
        """,
        [os.path.join(DATA, "products.csv")],
    )
    con.execute(
        """
        CREATE TEMP TABLE campaigns AS
        SELECT campaign_id, any_value(channel) AS channel
        FROM read_csv(?, header = true,
                      columns = {'campaign_id': 'VARCHAR', 'channel': 'VARCHAR'},
                      auto_detect = false)
        GROUP BY campaign_id
        """,
        [os.path.join(DATA, "campaigns.csv")],
    )
    with open(os.path.join(DATA, "manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    win_start, win_end = con.execute(
        "SELECT parse_ts(?), parse_ts(?)",
        [manifest["window_start"], manifest["window_end"]],
    ).fetchone()
    if win_start is None or win_end is None:
        raise SystemExit("manifest window is not valid F1")
    return int(win_start), int(win_end), int(win_start) - WEEK_MS


def load_person_map(con, link_clean, link_dirty):
    src = lines_sql(con, link_clean, link_dirty, "links")
    con.execute(
        f"""
        CREATE TEMP TABLE link_edges AS
        WITH lines AS (
            SELECT line FROM {src} s(line)
            WHERE line IS NOT NULL AND line <> ''
              AND regexp_full_match(line, {sql_quote(LINK_RE)})
        ),
        spl AS (
            SELECT str_split(line, ',') AS f FROM lines
        ),
        fields AS (
            SELECT unquote(f[1]) AS user_a, unquote(f[2]) AS user_b
            FROM spl
            WHERE len(f) = 2
        )
        SELECT user_a, user_b
        FROM fields
        WHERE user_a IS NOT NULL AND user_b IS NOT NULL
        """
    )
    uf = UnionFind()
    for user_a, user_b in con.execute("SELECT user_a, user_b FROM link_edges").fetchall():
        uf.union(user_a, user_b)
    ids, persons = uf.mapping()
    # unnest() of a Python list is extremely slow (tens of seconds at a few tens
    # of thousands of ids). Arrow registration is a column load.
    if ids:
        con.register(
            "person_map",
            pa.table(
                {
                    "user_id": pa.array(ids, type=pa.large_string()),
                    "person": pa.array(persons, type=pa.large_string()),
                }
            ),
        )
    else:
        con.execute("CREATE TEMP TABLE person_map (user_id VARCHAR, person VARCHAR)")


def load_events(con, clean, dirty, win_start, win_end):
    src = lines_sql(con, clean, dirty, "events")
    # Dedup on event_id before validation. ingest_ts is F1, so it sorts in time order.
    con.execute(
        f"""
        CREATE TEMP TABLE events_kept AS
        WITH lines AS (
            SELECT line FROM {src} s(line)
            WHERE line IS NOT NULL AND line <> ''
              AND regexp_full_match(line, {sql_quote(EVENT_RE)})
        ),
        spl AS (
            SELECT str_split(line, ',') AS f FROM lines
        ),
        fields AS (
            SELECT
                unquote(f[1]) AS event_id,
                unquote(f[2]) AS user_id,
                unquote(f[3]) AS ts_raw,
                unquote(f[4]) AS ingest_raw,
                unquote(f[5]) AS event_type,
                unquote(f[6]) AS product_id,
                unquote(f[7]) AS price_raw
            FROM spl
            WHERE len(f) = 7
        )
        SELECT event_id, user_id, ts_raw, ingest_raw, event_type, product_id, price_raw
        FROM fields
        WHERE event_id IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_raw) = 1
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE events_valid AS
        SELECT
            coalesce(m.person, v.user_id) AS person,
            v.event_id,
            v.ts,
            v.event_type,
            v.price_cents,
            CASE
                WHEN v.event_type = 'purchase' THEN coalesce(p.category, 'unknown')
                ELSE NULL
            END AS category
        FROM (
            SELECT
                event_id,
                user_id,
                product_id,
                event_type,
                parse_ts(ts_raw) AS ts,
                parse_ts(ingest_raw) AS ingest_ms,
                CASE
                    WHEN event_type = 'purchase' THEN purchase_cents(price_raw)
                    ELSE NULL
                END AS price_cents
            FROM events_kept
        ) v
        LEFT JOIN products p ON v.product_id = p.product_id
        LEFT JOIN person_map m ON v.user_id = m.user_id
        WHERE v.user_id IS NOT NULL
          AND v.product_id IS NOT NULL
          AND v.event_type IN ('view', 'cart', 'purchase')
          AND v.ts IS NOT NULL
          AND v.ts >= {win_start}
          AND v.ts < {win_end}
          AND (v.event_type <> 'purchase' OR v.price_cents IS NOT NULL)
          AND v.ingest_ms IS NOT NULL
          AND v.ingest_ms - v.ts <= {LATE_MS}
        """
    )
    con.execute("DROP TABLE events_kept")


def build_sessions(con):
    # A new session starts only when the gap from the previous event is strictly over 30 min.
    # Equal timestamps have gap 0, so they stay with the first of that timestamp.
    con.execute(
        f"""
        CREATE TEMP TABLE sess_events AS
        WITH ordered AS (
            SELECT
                person,
                event_id,
                ts,
                event_type,
                price_cents,
                category,
                lag(ts) OVER (PARTITION BY person ORDER BY ts, event_id) AS prev_ts
            FROM events_valid
        ),
        marked AS (
            SELECT
                person,
                event_id,
                ts,
                event_type,
                price_cents,
                category,
                CASE
                    WHEN prev_ts IS NULL OR ts - prev_ts > {GAP_MS} THEN 1
                    ELSE 0
                END AS is_new
            FROM ordered
        )
        SELECT
            person,
            event_type,
            price_cents,
            category,
            max(CASE WHEN is_new = 1 THEN ts END) OVER (
                PARTITION BY person
                ORDER BY ts, event_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS start_ts
        FROM marked
        """
    )
    con.execute("DROP TABLE events_valid")
    # DuckDB `/` is not integer division; day uses `//`.
    con.execute(
        f"""
        CREATE TEMP TABLE sessions AS
        SELECT
            person,
            start_ts,
            DATE '1970-01-01' + CAST((start_ts // {DAY_MS}) AS INTEGER) AS day,
            events,
            revenue_cents,
            has_purchase
        FROM (
            SELECT
                person,
                start_ts,
                count(*)::BIGINT AS events,
                coalesce(sum(price_cents), 0)::BIGINT AS revenue_cents,
                coalesce(bool_or(event_type = 'purchase'), false) AS has_purchase
            FROM sess_events
            GROUP BY person, start_ts
        )
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE top_cat AS
        WITH purch AS (
            SELECT
                person,
                DATE '1970-01-01' + CAST((start_ts // {DAY_MS}) AS INTEGER) AS day,
                category,
                price_cents
            FROM sess_events
            WHERE event_type = 'purchase'
        ),
        sums AS (
            SELECT person, day, category, sum(price_cents) AS revenue
            FROM purch
            GROUP BY person, day, category
        )
        SELECT person, day, category AS top_category
        FROM sums
        QUALIFY row_number() OVER (
            PARTITION BY person, day
            ORDER BY revenue DESC, category ASC
        ) = 1
        """
    )
    con.execute("DROP TABLE sess_events")


def load_clicks(con, clean, dirty, click_lo, win_end):
    src = lines_sql(con, clean, dirty, "clicks")
    con.execute(
        f"""
        CREATE TEMP TABLE clicks_kept AS
        WITH lines AS (
            SELECT line FROM {src} s(line)
            WHERE line IS NOT NULL AND line <> ''
              AND regexp_full_match(line, {sql_quote(CLICK_RE)})
        ),
        spl AS (
            SELECT str_split(line, ',') AS f FROM lines
        ),
        fields AS (
            SELECT
                unquote(f[1]) AS click_id,
                unquote(f[2]) AS user_id,
                unquote(f[3]) AS campaign_id,
                unquote(f[4]) AS ts_raw
            FROM spl
            WHERE len(f) = 4
        )
        SELECT
            click_id,
            any_value(user_id) AS user_id,
            any_value(campaign_id) AS campaign_id,
            any_value(ts_raw) AS ts_raw
        FROM fields
        WHERE click_id IS NOT NULL
        GROUP BY click_id
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE valid_clicks AS
        SELECT
            coalesce(m.person, c.user_id)::VARCHAR AS person,
            c.click_id::VARCHAR AS click_id,
            c.campaign_id::VARCHAR AS campaign_id,
            c.ts::BIGINT AS ts
        FROM (
            SELECT click_id, user_id, campaign_id, parse_ts(ts_raw) AS ts
            FROM clicks_kept
        ) c
        INNER JOIN campaigns cam ON c.campaign_id = cam.campaign_id
        LEFT JOIN person_map m ON c.user_id = m.user_id
        WHERE c.user_id IS NOT NULL
          AND c.ts IS NOT NULL
          AND c.ts >= {click_lo}
          AND c.ts < {win_end}
        """
    )
    con.execute("DROP TABLE clicks_kept")


def attribute(con):
    # ASOF does not break a ts tie by smallest click_id. Collapse first.
    con.execute(
        """
        CREATE TEMP TABLE clicks_at AS
        SELECT
            person::VARCHAR AS person,
            ts::BIGINT AS ts,
            arg_min(campaign_id, click_id)::VARCHAR AS campaign_id
        FROM valid_clicks
        GROUP BY person, ts
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE credited AS
        SELECT
            s.person,
            s.start_ts,
            s.day,
            s.events,
            s.revenue_cents,
            s.has_purchase,
            CASE
                WHEN c.ts IS NULL OR s.start_ts - c.ts > {WEEK_MS} THEN 'organic'
                ELSE c.campaign_id
            END AS campaign_id
        FROM sessions s
        ASOF LEFT JOIN clicks_at c
            ON s.person = c.person AND s.start_ts >= c.ts
        """
    )
    # One assist per campaign: latest click of that campaign at or before the start, inside 7 days.
    con.execute(
        """
        CREATE TEMP TABLE clicks_by_campaign AS
        SELECT DISTINCT
            person::VARCHAR AS person,
            campaign_id::VARCHAR AS campaign_id,
            ts::BIGINT AS ts
        FROM valid_clicks
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE session_campaigns AS
        SELECT s.person, s.start_ts, s.day, d.campaign_id
        FROM sessions s
        INNER JOIN (
            SELECT DISTINCT person, campaign_id FROM valid_clicks
        ) d ON s.person = d.person
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE assists AS
        SELECT sc.day, sc.campaign_id
        FROM session_campaigns sc
        ASOF LEFT JOIN clicks_by_campaign c
            ON sc.person = c.person
           AND sc.campaign_id = c.campaign_id
           AND sc.start_ts >= c.ts
        WHERE c.ts IS NOT NULL
          AND sc.start_ts - c.ts <= {WEEK_MS}
        """
    )


def reset_output(folder):
    os.makedirs(folder, exist_ok=True)
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if name.endswith(".parquet") and os.path.isfile(path) and not name.startswith((".", "_")):
            os.remove(path)


def write_outputs(con):
    con.execute(
        """
        CREATE TEMP TABLE user_daily AS
        SELECT
            s.person::VARCHAR AS user_id,
            s.day::DATE AS day,
            count(*)::BIGINT AS sessions,
            sum(s.events)::BIGINT AS events,
            sum(s.revenue_cents)::BIGINT AS revenue_cents,
            any_value(t.top_category)::VARCHAR AS top_category
        FROM sessions s
        LEFT JOIN top_cat t ON s.person = t.person AND s.day = t.day
        GROUP BY s.person, s.day
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE campaign_daily AS
        WITH cred AS (
            SELECT
                campaign_id,
                day,
                count(*)::BIGINT AS sessions,
                count(*) FILTER (WHERE has_purchase)::BIGINT AS purchasing_sessions,
                sum(revenue_cents)::BIGINT AS revenue_cents
            FROM credited
            GROUP BY campaign_id, day
        ),
        ass AS (
            SELECT campaign_id, day, count(*)::BIGINT AS assisted_sessions
            FROM assists
            GROUP BY campaign_id, day
        )
        SELECT
            coalesce(cred.campaign_id, ass.campaign_id)::VARCHAR AS campaign_id,
            CASE
                WHEN coalesce(cred.campaign_id, ass.campaign_id) = 'organic' THEN 'organic'
                ELSE cam.channel
            END::VARCHAR AS channel,
            coalesce(cred.day, ass.day)::DATE AS day,
            coalesce(cred.sessions, 0)::BIGINT AS sessions,
            coalesce(cred.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
            coalesce(cred.revenue_cents, 0)::BIGINT AS revenue_cents,
            CASE
                WHEN coalesce(cred.campaign_id, ass.campaign_id) = 'organic' THEN 0::BIGINT
                ELSE coalesce(ass.assisted_sessions, 0)::BIGINT
            END AS assisted_sessions
        FROM cred
        FULL OUTER JOIN ass
            ON cred.campaign_id = ass.campaign_id AND cred.day = ass.day
        LEFT JOIN campaigns cam
            ON cam.campaign_id = coalesce(cred.campaign_id, ass.campaign_id)
        WHERE coalesce(cred.sessions, 0) > 0
           OR coalesce(ass.assisted_sessions, 0) > 0
        """
    )
    user_dir = os.path.join(OUT, "user_daily")
    camp_dir = os.path.join(OUT, "campaign_daily")
    reset_output(user_dir)
    reset_output(camp_dir)
    user_path = os.path.join(user_dir, "part-0.parquet")
    camp_path = os.path.join(camp_dir, "part-0.parquet")
    con.execute(
        f"""
        COPY (
            SELECT user_id, day, sessions, events, revenue_cents, top_category
            FROM user_daily
        ) TO {sql_quote(user_path)} (FORMAT PARQUET)
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT
                campaign_id, channel, day, sessions, purchasing_sessions,
                revenue_cents, assisted_sessions
            FROM campaign_daily
        ) TO {sql_quote(camp_path)} (FORMAT PARQUET)
        """
    )


def main():
    started = time.perf_counter()
    mark = started
    con = connect()
    create_macros(con)
    win_start, win_end, click_lo = load_dimensions(con)
    event_clean, event_dirty = classify(list_csvs("events"))
    click_clean, click_dirty = classify(list_csvs("clicks"))
    link_clean, link_dirty = classify(list_csvs("links"))
    mark = log_stage("read", mark)

    load_person_map(con, link_clean, link_dirty)
    mark = log_stage("links", mark)

    load_events(con, event_clean, event_dirty, win_start, win_end)
    mark = log_stage("events", mark)

    build_sessions(con)
    mark = log_stage("sessions", mark)

    load_clicks(con, click_clean, click_dirty, click_lo, win_end)
    attribute(con)
    mark = log_stage("attribute", mark)

    write_outputs(con)
    log_stage("write", mark)
    print(f"total: {time.perf_counter() - started:.3f}s", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
