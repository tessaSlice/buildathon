# Shopping Sessions architecture

Shared design for the DuckDB, Polars, and PySpark pipelines. `rules.md` wins if this file and the rules ever disagree. A row that is almost right scores nothing. Beat the PySpark baseline of **171.91 seconds**. A practice time over about 60 seconds will probably time out on the 60M-event final (3 minute cap), so the target is well under 60 seconds on 20M events and comfortably under 3 minutes on 60M.

The scored data is produced by `generator/` (`generate --seed 1 --events 1000000 --clicks 250000` is the public dev set). The generator never writes a `\r`. The 36 fixtures do, including `E20`, and they must pass.

## What the pipeline computes

```
manifest.json ───────────── window [start, end)
products.csv  ───────────── product_id -> category (missing => "unknown")
campaigns.csv ───────────── campaign_id -> channel
links/*.csv ── parse ── union-find ── person = bytewise min user id
events/*.csv ─ parse ─ dedup(event_id, min ingest_ts) ─ validate ─ drop late
              ─ map user_id -> person ─ sort ─ sessionize (gap > 30 min)
              ─ day = UTC date of session start
clicks/*.csv ─ parse ─ dedup(click_id) ─ validate (campaign must exist, click window)
              ─ map user_id -> person
sessions ── credit: latest click with click.ts <= start and start-click.ts <= 7d
            tie on ts -> smallest click_id; else campaign "organic" / channel "organic"
         ── assists: each campaign with any click in [start-7d, start], once
            the crediting campaign assists too; organic never assists
aggregate user_daily and campaign_daily
```

Constants, all in milliseconds:

| Name | Value | Rule |
| --- | --- | --- |
| `DAY_MS` | 86_400_000 | UTC day and the late-event cutoff |
| `GAP_MS` | 1_800_000 | New session only when gap is **strictly greater** |
| `WEEK_MS` | 604_800_000 | Click credit and assist lookback, **inclusive** |
| `LATE_MS` | 86_400_000 | Drop event when `ingest_ts - ts > LATE_MS` |

Event window: `window_start <= ts < window_end`.
Click window: `window_start - 7 days <= ts < window_end`.
`window_start` and `window_end` are F1 timestamps in `manifest.json`.

## Output

Write one or more Parquet files directly in:

- `$OUTPUT_DIR/user_daily/`
- `$OUTPUT_DIR/campaign_daily/`

`day` is a Parquet DATE, not a string or timestamp. Other integers may be any width. No extra columns. Null `top_category` is a real null, not `""`. Row order does not matter.

`user_daily`: one row per `(person, day)` with at least one session.

| Column | Definition |
| --- | --- |
| `user_id` | Person name (min linked id) |
| `day` | UTC date of session start |
| `sessions` | Sessions starting that day |
| `events` | Surviving events in those sessions |
| `revenue_cents` | Sum of purchase `price_cents` in those sessions, else 0 |
| `top_category` | Category with the highest summed purchase revenue that day. Tie: bytewise smallest category. Null if no purchases. `unknown` competes |

`campaign_daily`: one row per `(campaign_id, day)` with at least one credited session or one assist.

| Column | Definition |
| --- | --- |
| `campaign_id` | Campaign, or `organic` |
| `channel` | From `campaigns.csv`, or `organic` |
| `day` | UTC date of session start |
| `sessions` | Sessions **credited** to this campaign that day |
| `purchasing_sessions` | Credited sessions with at least one purchase |
| `revenue_cents` | Purchase sum in credited sessions |
| `assisted_sessions` | Sessions that day this campaign assisted. Always 0 for `organic` |

A campaign that only assisted has `sessions = purchasing_sessions = revenue_cents = 0`.

Self-check, summed over all rows: `sessions` and `revenue_cents` match across the two tables. Summed `assisted_sessions` is at least the credited non-organic `sessions`.

## CSV dialect

Comma-separated, UTF-8, only `\n` ends a line. A `\r` is data. Do not let the reader treat `\r\n` as a newline: that turns a price `1000\r` into `1000` and a click ts `...Z\r` into a valid timestamp (`E20`).

Every comma separates fields. No value contains a comma, quote, or newline. Split on `,` is always correct.

A line is dropped when:

- it is empty
- the field count is wrong (events 7, clicks 4, links 2)
- any field contains `"` except as quotes wrapped around the whole field

A field is quoted only when it has length at least 2, starts and ends with `"`, and the inside contains no `"`. `""` is an empty field. After unquoting, an empty field is null. Spaces stay. Do not trim.

Skip the header row of **each file** only. A later data row whose text equals the header is real (`L3`: `user_id_a,user_id_b` links those two literal ids). Header-only files contribute no rows.

Empty `event_id` / `click_id` drops the line. A link with either side null is dropped.

### Fast reader that stays correct

The engine CSV readers are not spec-correct if you let them unquote and repair rows. Measured on DuckDB 1.5.6:

- `ignore_errors=true` **keeps** extra-field rows and truncates them (`u1,u2,u3` becomes `u1,u2`). That fails `L3`.
- It **keeps** a quote that is not wrapped around the field (`u5"`).
- It treats `\r` as a line break.

Do this instead.

1. If a file contains the byte `\r`, parse that file with a strict binary reader (split on `\n` only, keep `\r`). The generator never emits `\r`, so this path is the fixtures. A 1 MB Python scan of a file is enough to decide.
2. Otherwise read each file as **raw lines**, not as typed CSV. DuckDB options that do this:

```sql
SELECT line
FROM read_csv('events/*.csv',
    header = true,                       -- drops the first row of every file
    columns = {'line': 'VARCHAR'},
    delim = chr(30),                     -- does not occur in the data
    quote = chr(1),
    escape = chr(1),
    auto_detect = false,
    strict_mode = false,
    ignore_errors = true,
    parallel = true,
    max_line_size = 10485760)
```

Empty lines arrive as NULL. Drop them. Then `str_split(line, ',')` and validate in SQL. Glob with `header=true` was checked: each file's header is skipped, a later header-shaped data row is kept, a header-only file yields nothing.

Polars equivalent (also checked: internal `\r` survives, a trailing `\r` immediately before `\n` does **not**, so the `\r` scan is still required):

```python
pl.read_csv(
    paths, has_header=True, separator="\x1e", quote_char=None,
    eol_char="\n", infer_schema=False, truncate_ragged_lines=False,
)
```

Use one string column. `split_exact(",", n_fields-1)` and validate with expressions. No Python row loop on this path.

PySpark: `spark.read.option("lineSep", "\n").text(files)` and the same split/validate in Spark SQL. Confirm against `E20` and `L3`. If `text` strips `\r`, use the binary fallback for files that contain `\r`.

Products (50k) and campaigns (2k) are clean and unique. A normal CSV read is fine for those two files.

## Timestamps

Three forms. Anything else is bad, including a trailing space (`try_strptime` alone accepts `'2026-10-03T10:00:00Z '` — gate on the regex first).

ASCII digits only. `[0-9]`, never `\d`.

**F1** length 20 and full match `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$`, then `try_strptime(ts, '%Y-%m-%dT%H:%M:%SZ')`. DuckDB's `try_strptime` rejects Feb 30, Feb 29 in a non-leap year, hour 24, second 60, lowercase `z`, fractional seconds, and a space instead of `T`. It accepts Feb 29 2028. Use `epoch_ms` of that timestamp.

**F2** length 25 and full match `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}$`. Offset hours must be 0..23 and minutes 0..59 (`+24:00`, `+05:60`, `+0200` are bad; `+23:59` and `-00:00` are good). Parse the first 19 characters with `try_strptime(..., '%Y-%m-%dT%H:%M:%S')` as a naive UTC timestamp, then subtract the offset:

```
sign = -1 when char 20 is '-', else +1
offset_ms = sign * (hour * 3600 + minute * 60) * 1000
utc_ms = epoch_ms(naive) - offset_ms
```

Checked: `2026-10-03T19:58:10-04:00` equals `2026-10-03T23:58:10Z` equals `1791071890000`. `2026-10-04T01:30:00+02:00` is `2026-10-03 23:30:00Z`.

**F3** full match `^[0-9]+$`. Strip leading zeros. If the remaining string is empty, the value is 0. If it is longer than 18 digits, it does not fit in a positive int64: treat it as outside the window (drop), do not cast. Otherwise `CAST` to BIGINT. Leading zeros are the same instant (`0001791021600000`). Unicode digits (`١٧٩…`, `２５０`) fail the regex and are bad.

`ingest_ts` is always valid F1. F1 strings also sort in time order, so dedup can `ORDER BY ingest_ts` without parsing. Parse the kept row.

A bad timestamp, or a timestamp outside that file's window, drops the row. An F3 value that parses but falls outside the window is dropped the same way.

UTC date of a millisecond timestamp `ts >= 0`:

```sql
DATE '1970-01-01' + CAST((ts // 86400000) AS INTEGER)
```

DuckDB `/` is not integer division. Use `//`. Checked: `1791021600000 // 86400000` is `2026-10-03`.

## Events

1. Parse. Drop malformed lines and empty `event_id`.
2. Dedup on `event_id` **before** validation. Keep the row with the smallest `ingest_ts`. Equal `ingest_ts` copies are identical. A later valid copy must not rescue a kept invalid copy (`E5`).
3. Validate the kept row. Keep only when `user_id` is non-empty, `ts` is valid and in the event window, `event_type` is exactly `view`, `cart`, or `purchase`, and `product_id` is non-empty. A `purchase` also needs `price_cents` to be ASCII digits whose integer value is in `1 .. 10_000_000` inclusive (`0250` is 250, `10000000` is kept, `0`, empty, signs, decimals, and values above 10_000_000 are dropped). More than 8 significant digits is above the max. `view` and `cart` ignore `price_cents`.
4. Drop late events: `ingest_ms - ts_ms > 86400000`. Exactly 24 hours stays. An ingest time before `ts` stays.
5. Map `user_id` to the person.
6. Sessionize per person, ordered by `ts`. `lag(ts)` within the person: a new session starts when there is no previous row or `ts - prev_ts > 1800000`. Equal timestamps have gap 0, so they stay in the session the first of them joined (`E1`, `E2`, `E16`). A deterministic extra sort key such as `event_id` does not change membership.
7. Session start is `min(ts)` in the session. The day is the UTC date of that start. Later events, including those after midnight, count on that day (`E3`).

Session key: `(person, start_ts)`. Starts are unique per person because a new session requires a strictly later timestamp.

## Linked ids

Parse links, drop malformed lines and empty sides, then union-find. Direction does not matter. Duplicates and self-links do nothing. Chains across files are one person. The name is the bytewise minimum id in the component, even if that id never appears in an event or click (`L2`: `U7` names `u3`/`u9`/`u2` because `U` < `u`). Ids that appear in no surviving link are their own person.

Do **not** iterate "take min of neighbors" once per hop. Chains are hundreds of links long. Use union-find with path compression, or pointer jumping (`parent = parent[parent]`) which doubles each round. Python union-find over the link table is fast enough at full size (a few million edges) and is easier to get right than a recursive CTE. Build a map only for ids whose person name is not themselves, register it, and left-join. Comparison is raw string order, case-sensitive, untrimmed.

Links apply to the whole window and the 7 days before it. Link first, then attribute. An event and a click join if any link path connects their user ids (`L1`, `L4`, `A12`).

## Clicks

1. Parse. Drop malformed lines and empty `click_id`.
2. Dedup on `click_id`. Copies are identical, so keep any one.
3. Validate: non-empty `user_id`, `ts` valid and in the click window, `campaign_id` present in `campaigns.csv`. Unknown campaigns drop the click (`A6`). No case folding (`A7`).
4. Map to person.

### Credit

Among the person's valid clicks with `click.ts <= session_start` and `session_start - click.ts <= WEEK_MS`, pick the greatest `ts`. Tie: smallest `click_id`, bytewise. A click at exactly the start counts. A click 1 ms later does not. One click may credit many sessions. No qualifying click => `organic`.

DuckDB `ASOF LEFT JOIN` on `person` and `start_ts >= click.ts` picks one row, and on a `ts` tie it does **not** promise the smallest `click_id` (checked: it picked `k2` over `k1`). Collapse first:

```sql
SELECT person, ts,
       arg_min(campaign_id, click_id) AS campaign_id
FROM valid_clicks
GROUP BY person, ts
```

Then ASOF. If the matched `ts` is null or `start_ts - ts > WEEK_MS`, the campaign is `organic`.

Polars: `join_asof(..., strategy="backward", by="person")` on the same collapsed table. The right frame must be sorted by the asof key.

PySpark has no asof join. Collapse the same way, union session markers `(kind=1, ts=start_ts)` with clicks `(kind=0)`, and take `last(campaign_id, true)` over `PARTITION BY person ORDER BY ts, kind`. Clicks at the same timestamp come first (`kind=0`), so a click at the start is visible. Then apply the 7-day check.

### Assists

A campaign assists a session once if the person has any valid click of that campaign with `start_ts - WEEK_MS <= click.ts <= start_ts`. That is exactly: the latest click of that campaign at or before `start_ts` is still inside 7 days.

Do not inequality-join every click to every session. Whales have on the order of a million events and hundreds of thousands of clicks. Build the distinct `(person, campaign_id)` pairs the person actually clicked (about a handful per person, thousands in the worst whale), join those to sessions, then ASOF to clicks of that `(person, campaign_id)`. Filter `start_ts - ts <= WEEK_MS`.

`organic` is not in this result. The crediting campaign is included when its click is in the window, which it is by construction.

## Aggregates

Purchases carry `price_cents` and `coalesce(category, 'unknown')`.

`user_daily` from sessions grouped by `(person, day)`:

- `sessions = count(*)`
- `events = sum(events)`
- `revenue_cents = sum(revenue_cents)`

`top_category` from purchases, not from per-session winners: group purchases by `(person, day, category)`, sum revenue, then `row_number() OVER (PARTITION BY person, day ORDER BY revenue DESC, category ASC) = 1`. Left-join onto `user_daily`. No purchases => null.

`campaign_daily`:

- credited side: group sessions by `(campaign_id, day)` for `sessions`, `purchasing_sessions`, `revenue_cents`
- assist side: group assist rows by `(campaign_id, day)` for `assisted_sessions`
- full outer join, fill zeros
- drop rows where credited sessions and assisted sessions are both 0
- attach `channel`; `organic` => `organic`

## Engine notes

### All three

- Read `$DATA_DIR`, `$OUTPUT_DIR`, `$SCRATCH_DIR`, `$NCPU`, `$MEM_GB`. Defaults match the starters.
- Threads = `NCPU`. Leave memory headroom (DuckDB `memory_limit` around 80% of `MEM_GB`, temp directory under `$SCRATCH_DIR`).
- Root is read-only at score time except `/tmp` (64 MB) and the scratch/output mounts. `HOME` and `TMPDIR` are scratch. Do not write anywhere else.
- Stdout is capped at 50 MB. Log timings to stderr.
- No network at run time. Install every dependency in the image.
- Image is `linux/amd64`, uid 1000, at most 8 GB. Do not copy the dataset into the image.
- Keep the starter `Dockerfile`, `run.sh`, and `scripts/` behavior. The checker ignores files in the output folders whose names start with `.` or `_`.

### DuckDB

In-memory database, `preserve_insertion_order=false`, `threads=NCPU`. Prefer one SQL script after the link map is registered. `QUALIFY` is available. Register the person map with `con.register` from a PyArrow or Polars table, or insert it. Write with `COPY ... TO '...parquet' (FORMAT parquet)`.

### Polars

Set `POLARS_MAX_THREADS` before import, as the starter does. Lazy frames are appropriate for the scan and the joins. `join_asof` requires the right side sorted by the on-key within `by`. Cast `day` with `pl.Date` before `write_parquet`. Streaming collect is useful if a plan exceeds memory. The dev machine here has 4 cores and about 13 GB free. The scorer has 8 vCPUs and 24 GB.

### PySpark

Keep the starter session config (`local[NCPU]`, driver memory, `spark.local.dir`, UTC, no UI). Parse and aggregate in the JVM, not with Python UDFs. `spark.sql.shuffle.partitions` around `NCPU * 4` is a starting point. Broadcast `products` and `campaigns`. Union-find stays on the driver. The link map is small enough to broadcast. Cache only the surviving events and the valid clicks if they are reused. Write with `.mode("overwrite").parquet`, and make sure `day` is `DateType`.

## Fixture pitfalls worth pinning

| Fixture | What a wrong implementation does |
| --- | --- |
| E1 / E2 | Gap of exactly 30:00.000 stays one session. 30:00.001 splits |
| E3 | Session that crosses midnight belongs to the start day, revenue included |
| E4 / E5 | Earliest `ingest_ts` wins, even when that copy is invalid or is a different user |
| E6 | Exactly 24 h late is kept. 24 h + 1 s is dropped |
| E8 | Category tie goes to the bytewise smallest name |
| E9 | Missing product is `unknown` and can win `top_category` |
| E10 | Bad purchase price drops the event. Bad view price is ignored |
| E16 | Several events with one `ts` join the session of the first of them |
| E17 | `+24:00`, `+05:60`, `+0200` dropped. `+23:59` and `-00:00` kept |
| E18 | Leading zeros kept. Unicode digits and an oversized F3 dropped. `0250` kept |
| E19 | ` u1`, `u1 `, `" u1"` are three ids. ` c_1` is not `c_1` |
| E20 | `\r` is data. `1000\r` is a bad price. `...Z\r` is a bad click ts. `e3\r` is a different event id |
| A2 / A10 | 7 days and `window_start - 7 days` are inclusive. 1 ms earlier and `window_end` are out |
| A4 / A5 | Click at the start counts. Same `ts`: smallest `click_id` |
| A8 / A9 | One click credits many later sessions. A click mid-session does not credit the current one |
| A11 | Every campaign in the 7 days assists once, including the credited one. Assist-only rows have 0 credited metrics. `organic` assists nothing |
| A12 | A click assists each later session on that session's own day, across linked ids |
| L2 | Transitive links. The min id can be one that has no events. Self-link is a no-op |
| L3 | Bad link lines are dropped. A data row equal to the header is kept. The person name can contain a tab |

## Suggested layout in each solution

The starter tree is already copied to `solutions/<engine>/` with `fixtures` symlinked at the shared fixture set. Implement the rules in `pipeline/pipeline.py` only, unless a script or Dockerfile change is required to run. Time the stages to stderr (`read`, `links`, `events`, `sessions`, `attribute`, `write`).

Run, from `solutions/<engine>`:

```sh
python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py
```

Polars must be importable for the checker (`polars==2.1.0` is installed for this workspace). DuckDB is `duckdb==1.5.6`. PySpark is `pyspark==4.0.1` and needs Java, which is on this machine.

Done means **36/36 fixtures passed**, both output schemas accepted by `scripts/run_fixtures.py`, and the pipeline is the vectorized plan above rather than a Python loop over events.

## Rust fast path

`solutions/rust/` is a separate tree. It follows the same rules. A std-only binary parses, links, sessionizes, and attributes. `pipeline/pipeline.py` turns the two columnar dumps into Parquet with PyArrow, because `day` has to be a Parquet DATE. The Dockerfile is a multi-stage build (`rust:1.85` then `python:3.12`) and does not copy the dataset.

Hot path choices that the SQL engines do not get:

- Read each file as bytes and split only on `\n`. One arena per thread holds field bytes. Dedup sorts an inline `(hash, shard, row)` key, then compares id bytes only inside equal-hash runs. Smallest `ingest_ts` string wins, before validation.
- Person ids live in one open-addressed table and one string arena. The root is the bytewise-minimum id.
- Credit is a forward pointer over clicks sorted by `(person, ts, click_id)`. Assists are a sliding window of campaigns in `[start - 7d, start]`, aggregated straight into `(campaign, day)` counts. There is no session×campaign pair table.
- Timestamp parsing branches on length. F1 and F2 use a Gregorian conversion. F3 rejects more than 18 significant digits instead of overflowing.

Measured on this 4-core machine, musl build, nothing else busy: 1,000,000 events in 1.69s (exact match to `dev-answer`), 20,000,000 events and 5,000,000 clicks (seed 2) in 35.60s, peak RSS 4.6 GB. That is under the 171.91s baseline. A 60M run was not executed here. Three times the 20M time is about 107s and three times the RSS is about 14 GB, inside the 24 GB and 3 minute final limits. The scorer has 8 vCPUs; ingest and event validation are the stages that use the extra cores.
