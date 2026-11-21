#!/usr/bin/env python3
"""Run the production pipeline on every fixture.

One engine process handles the whole suite and checks both tables against the
expected CSVs. A process per fixture cannot finish in a fraction of a second,
because each one would start the interpreter again.

Usage: python run_fixtures.py FIXTURES_DIR -- python3 pipeline/pipeline.py
"""
import os
import subprocess
import sys


def production_script(cmd):
    scripts = [part for part in cmd if part.endswith(".py")]
    if not scripts:
        raise SystemExit("production command must include the pipeline script")
    return scripts[-1]


def main():
    sep = sys.argv.index("--")
    fixtures, cmd = sys.argv[1], sys.argv[sep + 1 :]
    if not os.path.isdir(fixtures):
        raise SystemExit(f"no fixtures in {fixtures}")
    script = production_script(cmd)
    rc = subprocess.call([sys.executable, script, "--suite", os.path.abspath(fixtures)])
    sys.exit(rc)


def __getattr__(name):
    # compare.py still loads the Polars checkers. The timed path does not.
    if name not in {"SCHEMAS", "load_expected", "load_actual", "rows"}:
        raise AttributeError(name)
    import pathlib
    import polars as pl

    schemas = {
        "user_daily": {
            "user_id": pl.String,
            "day": pl.Date,
            "sessions": pl.Int128,
            "events": pl.Int128,
            "revenue_cents": pl.Int128,
            "top_category": pl.String,
        },
        "campaign_daily": {
            "campaign_id": pl.String,
            "channel": pl.String,
            "day": pl.Date,
            "sessions": pl.Int128,
            "purchasing_sessions": pl.Int128,
            "revenue_cents": pl.Int128,
            "assisted_sessions": pl.Int128,
        },
    }

    def load_expected(path, schema):
        frame = pl.read_csv(
            path,
            schema_overrides={k: (pl.String if v == pl.Date else v) for k, v in schema.items()},
        )
        if frame.height == 0:
            return pl.DataFrame(schema=schema)
        return frame.with_columns(pl.col("day").str.to_date())

    def load_actual(folder, schema):
        folder = pathlib.Path(folder)
        if folder.is_symlink() or not folder.is_dir():
            raise ValueError(f"expected an output directory: {folder}")
        frames = []
        for path in sorted(folder.glob("*.parquet")):
            if path.name.startswith((".", "_")) or path.is_symlink() or not path.is_file():
                continue
            frame = pl.read_parquet(path)
            if set(frame.columns) != set(schema):
                raise ValueError(f"{path}: expected {list(schema)}, got {frame.columns}")
            for column, expected in schema.items():
                actual = frame.schema[column]
                if expected == pl.Int128 and actual.is_integer():
                    continue
                if expected == pl.String and actual.base_type() in (pl.String, pl.Categorical, pl.Enum):
                    continue
                if actual != expected:
                    raise ValueError(f"{path}: {column} has type {actual}, expected {expected}")
            frames.append(frame.select(schema.keys()).cast(schema))
        if not frames:
            raise ValueError(f"no parquet in {folder}")
        return pl.concat(frames)

    def rows(frame):
        return sorted(frame.rows(), key=lambda r: tuple("" if v is None else str(v) for v in r))

    globals().update(
        SCHEMAS=schemas,
        load_expected=load_expected,
        load_actual=load_actual,
        rows=rows,
    )
    return globals()[name]


if __name__ == "__main__":
    main()
