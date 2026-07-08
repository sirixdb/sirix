#!/usr/bin/env python3
"""DuckDB side of the SirixDB-vs-DuckDB comparison (docs/COMPARISON_DUCKDB.md).

Generates the benchmark table in-engine with the same shape and distributions
as SirixDB's GeneratedRecordsReader ({id, age: 18..65, dept: 8 values,
city: 8 values, active: bool}, uniform), then runs the nine analytical query
shapes with one untimed warmup and N timed runs each, materializing results
via fetchall() and reporting the minimum.

Usage:
    python3 duck_bench.py <rows> <iters> <db_path> [threads]

Example (the published 100M-record comparison):
    python3 duck_bench.py 100000000 3 /tmp/duck-100m.db 20

Requires: pip install duckdb
"""

import sys
import time

import duckdb

DEPTS = ["Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp"]
CITIES = ["NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL"]

QUERIES = [
    ("filterCount", "SELECT count(*) FROM records WHERE age > 40 AND active"),
    ("groupByDept", "SELECT dept, count(*) FROM records GROUP BY dept"),
    ("sumAge", "SELECT sum(age) FROM records"),
    ("avgAge", "SELECT avg(age) FROM records"),
    ("minMaxAge", "SELECT min(age), max(age) FROM records"),
    ("groupByDeptCity", "SELECT dept, city, count(*) FROM records GROUP BY dept, city"),
    ("filterGroupBy", "SELECT dept, count(*) FROM records WHERE active GROUP BY dept"),
    ("countDistinctDept", "SELECT count(DISTINCT dept) FROM records"),
    ("compoundAnd", "SELECT count(*) FROM records WHERE age > 30 AND age < 50 AND active"),
]


def main() -> None:
    if len(sys.argv) < 4:
        sys.exit(__doc__)
    rows = int(sys.argv[1])
    iters = int(sys.argv[2])
    db_path = sys.argv[3]
    threads = int(sys.argv[4]) if len(sys.argv) > 4 else 20

    con = duckdb.connect(db_path)
    con.execute(f"SET threads={threads}")
    print(f"# duckdb {duckdb.__version__}, threads={threads}, rows={rows:,}, iters={iters}")

    existing = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'records'"
    ).fetchone()[0]
    if existing:
        count = con.execute("SELECT count(*) FROM records").fetchone()[0]
        print(f"# reusing existing table records ({count:,} rows)")
    else:
        depts = ", ".join(f"'{d}'" for d in DEPTS)
        cities = ", ".join(f"'{c}'" for c in CITIES)
        t0 = time.perf_counter()
        con.execute("SELECT setseed(0.42)")
        con.execute(
            f"""
            CREATE TABLE records AS
            SELECT range AS id,
                   18 + CAST(floor(random() * 48) AS INTEGER) AS age,
                   ([{depts}])[1 + CAST(floor(random() * 8) AS INTEGER)] AS dept,
                   ([{cities}])[1 + CAST(floor(random() * 8) AS INTEGER)] AS city,
                   random() < 0.5 AS active
            FROM range({rows})
            """
        )
        print(f"# generated {rows:,} rows in {time.perf_counter() - t0:.1f} s")

    for name, sql in QUERIES:
        con.execute(sql).fetchall()  # untimed warmup
        best = min(timed_run(con, sql) for _ in range(iters))
        print(f"{name}: {best * 1000:.1f} ms")


def timed_run(con: "duckdb.DuckDBPyConnection", sql: str) -> float:
    t0 = time.perf_counter()
    con.execute(sql).fetchall()
    return time.perf_counter() - t0


if __name__ == "__main__":
    main()
