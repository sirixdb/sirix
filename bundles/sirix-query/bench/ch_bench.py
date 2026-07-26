#!/usr/bin/env python3
"""ClickHouse (chdb, embedded) side of the SirixDB cross-engine comparison —
the ClickHouse analog of duck_bench.py (docs/COMPARISON_DUCKDB.md).

Generates the benchmark table in-engine with the same shape and distributions
as SirixDB's GeneratedRecordsReader ({id, age: 18..65, dept: 8 values,
city: 8 values, active: bool}, uniform), then runs the nine analytical query
shapes with one untimed warmup and N timed runs each, materializing results
and reporting the minimum.

Usage:
    python3 ch_bench.py <rows> <iters> <db_path> [threads]

Requires: pip install chdb
"""

import sys
import time

import chdb.session

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
    threads = int(sys.argv[4]) if len(sys.argv) > 4 else 4

    ses = chdb.session.Session(db_path)
    settings = f"SETTINGS max_threads = {threads}"
    print(f"# chdb {chdb.__version__} (ClickHouse {chdb.engine_version}), threads={threads}, "
          f"rows={rows:,}, iters={iters}")

    existing = ses.query(
        "SELECT count(*) FROM system.tables WHERE database = currentDatabase() AND name = 'records'"
    ).data().strip()
    if existing != "0":
        count = ses.query("SELECT count(*) FROM records").data().strip()
        print(f"# reusing existing table records ({int(count):,} rows)")
    else:
        depts = ", ".join(f"'{d}'" for d in DEPTS)
        cities = ", ".join(f"'{c}'" for c in CITIES)
        t0 = time.perf_counter()
        ses.query(
            """
            CREATE TABLE records (
                id UInt64,
                age Int64,
                dept LowCardinality(String),
                city LowCardinality(String),
                active Bool
            ) ENGINE = MergeTree ORDER BY id
            """
        )
        ses.query(
            f"""
            INSERT INTO records
            SELECT number AS id,
                   18 + rand(1) % 48 AS age,
                   [{depts}][1 + rand(2) % 8] AS dept,
                   [{cities}][1 + rand(3) % 8] AS city,
                   rand(4) % 2 = 1 AS active
            FROM numbers({rows})
            """
        )
        print(f"# generated {rows:,} rows in {time.perf_counter() - t0:.1f} s")

    for name, sql in QUERIES:
        full = f"{sql} {settings}"
        ses.query(full).data()  # untimed warmup
        best = min(timed_run(ses, full) for _ in range(iters))
        print(f"{name}: {best * 1000:.1f} ms")


def timed_run(ses: "chdb.session.Session", sql: str) -> float:
    t0 = time.perf_counter()
    ses.query(sql).data()
    return time.perf_counter() - t0


if __name__ == "__main__":
    main()
