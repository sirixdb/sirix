#!/usr/bin/env python3
"""DuckDB reference runner for the SirixDB ClickBench port.

This is the engine the SirixDB answers are diffed against. It does two jobs:

  1. build a DuckDB table ``hits`` whose column types are EXACTLY the ones in
     ClickBench's ``duckdb/create.sql``, from EITHER the official parquet file
     OR the very same JSON array file that SirixDB ingested -- so the
     differential compares identical data, not merely similar data;
  2. run the 43 canonical queries with the ClickBench protocol (3 tries,
     results materialised inside the timed region) and dump every query's
     RESULT SET in a canonical form that ``compare-results.py`` can diff.

Usage
-----
    ./duckdb_reference.py --source hits.parquet --format parquet \\
        --db :memory: --out results-duckdb --tries 3

    ./duckdb_reference.py --source hits.json --format json \\
        --db /tmp/hits.duckdb --out results-duckdb --rows 1000000

Output
------
    <out>/qNN.jsonl              one JSON ARRAY PER LINE, one line per result
                                 row, values in SELECT order, canonicalised
                                 (see ``canon`` below). NN is the 0-based
                                 ClickBench query index, matching
                                 ``ClickBenchQueries.byIndex(NN)``.
    <out>/clickbench-results.txt the ClickBench results block
    <out>/summary.json           timings, row counts, errors, engine version

Column types
------------
``COLUMNS`` below is the single source of truth and is a verbatim transcription
of ClickBench ``duckdb/create.sql`` (105 columns, in file order). The same
mapping is emitted by ``ClickBenchSchema.duckdbColumnSpecJson()`` on the Java
side; pass ``--column-spec <file>`` to cross-check this table against that JSON
and fail if the two ever drift apart.

The JSON encoding has no date type, so ``EventDate`` and the three TIMESTAMP
columns arrive as ISO-8601 strings. They are therefore READ as VARCHAR (with an
explicit ``columns=`` map, so DuckDB's sampler cannot guess BIGINT as DOUBLE for
the wide 18-digit ids either) and CAST on insert, which leaves the ``hits``
table itself bit-identical to a parquet-sourced load.

Known cross-engine semantics (documented, not worked around -- see README.md):
  * Q27/Q28 use ``STRLEN``, which is BYTES in DuckDB, while XQuery
    ``string-length`` counts code points. Identical for ASCII URLs only.
  * Q42's ``DATE_TRUNC('minute', EventTime)`` is a TIMESTAMP here and renders
    as ``YYYY-MM-DDTHH:MM:SS``; the JSONiq port's ``substring(t, 1, 16)``
    renders ``YYYY-MM-DDTHH:MM``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import duckdb

# --------------------------------------------------------------------------
# duckdb/create.sql, verbatim: (column name, DDL type) in file order.
# --------------------------------------------------------------------------
COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("WatchID", "BIGINT NOT NULL"),
    ("JavaEnable", "SMALLINT NOT NULL"),
    ("Title", "TEXT"),
    ("GoodEvent", "SMALLINT NOT NULL"),
    ("EventTime", "TIMESTAMP NOT NULL"),
    ("EventDate", "Date NOT NULL"),
    ("CounterID", "INTEGER NOT NULL"),
    ("ClientIP", "INTEGER NOT NULL"),
    ("RegionID", "INTEGER NOT NULL"),
    ("UserID", "BIGINT NOT NULL"),
    ("CounterClass", "SMALLINT NOT NULL"),
    ("OS", "SMALLINT NOT NULL"),
    ("UserAgent", "SMALLINT NOT NULL"),
    ("URL", "TEXT"),
    ("Referer", "TEXT"),
    ("IsRefresh", "SMALLINT NOT NULL"),
    ("RefererCategoryID", "SMALLINT NOT NULL"),
    ("RefererRegionID", "INTEGER NOT NULL"),
    ("URLCategoryID", "SMALLINT NOT NULL"),
    ("URLRegionID", "INTEGER NOT NULL"),
    ("ResolutionWidth", "SMALLINT NOT NULL"),
    ("ResolutionHeight", "SMALLINT NOT NULL"),
    ("ResolutionDepth", "SMALLINT NOT NULL"),
    ("FlashMajor", "SMALLINT NOT NULL"),
    ("FlashMinor", "SMALLINT NOT NULL"),
    ("FlashMinor2", "TEXT"),
    ("NetMajor", "SMALLINT NOT NULL"),
    ("NetMinor", "SMALLINT NOT NULL"),
    ("UserAgentMajor", "SMALLINT NOT NULL"),
    ("UserAgentMinor", "VARCHAR(255) NOT NULL"),
    ("CookieEnable", "SMALLINT NOT NULL"),
    ("JavascriptEnable", "SMALLINT NOT NULL"),
    ("IsMobile", "SMALLINT NOT NULL"),
    ("MobilePhone", "SMALLINT NOT NULL"),
    ("MobilePhoneModel", "TEXT"),
    ("Params", "TEXT"),
    ("IPNetworkID", "INTEGER NOT NULL"),
    ("TraficSourceID", "SMALLINT NOT NULL"),
    ("SearchEngineID", "SMALLINT NOT NULL"),
    ("SearchPhrase", "TEXT"),
    ("AdvEngineID", "SMALLINT NOT NULL"),
    ("IsArtifical", "SMALLINT NOT NULL"),
    ("WindowClientWidth", "SMALLINT NOT NULL"),
    ("WindowClientHeight", "SMALLINT NOT NULL"),
    ("ClientTimeZone", "SMALLINT NOT NULL"),
    ("ClientEventTime", "TIMESTAMP NOT NULL"),
    ("SilverlightVersion1", "SMALLINT NOT NULL"),
    ("SilverlightVersion2", "SMALLINT NOT NULL"),
    ("SilverlightVersion3", "INTEGER NOT NULL"),
    ("SilverlightVersion4", "SMALLINT NOT NULL"),
    ("PageCharset", "TEXT"),
    ("CodeVersion", "INTEGER NOT NULL"),
    ("IsLink", "SMALLINT NOT NULL"),
    ("IsDownload", "SMALLINT NOT NULL"),
    ("IsNotBounce", "SMALLINT NOT NULL"),
    ("FUniqID", "BIGINT NOT NULL"),
    ("OriginalURL", "TEXT"),
    ("HID", "INTEGER NOT NULL"),
    ("IsOldCounter", "SMALLINT NOT NULL"),
    ("IsEvent", "SMALLINT NOT NULL"),
    ("IsParameter", "SMALLINT NOT NULL"),
    ("DontCountHits", "SMALLINT NOT NULL"),
    ("WithHash", "SMALLINT NOT NULL"),
    ("HitColor", "CHAR NOT NULL"),
    ("LocalEventTime", "TIMESTAMP NOT NULL"),
    ("Age", "SMALLINT NOT NULL"),
    ("Sex", "SMALLINT NOT NULL"),
    ("Income", "SMALLINT NOT NULL"),
    ("Interests", "SMALLINT NOT NULL"),
    ("Robotness", "SMALLINT NOT NULL"),
    ("RemoteIP", "INTEGER NOT NULL"),
    ("WindowName", "INTEGER NOT NULL"),
    ("OpenerName", "INTEGER NOT NULL"),
    ("HistoryLength", "SMALLINT NOT NULL"),
    ("BrowserLanguage", "TEXT"),
    ("BrowserCountry", "TEXT"),
    ("SocialNetwork", "TEXT"),
    ("SocialAction", "TEXT"),
    ("HTTPError", "SMALLINT NOT NULL"),
    ("SendTiming", "INTEGER NOT NULL"),
    ("DNSTiming", "INTEGER NOT NULL"),
    ("ConnectTiming", "INTEGER NOT NULL"),
    ("ResponseStartTiming", "INTEGER NOT NULL"),
    ("ResponseEndTiming", "INTEGER NOT NULL"),
    ("FetchTiming", "INTEGER NOT NULL"),
    ("SocialSourceNetworkID", "SMALLINT NOT NULL"),
    ("SocialSourcePage", "TEXT"),
    ("ParamPrice", "BIGINT NOT NULL"),
    ("ParamOrderID", "TEXT"),
    ("ParamCurrency", "TEXT"),
    ("ParamCurrencyID", "SMALLINT NOT NULL"),
    ("OpenstatServiceName", "TEXT"),
    ("OpenstatCampaignID", "TEXT"),
    ("OpenstatAdID", "TEXT"),
    ("OpenstatSourceID", "TEXT"),
    ("UTMSource", "TEXT"),
    ("UTMMedium", "TEXT"),
    ("UTMCampaign", "TEXT"),
    ("UTMContent", "TEXT"),
    ("UTMTerm", "TEXT"),
    ("FromTag", "TEXT"),
    ("HasGCLID", "SMALLINT NOT NULL"),
    ("RefererHash", "BIGINT NOT NULL"),
    ("URLHash", "BIGINT NOT NULL"),
    ("CLID", "INTEGER NOT NULL"),
)

TIMESTAMP_COLUMNS = frozenset({"EventTime", "ClientEventTime", "LocalEventTime"})
DATE_COLUMNS = frozenset({"EventDate"})

TS_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
SIGNIFICANT_DIGITS = 6


def base_type(ddl_type: str) -> str:
    """'VARCHAR(255) NOT NULL' -> 'VARCHAR'; 'Date NOT NULL' -> 'DATE'."""
    head = ddl_type.split()[0].upper()
    paren = head.find("(")
    return head[:paren] if paren >= 0 else head


def json_read_type(name: str, ddl_type: str) -> str:
    """The type DuckDB must use when READING the SirixDB JSON encoding."""
    if name in TIMESTAMP_COLUMNS or name in DATE_COLUMNS:
        return "VARCHAR"  # ISO-8601 strings in JSON; CAST happens on insert
    bt = base_type(ddl_type)
    if bt in ("TEXT", "CHAR", "VARCHAR"):
        return "VARCHAR"
    return bt  # SMALLINT / INTEGER / BIGINT, never inferred


def create_table_ddl(table: str) -> str:
    body = ",\n    ".join(f'"{name}" {ddl}' for name, ddl in COLUMNS)
    return f"CREATE TABLE {table}\n(\n    {body}\n);"


def sql_lit(value: str) -> str:
    return value.replace("'", "''")


def load_column_spec(path: Path) -> Dict[str, str]:
    """Read ClickBenchSchema.duckdbColumnSpecJson()'s output: {name: ddl type}."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: expected a JSON object of name -> DuckDB type")
    return {str(k): str(v) for k, v in raw.items()}


def check_column_spec(spec: Dict[str, str]) -> None:
    """Fail loudly if the Java side and this table ever disagree."""
    mine = {name: base_type(ddl) for name, ddl in COLUMNS}
    theirs = {name: base_type(ddl) for name, ddl in spec.items()}
    if list(mine.keys()) != list(theirs.keys()):
        only_mine = [c for c in mine if c not in theirs]
        only_theirs = [c for c in theirs if c not in mine]
        raise SystemExit(
            "column spec mismatch: order/membership differs "
            f"(missing here: {only_theirs}; missing there: {only_mine})"
        )
    bad = {c: (mine[c], theirs[c]) for c in mine if mine[c] != theirs[c]}
    if bad:
        raise SystemExit(f"column spec mismatch: {bad}")


# --------------------------------------------------------------------------
# canonical result rendering
# --------------------------------------------------------------------------
def round_significant(value: float, digits: int = SIGNIFICANT_DIGITS) -> float:
    """Round to `digits` significant digits; the two engines' last ULPs differ."""
    if value == 0.0 or not math.isfinite(value):
        return value
    return float(f"{value:.{digits}g}")


def canon(value: Any) -> Any:
    """One result cell -> a JSON-serialisable, engine-neutral value."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):  # BIGINT / HUGEINT: Python ints are exact
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        return round_significant(value)
    if isinstance(value, decimal.Decimal):
        if value.is_nan():
            return "nan"
        return round_significant(float(value))
    if isinstance(value, dt.datetime):
        base = value.strftime(TS_FORMAT)
        # ClickBench timestamps are second-resolution; keep sub-second digits
        # rather than silently dropping them if a future query produces them.
        return base if value.microsecond == 0 else f"{base}.{value.microsecond:06d}"
    if isinstance(value, dt.date):
        return value.strftime(DATE_FORMAT)
    if isinstance(value, dt.time):
        return value.strftime("%H:%M:%S")
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return [canon(v) for v in value]
    if isinstance(value, dict):
        return {str(k): canon(v) for k, v in value.items()}
    return str(value)


def write_jsonl(path: Path, rows: Sequence[Sequence[Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps([canon(c) for c in row], ensure_ascii=False,
                                separators=(",", ":")))
            fh.write("\n")


# --------------------------------------------------------------------------
# loading
# --------------------------------------------------------------------------
def parse_queries(path: Path) -> List[str]:
    """ClickBench queries.sql: exactly one query per non-empty line."""
    queries: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("--"):
            continue
        queries.append(text.rstrip(";"))
    return queries


def parquet_select_list(con: duckdb.DuckDBPyConnection, read_src: str) -> str:
    """Projection that normalises the official parquet's temporal encoding.

    datasets.clickhouse.com/hits_compatible/athena/hits.parquet stores
    EventTime/ClientEventTime/LocalEventTime as INTEGER unix-seconds and
    EventDate as INTEGER days-since-epoch (hence ClickBench's own
    `epoch_ms(EventTime*1000)` / `make_date(EventDate)`), while a re-exported
    parquet has real TIMESTAMP/DATE. Both are accepted.
    """
    described = con.execute(f"DESCRIBE SELECT * FROM {read_src}").fetchall()
    source_types = {row[0]: str(row[1]).upper() for row in described}

    parts: List[str] = []
    for name, _ddl in COLUMNS:
        src = source_types.get(name)
        if src is None:
            raise SystemExit(f"column '{name}' missing from the parquet source")
        quoted = f'"{name}"'
        if name in TIMESTAMP_COLUMNS:
            if src.startswith("TIMESTAMP"):
                parts.append(quoted)
            elif src == "VARCHAR":
                parts.append(f"CAST({quoted} AS TIMESTAMP)")
            else:
                parts.append(f"epoch_ms(CAST({quoted} AS BIGINT) * 1000)")
        elif name in DATE_COLUMNS:
            if src == "DATE":
                parts.append(quoted)
            elif src == "VARCHAR":
                parts.append(f"CAST({quoted} AS DATE)")
            else:
                parts.append(f"make_date(CAST({quoted} AS INTEGER))")
        elif base_type(_ddl) in ("TEXT", "CHAR", "VARCHAR"):
            parts.append(f"COALESCE({quoted}, '')")
        else:
            parts.append(quoted)
    return ",\n       ".join(parts)


def json_select_list() -> str:
    parts: List[str] = []
    for name, ddl in COLUMNS:
        quoted = f'"{name}"'
        if name in TIMESTAMP_COLUMNS:
            parts.append(f"CAST({quoted} AS TIMESTAMP)")
        elif name in DATE_COLUMNS:
            parts.append(f"CAST({quoted} AS DATE)")
        elif base_type(ddl) in ("TEXT", "CHAR", "VARCHAR"):
            parts.append(f"COALESCE({quoted}, '')")
        else:
            parts.append(quoted)
    return ",\n       ".join(parts)


def build_hits(con: duckdb.DuckDBPyConnection, source: str, fmt: str,
               rows: int, table: str) -> float:
    """Create and fill `table`; returns the load time in seconds."""
    literal = sql_lit(source)
    start = time.perf_counter()
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(create_table_ddl(table))

    if fmt == "parquet":
        read_src = f"read_parquet('{literal}', binary_as_string=true)"
        if rows > 0:
            # file_row_number keeps the subset reproducible across runs and
            # thread counts, which a bare LIMIT is not.
            read_src = (
                f"(SELECT * FROM read_parquet('{literal}', binary_as_string=true,"
                f" file_row_number=true) WHERE file_row_number < {rows}"
                f" ORDER BY file_row_number)"
            )
        select_list = parquet_select_list(con, read_src)
        con.execute(f"INSERT INTO {table} SELECT {select_list} FROM {read_src}")
    else:
        spec = ", ".join(f"'{name}': '{json_read_type(name, ddl)}'"
                         for name, ddl in COLUMNS)
        read_src = f"read_json('{literal}', format='array', columns={{{spec}}})"
        if rows > 0:
            read_src = f"(SELECT * FROM {read_src} LIMIT {rows})"
        con.execute(f"INSERT INTO {table} SELECT {json_select_list()} FROM {read_src}")

    return time.perf_counter() - start


def data_size_bytes(con: duckdb.DuckDBPyConnection, db: str, source: str) -> Tuple[int, str]:
    """(bytes, what was measured). An in-memory DB has no on-disk footprint."""
    if db and db != ":memory:":
        con.execute("CHECKPOINT")
        total = os.path.getsize(db)
        wal = f"{db}.wal"
        if os.path.exists(wal):
            total += os.path.getsize(wal)
        return total, "duckdb database file"
    return os.path.getsize(source), "source file (in-memory database)"


# --------------------------------------------------------------------------
def main(argv: Sequence[str] | None = None) -> int:
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(
        description="DuckDB reference runner for the SirixDB ClickBench port")
    ap.add_argument("--source", required=True,
                    help="hits.parquet, or the JSON array file SirixDB ingested")
    ap.add_argument("--format", required=True, choices=("parquet", "json"))
    ap.add_argument("--rows", type=int, default=0,
                    help="load only the first N rows (0 = all)")
    ap.add_argument("--db", default=":memory:",
                    help="DuckDB database path, or :memory: (default)")
    ap.add_argument("--out", default="results-duckdb",
                    help="directory for qNN.jsonl (default: results-duckdb)")
    ap.add_argument("--tries", type=int, default=3,
                    help="timed runs per query (default: 3, the ClickBench protocol)")
    ap.add_argument("--queries", default=str(here / "queries.sql"),
                    help="queries.sql, one query per line")
    ap.add_argument("--table", default="hits", help="table name (default: hits)")
    ap.add_argument("--threads", type=int, default=0, help="0 = DuckDB default")
    ap.add_argument("--column-spec", default="",
                    help="JSON from ClickBenchSchema.duckdbColumnSpecJson(); "
                         "cross-checked against the embedded table")
    ap.add_argument("--only", default="",
                    help="comma-separated 0-based query indices (debugging)")
    args = ap.parse_args(argv)

    if args.tries < 1:
        ap.error("--tries must be >= 1")
    if args.rows < 0:
        ap.error("--rows must be >= 0")

    source = Path(args.source)
    if not source.is_file():
        print(f"source not found: {source}", file=sys.stderr)
        return 2

    qpath = Path(args.queries)
    if not qpath.is_file():
        print(f"queries file not found: {qpath}", file=sys.stderr)
        return 2
    queries = parse_queries(qpath)
    if len(queries) != 43:
        print(f"expected 43 ClickBench queries in {qpath}, found {len(queries)}",
              file=sys.stderr)
        return 2

    if args.column_spec:
        check_column_spec(load_column_spec(Path(args.column_spec)))

    wanted = None
    if args.only.strip():
        wanted = {int(x) for x in args.only.split(",") if x.strip()}

    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    if args.db != ":memory:":
        # A stale database would silently answer the queries from old data.
        for stale in (args.db, f"{args.db}.wal"):
            if os.path.exists(stale):
                os.remove(stale)
    con = duckdb.connect(args.db)
    try:
        if args.threads:
            con.execute(f"SET threads={int(args.threads)}")
        con.execute("PRAGMA disable_progress_bar")

        load_seconds = build_hits(con, str(source), args.format, args.rows, args.table)
        table_rows = con.execute(f"SELECT count(*) FROM {args.table}").fetchone()[0]
        size, size_of = data_size_bytes(con, args.db, str(source))
        version = con.execute("SELECT version()").fetchone()[0]
        threads = con.execute("SELECT current_setting('threads')").fetchone()[0]

        print(f"# duckdb {version}  source={source} ({args.format})  "
              f"rows={table_rows}  threads={threads}  tries={args.tries}")
        print(f"# load {load_seconds:.3f}s  data size {size} bytes ({size_of})")
        print(f"# {'q':>3}  {'tries (s)':<34}  {'rows':>8}  status")

        summary: Dict[str, Any] = {
            "engine": "duckdb",
            "version": version,
            "source": str(source),
            "format": args.format,
            "rows": table_rows,
            "threads": threads,
            "tries": args.tries,
            "load_time_s": round(load_seconds, 6),
            "data_size_bytes": size,
            "data_size_of": size_of,
            "queries": [],
        }
        timing_lines: List[str] = []
        failures = 0

        for index, query in enumerate(queries):
            if wanted is not None and index not in wanted:
                continue

            timings: List[float] = []
            rows: List[Sequence[Any]] = []
            captured = False
            error: str | None = None

            for _ in range(args.tries):
                started = time.perf_counter()
                try:
                    cursor = con.execute(query)
                    fetched = cursor.fetchall()  # materialise INSIDE the timing
                except Exception as exc:  # noqa: BLE001 - report, do not abort
                    error = f"{type(exc).__name__}: {exc}".strip()
                    break
                timings.append(time.perf_counter() - started)
                if not captured:  # try 1 is the dumped result, ties and all
                    rows = fetched
                    captured = True

            entry: Dict[str, Any] = {
                "index": index,
                "sql": query,
                "timings_s": [round(t, 6) for t in timings],
                "rows": len(rows) if error is None else None,
                "error": error,
            }
            summary["queries"].append(entry)

            if error is None:
                write_jsonl(outdir / f"q{index:02d}.jsonl", rows)
                timing_lines.append(
                    "[" + ", ".join(f"{t:.6f}" for t in timings) + "],")
                shown = ", ".join(f"{t:.4f}" for t in timings)
                print(f"  {index:>3}  {shown:<34}  {len(rows):>8}  ok")
            else:
                failures += 1
                timing_lines.append("[" + ", ".join(["null"] * args.tries) + "],")
                print(f"  {index:>3}  {'-':<34}  {'-':>8}  ERROR {error}")

        block = "\n".join(
            [f"Load time: {load_seconds:.3f}", f"Data size: {size}"] + timing_lines)
        print()
        print(block)
        (outdir / "clickbench-results.txt").write_text(block + "\n", encoding="utf-8")
        (outdir / "summary.json").write_text(
            json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

        print()
        print(f"# {len(summary['queries'])} queries, {failures} failed; "
              f"results in {outdir}/")
        return 1 if failures else 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
