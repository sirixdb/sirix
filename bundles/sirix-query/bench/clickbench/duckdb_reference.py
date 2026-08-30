#!/usr/bin/env python3
"""DuckDB reference runner for the SirixDB ClickBench port.

This is the engine the SirixDB answers are diffed against. It does two jobs:

  1. build a DuckDB table ``hits`` whose column types are EXACTLY the ones in
     ClickBench's ``duckdb/create.sql``, from EITHER the official parquet file
     OR the same JSON-array/JSONEachRow file that SirixDB ingested -- so the
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

    ./duckdb_reference.py --source hits.json --format json \\
        --out results-duckdb --candidate-reference vectorized=results-vectorized \\
        --candidate-reference generic=results-generic

Output
------
    <out>/.clickbench-result-format
                                 result encoding version; old rounded result
                                 directories are rejected rather than upgraded
    <out>/qNN.jsonl              one JSON ARRAY PER LINE, one line per result
                                 row, values in SELECT order, canonicalised
                                 (see ``canon`` below). NN is the 0-based
                                 ClickBench query index, matching
                                 ``ClickBenchQueries.byIndex(NN)``.
    <out>/qNN.full.jsonl         with --full-reference, the complete untimed
                                 relation behind each LIMIT query; Q24/Q26
                                 additionally carry their hidden EventTime key
    <out>/qNN.oracle-ID.json     with --candidate-reference ID=DIR, a bounded
                                 exact membership oracle tied to that result
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

Cross-engine representation differences are normalized by the JSONiq port:
Q27/Q28 use its UTF-8 byte-length function, and Q42 appends the timestamp's
seconds component. See README.md.
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import math
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

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
QUERY_COUNT = 43
RESULT_FORMAT_MARKER = ".clickbench-result-format"
RESULT_FORMAT_VERSION = "clickbench-jsonl-v2-lossless-float64"

# Every ClickBench window is a top-level ``LIMIT n [OFFSET k]`` suffix.  The
# diagnostic full-reference gate asks DuckDB for the complete relation as an
# untimed correctness oracle as well as running the canonical windowed query.
WINDOW_SUFFIX_RE = re.compile(
    r"\s+LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?\s*$", re.IGNORECASE)

# Q24 and Q26 project SearchPhrase but order first by EventTime.  Their
# canonical result cannot carry that hidden key, so the full-reference query
# appends EventTime solely to the sidecar.  (output width, key tuple indices in
# the widened result).  All other window keys are already in the SELECT row and
# are resolved from queries.sql by compare-results.py.
HIDDEN_ORDER_KEY_SPECS: Dict[int, Tuple[int, Tuple[int, ...]]] = {
    24: (1, (1,)),
    26: (1, (1, 0)),
}

# These are the only limited ClickBench relations with floating-point output.
# Their listed columns are the exact, non-floating GROUP BY identity, so at
# most one full-relation row can match each candidate identity.  A future
# floating-point LIMIT query is rejected until its identity is reviewed rather
# than falling back to an unbounded scan across its result relation.
FLOATING_RELATION_IDENTITIES: Dict[int, Tuple[int, ...]] = {
    9: (0,),       # RegionID
    27: (0,),      # CounterID
    28: (0,),      # regexp-derived host k
    30: (0, 1),    # SearchEngineID, ClientIP
    31: (0, 1),    # WatchID, ClientIP
    32: (0, 1),    # WatchID, ClientIP
}

BOUNDED_REFERENCE_FORMAT = "clickbench-bounded-reference-v3"
CANDIDATE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
FLOATING_DUCKDB_TYPES = frozenset({"DOUBLE", "FLOAT", "REAL", "DECIMAL",
                                   "NUMERIC"})
MAX_CANDIDATE_REFERENCES = 8


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
def require_result_format(directory: Path, label: str = "result directory") -> None:
    """Require the lossless marker; an absent marker means legacy rounded data."""
    marker = directory / RESULT_FORMAT_MARKER
    try:
        actual = marker.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ValueError(f"{label} lacks readable {RESULT_FORMAT_MARKER}: "
                         f"{directory}") from exc
    if actual != RESULT_FORMAT_VERSION:
        raise ValueError(f"{label} uses unsupported result encoding "
                         f"{actual!r}, expected {RESULT_FORMAT_VERSION!r}: "
                         f"{directory}")


def prepare_result_directory(directory: Path) -> None:
    """Create or validate a result directory without blessing legacy files."""
    directory.mkdir(parents=True, exist_ok=True)
    marker = directory / RESULT_FORMAT_MARKER
    if marker.exists():
        require_result_format(directory)
        return
    try:
        next(directory.iterdir())
    except StopIteration:
        pass
    else:
        raise ValueError("refusing to mark non-empty legacy ClickBench result "
                         f"directory as {RESULT_FORMAT_VERSION}: {directory}")
    # Exclusive creation keeps a concurrent/stale publisher from being relabelled.
    with marker.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(RESULT_FORMAT_VERSION + "\n")


def invalidate_selected_outputs(directory: Path,
                                selected: Optional[set[int]]) -> None:
    """Remove selected final/partial rows and oracles before DuckDB work."""
    indices = range(QUERY_COUNT) if selected is None else selected
    for index in indices:
        stem = f"q{index:02d}"
        for name in (f"{stem}.jsonl", f"{stem}.jsonl.tmp",
                     f"{stem}.full.jsonl", f"{stem}.full.jsonl.tmp"):
            (directory / name).unlink(missing_ok=True)
        for oracle in directory.glob(f"{stem}.oracle-*.json*"):
            oracle.unlink()


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
        # json.dumps uses Python's shortest round-trip binary64 representation.
        return value
    if isinstance(value, decimal.Decimal):
        if value.is_nan():
            return "nan"
        if value.is_infinite():
            return "inf" if value > 0 else "-inf"
        if value == value.to_integral_value():
            return int(value)
        raise ValueError("non-integral DuckDB DECIMAL result has no lossless "
                         "ClickBench JSON cell encoding")
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


def exact_token(value: Any) -> List[Any]:
    """Lossless typed token for SQL equality of an ORDER BY cell.

    Result rows now retain the complete binary64 value. These independently
    typed tokens also keep the bounded oracle's SQL key identity fail-closed;
    signed zero is normalized because SQL equality treats -0.0 and 0.0 as the
    same key.
    """
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", value]
    if isinstance(value, float):
        normalized = 0.0 if value == 0.0 else value
        return ["float64", normalized.hex()]
    if isinstance(value, decimal.Decimal):
        normalized_decimal = decimal.Decimal(0) if value == 0 else value
        return ["decimal", str(normalized_decimal)]
    if isinstance(value, dt.datetime):
        return ["datetime", value.isoformat(timespec="microseconds")]
    if isinstance(value, dt.date):
        return ["date", value.isoformat()]
    if isinstance(value, dt.time):
        return ["time", value.isoformat(timespec="microseconds")]
    if isinstance(value, (bytes, bytearray)):
        return ["bytes", bytes(value).hex()]
    if isinstance(value, str):
        return ["string", value]
    return ["json", json.dumps(canon(value), ensure_ascii=False,
                               sort_keys=True, separators=(",", ":"))]


def write_jsonl(path: Path, rows: Sequence[Sequence[Any]]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    path.unlink(missing_ok=True)
    temporary.unlink(missing_ok=True)
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as fh:
            for row in rows:
                fh.write(json.dumps([canon(c) for c in row], ensure_ascii=False,
                                    separators=(",", ":")))
                fh.write("\n")
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def widen_hidden_order_key(index: int, statement: str) -> str:
    """Append EventTime to Q24/Q26's SELECT without changing their window."""
    if index not in HIDDEN_ORDER_KEY_SPECS:
        return statement
    marker = " FROM "
    select_end = statement.upper().find(marker)
    if select_end < 0:
        raise ValueError(f"q{index:02d}: cannot widen hidden ORDER BY key")
    return statement[:select_end] + ", EventTime" + statement[select_end:]


def top_level_keyword(statement: str, keyword: str) -> int:
    """Find a whole SQL keyword outside literals and parentheses."""
    pattern = re.compile(r"\b" + keyword.replace(" ", r"\s+") + r"\b",
                         re.IGNORECASE)
    depth = 0
    quoted = False
    index = 0
    while index < len(statement):
        char = statement[index]
        if quoted:
            if char == "'":
                if index + 1 < len(statement) and statement[index + 1] == "'":
                    index += 1
                else:
                    quoted = False
        elif char == "'":
            quoted = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0 and pattern.match(statement, index):
            return index
        index += 1
    return -1


def full_reference_query(index: int, query: str) -> Tuple[str, int | None,
                                                           Tuple[int, ...]] | None:
    """Untimed complete relation behind one canonical LIMIT query.

    The returned output width is ``None`` when the fetched row is already the
    canonical SELECT row.  For Q24/Q26 it is one and the widened result's key
    indices identify the otherwise-hidden ORDER BY cells.
    """
    statement = query.strip().rstrip(";")
    match = WINDOW_SUFFIX_RE.search(statement)
    if match is None:
        return None
    full = widen_hidden_order_key(index, statement[:match.start()].rstrip())
    spec = HIDDEN_ORDER_KEY_SPECS.get(index)
    if spec is None:
        return full, None, ()
    output_width, key_indices = spec
    return full, output_width, key_indices


def write_full_reference(con: "duckdb.DuckDBPyConnection", path: Path,
                         index: int, query: str) -> int | None:
    """Stream qNN.full.jsonl; return its row count, or None for no LIMIT.

    Each line is ``{"row": [...]}``.  Q24/Q26 additionally carry
    ``"key": [...]`` containing the hidden EventTime-based ORDER BY tuple.
    A temporary file is atomically published only after DuckDB and JSON writing
    both complete, so an interrupted run cannot masquerade as a full oracle.
    """
    reference = full_reference_query(index, query)
    if reference is None:
        path.unlink(missing_ok=True)
        return None
    statement, output_width, key_indices = reference
    temporary = path.with_name(path.name + ".tmp")
    temporary.unlink(missing_ok=True)
    count = 0
    try:
        cursor = con.execute(statement)
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            while True:
                batch = cursor.fetchmany(4096)
                if not batch:
                    break
                for fetched in batch:
                    output = fetched if output_width is None else fetched[:output_width]
                    payload: Dict[str, Any] = {
                        "row": [canon(cell) for cell in output]
                    }
                    if key_indices:
                        payload["key"] = [canon(fetched[position])
                                          for position in key_indices]
                    handle.write(json.dumps(payload, ensure_ascii=False,
                                            separators=(",", ":")))
                    handle.write("\n")
                    count += 1
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return count


def membership_relation_query(index: int, query: str) -> Tuple[
        str, Optional[int], Tuple[int, ...]] | None:
    """Unordered full relation used by the bounded exact membership join."""
    reference = full_reference_query(index, query)
    if reference is None:
        return None
    statement, output_width, key_indices = reference
    order_at = top_level_keyword(statement, "ORDER BY")
    if order_at >= 0:
        statement = statement[:order_at].rstrip()
    return statement, output_width, key_indices


def hidden_window_keys(con: "duckdb.DuckDBPyConnection", index: int,
                       query: str) -> Optional[Tuple[List[List[Any]],
                                                     List[List[Any]]]]:
    """Canonical hidden ORDER BY keys for Q24/Q26's bounded window."""
    spec = HIDDEN_ORDER_KEY_SPECS.get(index)
    if spec is None:
        return None
    output_width, key_indices = spec
    widened = widen_hidden_order_key(index, query.strip().rstrip(";"))
    fetched = con.execute(widened).fetchall()
    keys: List[List[Any]] = []
    exact_keys: List[List[Any]] = []
    for row in fetched:
        if len(row) <= max(key_indices):
            raise ValueError(f"q{index:02d}: widened window returned {len(row)} "
                             f"columns, but key needs {max(key_indices) + 1}")
        if len(row[:output_width]) != output_width:
            raise ValueError(f"q{index:02d}: widened window output-width drift")
        keys.append([canon(row[position]) for position in key_indices])
        exact_keys.append([exact_token(row[position]) for position in key_indices])
    return keys, exact_keys


def read_candidate_rows(path: Path, maximum: int) -> List[List[Any]]:
    """Read a bounded candidate result, rejecting malformed or oversized input."""
    rows: List[List[Any]] = []
    with path.open(encoding="utf-8") as handle:
        for lineno, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            value = json.loads(text)
            if not isinstance(value, list):
                raise ValueError(f"{path}:{lineno}: expected a JSON array")
            rows.append(value)
            if len(rows) > maximum:
                raise ValueError(f"{path}: more than LIMIT {maximum} rows")
    return rows


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def is_floating_type(type_name: str) -> bool:
    upper = type_name.upper()
    return any(upper == name or upper.startswith(name + "(")
               for name in FLOATING_DUCKDB_TYPES)


def candidate_relation_row(index: int, row: Sequence[Any],
                           hidden_key: Optional[Sequence[Any]],
                           relation_width: int) -> List[Any]:
    """Expand one projected result row to the widened membership relation."""
    spec = HIDDEN_ORDER_KEY_SPECS.get(index)
    if spec is None:
        if len(row) != relation_width:
            raise ValueError(f"q{index:02d}: candidate has {len(row)} columns; "
                             f"reference has {relation_width}")
        return list(row)

    output_width, key_indices = spec
    if len(row) != output_width:
        raise ValueError(f"q{index:02d}: candidate has {len(row)} columns; "
                         f"expected {output_width}")
    if hidden_key is None or len(hidden_key) != len(key_indices):
        raise ValueError(f"q{index:02d}: missing complete hidden window key")
    unset = object()
    expanded: List[Any] = [unset] * relation_width
    expanded[:output_width] = row
    for key_position, relation_position in enumerate(key_indices):
        value = hidden_key[key_position]
        present = expanded[relation_position]
        if present is not unset and present != value:
            raise ValueError(f"q{index:02d}: projected row disagrees with its "
                             "visible component of the hidden key")
        expanded[relation_position] = value
    if any(value is unset for value in expanded):
        raise ValueError(f"q{index:02d}: hidden-key expansion left a column unset")
    return expanded


def bounded_reference_matches(
        con: "duckdb.DuckDBPyConnection", index: int, query: str,
        request_rows: Sequence[Sequence[Any]],
        request_keys: Sequence[Optional[Sequence[Any]]]) -> Tuple[
            int, List[Dict[str, Any]]]:
    """Exact full-relation multiplicities for a bounded set of requests.

    DuckDB performs the full relation work.  Python receives only grouped rows
    whose exact GROUP BY identity (or complete non-floating raw row) appeared
    in a candidate.  The SQL LIMIT is one past the proven identity count: if a
    reviewed identity ever stops being unique, the oracle fails closed before
    transferring or materialising an unbounded result.
    """
    relation = membership_relation_query(index, query)
    if relation is None:
        raise ValueError(f"q{index:02d}: bounded reference requires LIMIT")
    statement, output_width, key_indices = relation
    cursor = con.execute(f"SELECT * FROM ({statement}) AS r LIMIT 0")
    description = cursor.description or ()
    relation_width = len(description)
    if relation_width == 0:
        raise ValueError(f"q{index:02d}: membership relation has no columns")
    names = [str(column[0]) for column in description]
    if len({name.lower() for name in names}) != len(names):
        raise ValueError(f"q{index:02d}: membership relation has duplicate "
                         "column names")
    type_names = [str(column[1]) for column in description]
    floating = tuple(position for position, type_name in enumerate(type_names)
                     if is_floating_type(type_name))
    if floating:
        identity = FLOATING_RELATION_IDENTITIES.get(index)
        if identity is None:
            raise ValueError(f"q{index:02d}: floating result columns {floating} "
                             "have no reviewed bounded identity")
        if any(position >= relation_width for position in identity):
            raise ValueError(f"q{index:02d}: reviewed identity is out of range")
        if any(position in floating for position in identity):
            raise ValueError(f"q{index:02d}: reviewed identity contains a "
                             "floating-point column")
    else:
        identity = tuple(range(relation_width))

    expanded_rows = [candidate_relation_row(index, row, key, relation_width)
                     for row, key in zip(request_rows, request_keys)]
    if len(expanded_rows) != len(request_rows) or len(request_keys) != len(request_rows):
        raise ValueError(f"q{index:02d}: request row/key count differs")
    if not expanded_rows:
        return (output_width if output_width is not None else relation_width), []

    table = f"__clickbench_candidates_{index}"
    con.execute(f"DROP TABLE IF EXISTS {table}")
    columns = ", ".join(
        f"c{slot} {type_names[position]}"
        for slot, position in enumerate(identity))
    try:
        con.execute(f"CREATE TEMP TABLE {table} ({columns})")
        placeholders = ", ".join("?" for _ in identity)
        values = [tuple(row[position] for position in identity)
                  for row in expanded_rows]
        con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", values)
        distinct = int(con.execute(
            f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table})").fetchone()[0])
        if distinct < 1:
            raise ValueError(f"q{index:02d}: non-empty requests made no filters")

        conditions = " AND ".join(
            f"r.{quote_identifier(names[position])} IS NOT DISTINCT FROM f.c{slot}"
            for slot, position in enumerate(identity)
        )
        matched_sql = (
            f"SELECT r.*, COUNT(*) AS __clickbench_multiplicity "
            f"FROM ({statement}) AS r "
            f"INNER JOIN (SELECT DISTINCT * FROM {table}) AS f ON {conditions} "
            f"GROUP BY ALL LIMIT {distinct + 1}"
        )
        matched = con.execute(matched_sql).fetchall()
        if len(matched) > distinct:
            raise ValueError(f"q{index:02d}: reviewed identity matched more than "
                             f"{distinct} distinct relation row(s)")
    finally:
        con.execute(f"DROP TABLE IF EXISTS {table}")

    combined: Dict[str, Dict[str, Any]] = {}
    for fetched in matched:
        relation_row, multiplicity = fetched[:-1], fetched[-1]
        canonical = [canon(value) for value in relation_row]
        exact = [exact_token(value) for value in relation_row]
        projected = (canonical if output_width is None
                     else canonical[:output_width])
        projected_exact = (exact if output_width is None
                           else exact[:output_width])
        entry: Dict[str, Any] = {
            "row": projected,
            "exact_row": projected_exact,
            "multiplicity": int(multiplicity),
        }
        if key_indices:
            entry["key"] = [canonical[position] for position in key_indices]
            entry["exact_key"] = [exact[position] for position in key_indices]
        token = json.dumps({"exact_row": entry["exact_row"],
                            "exact_key": entry.get("exact_key")},
                           ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"))
        prior = combined.get(token)
        if prior is None:
            combined[token] = entry
        else:
            prior["multiplicity"] += entry["multiplicity"]
    return (output_width if output_width is not None else relation_width,
            list(combined.values()))


def bounded_reference_path(outdir: Path, index: int, candidate_id: str) -> Path:
    return outdir / f"q{index:02d}.oracle-{candidate_id}.json"


def write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.unlink(missing_ok=True)
    try:
        temporary.write_text(json.dumps(payload, ensure_ascii=False,
                                        separators=(",", ":")) + "\n",
                             encoding="utf-8")
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def write_bounded_references(
        con: "duckdb.DuckDBPyConnection", outdir: Path, index: int,
        query: str, duck_rows: Sequence[Sequence[Any]], limit: int,
        candidates: Dict[str, Path]) -> int | None:
    """Publish candidate-bound, bounded exact oracles for one LIMIT query."""
    if WINDOW_SUFFIX_RE.search(query.strip().rstrip(";")) is None:
        return None
    canonical_duck = [[canon(value) for value in row] for row in duck_rows]
    exact_duck = [[exact_token(value) for value in row] for row in duck_rows]
    hidden = hidden_window_keys(con, index, query)
    keys = None if hidden is None else hidden[0]
    exact_keys = None if hidden is None else hidden[1]
    if keys is not None and len(keys) != len(canonical_duck):
        raise ValueError(f"q{index:02d}: hidden key window has {len(keys)} rows, "
                         f"DuckDB result has {len(canonical_duck)}")

    candidate_rows: Dict[str, List[List[Any]]] = {}
    all_rows: List[Sequence[Any]] = list(canonical_duck)
    all_keys: List[Optional[Sequence[Any]]] = [
        None if keys is None else keys[position]
        for position in range(len(canonical_duck))
    ]
    for candidate_id, directory in candidates.items():
        path = directory / f"q{index:02d}.jsonl"
        if not path.is_file():
            raise ValueError(f"q{index:02d}: candidate {candidate_id!r} lacks {path}")
        rows = read_candidate_rows(path, limit)
        candidate_rows[candidate_id] = rows
        all_rows.extend(rows)
        all_keys.extend(None if keys is None else keys[position]
                        for position in range(len(rows)))

    row_width, matches = bounded_reference_matches(
        con, index, query, all_rows, all_keys)
    for candidate_id, rows in candidate_rows.items():
        payload: Dict[str, Any] = {
            "format": BOUNDED_REFERENCE_FORMAT,
            "query_index": index,
            "candidate_id": candidate_id,
            "candidate_rows": rows,
            "duckdb_rows": canonical_duck,
            "duckdb_exact_rows": exact_duck,
            "row_width": row_width,
            "hidden_window_keys": keys,
            "hidden_window_exact_keys": exact_keys,
            "reference_matches": matches,
        }
        write_json_atomic(bounded_reference_path(outdir, index, candidate_id),
                          payload)
    return len(matches)


def parse_candidate_references(values: Sequence[str]) -> Dict[str, Path]:
    if len(values) > MAX_CANDIDATE_REFERENCES:
        raise ValueError(f"at most {MAX_CANDIDATE_REFERENCES} candidate "
                         "references are supported")
    candidates: Dict[str, Path] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--candidate-reference needs ID=DIR")
        candidate_id, raw_path = value.split("=", 1)
        if not CANDIDATE_ID_RE.fullmatch(candidate_id):
            raise ValueError(f"invalid candidate id {candidate_id!r}")
        if candidate_id in candidates:
            raise ValueError(f"duplicate candidate id {candidate_id!r}")
        directory = Path(raw_path)
        if not directory.is_dir():
            raise ValueError(f"candidate directory not found: {directory}")
        require_result_format(directory, f"candidate {candidate_id!r}")
        candidates[candidate_id] = directory
    return candidates


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
        # DuckDB's structural auto-detection accepts both the generated top-level array and the
        # official gzip-compressed JSONEachRow corpus. Column types remain explicit, so this changes
        # framing only and cannot reintroduce sampler-dependent BIGINT inference.
        read_src = f"read_json('{literal}', format='auto', columns={{{spec}}})"
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
    ap.add_argument("--memory-limit", default="",
                    help="DuckDB memory limit, for example 8GB (default: DuckDB setting)")
    ap.add_argument("--temp-directory", default="",
                    help="explicit spill directory (default: DuckDB setting)")
    ap.add_argument("--column-spec", default="",
                    help="JSON from ClickBenchSchema.duckdbColumnSpecJson(); "
                         "cross-checked against the embedded table")
    ap.add_argument("--only", default="",
                    help="comma-separated 0-based query indices (debugging)")
    ap.add_argument("--full-reference", action="store_true",
                    help="emit untimed qNN.full.jsonl correctness oracles for "
                         "every LIMIT query")
    ap.add_argument("--candidate-reference", action="append", default=[],
                    metavar="ID=DIR",
                    help="emit bounded exact oracles tied to a candidate "
                         "result directory; repeat for multiple Sirix paths")
    args = ap.parse_args(argv)

    if args.tries < 1:
        ap.error("--tries must be >= 1")
    if args.rows < 0:
        ap.error("--rows must be >= 0")
    try:
        candidates = parse_candidate_references(args.candidate_reference)
    except ValueError as exc:
        ap.error(str(exc))
    if args.full_reference and candidates:
        ap.error("--full-reference and --candidate-reference are mutually exclusive")

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
    try:
        prepare_result_directory(outdir)
        invalidate_selected_outputs(outdir, wanted)
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.db != ":memory:":
        # A stale database would silently answer the queries from old data.
        for stale in (args.db, f"{args.db}.wal"):
            if os.path.exists(stale):
                os.remove(stale)
    con = duckdb.connect(args.db)
    try:
        if args.threads:
            con.execute(f"SET threads={int(args.threads)}")
        if args.memory_limit:
            con.execute("SET memory_limit = ?", [args.memory_limit])
        if args.temp_directory:
            con.execute("SET temp_directory = ?", [str(Path(args.temp_directory))])
        con.execute("PRAGMA disable_progress_bar")

        load_seconds = build_hits(con, str(source), args.format, args.rows, args.table)
        table_rows = con.execute(f"SELECT count(*) FROM {args.table}").fetchone()[0]
        size, size_of = data_size_bytes(con, args.db, str(source))
        version = con.execute("SELECT version()").fetchone()[0]
        threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
        memory_limit = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        temp_directory = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]

        print(f"# duckdb {version}  source={source} ({args.format})  "
              f"rows={table_rows}  threads={threads}  memory={memory_limit}  tries={args.tries}")
        print(f"# temp directory {temp_directory}")
        print(f"# load {load_seconds:.3f}s  data size {size} bytes ({size_of})")
        print(f"# {'q':>3}  {'tries (s)':<34}  {'rows':>8}  status")

        summary: Dict[str, Any] = {
            "engine": "duckdb",
            "version": version,
            "source": str(source),
            "format": args.format,
            "rows": table_rows,
            "threads": threads,
            "memory_limit": memory_limit,
            "temp_directory": temp_directory,
            "tries": args.tries,
            "load_time_s": round(load_seconds, 6),
            "data_size_bytes": size,
            "data_size_of": size_of,
            "candidate_references": list(candidates),
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
            result_path = outdir / f"q{index:02d}.jsonl"
            full_reference_rows: int | None = None
            bounded_reference_matches_count: int | None = None
            full_reference_path = outdir / f"q{index:02d}.full.jsonl"
            result_path.unlink(missing_ok=True)
            # Never let a normal, failed, or partial rerun leave a prior oracle
            # looking current.
            full_reference_path.unlink(missing_ok=True)
            bounded_paths = [bounded_reference_path(outdir, index, candidate_id)
                             for candidate_id in candidates]
            for bounded_path in bounded_paths:
                bounded_path.unlink(missing_ok=True)

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

            if error is None and args.full_reference:
                try:
                    # Correctness-only work: deliberately outside every timed
                    # try, and streamed so q23's wide SELECT * does not become
                    # a second in-memory result set.
                    full_reference_rows = write_full_reference(
                        con, full_reference_path, index, query)
                except Exception as exc:  # noqa: BLE001 - report, do not abort
                    error = ("full reference " +
                             f"{type(exc).__name__}: {exc}").strip()
            elif error is None and candidates:
                try:
                    window = WINDOW_SUFFIX_RE.search(query.strip().rstrip(";"))
                    if window is not None:
                        bounded_reference_matches_count = write_bounded_references(
                            con, outdir, index, query, rows,
                            int(window.group(1)), candidates)
                except Exception as exc:  # noqa: BLE001 - report, do not abort
                    for bounded_path in bounded_paths:
                        bounded_path.unlink(missing_ok=True)
                    error = ("bounded reference " +
                             f"{type(exc).__name__}: {exc}").strip()

            entry: Dict[str, Any] = {
                "index": index,
                "sql": query,
                "timings_s": [round(t, 6) for t in timings],
                "rows": len(rows) if error is None else None,
                "full_reference_rows": full_reference_rows,
                "bounded_reference_matches": bounded_reference_matches_count,
                "error": error,
            }
            summary["queries"].append(entry)

            if error is None:
                write_jsonl(result_path, rows)
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
