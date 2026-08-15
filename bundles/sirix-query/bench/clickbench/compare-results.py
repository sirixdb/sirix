#!/usr/bin/env python3
"""Differential: SirixDB's ClickBench answers vs DuckDB's.

    ./compare-results.py <sirix-results-dir> <duckdb-results-dir>

Both directories hold ``qNN.jsonl`` files in the canonical form written by
``duckdb_reference.py``: ONE JSON ARRAY PER LINE, one line per result row,
values in SELECT order, floats already rounded to 6 significant digits,
integers as JSON ints, NULL as null, timestamps/dates as ISO-8601 strings
(``YYYY-MM-DDTHH:MM:SS`` / ``YYYY-MM-DD``). ``NN`` is the 0-based ClickBench
query index.

Comparison
----------
Rows are compared as a MULTISET after a canonical sort -- SQL GROUP BY has no
inherent order, so the line order of a query without a total ORDER BY carries
no information -- with:

  * integers      exact
  * floats        relative 1e-9   (both writers already round to 6 significant
                                   digits; the tolerance only absorbs decimal
                                   round-tripping noise)
  * int vs float  relative 1e-9   -- JSON has a single number type, so 1666 and
                                   1666.0 are a serialisation artefact, not a
                                   wrong answer
  * strings       exact
  * null          equal only to null

Additionally, whenever the ORDER BY key is resolvable, BOTH sides are checked
to be actually ordered by it: a multiset comparison alone would not notice an
engine that returns the right rows in the wrong order.

THE TIE AMBIGUITY
-----------------
``ORDER BY <agg> DESC LIMIT n [OFFSET k]`` does not define a total order: rows
that TIE on the sort key may come back in any order, so two correct engines can
keep different members of a tied group at the window boundary. That is not a
wrong answer and must not be reported as one -- but it must not be used as an
excuse either, so the test is explicit rather than fuzzy.

The sort key is taken from queries.sql, not guessed: the ORDER BY terms are
matched against the SELECT list (by alias, by expression text, or by ordinal)
to give the exact output column indices. ``SELECT *`` has no SELECT list to
match against, so its terms are resolved positionally against the 105 ``hits``
columns instead (``ClickBenchSchema.java``). A difference is TIE-AMBIGUOUS only
if

  1. the two results have the same number of rows;
  2. the query has a LIMIT (without one, the whole result is returned and
     there is no boundary to be ambiguous about);
  3. both sides carry the same sort key at the ambiguous window boundaries;
  4. EVERY row of the multiset difference, on both sides, has a sort key equal
     to one of those boundary keys.

The ambiguous boundaries are the LAST kept row always, plus the FIRST kept row
when the query has an OFFSET -- a window that starts part-way down can be cut
through a tied group at its top edge too, whereas with OFFSET 0 the top of the
window is the global maximum and is not ambiguous.

Three shapes cannot be decided from the result files alone, and all of them are
called out rather than hidden:

  * Q17 ``GROUP BY UserID, SearchPhrase LIMIT 10`` has no ORDER BY at all, so
    which ten groups come back is entirely engine-defined.
  * Q24/Q26 order by ``EventTime``, which is not in their SELECT list, so the
    sort key is not observable in the output and a boundary tie can neither be
    confirmed nor ruled out.
  * A window that lies ENTIRELY inside one tied group -- at synthetic scale the
    ``PageViews`` plateau of Q38..Q41 is all 1s -- has every returned row at a
    boundary, so the tie test accepts two completely disjoint answers. Both are
    legal, so the verdict stands, but no row was actually checked.

All of them are reported as TIE-AMBIGUOUS with "UNVERIFIABLE" in the detail and
are counted in the summary, so a run cannot be mistaken for fully checked.

Exit status
-----------
    0   every query MATCH or TIE-AMBIGUOUS
    1   at least one real MISMATCH
    2   no mismatches, but at least one MISSING result file, or the run could
        not be set up (bad directory, unusable queries.sql, stale schema copy)
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

FLOAT_REL_TOL = 1e-9
QUERY_COUNT = 43

# The `hits` columns in create.sql order -- the output column order of a
# `SELECT *`, and hence the only way to turn `ORDER BY EventTime` on such a
# query into an output column index (4).
#
# SOURCE OF TRUTH: ClickBenchSchema.java's COLUMN_TABLE (see SCHEMA_JAVA below).
# The list is embedded so the script keeps working when it is run from a copy
# outside the repo, and cross-checked against the Java file whenever that file
# is reachable -- a silently stale copy here would resolve ORDER BY terms to the
# wrong output column and turn a correct answer into a MISMATCH.
CLICKBENCH_COLUMNS: Tuple[str, ...] = (
    "WatchID", "JavaEnable", "Title", "GoodEvent", "EventTime", "EventDate",
    "CounterID", "ClientIP", "RegionID", "UserID", "CounterClass", "OS",
    "UserAgent", "URL", "Referer", "IsRefresh", "RefererCategoryID",
    "RefererRegionID", "URLCategoryID", "URLRegionID", "ResolutionWidth",
    "ResolutionHeight", "ResolutionDepth", "FlashMajor", "FlashMinor",
    "FlashMinor2", "NetMajor", "NetMinor", "UserAgentMajor", "UserAgentMinor",
    "CookieEnable", "JavascriptEnable", "IsMobile", "MobilePhone",
    "MobilePhoneModel", "Params", "IPNetworkID", "TraficSourceID",
    "SearchEngineID", "SearchPhrase", "AdvEngineID", "IsArtifical",
    "WindowClientWidth", "WindowClientHeight", "ClientTimeZone",
    "ClientEventTime", "SilverlightVersion1", "SilverlightVersion2",
    "SilverlightVersion3", "SilverlightVersion4", "PageCharset", "CodeVersion",
    "IsLink", "IsDownload", "IsNotBounce", "FUniqID", "OriginalURL", "HID",
    "IsOldCounter", "IsEvent", "IsParameter", "DontCountHits", "WithHash",
    "HitColor", "LocalEventTime", "Age", "Sex", "Income", "Interests",
    "Robotness", "RemoteIP", "WindowName", "OpenerName", "HistoryLength",
    "BrowserLanguage", "BrowserCountry", "SocialNetwork", "SocialAction",
    "HTTPError", "SendTiming", "DNSTiming", "ConnectTiming",
    "ResponseStartTiming", "ResponseEndTiming", "FetchTiming",
    "SocialSourceNetworkID", "SocialSourcePage", "ParamPrice", "ParamOrderID",
    "ParamCurrency", "ParamCurrencyID", "OpenstatServiceName",
    "OpenstatCampaignID", "OpenstatAdID", "OpenstatSourceID", "UTMSource",
    "UTMMedium", "UTMCampaign", "UTMContent", "UTMTerm", "FromTag", "HasGCLID",
    "RefererHash", "URLHash", "CLID",
)

COLUMN_COUNT = 105
SCHEMA_JAVA = Path("src/main/java/io/sirix/query/bench/clickbench"
                   "/ClickBenchSchema.java")

# name (lower-cased, as SQL is case-insensitive here) -> output column index
STAR_COLUMNS: Dict[str, int] = {name.lower(): position
                                for position, name in
                                enumerate(CLICKBENCH_COLUMNS)}

Row = List[Any]

MATCH = "MATCH"
TIE = "TIE-AMBIGUOUS"
MISMATCH = "MISMATCH"
MISSING = "MISSING"

KEY_FROM_SQL = "sql"
KEY_INFERRED = "inferred"
KEY_UNOBSERVABLE = "unobservable"
KEY_NONE = "none"


# --------------------------------------------------------------------------
# reading
# --------------------------------------------------------------------------
def read_jsonl(path: Path) -> List[Row]:
    rows: List[Row] = []
    with path.open(encoding="utf-8") as handle:
        for lineno, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            value = json.loads(text)
            if not isinstance(value, list):
                raise ValueError(f"{path}:{lineno}: expected a JSON array per "
                                 f"line, got {type(value).__name__}")
            rows.append(value)
    return rows


COLUMN_TABLE_RE = re.compile(r"COLUMN_TABLE\s*=\s*\{(.*?)\}\s*;", re.DOTALL)
JAVA_STRING_RE = re.compile(r'"([^"\\]*)"')


def schema_drift(schema_java: Path) -> Optional[str]:
    """Why CLICKBENCH_COLUMNS disagrees with ClickBenchSchema.java, or None.

    The embedded roster decides which output column an ``ORDER BY <name>`` on a
    ``SELECT *`` refers to, so a stale copy would not fail visibly -- it would
    check the ordering of the WRONG column and report a correct engine as
    broken. Whenever the Java file is reachable it is therefore re-read and
    compared; when it is not (the script was copied out of the repo) there is
    nothing to compare against and the embedded roster stands.

    In ``COLUMN_TABLE`` the types are constants (``BIGINT``) and only the names
    are string literals, so every quoted token in the initialiser is a name.
    """
    try:
        text = schema_java.read_text(encoding="utf-8")
    except OSError:
        return None  # not reachable: nothing to check against
    table = COLUMN_TABLE_RE.search(text)
    if table is None:
        return None  # no recognisable table: cannot check, do not guess
    names = tuple(JAVA_STRING_RE.findall(table.group(1)))
    if names == CLICKBENCH_COLUMNS:
        return None
    if len(names) != len(CLICKBENCH_COLUMNS):
        return (f"{schema_java} declares {len(names)} columns but this script "
                f"embeds {len(CLICKBENCH_COLUMNS)}")
    differing = [f"#{i} {a} != {b}"
                 for i, (a, b) in enumerate(zip(names, CLICKBENCH_COLUMNS))
                 if a != b]
    return (f"{schema_java} and this script disagree about the hits columns: "
            + ", ".join(differing[:5]))


# --------------------------------------------------------------------------
# minimal SQL shape parsing (the 43 ClickBench queries are single-level SELECTs)
# --------------------------------------------------------------------------
def split_top_level(text: str, separator: str = ",") -> List[str]:
    """Split on `separator` at parenthesis depth 0, outside '...' literals."""
    parts: List[str] = []
    depth = 0
    quoted = False
    start = 0
    index = 0
    while index < len(text):
        char = text[index]
        if quoted:
            if char == "'":
                if index + 1 < len(text) and text[index + 1] == "'":
                    index += 1  # doubled quote inside a literal
                else:
                    quoted = False
        elif char == "'":
            quoted = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == separator and depth == 0:
            parts.append(text[start:index])
            start = index + 1
        index += 1
    parts.append(text[start:])
    return [part.strip() for part in parts]


def find_keyword(text: str, keyword: str, start: int = 0) -> int:
    """Offset of `keyword` as a whole word at depth 0 outside literals, or -1."""
    pattern = re.compile(r"\b" + keyword.replace(" ", r"\s+") + r"\b",
                         re.IGNORECASE)
    depth = 0
    quoted = False
    index = start
    while index < len(text):
        char = text[index]
        if quoted:
            if char == "'":
                if index + 1 < len(text) and text[index + 1] == "'":
                    index += 1
                else:
                    quoted = False
        elif char == "'":
            quoted = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0:
            match = pattern.match(text, index)
            if match:
                return index
        index += 1
    return -1


def normalise(expression: str) -> str:
    return re.sub(r"\s+", " ", expression.strip().lower())


ALIAS_RE = re.compile(r"\s+AS\s+\"?([A-Za-z_]\w*)\"?$", re.IGNORECASE)
IDENT_RE = re.compile(r"^\"?([A-Za-z_]\w*)\"?$")
DIRECTION_RE = re.compile(r"\s+(ASC|DESC)$", re.IGNORECASE)


class QueryShape:
    """What queries.sql says about a query's ordering and its window."""

    __slots__ = ("sql", "limit", "offset", "ordered",
                 "key_columns", "key_descending", "key_source", "key_text")

    def __init__(self, sql: str) -> None:
        # The SQL is mandatory: a shape guessed in its absence used to answer
        # "windowed" and "offset_ambiguous" with an unconditional True, which
        # puts every no-LIMIT query on the tie path -- see read_query_shapes.
        if not sql or not sql.strip():
            raise ValueError("QueryShape needs the query text")
        self.sql = sql
        self.limit: Optional[int] = None
        self.offset: Optional[int] = None
        self.ordered = True
        self.key_columns: List[int] = []
        self.key_descending: List[bool] = []
        self.key_source = KEY_INFERRED
        self.key_text = ""
        self._parse(sql)

    def _parse(self, sql: str) -> None:
        limit_at = find_keyword(sql, "LIMIT")
        offset_at = find_keyword(sql, "OFFSET")
        order_at = find_keyword(sql, "ORDER BY")
        select_at = find_keyword(sql, "SELECT")
        from_at = find_keyword(sql, "FROM", select_at + 6 if select_at >= 0 else 0)

        if limit_at >= 0:
            number = re.match(r"LIMIT\s+(\d+)", sql[limit_at:], re.IGNORECASE)
            self.limit = int(number.group(1)) if number else None
        if offset_at >= 0:
            number = re.match(r"OFFSET\s+(\d+)", sql[offset_at:], re.IGNORECASE)
            self.offset = int(number.group(1)) if number else None

        self.ordered = order_at >= 0
        if not self.ordered:
            self.key_source = KEY_NONE
            return
        if select_at < 0 or from_at < 0:
            self.key_source = KEY_UNOBSERVABLE
            return

        select_items = split_top_level(sql[select_at + len("SELECT"):from_at])
        star = any(item.strip() == "*" for item in select_items)

        aliases: Dict[str, int] = {}
        expressions: Dict[str, int] = {}
        if star:
            # `SELECT *` has no SELECT list to match the ORDER BY against, but
            # its output IS the hits table in create.sql order, so the term
            # resolves POSITIONALLY: EventTime is output column 4. Leaving the
            # shape at KEY_INFERRED instead (what this used to do) skips the
            # ordering guard entirely, and Q23's ten correct rows in REVERSED
            # EventTime order then pass as a MATCH.
            aliases = STAR_COLUMNS  # only ever read below, so no copy is needed
        else:
            for position, item in enumerate(select_items):
                alias = ALIAS_RE.search(item)
                body = item[:alias.start()] if alias else item
                if alias:
                    aliases.setdefault(alias.group(1).lower(), position)
                identifier = IDENT_RE.match(body.strip())
                if identifier:
                    aliases.setdefault(identifier.group(1).lower(), position)
                expressions.setdefault(normalise(body), position)

        end = min(x for x in (limit_at, offset_at, len(sql)) if x >= 0)
        terms = split_top_level(sql[order_at + len("ORDER BY"):end])
        columns: List[int] = []
        descending: List[bool] = []
        for term in terms:
            direction = DIRECTION_RE.search(term)
            descending.append(bool(direction)
                              and direction.group(1).upper() == "DESC")
            body = term[:direction.start()] if direction else term
            body = body.strip()
            if body.isdigit():  # ORDER BY <ordinal>
                columns.append(int(body) - 1)
                continue
            position = aliases.get(body.strip('"').lower())
            if position is None:
                position = expressions.get(normalise(body))
            if position is None:
                # A `SELECT *` term that is not a plain hits column (an
                # expression, say) is not a column position at all, so the data
                # is all there is to go on; anything else names something the
                # SELECT list does not expose.
                self.key_source = KEY_INFERRED if star else KEY_UNOBSERVABLE
                self.key_text = body
                return
            columns.append(position)
        self.key_columns = columns
        self.key_descending = descending
        self.key_source = KEY_FROM_SQL
        self.key_text = ", ".join(terms)

    @property
    def windowed(self) -> bool:
        """A LIMIT makes the result a window into an incompletely ordered list."""
        return self.limit is not None

    @property
    def offset_ambiguous(self) -> bool:
        """OFFSET k > 0 makes the TOP edge of the window ambiguous as well."""
        return bool(self.offset)


def read_query_shapes(path: Path) -> List[QueryShape]:
    """The 43 shapes of queries.sql -- one query per non-comment line.

    Short files are a hard error, exactly as in duckdb_reference.py, and for a
    sharper reason here: padding the tail with unknown shapes made `windowed`
    answer True for queries that have no LIMIT, which sends every difference in
    them onto the tie path. A mistyped --queries then downgrades the strictest
    checks in the suite and still reports a clean, zero-exit run.
    """
    statements = [line.strip().rstrip(";")
                  for line in path.read_text(encoding="utf-8").splitlines()
                  if line.strip() and not line.strip().startswith("--")]
    if len(statements) != QUERY_COUNT:
        raise ValueError(f"expected {QUERY_COUNT} ClickBench queries in {path}, "
                         f"found {len(statements)}")
    return [QueryShape(sql) for sql in statements]


# --------------------------------------------------------------------------
# value ordering / equality
# --------------------------------------------------------------------------
def sort_key(value: Any) -> Tuple[Any, ...]:
    """A total order across the mixed types a JSON row can hold."""
    if value is None:
        return (0,)
    if isinstance(value, bool):
        return (1, value)
    if isinstance(value, (int, float)):
        # Python compares int and float exactly, so 18-digit ids keep their
        # precision here.
        return (2, value)
    if isinstance(value, str):
        return (3, value)
    return (4, json.dumps(value, sort_keys=True, ensure_ascii=False))


def row_sort_key(row: Sequence[Any]) -> Tuple[Any, ...]:
    return (len(row),) + tuple(sort_key(v) for v in row)


def values_equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    left_number = isinstance(left, (int, float))
    right_number = isinstance(right, (int, float))
    if left_number != right_number:
        return False
    if left_number:
        if isinstance(left, int) and isinstance(right, int):
            return left == right  # exact for integers
        if left == right:
            return True
        if math.isnan(float(left)) or math.isnan(float(right)):
            return False
        return math.isclose(float(left), float(right),
                            rel_tol=FLOAT_REL_TOL, abs_tol=0.0)
    if isinstance(left, str) or isinstance(right, str):
        return isinstance(left, str) and isinstance(right, str) and left == right
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            values_equal(a, b) for a, b in zip(left, right))
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            values_equal(left[k], right[k]) for k in left)
    return left == right


def rows_equal(left: Sequence[Any], right: Sequence[Any]) -> bool:
    return len(left) == len(right) and all(
        values_equal(a, b) for a, b in zip(left, right))


def integral_float(value: float) -> bool:
    """True for a float that stands for an exact whole number.

    Past 2**53 a float no longer represents every integer, so `is_integer()`
    there says something about the encoding, not about the value the writer
    meant -- 1e18 must not be claimed to be the integer 1000000000000000000.
    NaN and +/-inf are not integral, so the bound is never asked of them.
    """
    return value.is_integer() and abs(value) < 2 ** 53


def exact_key(value: Any) -> Any:
    """Hashable key for the fast multiset pass.

    An integral float folds onto the int key: DuckDB writes an all-whole-number
    AVG column as 2048.0, brackit renders xs:double(2048) as 2048 and the Java
    dump keeps a dot-free token as an exact integer. values_equal already calls
    those equal, so pass 1 has to pair them too -- otherwise both rows fall
    through to the tolerance pass and a rendering difference gets a chance to be
    reported as a wrong answer. Non-integral floats keep their written form.
    """
    if value is None:
        return ("n",)
    if isinstance(value, bool):
        return ("b", value)
    if isinstance(value, int):
        return ("i", value)
    if isinstance(value, float):
        return ("i", int(value)) if integral_float(value) else ("f", repr(value))
    if isinstance(value, str):
        return ("s", value)
    return ("j", json.dumps(value, sort_keys=True, ensure_ascii=False))


def row_exact_key(row: Sequence[Any]) -> Tuple[Any, ...]:
    return tuple(exact_key(v) for v in row)


def bucket_key(row: Sequence[Any]) -> Tuple[Any, ...]:
    """Everything a tolerance cannot move, so tolerance pairing stays near-linear.

    Only NON-integral floats are wildcards. Whole numbers keep their exact key,
    and exact_key has already folded the integral floats onto it, so DuckDB's
    1774.0 and brackit's 1774 land in the SAME bucket -- bucketing them apart is
    what once stopped the two rows from ever being paired and had a rendering
    difference reported as a wrong answer. Wildcarding EVERY number fixes that
    too but is far coarser: it merges rows that share only their strings, and
    the pairing inside a bucket is quadratic.

    Both writers round to 6 significant digits, so a difference small enough for
    FLOAT_REL_TOL cannot straddle the integral/non-integral divide and no equal
    pair is separated by this key. The one pair it does separate is an int
    against a float too large to be an exact integer (an int64 id written as a
    double): values_equal's tolerance would bless those, and separating them is
    the honest verdict, because such a float has already lost digits the id
    needs.
    """
    return tuple(("~",) if isinstance(v, float) and not integral_float(v)
                 else exact_key(v)
                 for v in row)


def multiset_difference(left: Sequence[Row],
                        right: Sequence[Row]) -> Tuple[List[Row], List[Row]]:
    """Rows of `left` unmatched in `right` and vice versa, tolerance-aware."""
    pending: Dict[Tuple[Any, ...], List[int]] = {}
    for index, row in enumerate(right):
        pending.setdefault(row_exact_key(row), []).append(index)

    consumed = [False] * len(right)
    left_rest: List[Row] = []
    for row in left:  # pass 1: exact, hash-driven
        slot = pending.get(row_exact_key(row))
        if slot:
            consumed[slot.pop()] = True
        else:
            left_rest.append(row)
    right_rest = [row for i, row in enumerate(right) if not consumed[i]]

    if not left_rest or not right_rest:
        return sorted(left_rest, key=row_sort_key), sorted(right_rest,
                                                           key=row_sort_key)

    # pass 2: tolerance pairing over the canonically sorted leftovers (so the
    # pairing does not depend on the order the engines happened to emit),
    # bucketed on the non-float columns so the quadratic scan only ever runs
    # inside a bucket
    left_rest.sort(key=row_sort_key)
    right_rest.sort(key=row_sort_key)
    buckets: Dict[Tuple[Any, ...], List[int]] = {}
    for index, row in enumerate(right_rest):
        buckets.setdefault(bucket_key(row), []).append(index)
    taken = [False] * len(right_rest)
    unmatched_left: List[Row] = []
    for row in left_rest:
        found = -1
        for index in buckets.get(bucket_key(row), ()):
            if not taken[index] and rows_equal(row, right_rest[index]):
                found = index
                break
        if found >= 0:
            taken[found] = True
        else:
            unmatched_left.append(row)
    unmatched_right = [row for i, row in enumerate(right_rest) if not taken[i]]
    return unmatched_left, unmatched_right  # already canonically sorted


# --------------------------------------------------------------------------
# sort-key handling
# --------------------------------------------------------------------------
def monotone(values: Sequence[Any]) -> Tuple[bool, bool]:
    """(non-decreasing, non-increasing) over the given (file) order."""
    up = down = True
    for previous, current in zip(values, values[1:]):
        a, b = sort_key(previous), sort_key(current)
        if a > b:
            up = False
        if a < b:
            down = False
        if not up and not down:
            break
    return up, down


def infer_sort_columns(left: Sequence[Row], right: Sequence[Row]) -> List[int]:
    """Fallback when the SQL is unavailable (or the query is ``SELECT *``).

    A column can only be part of the real sort key if (a) its values are
    monotone in file order in the same direction on both sides and (b) the two
    sides agree position by position -- if both answers are correct, the key
    sequence itself cannot differ, only the membership of the tied groups.
    """
    if not left or not right or len(left) != len(right):
        return []
    width = min(len(left[0]), len(right[0]))
    columns: List[int] = []
    for column in range(width):
        left_values = [row[column] for row in left]
        right_values = [row[column] for row in right]
        if not all(values_equal(a, b) for a, b in zip(left_values, right_values)):
            continue
        left_up, left_down = monotone(left_values)
        right_up, right_down = monotone(right_values)
        if (left_up and right_up) or (left_down and right_down):
            columns.append(column)
    return columns


def key_of(row: Sequence[Any], columns: Sequence[int]) -> Tuple[Any, ...]:
    return tuple(sort_key(row[column]) for column in columns)


def ordering_violation(rows: Sequence[Row], columns: Sequence[int],
                       descending: Sequence[bool]) -> Optional[int]:
    """First index whose key breaks the declared ORDER BY, or None.

    A row whose key cell is NULL (or missing) is SKIPPED, not a reason to stop
    checking the side: engines differ on NULLS FIRST/LAST, so such a row can sit
    anywhere and tells us nothing -- but the rows around it still have to be in
    order, and abandoning the whole check for one NULL is how a reversed result
    would slip through. None of the ClickBench sort keys can be NULL on the real
    dataset anyway.
    """
    previous: Optional[Row] = None
    for index, current in enumerate(rows):
        if any(column >= len(current) or current[column] is None
               for column in columns):
            continue
        if previous is not None:
            for position, column in enumerate(columns):
                a = sort_key(previous[column])
                b = sort_key(current[column])
                if a == b:
                    continue
                wants_descending = (descending[position]
                                    if position < len(descending) else False)
                if (a < b) if wants_descending else (a > b):
                    return index
                break
        previous = current
    return None


# --------------------------------------------------------------------------
# per-query verdict
# --------------------------------------------------------------------------
class Verdict:
    __slots__ = ("index", "status", "detail", "left_only", "right_only",
                 "unverifiable")

    def __init__(self, index: int, status: str, detail: str = "",
                 left_only: Sequence[Row] = (), right_only: Sequence[Row] = (),
                 unverifiable: bool = False) -> None:
        self.index = index
        self.status = status
        self.detail = detail
        self.left_only = list(left_only)
        self.right_only = list(right_only)
        self.unverifiable = unverifiable


def judge(index: int, left: List[Row], right: List[Row], shape: QueryShape,
          labels: Tuple[str, str] = ("left", "right")) -> Verdict:
    if not left and not right:
        return Verdict(index, MATCH, "both empty")

    widths = {len(row) for row in left} | {len(row) for row in right}
    if len(widths) > 1:
        return Verdict(index, MISMATCH,
                       f"rows have different column counts: {sorted(widths)}")
    width = widths.pop()
    if any(column >= width for column in shape.key_columns):
        return Verdict(index, MISMATCH,
                       f"ORDER BY [{shape.key_text}] refers to output column "
                       f"{max(shape.key_columns)} but the rows have {width}")

    # An engine may return the right rows in the wrong order; a multiset
    # comparison would not notice, so check the declared ORDER BY first.
    if shape.key_source == KEY_FROM_SQL and shape.key_columns:
        for label, rows in zip(labels, (left, right)):
            broken = ordering_violation(rows, shape.key_columns,
                                        shape.key_descending)
            if broken is not None:
                return Verdict(index, MISMATCH,
                               f"{label} result is not ordered by "
                               f"[{shape.key_text}] (row {broken} breaks it)")

    left_only, right_only = multiset_difference(left, right)
    if not left_only and not right_only:
        return Verdict(index, MATCH, f"{len(left)} rows")

    def mismatch(reason: str) -> Verdict:
        return Verdict(index, MISMATCH, reason, left_only, right_only)

    if len(left) != len(right):
        return mismatch(f"row count differs: {len(left)} vs {len(right)}")
    if not shape.windowed:
        return mismatch(f"{len(left_only)} differing row(s); the query has no "
                        "LIMIT, so the full result must match")

    if shape.key_source == KEY_NONE:
        return Verdict(index, TIE,
                       f"UNVERIFIABLE: {len(left_only)} differing row(s); LIMIT "
                       "without ORDER BY, so the returned subset is engine-defined",
                       left_only, right_only, unverifiable=True)
    if shape.key_source == KEY_UNOBSERVABLE:
        return Verdict(index, TIE,
                       f"UNVERIFIABLE: {len(left_only)} differing row(s); the "
                       f"ORDER BY key ({shape.key_text}) is not in the SELECT "
                       "list, so a boundary tie cannot be confirmed or ruled out",
                       left_only, right_only, unverifiable=True)

    if shape.key_source == KEY_FROM_SQL:
        columns = shape.key_columns
        origin = f"ORDER BY [{shape.key_text}]"
    else:
        columns = infer_sort_columns(left, right)
        origin = "a sort key inferred from the data"
    if not columns:
        return mismatch(f"{len(left_only)} differing row(s); no sort key could "
                        "be established, so the difference cannot be a "
                        "boundary tie")

    boundaries_left = {key_of(left[-1], columns)}
    boundaries_right = {key_of(right[-1], columns)}
    edge = "the LIMIT cut-off"
    if shape.offset_ambiguous:
        boundaries_left.add(key_of(left[0], columns))
        boundaries_right.add(key_of(right[0], columns))
        edge = "an edge of the OFFSET/LIMIT window"

    if boundaries_left != boundaries_right:
        return mismatch(f"{len(left_only)} differing row(s); the boundary sort "
                        f"keys themselves differ on {origin}")

    stray = [row for row in left_only
             if key_of(row, columns) not in boundaries_left]
    stray += [row for row in right_only
              if key_of(row, columns) not in boundaries_right]
    if stray:
        return mismatch(f"{len(left_only)}+{len(right_only)} differing row(s), "
                        f"{len(stray)} of which are NOT at a window boundary "
                        f"of {origin}")

    # A window that is ENTIRELY inside one tied group has every row at a
    # boundary, so the test above accepts two completely disjoint answers -- it
    # verified nothing. That is still the right verdict (both answers are legal
    # under the ORDER BY), but it must be counted as unchecked rather than
    # advertised as a tie of a few boundary rows: at synthetic scale the
    # PageViews plateau of Q38..Q41 is all 1s and every row lands here.
    fully_tied = (all(key_of(row, columns) in boundaries_left for row in left)
                  or all(key_of(row, columns) in boundaries_right
                         for row in right))
    if fully_tied:
        return Verdict(index, TIE,
                       f"UNVERIFIABLE: {len(left_only)} of {len(left)} row(s) "
                       f"differ and EVERY returned row is tied at {edge} on "
                       f"{origin}, so no row's membership was checked",
                       left_only, right_only, unverifiable=True)

    return Verdict(index, TIE,
                   f"{len(left_only)} of {len(left)} row(s) differ, all tied at "
                   f"{edge} on {origin}", left_only, right_only)


# --------------------------------------------------------------------------
def render(row: Row, width: int = 160) -> str:
    text = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
    return text if len(text) <= width else text[:width - 3] + "..."


def main(argv: Optional[Sequence[str]] = None) -> int:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="compare SirixDB ClickBench results against DuckDB's")
    parser.add_argument("sirix_dir", help="directory with SirixDB's qNN.jsonl")
    parser.add_argument("duckdb_dir", help="directory with DuckDB's qNN.jsonl")
    parser.add_argument("--queries", default=str(here / "queries.sql"),
                        help="queries.sql; used to tell a legitimate "
                             "LIMIT/OFFSET tie from a wrong answer")
    parser.add_argument("--max-diff-rows", type=int, default=5,
                        help="differing rows printed per query per side "
                             "(default: 5; 0 = all)")
    args = parser.parse_args(argv)

    sirix_dir = Path(args.sirix_dir)
    duckdb_dir = Path(args.duckdb_dir)
    for directory in (sirix_dir, duckdb_dir):
        if not directory.is_dir():
            print(f"not a directory: {directory}")
            return 2

    # run-differential.sh also uses this tool to diff SirixDB's fast path
    # against its own generic interpreter, so the sides are labelled with the
    # directory names rather than hard-coded "sirix"/"duckdb".
    labels = (sirix_dir.name or "left", duckdb_dir.name or "right")
    label_width = max(len(labels[0]), len(labels[1])) + 5

    # The queries are what tells a legitimate tie from a wrong answer, so an
    # unusable queries.sql aborts the run instead of quietly weakening it.
    queries_path = Path(args.queries)
    if not queries_path.is_file():
        print(f"queries file not found: {queries_path}", file=sys.stderr)
        return 2
    try:
        shapes = read_query_shapes(queries_path)
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    drift = schema_drift(here.parents[1] / SCHEMA_JAVA)
    if drift is not None:
        print(drift, file=sys.stderr)
        return 2

    verdicts: List[Verdict] = []
    for index in range(QUERY_COUNT):
        name = f"q{index:02d}.jsonl"
        left_path, right_path = sirix_dir / name, duckdb_dir / name
        absent = [str(p) for p in (left_path, right_path) if not p.is_file()]
        if absent:
            verdicts.append(Verdict(index, MISSING, "no " + ", ".join(absent)))
            continue
        try:
            # Rows stay in FILE order: the multiset comparison is
            # order-independent by construction (see multiset_difference), and
            # the boundary rows of the tie test are exactly the first and last
            # rows the engine returned.
            left = read_jsonl(left_path)
            right = read_jsonl(right_path)
        except (ValueError, json.JSONDecodeError) as exc:
            verdicts.append(Verdict(index, MISMATCH, f"unreadable: {exc}"))
            continue
        verdicts.append(judge(index, left, right, shapes[index], labels))

    width = max(len(v.status) for v in verdicts)
    print(f"{'query':<6} {'verdict':<{width}}  detail")
    print("-" * (8 + width + 62))
    for verdict in verdicts:
        print(f"q{verdict.index:02d}    {verdict.status:<{width}}  {verdict.detail}")

    for verdict in verdicts:
        if verdict.status not in (TIE, MISMATCH):
            continue
        if not verdict.left_only and not verdict.right_only:
            continue
        print()
        print(f"--- q{verdict.index:02d} {verdict.status}: {verdict.detail}")
        limit = args.max_diff_rows if args.max_diff_rows > 0 else None
        for side, rows in zip(labels, (verdict.left_only, verdict.right_only)):
            label = f"{side} only"
            shown = rows if limit is None else rows[:limit]
            for row in shown:
                print(f"    {label:<{label_width}} {render(row)}")
            if limit is not None and len(rows) > limit:
                print(f"    {label:<{label_width}} ... {len(rows) - limit} more")

    tally = {status: sum(1 for v in verdicts if v.status == status)
             for status in (MATCH, TIE, MISMATCH, MISSING)}
    unverifiable = [v.index for v in verdicts if v.unverifiable]
    print()
    # The unverifiable count rides on the summary line itself: it is not a
    # failure, but a run with unverifiable verdicts is not a fully checked run
    # and the one-line answer must not suggest otherwise.
    print(f"summary: {tally[MATCH]} match, {tally[TIE]} tie-ambiguous "
          f"({len(unverifiable)} unverifiable), {tally[MISMATCH]} mismatch, "
          f"{tally[MISSING]} missing (of {QUERY_COUNT} queries)")
    if unverifiable:
        listed = ", ".join(f"q{i:02d}" for i in unverifiable)
        print(f"         UNVERIFIABLE from the results alone "
              f"({len(unverifiable)}): {listed}")

    if tally[MISMATCH]:
        return 1
    if tally[MISSING]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
