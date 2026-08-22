#!/usr/bin/env python3
"""Differential check: SirixDB's JSONBench dump against the ClickHouse reference.

SirixDB writes ``qN.jsonl`` (one JSON array per result row) via
``JsonBenchRunMain --dump DIR``; ClickHouse writes ``qN.tsv`` in ``FORMAT TSV``.
The two are not textually comparable, so this normalises both sides to a list of
typed Python tuples and then compares them.

What has to be normalised, and why:

* **Absent paths.** ClickHouse types ``data.commit.collection`` as
  ``LowCardinality(String)``, so an event with no ``commit`` object reads as
  ``''`` and Q1's group for the 5328 ``kind='identity'`` events prints an empty
  first column. Our Q1 wraps the key in ``fn:string(...)``, which maps the empty
  sequence to ``""`` — the same value, so the two agree without a special case
  here. A TSV empty field and a JSON ``""`` both normalise to ``''``.
* **Timestamps.** Q4 selects ``min(fromUnixTimestamp64Micro(time_us))``, which
  ClickHouse prints as ``YYYY-MM-DD HH:MM:SS.ffffff``. We emit the raw
  microseconds, because our query never leaves integer arithmetic. Normalisation
  happens on the *reference* side — the printed timestamp is parsed back to
  microseconds — rather than by formatting ours, so no timezone or rounding
  decision is introduced on the side under test. That parse is only well defined
  because the reference is regenerated with ``session_timezone='UTC'``; see
  ``README.md``.
* **Tie ordering.** ``ORDER BY count DESC`` leaves the order of equal-count rows
  unspecified, and the two engines do disagree there on some corpora. Comparing
  as an unordered multiset would hide a genuinely wrong ordering, so instead the
  rows are cut into runs of equal ordering key: the *sequence of keys* must match
  exactly, and within each run the rows are compared as multisets. That accepts
  exactly the permutations SQL leaves free and nothing else.

Usage::

    compare-results.py --dump /tmp/jb/dump-sirix-1 --ref /tmp/jb/ch-ref
    compare-results.py --dump ... --ref ... --queries 1,3-5 --verbose
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

MICROS_PER_SECOND = 1_000_000


def as_text(value):
    """A string cell. ClickHouse's NULL marker and JSON null both become None."""
    if value is None:
        return None
    return str(value)


def as_int(value):
    """An integer cell, accepting either a JSON number or its TSV spelling."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"expected an integer, got a boolean: {value!r}")
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    return int(text)


def as_micros(value):
    """Microseconds since the epoch, from either raw micros or a ClickHouse datetime.

    ClickHouse prints a ``DateTime64(6)`` as ``YYYY-MM-DD HH:MM:SS.ffffff``, with
    the fractional part omitted when it is zero. It is parsed as UTC, which is
    only correct because the reference is regenerated with
    ``SETTINGS session_timezone='UTC'``.
    """
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit() or (text[0] == "-" and text[1:].isdigit()):
        return int(text)
    fmt = "%Y-%m-%d %H:%M:%S.%f" if "." in text else "%Y-%m-%d %H:%M:%S"
    parsed = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
    return int(parsed.timestamp()) * MICROS_PER_SECOND + parsed.microsecond


class QuerySpec:
    """How one query's rows are typed and ordered.

    ``columns`` are per-cell normalisers, applied to both sides; ``order_by`` are
    the zero-based indexes of the ORDER BY columns, in ORDER BY order, which is
    what defines the runs within which row order is free.
    """

    def __init__(self, index, name, columns, order_by):
        self.index = index
        self.name = name
        self.columns = columns
        self.order_by = order_by

    def normalise(self, cells, source):
        if len(cells) != len(self.columns):
            raise ValueError(
                f"q{self.index} ({source}): expected {len(self.columns)} columns, got {len(cells)}: {cells!r}"
            )
        return tuple(convert(cell) for convert, cell in zip(self.columns, cells))

    def key(self, row):
        return tuple(row[i] for i in self.order_by)


SPECS = {
    # SELECT collection AS event, count() ORDER BY count DESC
    1: QuerySpec(1, "collection histogram", [as_text, as_int], [1]),
    # SELECT collection AS event, count(), uniqExact(did) ORDER BY count DESC
    2: QuerySpec(2, "collection histogram with distinct users", [as_text, as_int, as_int], [1]),
    # SELECT collection AS event, hour_of_day, count() ORDER BY hour_of_day, event
    3: QuerySpec(3, "hourly histogram", [as_text, as_int, as_int], [1, 0]),
    # SELECT did AS user_id, min(ts) ORDER BY first_post_ts ASC LIMIT 3
    4: QuerySpec(4, "earliest posters", [as_text, as_micros], [1]),
    # SELECT did AS user_id, activity span in ms ORDER BY activity_span DESC LIMIT 3
    5: QuerySpec(5, "longest activity spans", [as_text, as_int], [1]),
}


def unescape_tsv(field):
    """ClickHouse TSV escaping: backslash sequences for tab, newline and backslash."""
    if "\\" not in field:
        return field
    out = []
    i = 0
    while i < len(field):
        char = field[i]
        if char == "\\" and i + 1 < len(field):
            nxt = field[i + 1]
            out.append({"t": "\t", "n": "\n", "r": "\r", "0": "\0", "\\": "\\"}.get(nxt, nxt))
            i += 2
        else:
            out.append(char)
            i += 1
    return "".join(out)


def read_reference(path, spec):
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if not line:
                continue
            # A TSV NULL is the two-character sequence \N; every other field is text.
            cells = [None if cell == "\\N" else unescape_tsv(cell) for cell in line.split("\t")]
            rows.append(spec.normalise(cells, "clickhouse"))
    return rows


def read_dump(path, spec):
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            cells = json.loads(line)
            if not isinstance(cells, list):
                raise ValueError(f"q{spec.index} (sirix): dump row is not a JSON array: {line!r}")
            rows.append(spec.normalise(cells, "sirix"))
    return rows


def runs_of_equal_key(rows, spec):
    """Split rows into consecutive runs sharing one ordering key."""
    runs = []
    for row in rows:
        key = spec.key(row)
        if runs and runs[-1][0] == key:
            runs[-1][1].append(row)
        else:
            runs.append((key, [row]))
    return runs


def compare(spec, ours, theirs, verbose):
    """Return a list of human-readable problems; empty means the answers agree."""
    problems = []
    if len(ours) != len(theirs):
        problems.append(f"row count: sirix {len(ours)} vs clickhouse {len(theirs)}")

    our_runs = runs_of_equal_key(ours, spec)
    their_runs = runs_of_equal_key(theirs, spec)

    our_keys = [key for key, _ in our_runs]
    their_keys = [key for key, _ in their_runs]
    if our_keys != their_keys:
        for position, (mine, theirs_key) in enumerate(zip(our_keys, their_keys)):
            if mine != theirs_key:
                problems.append(
                    f"ordering diverges at key {position}: sirix {mine} vs clickhouse {theirs_key}"
                )
                break
        else:
            problems.append(
                f"ordering key count: sirix {len(our_keys)} vs clickhouse {len(their_keys)}"
            )
        return problems

    for position, ((key, our_rows), (_, their_rows)) in enumerate(zip(our_runs, their_runs)):
        # Within one ordering key SQL leaves row order free, so compare as a multiset.
        if Counter(our_rows) != Counter(their_rows):
            only_ours = Counter(our_rows) - Counter(their_rows)
            only_theirs = Counter(their_rows) - Counter(our_rows)
            problems.append(
                f"rows differ at ordering key {key} (position {position}): "
                f"only in sirix {sorted(only_ours.elements())}, "
                f"only in clickhouse {sorted(only_theirs.elements())}"
            )
            if not verbose and len(problems) >= 3:
                problems.append("... further differences suppressed; rerun with --verbose")
                break
    return problems


def parse_selection(spec_text):
    selected = []
    for part in spec_text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part[1:]:
            dash = part.index("-", 1)
            first, last = int(part[:dash]), int(part[dash + 1 :])
            if first > last:
                raise ValueError(f"empty range in --queries: {part}")
            selected.extend(range(first, last + 1))
        else:
            selected.append(int(part))
    unknown = [index for index in selected if index not in SPECS]
    if unknown:
        raise ValueError(f"no such query: {unknown}")
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dump", required=True, type=Path, help="SirixDB dump directory (qN.jsonl)")
    parser.add_argument("--ref", required=True, type=Path, help="ClickHouse reference directory (qN.tsv)")
    parser.add_argument("--queries", default=None, help="subset, e.g. 1,3-5 (default: all)")
    parser.add_argument("--verbose", action="store_true", help="list every differing row")
    args = parser.parse_args()

    selected = parse_selection(args.queries) if args.queries else sorted(SPECS)

    matched = 0
    failed = 0
    skipped = 0
    for index in selected:
        spec = SPECS[index]
        dump_path = args.dump / f"q{index}.jsonl"
        ref_path = args.ref / f"q{index}.tsv"
        if not dump_path.exists():
            print(f"q{index} SKIP   no sirix dump at {dump_path}")
            skipped += 1
            continue
        if not ref_path.exists():
            print(f"q{index} SKIP   no reference at {ref_path}")
            skipped += 1
            continue
        try:
            ours = read_dump(dump_path, spec)
            theirs = read_reference(ref_path, spec)
        except (ValueError, json.JSONDecodeError) as error:
            print(f"q{index} FAIL   unreadable: {error}")
            failed += 1
            continue
        problems = compare(spec, ours, theirs, args.verbose)
        if problems:
            print(f"q{index} FAIL   {spec.name} ({len(ours)} rows vs {len(theirs)})")
            for problem in problems:
                print(f"         {problem}")
            failed += 1
        else:
            print(f"q{index} MATCH  {spec.name} ({len(ours)} rows)")
            matched += 1

    total = matched + failed + skipped
    print(f"\n{matched}/{total} MATCH, {failed} FAIL, {skipped} SKIP")
    return 1 if failed or skipped else 0


if __name__ == "__main__":
    sys.exit(main())
