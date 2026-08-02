#!/usr/bin/env python3
"""Build the bulk-comparison corpus from a source JSON array, by repetition.

The published run of docs/COMPARISON_POSTGRES_BULK.md uses the in-repo
bundles/sirix-core/src/test/resources/json/movies.json (36,273 records, pretty-printed) minified and
repeated 96x -- 3,482,208 records, 2,116,427,425 bytes. Both of those figures are exact and
reproducible from a clean checkout, which is the whole point of this file: an earlier revision was
measured against a scratch file nobody could regenerate.

Repetition is a deliberate, documented compromise (caveat 3 in the document): the repeat period is
~22 MB, far beyond any per-page or per-row compression window, so it does not flatter either
engine's compression. It does make global distinct-value counts 96x lower than row counts, which
would flatter a global dictionary; neither engine here uses one.

The records are minified ONCE and the resulting text is written N times, so the cost is I/O rather
than N passes of JSON serialization.

Usage:
  docs/bench/make-corpus.py <source.json> <out.json> [repeats]
"""
import json
import sys


def main() -> int:
    if not 3 <= len(sys.argv) <= 4:
        print(__doc__, file=sys.stderr)
        return 2

    source, out = sys.argv[1], sys.argv[2]
    repeats = int(sys.argv[3]) if len(sys.argv) > 3 else 96
    if repeats < 1:
        print(f"repeats must be >= 1, was {repeats}", file=sys.stderr)
        return 2

    with open(source, encoding="utf-8") as handle:
        records = json.load(handle)
    if not isinstance(records, list):
        print(f"{source}: expected a top-level JSON array", file=sys.stderr)
        return 2
    if not records:
        print(f"{source}: empty array", file=sys.stderr)
        return 2

    # One minified copy of the element list, without the enclosing brackets.
    body = ",".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in records)

    written = 0
    with open(out, "w", encoding="utf-8") as handle:
        handle.write("[")
        for i in range(repeats):
            if i:
                handle.write(",")
            handle.write(body)
            written += len(records)
        handle.write("]")

    print(f"{out}: {written:,} records ({len(records):,} x {repeats})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
