#!/usr/bin/env python3
"""Merge isolated single-query/single-try JSONBench runs into one round."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


def merge(parts: list[Path], query_count: int, tries: int) -> dict:
    expected = query_count * tries
    if len(parts) != expected:
        raise ValueError(f"expected {expected} parts, got {len(parts)}")

    template = None
    merged_rows = None
    for query_index in range(query_count):
        timings = []
        for attempt in range(tries):
            path = parts[query_index * tries + attempt]
            document = json.loads(path.read_text())
            rows = document.get("result") or document.get("results")
            if rows is None or len(rows) != query_count:
                raise ValueError(f"{path}: expected {query_count} result rows")
            executed = [index for index, row in enumerate(rows) if row and row[0] is not None]
            if executed != [query_index]:
                raise ValueError(f"{path}: expected only Q{query_index + 1}, got {executed}")
            if len(rows[query_index]) != 1:
                raise ValueError(f"{path}: isolated process must carry exactly one timing")
            timing = rows[query_index][0]
            if (isinstance(timing, bool) or not isinstance(timing, (int, float))
                    or not math.isfinite(timing) or timing < 0):
                raise ValueError(f"{path}: invalid timing {timing}")
            timings.append(timing)
            if template is None:
                template = document
                merged_rows = [[None] * tries for _ in range(query_count)]
        merged_rows[query_index] = timings

    template["result"] = merged_rows
    template.pop("results", None)
    return template


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--queries", required=True, type=int)
    parser.add_argument("--tries", required=True, type=int)
    parser.add_argument("parts", nargs="+", type=Path)
    args = parser.parse_args()
    if args.queries < 1 or args.tries < 1:
        parser.error("--queries and --tries must be positive")
    merged = merge(args.parts, args.queries, args.tries)
    args.out.write_text(json.dumps(merged, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
