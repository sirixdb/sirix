#!/usr/bin/env python3
"""Byte-compare all SH1 answers from the oracle, Sirix and XTDB."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--sirix", type=Path, required=True)
    parser.add_argument("--xtdb", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args()


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def row_count(path: Path) -> int:
    with path.open("rb") as source:
        return sum(block.count(b"\n") for block in iter(lambda: source.read(1 << 20), b""))


def compare(left: Path, right: Path) -> None:
    result = subprocess.run(["cmp", "--", str(left), str(right)], capture_output=True, text=True)
    if result.returncode == 0:
        return
    left_rows = left.read_text().splitlines()
    right_rows = right.read_text().splitlines()
    for number, (ours, theirs) in enumerate(zip(left_rows, right_rows), start=1):
        if ours != theirs:
            raise SystemExit(
                f"mismatch {left.name} row {number}: oracle={ours!r} counterpart={theirs!r}"
            )
    raise SystemExit(
        f"mismatch {left.name}: oracle rows={len(left_rows)}, counterpart rows={len(right_rows)}"
    )


def main() -> None:
    args = parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    queries = []
    for query in range(1, 13):
        oracle = args.oracle / f"q{query}.tsv"
        sirix = args.sirix / f"q{query}.tsv"
        xtdb = args.xtdb / f"q{query}.tsv"
        for path in (oracle, sirix, xtdb):
            if not path.is_file():
                raise SystemExit(f"missing Q{query} result: {path}")
        compare(oracle, sirix)
        compare(oracle, xtdb)
        count = row_count(oracle)
        sha = digest(oracle)
        queries.append({"q": query, "rows": count, "sha256": sha, "status": "identical"})
        print(f"EXACT q={query} rows={count} sha256={sha} oracle=sirix=xtdb")
    args.out.write_text(json.dumps({"status": "PASS", "queries": queries}, indent=2) + "\n")


if __name__ == "__main__":
    main()
