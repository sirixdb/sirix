#!/usr/bin/env python3
"""Concatenate the Bluesky gzip files of one tier into a single canonical NDJSON corpus,
dropping the lines that are not valid JSON.

    clean-corpus.py <dir> <tier> [-o OUT] [--force]

The published dataset contains corrupt records
--------------------------------------------------
Some of the files at `clickhouse-public-datasets.s3.amazonaws.com/bluesky/` carry
records that were **truncated at a 64 KiB buffer boundary** by whatever wrote
them: the cut line is unterminated JSON and the following line is its tail, so
each incident costs two unparseable lines. Measured on the files this kit
downloads:

    tier 1m      0 corrupt lines
    tier 10m     6 corrupt lines  (3 truncation incidents)
    tier 100m   32 corrupt lines  (16 incidents)

ClickHouse's own JSONBench loader handles this by *retrying the whole file* with
`input_format_allow_errors_num = 1e9`, i.e. it silently drops whatever it cannot
parse -- which is fine for a single-engine benchmark and fatal for a
cross-engine differential, because the two engines then hold different rows and
every count differs by an unknown amount.

This script makes the drop explicit and shared: both engines load the *same*
cleaned file, and the dropped lines are printed with their global line numbers
so the loss is documented rather than assumed. Row counts after cleaning:

    1m      1,000,000
    10m     9,999,994
    100m   99,999,968

Cost: every line is fully parsed, because a cheap structural test cannot prove a
line is valid JSON. Measured at 5 s for the 1m tier on a 2024 laptop, so budget
about a minute at 10m and ten minutes at 100m, dominated by gunzip plus
json.loads. The output is written to a temporary file and renamed into place, so
an interrupted run cannot leave a half-corpus that looks finished.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from pathlib import Path

TIER_FILES = {"1m": 1, "10m": 10, "100m": 100, "1000m": 1000}


def human(num_bytes: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if num_bytes < 1024 or unit == "TiB":
            return f"{num_bytes:.1f} {unit}" if unit != "B" else f"{num_bytes} B"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TiB"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the canonical cleaned NDJSON corpus for one JSONBench tier.")
    parser.add_argument("directory", help="directory holding file_NNNN.json.gz")
    parser.add_argument("tier", choices=sorted(TIER_FILES), help="1m, 10m, 100m or 1000m")
    parser.add_argument("-o", "--out", help="output path "
                                            "(default: <dir>/bluesky-<tier>-clean.ndjson)")
    parser.add_argument("--force", action="store_true",
                        help="rebuild even if the output already exists")
    parser.add_argument("--max-report", type=int, default=64,
                        help="how many dropped line numbers to print (default 64)")
    args = parser.parse_args()

    directory = Path(args.directory)
    if not directory.is_dir():
        print(f"clean-corpus.py: no such directory: {directory}", file=sys.stderr)
        return 1

    out_path = Path(args.out) if args.out else directory / f"bluesky-{args.tier}-clean.ndjson"
    if out_path.exists() and not args.force:
        print(f"clean-corpus.py: {out_path} already exists "
              f"({human(out_path.stat().st_size)}); pass --force to rebuild")
        return 0

    count = TIER_FILES[args.tier]
    sources = []
    for index in range(1, count + 1):
        source = directory / f"file_{index:04d}.json.gz"
        if not source.is_file():
            print(f"clean-corpus.py: missing {source.name} -- run download-data.sh "
                  f"{args.tier} {directory} first", file=sys.stderr)
            return 1
        sources.append(source)

    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    kept = 0
    dropped = []
    line_no = 0
    started = time.monotonic()

    try:
        with open(tmp_path, "wb") as out:
            for source in sources:
                file_kept = 0
                file_dropped = 0
                with gzip.open(source, "rb") as stream:
                    for line in stream:
                        line_no += 1
                        try:
                            json.loads(line)
                        except Exception:
                            file_dropped += 1
                            dropped.append((line_no, source.name, len(line)))
                            continue
                        out.write(line)
                        file_kept += 1
                kept += file_kept
                print(f"  {source.name}: kept {file_kept:,}"
                      + (f", DROPPED {file_dropped}" if file_dropped else ""),
                      flush=True)
    except KeyboardInterrupt:
        tmp_path.unlink(missing_ok=True)
        print("clean-corpus.py: interrupted; partial output removed", file=sys.stderr)
        return 130
    except OSError as error:
        tmp_path.unlink(missing_ok=True)
        print(f"clean-corpus.py: write failed: {error}", file=sys.stderr)
        return 1

    os.replace(tmp_path, out_path)
    elapsed = time.monotonic() - started

    print()
    print(f"corpus:  {out_path}")
    print(f"rows:    {kept:,} kept, {len(dropped)} dropped, "
          f"{line_no:,} read in {elapsed:.0f}s")
    print(f"size:    {human(out_path.stat().st_size)}")
    if dropped:
        print(f"dropped lines (global 1-based line numbers across the {count}-file "
              f"concatenation):")
        for entry in dropped[:args.max_report]:
            print(f"  line {entry[0]:,} in {entry[1]} ({entry[2]} bytes)")
        if len(dropped) > args.max_report:
            print(f"  ... and {len(dropped) - args.max_report} more")
        print("These are the 64 KiB-truncation records described in this script's header. "
              "Both engines must load THIS file so their row sets are identical.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
