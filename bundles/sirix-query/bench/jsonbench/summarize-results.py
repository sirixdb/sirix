#!/usr/bin/env python3
"""Print a JSONBench scoreboard from complete isolated rounds."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


PROTOCOL = "jsonbench-isolated-v1"


def load_rounds(out: Path, round_count: int,
                try_count: int) -> tuple[list[list[list[float | None]]], list[int], int]:
    rows_per_round = []
    executed = None
    total_queries = 0
    for round_number in range(1, round_count + 1):
        path = out / f"round-{round_number}.json"
        if not path.exists():
            raise ValueError(f"missing round output: {path}")
        document = json.loads(path.read_text())
        rows = document.get("result") or document.get("results")
        if not rows:
            raise ValueError(f"{path} carries no result array")
        if any(not isinstance(row, list) or len(row) != try_count for row in rows):
            raise ValueError(f"{path}: expected exactly {try_count} timing slot(s) per query")
        for query_index, row in enumerate(rows, start=1):
            for value in row:
                if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))
                                          or not math.isfinite(value) or value < 0):
                    raise ValueError(f"{path}: invalid timing for Q{query_index}: {value}")
        ran = [index for index, row in enumerate(rows) if row and row[0] is not None]
        missing = [index + 1 for index in range(len(rows)) if index not in ran]
        if missing:
            joined = ", Q".join(str(index) for index in missing)
            raise ValueError(f"{path}: no timing for Q{joined}")
        if executed is None:
            executed = ran
            total_queries = len(rows)
        elif ran != executed:
            raise ValueError(f"round {round_number} ran a different query set than round 1")
        rows_per_round.append(rows)
    return rows_per_round, executed, total_queries


def best_complete_round(rows_per_round: list[list[list[float | None]]], executed: list[int], slot: int | None) -> list[float]:
    candidates = []
    for round_number, rows in enumerate(rows_per_round, start=1):
        values = []
        for query_index in executed:
            row = rows[query_index]
            if slot is None:
                hot = row[1:]
                if not hot or any(value is None for value in hot):
                    raise ValueError(f"round {round_number} has no complete hot timing for Q{query_index + 1}")
                values.append(min(hot))
            else:
                value = row[slot]
                if value is None:
                    raise ValueError(f"round {round_number} has no timing for Q{query_index + 1}")
                values.append(value)
        candidates.append(values)
    return min(candidates, key=sum)


def parse_timing(value: str, location: str) -> float | None:
    if value == "null":
        return None
    timing = float(value)
    if not math.isfinite(timing) or timing < 0:
        raise ValueError(f"{location}: invalid timing {value}")
    return timing


def baseline_for(path: Path, executed: list[int], expected_rounds: int,
                 expected_tries: int) -> tuple[list[tuple[float, float | None]] | None, str]:
    if not path.exists():
        raise ValueError(f"missing measured ClickHouse baseline: {path}")

    protocol = None
    declared_rounds = None
    declared_tries = None
    rounds: dict[int, dict[int, tuple[float, float | None]]] = {}
    for line_number, line in enumerate(path.read_text().splitlines(), start=1):
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "PROTOCOL" and len(parts) == 2:
            protocol = parts[1]
        elif parts[0] == "ROUNDS" and len(parts) == 2:
            declared_rounds = int(parts[1])
        elif parts[0] == "TRIES" and len(parts) == 2:
            declared_tries = int(parts[1])
        elif parts[0] == "ROUND" and len(parts) == 5 and parts[2].startswith("Q"):
            round_number = int(parts[1])
            query_index = int(parts[2][1:]) - 1
            row = rounds.setdefault(round_number, {})
            if query_index in row:
                raise ValueError(f"{path}:{line_number}: duplicate round/query timing")
            cold = parse_timing(parts[3], f"{path}:{line_number}")
            if cold is None:
                raise ValueError(f"{path}:{line_number}: cold timing cannot be null")
            row[query_index] = (cold, parse_timing(parts[4], f"{path}:{line_number}"))
        else:
            raise ValueError(f"{path}:{line_number}: legacy or malformed baseline protocol")

    if protocol != PROTOCOL:
        raise ValueError(f"{path}: expected protocol {PROTOCOL}, got {protocol or 'none'}")
    if declared_rounds != expected_rounds or declared_tries != expected_tries:
        raise ValueError(
            f"{path}: baseline uses {declared_rounds} round(s)/{declared_tries} tries; "
            f"Sirix uses {expected_rounds} round(s)/{expected_tries} tries"
        )
    expected_round_numbers = set(range(1, expected_rounds + 1))
    if set(rounds) != expected_round_numbers:
        raise ValueError(f"{path}: incomplete baseline rounds")
    query_set = set(rounds[1])
    if any(set(rows) != query_set for rows in rounds.values()):
        raise ValueError(f"{path}: baseline rounds use different query sets")
    if any(index not in query_set for index in executed):
        raise ValueError(f"{path}: baseline does not cover every Sirix query")

    cold_candidates = [[rounds[number][index][0] for index in executed]
                       for number in range(1, expected_rounds + 1)]
    cold = min(cold_candidates, key=sum)
    if expected_tries > 1:
        hot_candidates = []
        for number in range(1, expected_rounds + 1):
            values = [rounds[number][index][1] for index in executed]
            if any(value is None for value in values):
                raise ValueError(f"{path}: round {number} has incomplete hot timings")
            hot_candidates.append(values)
        hot = min(hot_candidates, key=sum)
    else:
        hot = [None] * len(executed)
    return list(zip(cold, hot)), f"measured with {PROTOCOL} ({path})"


def ratio(ours: float | None, theirs: float | None) -> str:
    if ours is None or not theirs:
        return "     -"
    value = ours / theirs
    return f"{value:5.2f}x" + (" " if value >= 1 else "*")


def milliseconds(value: float | None) -> str:
    return "       -" if value is None else f"{value * 1000:8.0f}"


def render(tier: str, out: Path, round_count: int, try_count: int, baseline_path: Path,
           diff_status: str) -> str:
    rows_per_round, executed, total_queries = load_rounds(out, round_count, try_count)
    cold = best_complete_round(rows_per_round, executed, 0)
    have_hot = len(rows_per_round[0][executed[0]]) > 1
    hot = best_complete_round(rows_per_round, executed, None) if have_hot else [None] * len(executed)
    baseline, baseline_source = baseline_for(baseline_path, executed, round_count, try_count)
    full_suite = len(executed) == total_queries

    lines = ["", f"  JSONBench, tier {tier} -- best complete suite round of {round_count}",
             f"  ClickHouse baseline: {baseline_source if baseline else 'unavailable'}"]
    if not full_suite:
        selected = ",".join(str(index + 1) for index in executed)
        lines.append(f"  SUBSET: {len(executed)} of {total_queries} queries ({selected}) -- the Σ row is NOT the suite figure.")
    lines.extend(["", "  query |  sirix cold |   sirix hot |     CH cold |      CH hot |  cold |   hot"])
    lines.append("  " + "-" * (len(lines[-1]) - 2))
    for slot, query_index in enumerate(executed):
        base_cold, base_hot = baseline[slot] if baseline else (None, None)
        lines.append(f"     Q{query_index + 1} | {milliseconds(cold[slot])} ms | {milliseconds(hot[slot])} ms |"
                     f" {milliseconds(base_cold)} ms | {milliseconds(base_hot)} ms |"
                     f" {ratio(cold[slot], base_cold)} | {ratio(hot[slot], base_hot)}")
    suite_cold = sum(cold)
    suite_hot = sum(hot) if have_hot else None
    base_cold = sum(value[0] for value in baseline) if baseline else None
    base_hot = (sum(value[1] for value in baseline)
                if baseline and all(value[1] is not None for value in baseline) else None)
    lines.append("  " + "-" * (len(lines[5 if full_suite else 6]) - 2))
    lines.append(f"      Σ | {milliseconds(suite_cold)} ms | {milliseconds(suite_hot)} ms |"
                 f" {milliseconds(base_cold)} ms | {milliseconds(base_hot)} ms |"
                 f" {ratio(suite_cold, base_cold)} | {ratio(suite_hot, base_hot)}")
    lines.extend(["", "  ratios are sirix/ClickHouse; '*' marks the rows where SirixDB is faster.",
                  f"  differential: {diff_status}"])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("tier")
    parser.add_argument("out", type=Path)
    parser.add_argument("rounds", type=int)
    parser.add_argument("tries", type=int)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("differential")
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("rounds must be positive")
    if args.tries < 1:
        parser.error("tries must be positive")
    try:
        print(render(args.tier, args.out, args.rounds, args.tries, args.baseline, args.differential))
    except (OSError, ValueError, json.JSONDecodeError) as exception:
        parser.exit(1, f"{exception}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
