#!/usr/bin/env python3
"""Independent dense-day oracle for the Supply History 1 workload."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Iterable

HORIZON = 366
PUBLICATIONS = 25
V = 166
L = 150
U = 210
SIGNED_LONG_MIN = -(1 << 63)
SIGNED_LONG_MAX = (1 << 63) - 1
TIERS = {
    "development": (2_000, 200, 100, 4_776),
    "t25k": (25_000, 2_500, 500, 58_724),
    "t100k": (100_000, 10_000, 2_000, 234_884),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tier", choices=TIERS, required=True)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--cross-check-intervals",
        action="store_true",
        help="also replay an interval-list implementation (development only)",
    )
    return parser.parse_args()


def new_relation(cardinality: int) -> list[list[tuple[int, ...] | None] | None]:
    return [None] + [[None] * HORIZON for _ in range(cardinality)]


def load_events(path: Path, tier: str) -> list[list[dict[str, int | str]]]:
    epochs: list[list[dict[str, int | str]]] = [[] for _ in range(PUBLICATIONS)]
    digest = hashlib.sha256()
    prior: tuple[int, str, int] | None = None
    count = 0
    with path.open("rb") as source:
        for raw in source:
            digest.update(raw)
            event = json.loads(raw)
            key = (event["epoch"], event["table"], event["id"])
            if prior is not None and key <= prior:
                raise ValueError(f"event order is not strict at line {count + 1}: {key} <= {prior}")
            prior = key
            validate_event(event, tier)
            epochs[event["epoch"]].append(event)
            count += 1
    expected = TIERS[tier][3]
    if count != expected:
        raise ValueError(f"event count {count} != {expected}")
    print(f"ORACLE_INPUT events={count} sha256={digest.hexdigest()}")
    return epochs


def validate_event(event: dict[str, int | str], tier: str) -> None:
    contracts, products, suppliers, _ = TIERS[tier]
    table = event["table"]
    limit = {"contracts": contracts, "products": products, "suppliers": suppliers}.get(table)
    if limit is None:
        raise ValueError(f"unknown table {table}")
    if not (0 <= event["epoch"] < PUBLICATIONS):
        raise ValueError(f"bad epoch: {event}")
    if not (1 <= event["id"] <= limit and 0 <= event["a"] < event["b"] <= HORIZON):
        raise ValueError(f"bad id/interval: {event}")
    if event["op"] == "delete":
        return
    if event["op"] != "put":
        raise ValueError(f"bad operation: {event}")
    if table == "contracts":
        if not (
            1 <= event["pid"] <= products
            and 1 <= event["sid"] <= suppliers
            and 10_000 <= event["cost"] <= 100_001
            and 1 <= event["qty"] <= 1_000
            and 0 <= event["grade"] < 4
        ):
            raise ValueError(f"bad contract payload: {event}")
    elif table == "products":
        if not (0 <= event["category"] < 16 and 150_000 <= event["retail"] <= 200_000):
            raise ValueError(f"bad product payload: {event}")
    elif not (0 <= event["region"] < 8 and 1 <= event["tier"] <= 3):
        raise ValueError(f"bad supplier payload: {event}")


def payload(event: dict[str, int | str]) -> tuple[int, ...] | None:
    if event["op"] == "delete":
        return None
    if event["table"] == "contracts":
        return (event["pid"], event["sid"], event["cost"], event["qty"], event["grade"])
    if event["table"] == "products":
        return (event["category"], event["retail"])
    return (event["region"], event["tier"])


def apply_dense(
    relations: dict[str, list[list[tuple[int, ...] | None] | None]],
    event: dict[str, int | str],
) -> None:
    value = payload(event)
    days = relations[event["table"]][event["id"]]
    assert days is not None
    days[event["a"] : event["b"]] = [value] * (event["b"] - event["a"])


def apply_intervals(
    relations: dict[str, list[list[tuple[int, int, tuple[int, ...]]] | None]],
    event: dict[str, int | str],
) -> None:
    old = relations[event["table"]][event["id"]]
    assert old is not None
    new: list[tuple[int, int, tuple[int, ...]]] = []
    for start, end, old_payload in old:
        if end <= event["a"] or event["b"] <= start:
            new.append((start, end, old_payload))
        else:
            if start < event["a"]:
                new.append((start, event["a"], old_payload))
            if event["b"] < end:
                new.append((event["b"], end, old_payload))
    value = payload(event)
    if value is not None:
        new.append((event["a"], event["b"], value))
    new.sort()
    coalesced: list[tuple[int, int, tuple[int, ...]]] = []
    for segment in new:
        if coalesced and coalesced[-1][1] == segment[0] and coalesced[-1][2] == segment[2]:
            coalesced[-1] = (coalesced[-1][0], segment[1], segment[2])
        else:
            coalesced.append(segment)
    relations[event["table"]][event["id"]] = coalesced


def cross_check(
    dense: dict[str, list[list[tuple[int, ...] | None] | None]],
    intervals: dict[str, list[list[tuple[int, int, tuple[int, ...]]] | None]],
    epoch: int,
) -> None:
    for table, identities in dense.items():
        for identity in range(1, len(identities)):
            expanded: list[tuple[int, ...] | None] = [None] * HORIZON
            for start, end, value in intervals[table][identity] or []:
                expanded[start:end] = [value] * (end - start)
            if expanded != identities[identity]:
                for day, (left, right) in enumerate(zip(identities[identity] or [], expanded)):
                    if left != right:
                        raise AssertionError(
                            f"dense/interval mismatch epoch={epoch} table={table} id={identity} day={day}"
                        )
                raise AssertionError("dense/interval mismatch without differing cell")


def point_snapshot(
    relation: list[list[tuple[int, ...] | None] | None], day: int
) -> list[tuple[int, ...] | None]:
    return [None] + [identity[day] for identity in relation[1:] if identity is not None]


def grouped_epoch(epoch: int, contracts: list[list[tuple[int, ...] | None] | None]) -> list[tuple[int, ...]]:
    groups: dict[int, list[int]] = {}
    for identity in contracts[1:]:
        assert identity is not None
        value = identity[V]
        if value is None:
            continue
        qty, grade = value[3], value[4]
        aggregate = groups.setdefault(grade, [0, 0])
        aggregate[0] += 1
        aggregate[1] += qty
    return [(epoch, grade, values[0], values[1]) for grade, values in sorted(groups.items())]


def evaluate(args: argparse.Namespace) -> dict[int, list[tuple[int, ...]]]:
    contract_count, product_count, supplier_count, _ = TIERS[args.tier]
    events = load_events(args.events, args.tier)
    dense = {
        "contracts": new_relation(contract_count),
        "products": new_relation(product_count),
        "suppliers": new_relation(supplier_count),
    }
    intervals = None
    if args.cross_check_intervals:
        if args.tier != "development":
            raise ValueError("--cross-check-intervals is intentionally bounded to development")
        intervals = {
            "contracts": [None] + [[] for _ in range(contract_count)],
            "products": [None] + [[] for _ in range(product_count)],
            "suppliers": [None] + [[] for _ in range(supplier_count)],
        }

    history: list[tuple[int, int, int]] = []
    grouped_history: list[tuple[int, ...]] = []
    at_a: list[tuple[int, ...] | None] | None = None
    at_d: list[tuple[int, ...] | None] | None = None
    for epoch in range(PUBLICATIONS):
        for event in events[epoch]:
            apply_dense(dense, event)
            if intervals is not None:
                apply_intervals(intervals, event)
        if intervals is not None:
            cross_check(dense, intervals, epoch)
        contract_one = dense["contracts"][1]
        assert contract_one is not None
        value = contract_one[V]
        if value is not None:
            history.append((epoch, value[2], value[3]))
        grouped_history.extend(grouped_epoch(epoch, dense["contracts"]))
        if epoch == 12:
            at_a = point_snapshot(dense["contracts"], V)
        if epoch == 18:
            at_d = point_snapshot(dense["contracts"], V)

    assert at_a is not None and at_d is not None
    at_b = point_snapshot(dense["contracts"], V)
    supplier_b = point_snapshot(dense["suppliers"], V)

    q1_value = at_b[1]
    q2_value = at_a[1]
    assert q1_value is not None and q2_value is not None
    q1 = [(1, q1_value[2], q1_value[3], q1_value[4])]
    q2 = [(1, q2_value[2], q2_value[3], q2_value[4])]

    contract_one = dense["contracts"][1]
    assert contract_one is not None
    prices = {value[2] for value in contract_one[L:U] if value is not None}
    q3 = [(min(prices), max(prices), len(prices))]

    q4 = []
    for identity in range(1, contract_count + 1):
        old, new = at_a[identity], at_b[identity]
        if old is not None and new is not None and (old[2] != new[2] or old[3] != new[3]):
            q4.append((identity, old[2], new[2], old[3], new[3]))

    q7_groups: dict[int, list[int]] = {}
    for value in at_b[1:]:
        if value is None:
            continue
        group = q7_groups.setdefault(value[4], [0, 0, 0])
        group[0] += 1
        group[1] += value[3]
        group[2] += value[2] * value[3]
    q7 = [(grade, *values) for grade, values in sorted(q7_groups.items())]

    q8_groups: dict[tuple[int, int], list[int]] = {}
    for value in at_a[1:]:
        if value is None:
            continue
        key = (value[1], value[4])
        group = q8_groups.setdefault(key, [0, value[2], value[2]])
        group[0] += 1
        group[1] = min(group[1], value[2])
        group[2] = max(group[2], value[2])
    q8 = [(sid, grade, *values) for (sid, grade), values in sorted(q8_groups.items())]

    q9_groups: dict[tuple[int, int], list[int]] = {}
    for value in at_b[1:]:
        if value is None:
            continue
        supplier = supplier_b[value[1]]
        if supplier is None:
            continue
        key = (supplier[0], value[4])
        group = q9_groups.setdefault(key, [0, 0])
        group[0] += 1
        group[1] += value[2] * value[3]
    q9 = [(region, grade, *values) for (region, grade), values in sorted(q9_groups.items())]

    q10_groups: dict[int, list[object]] = {}
    for identity in range(1, contract_count + 1):
        contract_days = dense["contracts"][identity]
        assert contract_days is not None
        for day in range(L, U):
            contract = contract_days[day]
            if contract is None:
                continue
            product_days = dense["products"][contract[0]]
            assert product_days is not None
            product = product_days[day]
            if product is None:
                continue
            category = product[0]
            margin = product[1] - contract[2]
            group = q10_groups.setdefault(category, [set(), margin, margin])
            group[0].add(identity)
            group[1] = min(group[1], margin)
            group[2] = max(group[2], margin)
    q10 = [
        (category, len(values[0]), values[1], values[2])
        for category, values in sorted(q10_groups.items())
    ]

    q11 = []
    for day in range(L, U):
        groups: dict[int, list[int]] = {}
        for identity in dense["contracts"][1:]:
            assert identity is not None
            value = identity[day]
            if value is None:
                continue
            group = groups.setdefault(value[4], [0, 0])
            group[0] += 1
            group[1] += value[2] * value[3]
        q11.extend((day, grade, *values) for grade, values in sorted(groups.items()))

    q12_groups: dict[int, list[int]] = {}
    for identity in range(1, contract_count + 1):
        old = at_a[identity]
        if old is None or at_d[identity] is not None:
            continue
        group = q12_groups.setdefault(old[4], [0, 0])
        group[0] += 1
        group[1] += old[2] * old[3]
    q12 = [(grade, *values) for grade, values in sorted(q12_groups.items())]

    if history[18][0] == 18 or history[19][0] == 19:
        raise AssertionError("fixture must be absent at E18 and E19")
    history_epochs = {row[0] for row in history}
    if 18 in history_epochs or 19 in history_epochs or 20 not in history_epochs:
        raise AssertionError("fixture history boundary invariant failed")
    if q1[0][1] == q2[0][1]:
        raise AssertionError("Q1 and Q2 fixture prices must differ")

    return {
        1: q1,
        2: q2,
        3: q3,
        4: q4,
        5: history,
        6: grouped_history,
        7: q7,
        8: q8,
        9: q9,
        10: q10,
        11: q11,
        12: q12,
    }


def write_results(results: dict[int, list[tuple[int, ...]]], output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    manifest = {"engine": "independent-dense-day-oracle", "queries": []}
    for query, rows in results.items():
        target = output / f"q{query}.tsv"
        digest = hashlib.sha256()
        with target.open("wb") as sink:
            for row in rows:
                for value in row:
                    if not isinstance(value, int) or not SIGNED_LONG_MIN <= value <= SIGNED_LONG_MAX:
                        raise OverflowError(f"Q{query} invalid signed-64 value: {value!r}")
                line = ("\t".join(str(value) for value in row) + "\n").encode()
                sink.write(line)
                digest.update(line)
        entry = {"q": query, "rows": len(rows), "sha256": digest.hexdigest()}
        manifest["queries"].append(entry)
        print(f"ORACLE_QUERY q={query} rows={len(rows)} sha256={digest.hexdigest()}")
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def main() -> None:
    args = parse_args()
    write_results(evaluate(args), args.out)


if __name__ == "__main__":
    main()
