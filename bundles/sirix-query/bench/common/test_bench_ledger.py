#!/usr/bin/env python3
"""Behaviour tests of the results ledger, the environment capture and the conditions record
(spec/CONV-conventions.md, CONV-303)."""

from __future__ import annotations

import contextlib
import datetime
import hashlib
import io
import json
import math
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Sequence
from unittest import mock

import bench_ledger
import timed_gate
from bench_ledger import LedgerError

COMMIT_A = "a" * 40
COMMIT_B = "b" * 40
NOW = datetime.datetime(2026, 11, 23, 18, 30, 5, tzinfo=datetime.timezone.utc)
QUIET = [0.004, -0.003, 0.001, -0.002, 0.0, 0.003, -0.004, 0.002, -0.001, 0.0]

CPUINFO = "".join(
    f"processor\t: {index}\nvendor_id\t: TestVendor\nmodel name\t: Test CPU {9000 if index < 4 else 9001}\n"
    f"flags\t\t: fpu vme\nphysical id\t: 0\ncore id\t\t: {index // 2}\n\n"
    for index in range(5)
)


def make_root(directory: Path, **overrides: str | None) -> Path:
    """A small /proc, /sys and /etc tree. An override of None removes the file."""
    root = directory / "root"
    files: dict[str, str | None] = {
        "proc/sys/kernel/osrelease": "6.8.0-138-generic\n",
        "etc/os-release": 'NAME="Linux Mint"\nPRETTY_NAME="Linux Mint 22.3"\nID=linuxmint\n',
        "sys/class/dmi/id/bios_vendor": "Dell Inc.\n",
        "sys/class/dmi/id/bios_version": "1.31.0\n",
        "sys/class/dmi/id/bios_date": "03/05/2025\n",
        "sys/class/dmi/id/product_name": "XPS 15 9520\n",
        "proc/cpuinfo": CPUINFO,
        "sys/devices/system/cpu/intel_pstate/no_turbo": "0\n",
        "sys/class/powercap/intel-rapl:0/constraint_0_power_limit_uw": "50000000\n",
        "sys/class/powercap/intel-rapl:0/energy_uj": "123456\n",
        "proc/meminfo": "MemTotal:       32527200 kB\nMemFree:         1000000 kB\n",
        "proc/self/status": "Name:\tpython3\nCpus_allowed_list:\t0-11\n",
        "proc/self/mountinfo": (
            f"25 1 259:2 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw\n"
            f"90 25 0:50 / {directory}/data rw,relatime shared:40 - xfs /dev/sdb1 rw\n"
        ),
    }
    for policy in ("policy0", "policy1"):
        base = f"sys/devices/system/cpu/cpufreq/{policy}"
        files.update(
            {
                f"{base}/scaling_driver": "intel_pstate\n",
                f"{base}/scaling_governor": "powersave\n",
                f"{base}/scaling_min_freq": "400000\n",
                f"{base}/scaling_max_freq": "4600000\n",
                f"{base}/scaling_cur_freq": "1234567\n",
                f"{base}/energy_performance_preference": "balance_power\n",
            }
        )
    files.update(overrides)
    for name, content in files.items():
        path = root / name
        if content is None:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    return root


class FakeRunner:
    """Stands in for lscpu, cpupower, sensors and java."""

    def __init__(self, **outputs: tuple[int, str, str] | None) -> None:
        self.outputs: dict[str, tuple[int, str, str] | None] = {
            "lscpu": (0, "Architecture: x86_64\nCPU(s) scaling MHz: 23%\n", ""),
            "cpupower": None,
            "sensors": (0, "Package id 0:  +47.0 C\n", ""),
            "java": (0, "", 'openjdk version "25.0.5" 2026-10-20 LTS\nOpenJDK Runtime Environment Temurin-25.0.5+8\n'),
        }
        self.outputs.update(outputs)
        self.calls: list[list[str]] = []

    def __call__(self, argv: Sequence[str]) -> tuple[int, str, str]:
        self.calls.append(list(argv))
        output = self.outputs.get(Path(argv[0]).name)
        if output is None:
            raise FileNotFoundError(argv[0])
        return output


def fake_environment(directory: Path, runner: FakeRunner | None = None, **overrides: str | None) -> dict[str, Any]:
    return bench_ledger.capture_environment(
        root=make_root(directory, **overrides), runner=runner or FakeRunner(), now=NOW
    )


def condition_entry(date: str, name: str, kind: str, fingerprint: str, **rows: str) -> str:
    cells = {row: "stated" for row in bench_ledger.CONDITION_ROWS}
    cells["Fingerprint"] = f"`{fingerprint}`"
    cells.update({key.replace("_", " "): value for key, value in rows.items()})
    body = "".join(f"| {row} | {cells[row]} |\n" for row in bench_ledger.CONDITION_ROWS if cells[row] is not None)
    return f"## {date} — {name} ({kind})\n\n| Field | Value |\n|---|---|\n{body}\n"


def run_row(
    environment: dict[str, Any],
    *,
    tier: str = "gate",
    run: str = "20261123T183005Z-0a1b2c3d",
    night: str | None = None,
    conditions: str = "laptop-1",
    runtime: str = 'openjdk version "25.0.5"',
    binary: str = "jvm",
    commit: str = COMMIT_B,
) -> dict[str, Any]:
    head = tier == "nightly"
    arm = {"commit": commit, "dirty": False, "binary": binary, "runtime": runtime}
    row: dict[str, Any] = {
        "schema": 1,
        "kind": "run",
        "run": run,
        "started_at": "2026-11-23T18:30:05Z",
        "tier": tier,
        "kit": "jmh",
        "plan": (
            {"min_processes": 20, "benchmarks": None}
            if head
            else {
                "floor": timed_gate.FLOOR,
                "stop_precision": timed_gate.STOP_PRECISION,
                "quantile": 0.99,
                "min_pairs": 10,
                "max_pairs": 20,
                "benchmarks": None,
            }
        ),
        "arms": {"head": arm} if head else {"baseline": dict(arm, commit=COMMIT_A), "candidate": arm},
        "conditions": conditions,
        "fingerprint": bench_ledger.fingerprint(environment),
        "environment": environment,
    }
    if night is not None:
        row["night"] = night
    return row


def measurement_row(**changes: Any) -> dict[str, Any]:
    row = {
        "schema": 1,
        "kind": "measurement",
        "run": "20261123T183005Z-0a1b2c3d",
        "benchmark": "scan",
        "params": {"keyBytes": "8"},
        "metric": "ns_per_op",
        "unit": "ns/op",
        "arm": "baseline",
        "pair": 1,
        "position": 1,
        "process": 1,
        "value": 24.5,
        "accepted": True,
    }
    row.update(changes)
    return {key: value for key, value in row.items() if value is not ...}


def verdict_row(**changes: Any) -> dict[str, Any]:
    row = {
        "schema": 1,
        "kind": "verdict",
        "run": "20261123T183005Z-0a1b2c3d",
        "benchmark": "scan",
        "params": {"keyBytes": "8"},
        "metric": "ns_per_op",
        "unit": "ns/op",
        "pairs": 10,
        "difference": 0.001,
        "threshold": 0.012,
        "verdict": "unchanged",
        "allocation": None,
    }
    row.update(changes)
    return {key: value for key, value in row.items() if value is not ...}


def jmh_result(
    method: str,
    score: float,
    *,
    mode: str = "avgt",
    unit: str = "ns/op",
    forks: int = 1,
    params: dict[str, str] | None = None,
    alloc: float | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "benchmark": f"io.sirix.benchmark.LeafBenchmark.{method}",
        "mode": mode,
        "forks": forks,
        "params": params if params is not None else {"keyBytes": "8"},
        "primaryMetric": {"score": score, "scoreUnit": unit, "rawData": [[score * 1.01, score, score * 0.99]]},
        "secondaryMetrics": {},
    }
    if alloc is not None:
        result["secondaryMetrics"]["gc.alloc.rate.norm"] = {"score": alloc, "scoreUnit": "B/op"}
    return result


class EnvironmentCaptureTest(unittest.TestCase):

    def test_capture_holds_every_required_reading(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(Path(directory))
            data = Path(directory) / "data" / "db"
            data.mkdir(parents=True)
            runner = FakeRunner()
            capture = bench_ledger.capture_environment(
                root=root,
                runner=runner,
                now=NOW,
                data=data,
                javas={"baseline": "/opt/jdk/bin/java"},
                runtimes={"candidate": "GraalVM 25.0.3"},
            )
        self.assertEqual("2026-11-23T18:30:05Z", capture["captured_at"])
        self.assertEqual("6.8.0-138-generic", capture["kernel_release"])
        self.assertEqual("Linux Mint 22.3", capture["os"])
        self.assertEqual(os.uname().machine, capture["architecture"])
        self.assertEqual({"vendor": "Dell Inc.", "version": "1.31.0", "date": "03/05/2025"}, capture["firmware"])
        self.assertEqual("XPS 15 9520", capture["product_name"])
        self.assertEqual(
            {
                "model": "Test CPU 9000",
                "sockets": 1,
                "cores": 3,
                "threads": 5,
                "lscpu": "Architecture: x86_64\nCPU(s) scaling MHz: 23%",
            },
            capture["cpu"],
        )
        self.assertEqual(
            {
                "driver": "intel_pstate",
                "governor": "powersave",
                "min_khz": "400000",
                "max_khz": "4600000",
                "energy_performance_preference": "balance_power",
            },
            capture["cpufreq"]["policy1"],
        )
        self.assertEqual(["policy0", "policy1"], list(capture["cpufreq"]))
        self.assertEqual("0", capture["turbo"]["intel_pstate_no_turbo"])
        self.assertEqual({"intel-rapl:0/constraint_0_power_limit_uw": "50000000"}, capture["powercap"])
        self.assertEqual("Package id 0:  +47.0 C", capture["sensors"])
        self.assertEqual(32527200, capture["memory_total_kb"])
        self.assertEqual("0-11", capture["allowed_cpus"])
        self.assertEqual(
            {
                "baseline": 'openjdk version "25.0.5" 2026-10-20 LTS\nOpenJDK Runtime Environment Temurin-25.0.5+8',
                "candidate": "GraalVM 25.0.3",
            },
            capture["runtimes"],
        )
        self.assertEqual("xfs", capture["data_filesystem"], "the most specific mount that holds the data path")
        self.assertIn(["/opt/jdk/bin/java", "-version"], runner.calls)

    def test_an_unreadable_probe_is_recorded_with_its_reason(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = {
                "sys/class/dmi/id/bios_version": None,
                "sys/devices/system/cpu/intel_pstate/no_turbo": None,
                "proc/meminfo": "MemFree: 1 kB\n",
                "etc/os-release": "NAME=x\n",
                "proc/self/status": "Name:\tx\n",
                "sys/class/powercap/intel-rapl:0/constraint_0_power_limit_uw": None,
            }
            runner = FakeRunner(sensors=(1, "", "No sensors found!"), lscpu=(0, "", ""), java=None)
            capture = bench_ledger.capture_environment(
                root=make_root(Path(directory), **missing), runner=runner, now=NOW, javas={"head": "/opt/jdk/bin/java"}
            )
        self.assertEqual(
            {"unavailable": "/sys/class/dmi/id/bios_version: No such file or directory"}, capture["firmware"]["version"]
        )
        self.assertEqual(
            {"unavailable": "/sys/devices/system/cpu/intel_pstate/no_turbo: No such file or directory"},
            capture["turbo"]["intel_pstate_no_turbo"],
        )
        self.assertEqual({"unavailable": "cpupower: command not found"}, capture["cpupower_frequency_info"])
        self.assertEqual({"unavailable": "sensors: exit status 1"}, capture["sensors"])
        self.assertEqual({"unavailable": "lscpu: no output"}, capture["cpu"]["lscpu"])
        self.assertEqual({"unavailable": "/proc/meminfo: no MemTotal"}, capture["memory_total_kb"])
        self.assertEqual({"unavailable": "/etc/os-release: no PRETTY_NAME"}, capture["os"])
        self.assertEqual({"unavailable": "/proc/self/status: no Cpus_allowed_list"}, capture["allowed_cpus"])
        self.assertEqual({"unavailable": "/sys/class/powercap: no power limit"}, capture["powercap"])
        self.assertEqual({"unavailable": "java: command not found"}, capture["runtimes"]["head"])
        self.assertEqual({"unavailable": "no benchmark data path was given"}, capture["data_filesystem"])

    def test_a_machine_without_these_sources_still_yields_a_complete_capture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            empty = Path(directory) / "empty"
            empty.mkdir()
            capture = bench_ledger.capture_environment(
                root=empty, runner=FakeRunner(lscpu=None, sensors=None), now=NOW, data=Path(directory)
            )
        for field in (
            "kernel_release",
            "os",
            "product_name",
            "cpufreq",
            "powercap",
            "memory_total_kb",
            "allowed_cpus",
            "data_filesystem",
            "sensors",
            "cpupower_frequency_info",
        ):
            self.assertIn("unavailable", capture[field], field)
        self.assertEqual(64, len(bench_ledger.fingerprint(capture)))

    def test_the_capture_names_no_host_user_or_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory) / "data"
            data.mkdir()
            capture = bench_ledger.capture_environment(
                root=make_root(Path(directory)),
                runner=FakeRunner(),
                now=NOW,
                data=data,
                javas={"head": f"{directory}/jdk/bin/java"},
            )
            text = json.dumps(capture)
            self.assertNotIn(directory, text)
            self.assertNotIn(os.uname().nodename, text)
            self.assertNotIn("/home/", text)

    def test_a_data_path_outside_every_mount_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(
                Path(directory), **{"proc/self/mountinfo": "90 25 0:50 / /nonexistent/mount rw - xfs /dev/sdb1 rw\n"}
            )
            capture = bench_ledger.capture_environment(root=root, runner=FakeRunner(), now=NOW, data=Path(directory))
        self.assertEqual(
            {"unavailable": "/proc/self/mountinfo: no mount holds the benchmark data path"}, capture["data_filesystem"]
        )


class FingerprintTest(unittest.TestCase):

    def test_the_fingerprint_is_the_sha256_of_the_canonical_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            environment = fake_environment(Path(directory))
        self.assertEqual(
            {
                "architecture": os.uname().machine,
                "cpu_model": "Test CPU 9000",
                "cpufreq_drivers": ["intel_pstate"],
                "firmware_version": "1.31.0",
                "governors": ["powersave"],
                "kernel_release": "6.8.0-138-generic",
                "logical_cpus": 5,
                "memory_gib": 31,
                "os": "Linux Mint 22.3",
                "product_name": "XPS 15 9520",
                "turbo": {
                    "cpufreq_boost": {
                        "unavailable": "/sys/devices/system/cpu/cpufreq/boost: No such file or directory"
                    },
                    "intel_pstate_no_turbo": "0",
                },
            },
            bench_ledger.fingerprint_fields(environment),
        )
        self.assertRegex(bench_ledger.fingerprint(environment), "^[0-9a-f]{64}$")

    def test_readings_that_vary_within_a_condition_do_not_change_it(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            reference = bench_ledger.fingerprint(fake_environment(Path(first)))
            varied = {
                "sys/devices/system/cpu/cpufreq/policy0/scaling_cur_freq": "4400000\n",
                "sys/class/powercap/intel-rapl:0/constraint_0_power_limit_uw": "45000000\n",
                "proc/self/status": "Cpus_allowed_list:\t12-19\n",
                "proc/meminfo": "MemTotal:       32600000 kB\n",
                "sys/devices/system/cpu/cpufreq/policy0/energy_performance_preference": "performance\n",
            }
            runner = FakeRunner(
                sensors=(0, "Package id 0:  +91.0 C\n", ""),
                java=(0, "", "java 26\n"),
                lscpu=(0, "CPU(s) scaling MHz: 80%\n", ""),
            )
            environment = bench_ledger.capture_environment(
                root=make_root(Path(second), **varied),
                runner=runner,
                javas={"head": "java"},
                now=NOW + datetime.timedelta(days=3),
            )
            self.assertEqual(reference, bench_ledger.fingerprint(environment))

    def test_every_identifying_field_changes_it(self) -> None:
        changes = {
            "kernel": {"proc/sys/kernel/osrelease": "6.8.0-140-generic\n"},
            "operating system": {"etc/os-release": 'PRETTY_NAME="Linux Mint 22.4"\n'},
            "firmware": {"sys/class/dmi/id/bios_version": "1.32.0\n"},
            "product": {"sys/class/dmi/id/product_name": "Precision 5570\n"},
            "cpu model": {"proc/cpuinfo": CPUINFO.replace("9000", "9100")},
            "logical cpus": {"proc/cpuinfo": CPUINFO + "processor\t: 5\nmodel name\t: Test CPU 9000\n\n"},
            "driver": {"sys/devices/system/cpu/cpufreq/policy1/scaling_driver": "acpi-cpufreq\n"},
            "governor": {"sys/devices/system/cpu/cpufreq/policy0/scaling_governor": "performance\n"},
            "turbo": {"sys/devices/system/cpu/intel_pstate/no_turbo": "1\n"},
            "memory": {"proc/meminfo": "MemTotal:       65000000 kB\n"},
        }
        with tempfile.TemporaryDirectory() as directory:
            seen = {bench_ledger.fingerprint(fake_environment(Path(directory) / "reference"))}
            for name, overrides in changes.items():
                with self.subTest(change=name):
                    value = bench_ledger.fingerprint(
                        fake_environment(Path(directory) / name.replace(" ", "-"), **overrides)
                    )
                    self.assertNotIn(value, seen)
                    seen.add(value)

    def test_the_canonical_form_is_pinned(self) -> None:
        environment = {
            "kernel_release": "k",
            "os": "o",
            "architecture": "a",
            "firmware": {"version": "f"},
            "product_name": "p",
            "cpu": {"model": "m", "threads": 2},
            "cpufreq": {"policy0": {"driver": "d", "governor": "g2"}, "policy1": {"driver": "d", "governor": "g1"}},
            "turbo": {"cpufreq_boost": "1", "intel_pstate_no_turbo": "0"},
            "memory_total_kb": 2 * 1024 * 1024,
        }
        canonical = (
            '{"architecture":"a","cpu_model":"m","cpufreq_drivers":["d"],"firmware_version":"f",'
            '"governors":["g1","g2"],"kernel_release":"k","logical_cpus":2,"memory_gib":2,"os":"o",'
            '"product_name":"p","turbo":{"cpufreq_boost":"1","intel_pstate_no_turbo":"0"}}'
        )
        self.assertEqual(hashlib.sha256(canonical.encode("utf-8")).hexdigest(), bench_ledger.fingerprint(environment))


class ConditionsRecordTest(unittest.TestCase):

    def test_an_entry_is_found_by_its_fingerprint(self) -> None:
        text = (
            "# Measurement conditions\n\nIntroductory prose.\n\n"
            + condition_entry("2026-11-23", "laptop-1", "new baseline", "1" * 64)
            + condition_entry("2026-12-02", "laptop-1", "continues", "2" * 64)
            + condition_entry("2027-01-10", "zen5-box", "new baseline", "3" * 64)
        )
        entries = bench_ledger.parse_conditions(text)
        self.assertEqual(
            [
                ("2026-11-23", "laptop-1", "new baseline"),
                ("2026-12-02", "laptop-1", "continues"),
                ("2027-01-10", "zen5-box", "new baseline"),
            ],
            [(entry["date"], entry["id"], entry["kind"]) for entry in entries],
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "CONDITIONS.md"
            path.write_text(text, encoding="utf-8")
            self.assertEqual("laptop-1", bench_ledger.conditions_id(path, "1" * 64))
            self.assertEqual("laptop-1", bench_ledger.conditions_id(path, "2" * 64), "a continuing entry keeps the id")
            self.assertEqual("zen5-box", bench_ledger.conditions_id(path, "3" * 64))
            self.assertIsNone(bench_ledger.conditions_id(path, "4" * 64))
            with self.assertRaisesRegex(LedgerError, "conditions record .*missing.md"):
                bench_ledger.conditions_id(Path(directory) / "missing.md", "1" * 64)

    def test_malformed_records_are_refused(self) -> None:
        good = condition_entry("2026-11-23", "laptop-1", "new baseline", "1" * 64)
        cases = {
            "an entry heading is": good.replace("(new baseline)", "(baseline)"),
            "an entry heading is '## YYYY": good.replace("laptop-1", "Laptop_1"),
            "not a calendar date": good.replace("2026-11-23", "2026-13-40"),
            "missing or empty row\\(s\\): Firmware, Changed": condition_entry(
                "2026-11-23", "laptop-1", "new baseline", "1" * 64, Firmware=None, Changed=""
            ),
            "row 'Machine' occurs twice": good + "| Machine | again |\n",
            "entries must be appended in date order": good
            + condition_entry("2026-11-01", "other", "new baseline", "2" * 64),
            "a new baseline needs a new id": good + condition_entry("2026-12-01", "laptop-1", "new baseline", "2" * 64),
            "continues a condition that no earlier entry started": condition_entry(
                "2026-11-23", "x", "continues", "1" * 64
            ),
            "the Fingerprint row holds 64-digit": condition_entry("2026-11-23", "laptop-1", "new baseline", "1" * 63),
            "Fingerprint row holds 64-digit lowercase": condition_entry(
                "2026-11-23", "l", "new baseline", "1" * 64 + " (approx)"
            ),
        }
        for message, text in cases.items():
            with self.subTest(message=message):
                with self.assertRaisesRegex(LedgerError, message):
                    bench_ledger.parse_conditions(text)

    def test_the_repository_record_is_well_formed_and_dated(self) -> None:
        entries = bench_ledger.parse_conditions(bench_ledger.default_conditions_path().read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(entries), 1)
        self.assertEqual("new baseline", entries[0]["kind"])


class RowSchemaTest(unittest.TestCase):

    def test_allocation_verdict_requires_an_integer_delta_and_consistent_pass(self) -> None:
        for delta, passes in ((1, False), (0, True), (-1, True)):
            bench_ledger.validate_row(
                verdict_row(
                    allocation={"baseline_bytes": 995, "candidate_bytes": 995, "delta_bytes": delta, "pass": passes}
                )
            )
        for extra in (
            {"pass": True},
            {"delta_bytes": True, "pass": True},
            {"delta_bytes": 0.5, "pass": True},
            {"delta_bytes": 1, "pass": True},
            {"delta_bytes": 0, "pass": False},
        ):
            with self.subTest(extra=extra):
                with self.assertRaises(LedgerError):
                    bench_ledger.validate_row(
                        verdict_row(allocation={"baseline_bytes": 995, "candidate_bytes": 995, **extra})
                    )

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.environment = fake_environment(Path(self.directory.name))

    def test_well_formed_rows_of_every_kind_are_accepted(self) -> None:
        rows = [
            run_row(self.environment),
            run_row(self.environment, tier="control"),
            run_row(self.environment, tier="nightly", night="2026-11-23"),
            measurement_row(),
            measurement_row(arm="head", pair=None, position=None),
            measurement_row(metric="alloc_bytes_per_op", unit="B/op", value=0.0, pair=None, position=None),
            measurement_row(accepted=False, reject_reason="foreign load 4.3 cores"),
            measurement_row(samples={"count": 3, "min": 24.0, "max": 25.0}, counters={"groupDense": 2}),
            measurement_row(future_field="ignored"),
            verdict_row(),
            verdict_row(allocation={"baseline_bytes": 168, "candidate_bytes": 168, "delta_bytes": 0, "pass": True}),
        ]
        for row in rows:
            bench_ledger.validate_row(row)

    def test_run_rows_are_checked_field_by_field(self) -> None:
        def damaged(path: Sequence[str], value: Any, **base: Any) -> dict[str, Any]:
            row = run_row(self.environment, **base)
            target = row
            for key in path[:-1]:
                target = target[key]
            if value is ...:
                del target[path[-1]]
            else:
                target[path[-1]] = value
            return row

        cases = {
            "unknown ledger schema 2": damaged(["schema"], 2),
            "unknown ledger schema True": damaged(["schema"], True),
            "'run' must be YYYYMMDDTHHMMSSZ": damaged(["run"], "run-1"),
            "'tier' must be one of": damaged(["tier"], "weekly"),
            "'night' belongs to a nightly run": damaged(["night"], "2026-11-23"),
            "'night' belongs to a nightly run and to no other": damaged(
                ["night"], ..., tier="nightly", night="2026-11-23"
            ),
            "'night' must be YYYY-MM-DD": damaged(["night"], "23.11.2026", tier="nightly", night="x"),
            "'kit' must name the kit": damaged(["kit"], ""),
            "the arms are baseline and candidate, or head": damaged(["arms", "baseline"], ...),
            "a gate run compares baseline and candidate": damaged(["tier"], "gate", tier="nightly", night=None),
            "a nightly run measures the single arm head": damaged(["tier"], "nightly") | {"night": "2026-11-23"},
            "arm candidate: 'commit' must be 40 lowercase": damaged(["arms", "candidate", "commit"], "HEAD"),
            "arm candidate: 'dirty' must be a boolean": damaged(["arms", "candidate", "dirty"], "no"),
            "arm candidate: 'binary' must be one of": damaged(["arms", "candidate", "binary"], "graal"),
            "arm baseline: 'runtime' must be": damaged(["arms", "baseline", "runtime"], ""),
            "plan 'floor' must be a positive number": damaged(["plan", "floor"], ...),
            "plan 'max_pairs' must be a positive integer": damaged(["plan", "max_pairs"], 20.0),
            "plan 'benchmarks' must be a non-empty list": damaged(["plan", "benchmarks"], []),
            "plan 'min_processes' must be a positive integer": damaged(
                ["plan", "min_processes"], 0, tier="nightly", night="2026-11-23"
            ),
            "'conditions' must be the id": damaged(["conditions"], ""),
            "'fingerprint' must be 64 lowercase": damaged(["fingerprint"], "abc"),
            "'fingerprint' is not the fingerprint of 'environment'": damaged(
                ["environment", "kernel_release"], "other"
            ),
            "'environment' must be the capture": damaged(["environment"], "see file"),
        }
        for message, row in cases.items():
            with self.subTest(message=message):
                with self.assertRaisesRegex(LedgerError, message):
                    bench_ledger.validate_row(row)

    def test_measurement_and_verdict_rows_are_checked_field_by_field(self) -> None:
        cases = {
            "'benchmark' must be a name": measurement_row(benchmark=""),
            "'params' must map strings to strings": measurement_row(params={"keyBytes": 8}),
            "'metric' and 'unit' must be names": measurement_row(unit=...),
            "'arm' must be baseline, candidate or head": measurement_row(arm="main"),
            "'pair' and 'position' are": measurement_row(position=3),
            "'pair' and 'position' are a pair number": measurement_row(pair=None),
            "a pair number from 1": measurement_row(pair=0),
            "'process' must be an ordinal from 1": measurement_row(process=0),
            "'value' must be finite": measurement_row(value=0.0),
            "'value' must be finite, and greater than zero": measurement_row(value=-1.0, metric="alloc_bytes_per_op"),
            "greater than zero unless": measurement_row(value=math.inf),
            "unless it is an allocation figure": measurement_row(value="24"),
            "'samples' must hold count, min and max": measurement_row(samples={"count": 3, "min": 26.0, "max": 25.0}),
            "'counters' must map names to numbers": measurement_row(counters={"groupDense": "2"}),
            "'accepted' must be a boolean": measurement_row(accepted=...),
            "'reject_reason' is present exactly when": measurement_row(accepted=False),
            "'reject_reason' is present exactly when 'accepted' is false": measurement_row(reject_reason="load"),
            "'pairs' must be a positive integer": verdict_row(pairs=0),
            "'difference' and 'threshold' must be finite": verdict_row(threshold=-0.1),
            "'verdict' must be regression": verdict_row(verdict="pass"),
            "'allocation' must be null or hold": verdict_row(allocation=...),
            "'allocation' must be null or hold baseline_bytes": verdict_row(
                allocation={"baseline_bytes": 1.5, "candidate_bytes": 1, "delta_bytes": 0, "pass": True}
            ),
            "unknown kind 'note'": measurement_row(kind="note"),
        }
        for message, row in cases.items():
            with self.subTest(message=message):
                with self.assertRaisesRegex(LedgerError, message):
                    bench_ledger.validate_row(row)
        with self.assertRaisesRegex(LedgerError, "a ledger row must be a JSON object"):
            bench_ledger.validate_row([1])


class LedgerFileTest(unittest.TestCase):

    def test_a_conflicting_identity_refuses_the_entire_append(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.jsonl"
            bench_ledger.append_rows(ledger, [measurement_row()])
            before = ledger.read_bytes()
            with self.assertRaisesRegex(LedgerError, "conflicting measurement row"):
                bench_ledger.append_rows(ledger, [measurement_row(pair=2), measurement_row(value=99.0)])
            self.assertEqual(before, ledger.read_bytes())

    def test_appending_never_changes_what_is_already_there(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "nested" / "ledger.jsonl"
            bench_ledger.append_rows(ledger, [measurement_row(), measurement_row(pair=2)])
            before = ledger.read_bytes()
            bench_ledger.append_rows(ledger, [measurement_row(pair=3)])
            bench_ledger.append_rows(ledger, [])
            after = ledger.read_bytes()
            self.assertTrue(after.startswith(before))
            self.assertEqual(3, after.count(b"\n"))
            self.assertEqual([1, 2, 3], [row["pair"] for row in bench_ledger.read_ledger(ledger)])

    def test_an_invalid_row_appends_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.jsonl"
            bench_ledger.append_rows(ledger, [measurement_row()])
            before = ledger.read_bytes()
            with self.assertRaisesRegex(LedgerError, "'value' must be finite"):
                bench_ledger.append_rows(ledger, [measurement_row(pair=2), measurement_row(pair=3, value=math.nan)])
            self.assertEqual(before, ledger.read_bytes())

    def test_a_ledger_with_a_torn_last_line_is_not_appended_to(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.jsonl"
            ledger.write_text(json.dumps(measurement_row()) + "\n" + '{"schema": 1, "kind": "meas', encoding="utf-8")
            before = ledger.read_bytes()
            with self.assertRaisesRegex(LedgerError, "the last line is incomplete"):
                bench_ledger.append_rows(ledger, [measurement_row(pair=2)])
            self.assertEqual(before, ledger.read_bytes())

    def test_a_reader_ignores_unknown_fields_and_refuses_unknown_schemas(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.jsonl"
            ledger.write_text(json.dumps(measurement_row(added_in_a_later_writer=[1, 2])) + "\n\n", encoding="utf-8")
            self.assertEqual([[1, 2]], [row["added_in_a_later_writer"] for row in bench_ledger.read_ledger(ledger)])
            ledger.write_text(
                json.dumps(measurement_row()) + "\n" + json.dumps(measurement_row(schema=2)) + "\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(LedgerError, "ledger.jsonl:2: unknown ledger schema 2"):
                bench_ledger.read_ledger(ledger)
            ledger.write_text("{not json}\n", encoding="utf-8")
            with self.assertRaisesRegex(LedgerError, "ledger.jsonl:1: not a JSON object"):
                bench_ledger.read_ledger(ledger)


class RunDirectoryTest(unittest.TestCase):
    """A run from its opening to the ledger, through the functions the command line calls."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)
        self.root = make_root(self.base)
        self.runner = FakeRunner()
        self.ledger = self.base / "ledger.jsonl"
        self.conditions = self.base / "CONDITIONS.md"
        self.fingerprint = bench_ledger.fingerprint(
            bench_ledger.capture_environment(root=self.root, runner=self.runner, now=NOW)
        )
        self.conditions.write_text(
            condition_entry("2026-11-23", "laptop-1", "new baseline", self.fingerprint), encoding="utf-8"
        )

    def open_run(
        self, name: str = "run", *, tier: str = "gate", arms: Sequence[str] | None = None, **options: Any
    ) -> Path:
        out = self.base / name
        out.mkdir()
        specs = (
            arms
            if arms is not None
            else (
                [f"head,commit={COMMIT_B},binary=native,runtime=GraalVM 25.0.3, built 2026-11-20"]
                if tier == "nightly"
                else [
                    f"baseline,commit={COMMIT_A},binary=jvm,java=/opt/jdk/bin/java",
                    f"candidate,commit={COMMIT_B},binary=jvm,dirty=true,java=/opt/jdk/bin/java",
                ]
            )
        )
        bench_ledger.begin(
            out,
            tier=tier,
            kit="jmh",
            arm_specs=specs,
            ledger=self.ledger,
            conditions=self.conditions,
            root=self.root,
            runner=self.runner,
            now=NOW,
            **options,
        )
        return out

    def record_pairs(
        self,
        out: Path,
        deviations: Sequence[float],
        shift: float = 0.0,
        *,
        benchmark: str = "scan",
        first_pair: int = 1,
        rejected: Sequence[int] = (),
    ) -> None:
        for offset, deviation in enumerate(deviations):
            pair = first_pair + offset
            reason = "foreign load" if pair in rejected else None
            for position, (arm, value) in enumerate(
                (("baseline", 1000.0), ("candidate", 1000.0 * math.exp(deviation + shift))), start=1
            ):
                row = {
                    "benchmark": benchmark,
                    "params": {},
                    "metric": "ns_per_op",
                    "unit": "ns/op",
                    "arm": arm,
                    "pair": pair,
                    "position": position,
                    "process": pair,
                    "value": value,
                    "accepted": reason is None,
                }
                if reason:
                    row["reject_reason"] = reason
                bench_ledger.record(out, [row])

    def test_opening_a_run_writes_the_capture_and_the_run_row(self) -> None:
        out = self.open_run(min_pairs=12, benchmarks=["scan"], data=self.base)
        run = json.loads((out / "run.json").read_text(encoding="utf-8"))
        self.assertEqual(run["environment"], json.loads((out / "environment.json").read_text(encoding="utf-8")))
        self.assertRegex(run["run"], "^20261123T183005Z-[0-9a-f]{8}$")
        self.assertEqual(
            ("gate", "jmh", "laptop-1", self.fingerprint, "2026-11-23T18:30:05Z"),
            (run["tier"], run["kit"], run["conditions"], run["fingerprint"], run["started_at"]),
        )
        self.assertEqual(
            {
                "floor": math.log(1.04),
                "stop_precision": math.log(1.03),
                "quantile": 0.99,
                "min_pairs": 12,
                "max_pairs": 20,
                "benchmarks": ["scan"],
            },
            run["plan"],
        )
        self.assertEqual(
            {
                "baseline": {
                    "commit": COMMIT_A,
                    "dirty": False,
                    "binary": "jvm",
                    "runtime": 'openjdk version "25.0.5" 2026-10-20 LTS',
                },
                "candidate": {
                    "commit": COMMIT_B,
                    "dirty": True,
                    "binary": "jvm",
                    "runtime": 'openjdk version "25.0.5" 2026-10-20 LTS',
                },
            },
            run["arms"],
        )
        self.assertFalse(self.ledger.exists(), "opening a run appends nothing")
        with self.assertRaisesRegex(LedgerError, "already holds a run; every run needs a fresh directory"):
            bench_ledger.begin(
                out,
                tier="gate",
                kit="jmh",
                arm_specs=[],
                ledger=self.ledger,
                conditions=self.conditions,
                root=self.root,
                runner=self.runner,
            )

    def test_a_single_arm_run_states_its_runtime_or_says_it_is_unavailable(self) -> None:
        nightly = json.loads((self.open_run("nightly", tier="nightly", night="2026-11-23") / "run.json").read_text())
        self.assertEqual("GraalVM 25.0.3, built 2026-11-20", nightly["arms"]["head"]["runtime"])
        self.assertEqual(({"min_processes": 20, "benchmarks": None}, "2026-11-23"), (nightly["plan"], nightly["night"]))
        control = json.loads(
            (
                self.open_run("control", tier="control", arms=[f"head,commit={COMMIT_B},binary=jvm"]) / "run.json"
            ).read_text()
        )
        self.assertEqual(
            "unavailable: the kit does not know which runtime this arm runs on", control["arms"]["head"]["runtime"]
        )
        self.assertEqual(
            {"unavailable": "the kit does not know which runtime this arm runs on"},
            control["environment"]["runtimes"]["head"],
        )
        self.assertEqual({"min_processes": 1, "benchmarks": None}, control["plan"])

    def test_a_run_is_refused_before_it_starts_when_no_entry_holds_its_fingerprint(self) -> None:
        first = self.open_run("first")
        self.record_pairs(first, QUIET)
        bench_ledger.judge(first, self.ledger)
        self.root = make_root(
            self.base / "changed",
            **{
                "proc/sys/kernel/osrelease": "6.8.0-140-generic\n",
                "sys/devices/system/cpu/cpufreq/policy0/scaling_governor": "performance\n",
            },
        )
        with self.assertRaisesRegex(
            LedgerError, "no entry of .*CONDITIONS.md holds this environment's fingerprint [0-9a-f]{64}"
        ) as caught:
            self.open_run("second")
        message = str(caught.exception)
        self.assertIn('kernel_release: "6.8.0-138-generic" -> "6.8.0-140-generic"', message)
        self.assertIn('governors: ["powersave"] -> ["performance", "powersave"]', message)
        self.assertNotIn("cpu_model", message)
        self.assertFalse((self.base / "second" / "run.json").exists())
        self.assertTrue(
            (self.base / "second" / "environment.json").exists(), "the capture is kept for writing the entry"
        )

    def test_the_refusal_says_so_when_the_ledger_has_no_earlier_run(self) -> None:
        self.conditions.write_text(condition_entry("2026-11-23", "other", "new baseline", "5" * 64), encoding="utf-8")
        with self.assertRaisesRegex(LedgerError, "the ledger holds no earlier run to compare with"):
            self.open_run()

    def test_arms_and_plan_are_validated_before_anything_is_measured(self) -> None:
        good = f"baseline,commit={COMMIT_A},binary=jvm,runtime=jdk"
        cases = {
            "a gate run compares baseline and candidate": dict(arms=[good.replace("baseline", "head")]),
            "the arms are baseline and candidate, or head$": dict(arms=[good]),
            "a nightly run measures the single arm head": dict(
                tier="nightly", night="2026-11-23", arms=[good, good.replace("baseline", "candidate")]
            ),
            "'night' belongs to a nightly run": dict(tier="nightly"),
            "'commit' must be 40 lowercase": dict(arms=[good, "candidate,commit=HEAD,binary=jvm,runtime=jdk"]),
            "'binary' must be one of": dict(arms=[good, f"candidate,commit={COMMIT_B},runtime=jdk"]),
            "the arms are baseline and candidate, or head": dict(arms=[good, f"new,commit={COMMIT_B},binary=jvm"]),
            "an arm is given twice": dict(arms=[good, good]),
            "give java=PATH or runtime=TEXT, not both": dict(arms=[good + ",java=/x", good]),
            "dirty must be true or false": dict(arms=[good + ",dirty=maybe", good]),
            "'commit' is given twice": dict(arms=[good + f",commit={COMMIT_A}", good]),
            "minimum number of pairs must be 10 to 20, got 9": dict(min_pairs=9),
            "minimum number of pairs must be 10 to 20, got 21": dict(min_pairs=21),
        }
        for index, (message, options) in enumerate(cases.items()):
            with self.subTest(message=message):
                with self.assertRaisesRegex(LedgerError, message):
                    self.open_run(f"bad-{index}", **options)
        with self.assertRaisesRegex(LedgerError, "run directory does not exist"):
            bench_ledger.begin(
                self.base / "absent",
                tier="gate",
                kit="jmh",
                arm_specs=[],
                ledger=self.ledger,
                conditions=self.conditions,
            )

    def test_without_a_named_ledger_no_run_is_opened(self) -> None:
        out = self.base / "run"
        out.mkdir()
        with mock.patch.dict(os.environ, clear=False) as environment:
            environment.pop(bench_ledger.LEDGER_ENV, None)
            with self.assertRaisesRegex(LedgerError, "SIRIX_BENCH_LEDGER is not set"):
                bench_ledger.begin(out, tier="gate", kit="jmh", arm_specs=[], conditions=self.conditions)

    def test_judging_records_run_measurements_and_verdicts_in_that_order(self) -> None:
        out = self.open_run()
        self.record_pairs(out, QUIET, rejected=(3,))
        self.record_pairs(out, QUIET[:1], first_pair=11)
        self.assertEqual(
            (True, [10]), (bench_ledger.stop(out)[0], [state.pairs for state in bench_ledger.stop(out)[1]])
        )
        judgement = bench_ledger.judge(out, self.ledger)
        self.assertEqual(["unchanged"], [row["verdict"] for row in judgement.verdicts])
        rows = bench_ledger.read_ledger(self.ledger)
        self.assertEqual(["run"] + ["measurement"] * 22 + ["verdict"], [row["kind"] for row in rows])
        self.assertEqual(1, len({row["run"] for row in rows}))
        rejected = [row for row in rows if row["kind"] == "measurement" and not row["accepted"]]
        self.assertEqual(
            [(3, "foreign load"), (3, "foreign load")], [(row["pair"], row["reject_reason"]) for row in rejected]
        )
        verdict = rows[-1]
        self.assertEqual(
            ("scan", {}, "ns_per_op", "ns/op", 10, "unchanged", None),
            (
                verdict["benchmark"],
                verdict["params"],
                verdict["metric"],
                verdict["unit"],
                verdict["pairs"],
                verdict["verdict"],
                verdict["allocation"],
            ),
        )
        self.assertAlmostEqual(judgement.verdicts[0]["threshold"], verdict["threshold"], places=15)
        self.assertEqual(rows[-1:], bench_ledger.read_ledger(out / "verdicts.jsonl"))

    def test_a_run_is_judged_once_and_closed_to_new_measurements(self) -> None:
        out = self.open_run()
        self.record_pairs(out, QUIET)
        bench_ledger.judge(out, self.ledger)
        before = self.ledger.read_bytes()
        with self.assertRaisesRegex(LedgerError, "has been judged already; verdicts are computed once"):
            bench_ledger.judge(out, self.ledger)
        with self.assertRaisesRegex(LedgerError, "no measurement may be added after a verdict"):
            self.record_pairs(out, QUIET[:1], first_pair=11)
        self.assertEqual({"appended": 0, "measurements": 20, "verdicts": 1}, bench_ledger.commit(out, self.ledger))
        self.assertEqual(before, self.ledger.read_bytes())
        self.assertEqual(["unchanged"], [row["verdict"] for row in bench_ledger.recorded_judgement(out).verdicts])

    def test_a_run_that_cannot_be_judged_leaves_no_verdict_behind(self) -> None:
        out = self.open_run()
        self.record_pairs(out, QUIET[:6])
        with self.assertRaisesRegex(timed_gate.GateError, "a verdict needs 10 to 20 accepted pairs"):
            bench_ledger.judge(out, self.ledger)
        self.assertFalse((out / "verdicts.jsonl").exists())
        self.assertFalse(self.ledger.exists())
        self.assertEqual(
            {"appended": 13, "measurements": 12, "verdicts": 0},
            bench_ledger.commit(out, self.ledger),
            "an aborted run still records the processes it measured",
        )
        with self.assertRaisesRegex(LedgerError, "has not been judged"):
            bench_ledger.recorded_judgement(out)

    def test_a_gate_run_obeys_the_stop_rule_and_a_control_run_does_not_have_to(self) -> None:
        gate = self.open_run("gate")
        self.record_pairs(gate, QUIET + QUIET[:2])
        with self.assertRaisesRegex(timed_gate.GateError, "continued past its stop point"):
            bench_ledger.judge(gate, self.ledger)
        control = self.open_run("control", tier="control")
        self.record_pairs(control, QUIET + QUIET[:2])
        self.assertEqual([12], [row["pairs"] for row in bench_ledger.judge(control, self.ledger).verdicts])

    def test_the_plan_of_the_run_row_governs_stop_and_judge(self) -> None:
        out = self.open_run(min_pairs=12, benchmarks=["scan"])
        self.record_pairs(out, QUIET)
        self.record_pairs(out, QUIET, math.log(1.06), benchmark="probe")
        self.assertFalse(bench_ledger.stop(out)[0], "the plan asks for 12 pairs")
        self.record_pairs(out, QUIET[:2], first_pair=11)
        self.assertTrue(bench_ledger.stop(out)[0])
        judgement = bench_ledger.judge(out, self.ledger)
        self.assertEqual(
            (["scan"], ["probe"], False),
            ([row["benchmark"] for row in judgement.verdicts], judgement.outside_plan, judgement.failed),
        )

    def test_new_runs_use_the_latest_condition_for_an_unchanged_fingerprint(self) -> None:
        old = self.open_run("old")
        with self.conditions.open("a") as sink:
            sink.write(condition_entry("2026-12-01", "power-cap", "new baseline", self.fingerprint))
            sink.write(condition_entry("2026-12-02", "power-cap", "continues", self.fingerprint))
        new = self.open_run("new")
        self.assertEqual("laptop-1", bench_ledger.load_run(old)["conditions"])
        self.assertEqual("power-cap", bench_ledger.load_run(new)["conditions"])
        self.assertEqual("power-cap", bench_ledger.conditions_id(self.conditions, self.fingerprint))

    def test_commit_retry_reconciles_a_durable_append_without_a_marker(self) -> None:
        out = self.open_run(tier="control")
        self.record_pairs(out, QUIET)
        with mock.patch.object(bench_ledger.os, "replace", side_effect=OSError("marker unavailable")):
            with self.assertRaisesRegex(OSError, "marker unavailable"):
                bench_ledger.commit(out, self.ledger)
        before = self.ledger.read_bytes()
        self.assertEqual(0, bench_ledger.commit(out, self.ledger)["appended"])
        self.assertEqual(before, self.ledger.read_bytes())
        bench_ledger.judge(out, self.ledger)
        (out / bench_ledger.RECORDED_FILE).unlink()
        before = self.ledger.read_bytes()
        self.assertEqual(0, bench_ledger.commit(out, self.ledger)["appended"])
        self.assertEqual(before, self.ledger.read_bytes())
        self.assertEqual(
            ["run"] + ["measurement"] * 20 + ["verdict"],
            [row["kind"] for row in bench_ledger.read_ledger(self.ledger)],
        )

    def test_commit_recovers_a_complete_line_prefix_of_an_append(self) -> None:
        out = self.open_run(tier="control")
        self.record_pairs(out, QUIET)
        rows = [bench_ledger.load_run(out)] + bench_ledger.load_measurements(out)
        bench_ledger.append_rows(self.ledger, rows[:8])
        self.assertEqual(13, bench_ledger.commit(out, self.ledger)["appended"])
        self.assertEqual(rows, bench_ledger.read_ledger(self.ledger))

    def test_rejection_refuses_durable_measurements_without_a_marker(self) -> None:
        out = self.open_run(tier="control")
        self.record_pairs(out, QUIET)
        with mock.patch.object(bench_ledger.os, "replace", side_effect=OSError("marker unavailable")):
            with self.assertRaisesRegex(OSError, "marker unavailable"):
                bench_ledger.commit(out, self.ledger)
        ledger_before = self.ledger.read_bytes()
        measurements = out / bench_ledger.MEASUREMENTS_FILE
        before = measurements.read_bytes()
        self.assertFalse((out / bench_ledger.RECORDED_FILE).exists())
        with self.assertRaisesRegex(LedgerError, "already judged or recorded"):
            bench_ledger.reject_run(out, "the differential failed", self.ledger)
        self.assertEqual(before, measurements.read_bytes())
        self.assertEqual(ledger_before, self.ledger.read_bytes())
        self.assertEqual(0, bench_ledger.commit(out, self.ledger)["appended"])

    def test_a_durable_run_row_alone_does_not_prevent_rejection(self) -> None:
        out = self.open_run(tier="control")
        self.record_pairs(out, QUIET[:1])
        bench_ledger.append_rows(self.ledger, [bench_ledger.load_run(out)])
        self.assertEqual(2, bench_ledger.reject_run(out, "wrong answers", self.ledger))
        self.assertEqual(2, bench_ledger.commit(out, self.ledger)["appended"])
        self.assertEqual([False, False], [row["accepted"] for row in bench_ledger.read_ledger(self.ledger)[1:]])

    def test_commit_refuses_conflicting_durable_rows_with_or_without_a_marker(self) -> None:
        for marker in (True, False):
            for kind, filename, changed in (
                ("run", bench_ledger.RUN_FILE, {"kit": "another-kit"}),
                ("measurement", bench_ledger.MEASUREMENTS_FILE, {"accepted": False, "reject_reason": "load"}),
                ("verdict", bench_ledger.VERDICTS_FILE, {"difference": 0.5}),
            ):
                with self.subTest(kind=kind, marker=marker):
                    out = self.open_run(f"{kind}-{marker}", tier="control")
                    self.record_pairs(out, QUIET)
                    bench_ledger.judge(out, self.ledger)
                    path = out / filename
                    if kind == "run":
                        rows = [bench_ledger.load_run(out)]
                    else:
                        rows = bench_ledger.read_ledger(path)
                    rows[0].update(changed)
                    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
                    if not marker:
                        (out / bench_ledger.RECORDED_FILE).unlink()
                    before = self.ledger.read_bytes()
                    with self.assertRaisesRegex(LedgerError, f"conflicting {kind} row"):
                        bench_ledger.commit(out, self.ledger)
                    self.assertEqual(before, self.ledger.read_bytes())

    def test_commit_appends_only_what_the_ledger_does_not_hold_yet(self) -> None:
        out = self.open_run("nightly", tier="nightly", night="2026-11-23")
        rows = [
            {
                "benchmark": "q1",
                "params": {},
                "metric": "cold_seconds",
                "unit": "s",
                "arm": "head",
                "pair": None,
                "position": None,
                "process": process,
                "value": 0.5,
                "accepted": True,
            }
            for process in (1, 2, 3)
        ]
        bench_ledger.record(out, rows[:2])
        self.assertEqual({"appended": 3, "measurements": 2, "verdicts": 0}, bench_ledger.commit(out, self.ledger))
        bench_ledger.record(out, rows[2:])
        self.assertEqual({"appended": 1, "measurements": 3, "verdicts": 0}, bench_ledger.commit(out, self.ledger))
        self.assertEqual(
            ["run", "measurement", "measurement", "measurement"],
            [row["kind"] for row in bench_ledger.read_ledger(self.ledger)],
        )
        with self.assertRaisesRegex(
            LedgerError, "is already recorded in .*ledger.jsonl; one run belongs to one ledger"
        ):
            bench_ledger.commit(out, self.base / "other.jsonl")
        (out / "measurements.jsonl").write_text("", encoding="utf-8")
        with self.assertRaisesRegex(LedgerError, "rows that were recorded in the ledger are gone"):
            bench_ledger.commit(out, self.ledger)
        with self.assertRaisesRegex(LedgerError, "only a paired run of baseline and candidate is judged"):
            bench_ledger.judge(out, self.ledger)
        with self.assertRaisesRegex(LedgerError, "the stop rule belongs to a paired run"):
            bench_ledger.stop(out)

    def test_a_rejected_run_keeps_its_rows_and_marks_every_one_rejected(self) -> None:
        out = self.open_run()
        self.record_pairs(out, QUIET[:3], rejected=(2,))
        self.assertEqual(6, bench_ledger.reject_run(out, "the differential failed", self.ledger))
        rows = bench_ledger.load_measurements(out)
        self.assertEqual([False] * 6, [row["accepted"] for row in rows])
        self.assertEqual(
            ["the differential failed", "the differential failed", "foreign load", "foreign load"],
            [row["reject_reason"] for row in rows[:4]],
            "a pair that was already rejected keeps its own reason",
        )
        self.assertEqual({"appended": 7, "measurements": 6, "verdicts": 0}, bench_ledger.commit(out, self.ledger))
        self.assertEqual([False] * 6, [row["accepted"] for row in bench_ledger.read_ledger(self.ledger)[1:]])
        self.assertEqual([], [path.name for path in out.iterdir() if path.name.endswith(".tmp")])

    def test_a_rejection_needs_a_reason_and_comes_before_the_ledger(self) -> None:
        out = self.open_run()
        self.record_pairs(out, QUIET)
        for reason in ("", "   "):
            with self.assertRaisesRegex(LedgerError, "a rejection needs its reason"):
                bench_ledger.reject_run(out, reason)
        self.assertEqual({True}, {row["accepted"] for row in bench_ledger.load_measurements(out)})
        recorded = self.open_run("recorded")
        self.record_pairs(recorded, QUIET[:2])
        bench_ledger.commit(recorded, self.ledger)
        with self.assertRaisesRegex(LedgerError, "already judged or recorded; rows in the ledger are never rewritten"):
            bench_ledger.reject_run(recorded, "too late")
        with mock.patch.object(bench_ledger, "commit"):
            bench_ledger.judge(out, self.ledger)  # judged, and by the patch not recorded
        with self.assertRaisesRegex(LedgerError, "already judged or recorded"):
            bench_ledger.reject_run(out, "too late")
        self.assertEqual({True}, {row["accepted"] for row in bench_ledger.load_measurements(out)})
        with self.assertRaisesRegex(LedgerError, "holds no opened run"):
            bench_ledger.reject_run(self.base, "no run here")

    def test_the_minimum_of_a_plan_may_be_the_pair_limit(self) -> None:
        run = json.loads((self.open_run(min_pairs=20) / "run.json").read_text(encoding="utf-8"))
        self.assertEqual((20, 20), (run["plan"]["min_pairs"], run["plan"]["max_pairs"]))

    def test_the_ledger_is_named_by_one_path_however_it_is_spelled(self) -> None:
        out = self.open_run()
        self.record_pairs(out, QUIET[:1])
        bench_ledger.commit(out, self.ledger)
        spelled = self.ledger.parent / "." / self.ledger.name
        self.assertEqual({"appended": 0, "measurements": 2, "verdicts": 0}, bench_ledger.commit(out, spelled))

    def test_measurements_are_checked_as_they_are_recorded(self) -> None:
        out = self.open_run()
        row = {
            "benchmark": "scan",
            "params": {},
            "metric": "ns_per_op",
            "unit": "ns/op",
            "arm": "baseline",
            "pair": 1,
            "position": 1,
            "process": 1,
            "value": 24.5,
            "accepted": True,
        }
        self.assertEqual(1, bench_ledger.record(out, [row]))
        with self.assertRaisesRegex(LedgerError, "arm baseline, pair 1, process 1 is already recorded"):
            bench_ledger.record(out, [row])
        with self.assertRaisesRegex(LedgerError, "measurement of arm 'head', which run .* does not have"):
            bench_ledger.record(out, [dict(row, arm="head", pair=None, position=None)])
        with self.assertRaisesRegex(LedgerError, "'value' must be finite"):
            bench_ledger.record(out, [dict(row, pair=2, value=0.0)])
        self.assertEqual(1, len(bench_ledger.load_measurements(out)))
        with self.assertRaisesRegex(LedgerError, "holds no opened run"):
            bench_ledger.record(self.base, [row])

    def test_verdicts_are_reported_only_once_the_ledger_holds_them(self) -> None:
        out = self.open_run()
        self.record_pairs(out, QUIET)
        with mock.patch.object(bench_ledger, "append_rows", side_effect=LedgerError("disk full")):
            with self.assertRaisesRegex(LedgerError, "disk full"):
                bench_ledger.judge(out, self.ledger)
        with self.assertRaisesRegex(LedgerError, "the verdicts are not in the ledger yet"):
            bench_ledger.recorded_judgement(out)
        self.assertEqual({"appended": 22, "measurements": 20, "verdicts": 1}, bench_ledger.commit(out, self.ledger))
        self.assertEqual(1, len(bench_ledger.recorded_judgement(out).verdicts))


class ImporterTest(unittest.TestCase):

    def test_a_timed_jmh_process_becomes_one_row_per_benchmark(self) -> None:
        rows = bench_ledger.jmh_rows(
            [jmh_result("warmLookup", 24.5), jmh_result("coldCopy", 1100.0, params={})],
            arm="candidate",
            pair=4,
            position=2,
            process=4,
        )
        self.assertEqual(
            [
                {
                    "arm": "candidate",
                    "pair": 4,
                    "position": 2,
                    "process": 4,
                    "accepted": True,
                    "benchmark": "LeafBenchmark.warmLookup",
                    "params": {"keyBytes": "8"},
                    "metric": "ns_per_op",
                    "unit": "ns/op",
                    "value": 24.5,
                    "samples": {"count": 3, "min": 24.5 * 0.99, "max": 24.5 * 1.01},
                },
                {
                    "arm": "candidate",
                    "pair": 4,
                    "position": 2,
                    "process": 4,
                    "accepted": True,
                    "benchmark": "LeafBenchmark.coldCopy",
                    "params": {},
                    "metric": "ns_per_op",
                    "unit": "ns/op",
                    "value": 1100.0,
                    "samples": {"count": 3, "min": 1089.0, "max": 1111.0},
                },
            ],
            rows,
        )

    def test_an_allocation_leg_contributes_no_timed_measurement(self) -> None:
        rows = bench_ledger.jmh_rows(
            [jmh_result("warmLookup", 24.5, alloc=167.99)],
            arm="baseline",
            pair=None,
            position=None,
            process=1,
            allocation_leg=True,
            reject_reason="identity check failed",
        )
        self.assertEqual(
            [
                {
                    "arm": "baseline",
                    "pair": None,
                    "position": None,
                    "process": 1,
                    "accepted": False,
                    "reject_reason": "identity check failed",
                    "benchmark": "LeafBenchmark.warmLookup",
                    "params": {"keyBytes": "8"},
                    "metric": "alloc_bytes_per_op",
                    "unit": "B/op",
                    "value": 167.99,
                }
            ],
            rows,
        )

    def test_jmh_results_that_break_the_protocol_are_refused(self) -> None:
        cases = {
            "a timed process must not carry the allocation profiler": ([jmh_result("m", 1.0, alloc=8.0)], {}),
            "an allocation leg needs the GC profiler": ([jmh_result("m", 1.0)], {"allocation_leg": True}),
            "mode 'thrpt' is not a time per operation": ([jmh_result("m", 1.0, mode="thrpt", unit="ops/s")], {}),
            "needs forks = 1, got 3": ([jmh_result("m", 1.0, forks=3)], {}),
            "not a JMH JSON result list": ({"result": []}, {}),
        }
        for message, (document, options) in cases.items():
            with self.subTest(message=message):
                with self.assertRaisesRegex(LedgerError, message):
                    bench_ledger.jmh_rows(
                        document, arm="baseline", pair=1, position=1, process=1, source="r.json", **options
                    )

    def test_a_clickbench_process_gives_cold_hot_and_suite_figures(self) -> None:
        document = {"result": [[0.039, 0.004, 0.002], [0.393, 0.04, 0.031]]}
        rows, skipped = bench_ledger.clickbench_rows(
            document, arm="baseline", pair=2, position=1, process=2, counters={"groupDense": 3}
        )
        self.assertEqual([], skipped)
        self.assertEqual(
            [
                ("q00", "cold_seconds", 0.039),
                ("q00", "hot_seconds", 0.002),
                ("q01", "cold_seconds", 0.393),
                ("q01", "hot_seconds", 0.031),
                ("suite", "cold_seconds", 0.432),
                ("suite", "hot_seconds", 0.033),
            ],
            [(row["benchmark"], row["metric"], round(row["value"], 6)) for row in rows],
        )
        self.assertEqual({"count": 2, "min": 0.002, "max": 0.004}, rows[1]["samples"])
        self.assertNotIn("samples", rows[0])
        self.assertEqual(
            {("baseline", 2, 1, 2, True, "s")},
            {(row["arm"], row["pair"], row["position"], row["process"], row["accepted"], row["unit"]) for row in rows},
        )
        self.assertEqual([{"groupDense": 3}] * 6, [row["counters"] for row in rows])

    def test_a_failed_hot_try_withholds_query_and_suite_hot_figures(self) -> None:
        for later in ([0.004, None], [None, 0.004], [None, None], [0.004, None, 0.002]):
            with self.subTest(later=later):
                rows, skipped = bench_ledger.clickbench_rows(
                    {"result": [[0.039, *later], [0.393, 0.04, 0.031]]},
                    arm="head",
                    pair=None,
                    position=None,
                    process=1,
                )
                self.assertEqual(
                    [
                        ("q00", "cold_seconds"),
                        ("q01", "cold_seconds"),
                        ("q01", "hot_seconds"),
                        ("suite", "cold_seconds"),
                    ],
                    [(row["benchmark"], row["metric"]) for row in rows],
                )
                self.assertAlmostEqual(0.432, rows[-1]["value"])
                self.assertEqual(["q00 hot_seconds contains a failed try"], skipped)

    def test_a_clickbench_subset_or_an_unresolved_timing_has_no_suite_figure(self) -> None:
        subset, skipped = bench_ledger.clickbench_rows(
            {"result": [[0.039, 0.004], [None, None]]}, arm="head", pair=None, position=None, process=1
        )
        self.assertEqual((["q00", "q00"], []), ([row["benchmark"] for row in subset], skipped))
        zero, skipped = bench_ledger.clickbench_rows(
            {"results": [[0.039, 0.0], [0.0, 0.001]]}, arm="head", pair=None, position=None, process=1
        )
        self.assertEqual(
            [("q00", "cold_seconds"), ("q01", "hot_seconds")], [(row["benchmark"], row["metric"]) for row in zero]
        )
        self.assertEqual(
            ["q00 hot_seconds is below the timer's resolution", "q01 cold_seconds is below the timer's resolution"],
            skipped,
        )
        hot_complete, skipped = bench_ledger.clickbench_rows(
            {"result": [[0.0, 0.004], [0.2, 0.1]]}, arm="head", pair=None, position=None, process=1
        )
        self.assertEqual(
            [("q00", "hot_seconds"), ("q01", "cold_seconds"), ("q01", "hot_seconds"), ("suite", "hot_seconds")],
            [(row["benchmark"], row["metric"]) for row in hot_complete],
            "each suite figure needs its own queries",
        )
        self.assertEqual(["q00 cold_seconds is below the timer's resolution"], skipped)
        cold_only, _ = bench_ledger.clickbench_rows(
            {"result": [[0.039], [0.012]]}, arm="head", pair=None, position=None, process=1
        )
        self.assertEqual(
            [("q00", "cold_seconds"), ("q01", "cold_seconds"), ("suite", "cold_seconds")],
            [(row["benchmark"], row["metric"]) for row in cold_only],
        )
        for bad in ({"result": []}, {"other": 1}, [1], {"result": [[-0.1]]}, {"result": [["fast"]]}):
            with self.subTest(document=bad):
                with self.assertRaises(LedgerError):
                    bench_ledger.clickbench_rows(bad, arm="head", pair=None, position=None, process=1)

    def test_an_isolated_jsonbench_process_is_cold_on_its_first_attempt_and_hot_after(self) -> None:
        document = {"result": [[None], [0.812], [None]]}
        cold, _ = bench_ledger.jsonbench_rows(document, query=2, attempt=1, arm="head", process=1)
        hot, _ = bench_ledger.jsonbench_rows(
            document, query=2, attempt=3, arm="head", process=3, counters={"groupDense": 0}
        )
        self.assertEqual(
            [
                {
                    "arm": "head",
                    "pair": None,
                    "position": None,
                    "process": 1,
                    "accepted": True,
                    "benchmark": "q2",
                    "params": {},
                    "metric": "cold_seconds",
                    "unit": "s",
                    "value": 0.812,
                }
            ],
            cold,
        )
        self.assertEqual(("hot_seconds", 3), (hot[0]["metric"], hot[0]["process"]))
        self.assertEqual({"groupDense": 0}, hot[0]["counters"])
        self.assertNotIn("counters", cold[0])
        self.assertEqual(
            ([], ["q2 cold_seconds is below the timer's resolution"]),
            bench_ledger.jsonbench_rows({"result": [[None], [0.0], [None]]}, query=2, attempt=1, arm="head", process=1),
        )
        for options in (dict(query=1), dict(query=4), dict(query=0), dict(query=2, attempt=0)):
            with self.subTest(options=options):
                with self.assertRaises(LedgerError):
                    bench_ledger.jsonbench_rows(document, arm="head", process=1, **{"attempt": 1, **options})
        with self.assertRaisesRegex(LedgerError, "exactly one timing of Q2"):
            bench_ledger.jsonbench_rows(
                {"result": [[None], [0.8, 0.7], [None]]}, query=2, attempt=1, arm="head", process=1
            )

    def test_route_counters_are_read_from_the_served_lines_of_a_log(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "round.log"
            log.write_text(
                "starting\n# served: predicateCounts=2 groupAggregates=0 groupDense=5\nrows=7 ignored=1\n"
                "# served: predicateCounts=1 groupDense=1\n",
                encoding="utf-8",
            )
            self.assertEqual(
                {"predicateCounts": 3, "groupAggregates": 0, "groupDense": 6}, bench_ledger.served_counters(log)
            )
            log.write_text("no counters here\n", encoding="utf-8")
            self.assertIsNone(bench_ledger.served_counters(log))
        self.assertIsNone(bench_ledger.served_counters(None))


class CommandLineCase(unittest.TestCase):
    """A ledger and a conditions record of its own for every command-line test."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)
        self.ledger = self.base / "ledger.jsonl"
        self.conditions = self.base / "CONDITIONS.md"
        patch = mock.patch.dict(
            os.environ, {bench_ledger.LEDGER_ENV: str(self.ledger), bench_ledger.CONDITIONS_ENV: str(self.conditions)}
        )
        patch.start()
        self.addCleanup(patch.stop)

    def cli(self, *arguments: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = bench_ledger.main(list(arguments))
        return status, out.getvalue(), err.getvalue()

    def register_this_machine(self) -> None:
        status, out, _ = self.cli("fingerprint")
        self.assertEqual(0, status)
        value = out.splitlines()[0]
        self.assertRegex(value, "^[0-9a-f]{64}$")
        self.assertEqual(
            bench_ledger.fingerprint_fields(bench_ledger.capture_environment()), json.loads(out.split("\n", 1)[1])
        )
        self.conditions.write_text(
            condition_entry("2026-11-23", "this-machine", "new baseline", value), encoding="utf-8"
        )


class CommandLineTest(CommandLineCase):

    def test_kit_import_commands_keep_repeatable_dataset_parameters(self) -> None:
        self.register_this_machine()
        out = self.base / "run"
        out.mkdir()
        status, _, err = self.cli(
            "begin", "--out", str(out), "--tier", "control", "--kit", "kits",
            "--arm", f"head,commit={COMMIT_B},binary=native",
        )
        self.assertEqual(0, status, err)
        document = self.base / "timing.json"
        document.write_text(json.dumps({"result": [[0.1]]}))
        for tier in ("1m", "100m"):
            for attempt in (1, 2):
                status, _, err = self.cli(
                    "import-jsonbench", "--run", str(out), "--json", str(document),
                    "--query", "1", "--attempt", str(attempt), "--process", str(attempt),
                    "--param", f"tier={tier}", "--param", "content=events",
                )
                self.assertEqual(0, status, err)
        document.write_text(json.dumps({"result": [[0.1, 0.01]]}))
        status, _, err = self.cli(
            "import-clickbench", "--run", str(out), "--json", str(document),
            "--arm", "head", "--process", "1", "--param", "dataset=hits-100m", "--param", "content=hits",
        )
        self.assertEqual(0, status, err)
        rows = bench_ledger.load_measurements(out)
        self.assertEqual(
            [{"tier": "1m", "content": "events"}] * 2 + [{"tier": "100m", "content": "events"}] * 2,
            [row["params"] for row in rows[:4]],
        )
        self.assertEqual([{"dataset": "hits-100m", "content": "hits"}] * 4, [row["params"] for row in rows[4:]])

    def test_a_gate_run_from_begin_to_report(self) -> None:
        self.register_this_machine()
        out = self.base / "run"
        out.mkdir()
        arms = [
            f"baseline,commit={COMMIT_A},binary=jvm,runtime=jdk 25",
            f"candidate,commit={COMMIT_B},binary=jvm,runtime=jdk 25",
        ]
        status, text, err = self.cli(
            "begin",
            "--out",
            str(out),
            "--tier",
            "gate",
            "--kit",
            "jmh",
            "--arm",
            arms[0],
            "--arm",
            arms[1],
            "--benchmark",
            "LeafBenchmark.warmLookup",
        )
        self.assertEqual((0, ""), (status, err))
        self.assertRegex(text, "^run 2\\d{7}T\\d{6}Z-[0-9a-f]{8} opened under condition this-machine\n$")
        for pair, deviation in enumerate(QUIET, start=1):
            for position, (arm, score) in enumerate(
                (("baseline", 24.0), ("candidate", 24.0 * math.exp(deviation + math.log(1.06)))), 1
            ):
                document = self.base / f"{pair}-{arm}.json"
                document.write_text(json.dumps([jmh_result("warmLookup", score)]), encoding="utf-8")
                status, text, err = self.cli(
                    "import-jmh",
                    "--run",
                    str(out),
                    "--json",
                    str(document),
                    "--arm",
                    arm,
                    "--pair",
                    str(pair),
                    "--position",
                    str(position),
                    "--process",
                    str(pair),
                )
                self.assertEqual((0, "recorded 1 measurement(s)\n", ""), (status, text, err))
        for arm, alloc in (("baseline", 168.0), ("candidate", 168.2)):
            document = self.base / f"alloc-{arm}.json"
            document.write_text(json.dumps([jmh_result("warmLookup", 30.0, alloc=alloc)]), encoding="utf-8")
            self.assertEqual(
                0,
                self.cli(
                    "import-jmh",
                    "--run",
                    str(out),
                    "--json",
                    str(document),
                    "--arm",
                    arm,
                    "--process",
                    "1",
                    "--allocation-leg",
                )[0],
            )
        status, text, _ = self.cli("stop", "--run", str(out))
        self.assertEqual((0, "stop"), (status, text.splitlines()[-1]))
        status, text, err = self.cli("judge", "--run", str(out))
        self.assertEqual((1, ""), (status, err))
        self.assertIn("regression", text)
        self.assertIn("168 -> 168 B/op delta +0 pass", text)
        self.assertIn("outcome: FAIL - 1 regression(s), 0 failed allocation check(s)", text)
        self.assertEqual(
            ["run"] + ["measurement"] * 22 + ["verdict"], [row["kind"] for row in bench_ledger.read_ledger(self.ledger)]
        )
        status, again, _ = self.cli("report", "--run", str(out))
        self.assertEqual((1, text.split("measured outside")[0]), (status, again))
        status, _, err = self.cli("judge", "--run", str(out))
        self.assertEqual(2, status)
        self.assertIn("verdicts are computed once", err)

    def test_a_single_arm_run_is_committed_without_a_verdict(self) -> None:
        self.register_this_machine()
        out = self.base / "run"
        out.mkdir()
        self.assertEqual(
            0,
            self.cli(
                "begin",
                "--out",
                str(out),
                "--tier",
                "control",
                "--kit",
                "jsonbench",
                "--data",
                str(out),
                "--arm",
                f"head,commit={COMMIT_B},binary=native",
            )[0],
        )
        part = self.base / "q2.json"
        part.write_text(json.dumps({"result": [[None], [0.812]]}), encoding="utf-8")
        log = self.base / "q2.log"
        log.write_text("# served: groupDense=4\n", encoding="utf-8")
        self.assertEqual(
            0,
            self.cli(
                "import-jsonbench",
                "--run",
                str(out),
                "--json",
                str(part),
                "--query",
                "2",
                "--attempt",
                "1",
                "--process",
                "1",
                "--log",
                str(log),
            )[0],
        )
        round_json = self.base / "round.json"
        round_json.write_text(json.dumps({"result": [[0.0, 0.004], [0.2, 0.1]]}), encoding="utf-8")
        status, _, err = self.cli(
            "import-clickbench",
            "--run",
            str(out),
            "--json",
            str(round_json),
            "--arm",
            "head",
            "--process",
            "1",
            "--rejected",
            "foreign load",
        )
        self.assertEqual(0, status)
        self.assertIn("not recorded: q00 cold_seconds is below the timer's resolution", err)
        self.assertEqual(
            0,
            self.cli(
                "record",
                "--run",
                str(out),
                "--benchmark",
                "load",
                "--param",
                "tier=1m",
                "--metric",
                "load_seconds",
                "--unit",
                "s",
                "--arm",
                "head",
                "--process",
                "1",
                "--value",
                "12.5",
            )[0],
        )
        status, text, _ = self.cli("commit", "--run", str(out))
        self.assertEqual(
            (0, "ledger: appended 7 row(s); the run has 6 measurement(s) and 0 verdict(s)\n"), (status, text)
        )
        rows = bench_ledger.read_ledger(self.ledger)
        self.assertEqual({"groupDense": 4}, rows[1]["counters"])
        self.assertEqual(
            [
                ("q00", "hot_seconds", False),
                ("q01", "cold_seconds", False),
                ("q01", "hot_seconds", False),
                ("suite", "hot_seconds", False),
            ],
            [(row["benchmark"], row["metric"], row["accepted"]) for row in rows[2:6]],
        )
        self.assertEqual(({"tier": "1m"}, 12.5), (rows[6]["params"], rows[6]["value"]))

    def test_capture_needs_no_ledger_and_begin_refuses_an_unregistered_machine(self) -> None:
        out = self.base / "run"
        out.mkdir()
        with mock.patch.dict(os.environ):
            os.environ.pop(bench_ledger.LEDGER_ENV)
            self.assertEqual(
                (0, "", ""), self.cli("capture", "--out", str(out), "--data", str(out), "--java", "head=java-not-there")
            )
            self.assertEqual(2, self.cli("commit", "--run", str(out))[0])
        capture = json.loads((out / "environment.json").read_text(encoding="utf-8"))
        self.assertEqual({"unavailable": "java-not-there: command not found"}, capture["runtimes"]["head"])
        self.conditions.write_text(
            condition_entry("2026-11-23", "elsewhere", "new baseline", "5" * 64), encoding="utf-8"
        )
        status, _, err = self.cli(
            "begin",
            "--out",
            str(out),
            "--tier",
            "control",
            "--kit",
            "jmh",
            "--arm",
            f"head,commit={COMMIT_B},binary=jvm",
        )
        self.assertEqual(2, status)
        self.assertIn("holds this environment's fingerprint", err)
        self.assertIn("Add a dated entry", err)
        self.assertEqual(2, self.cli("capture", "--out", str(self.base / "absent"))[0])
        self.assertEqual(2, self.cli("capture", "--out", str(out), "--java", "nopath")[0])

    def test_malformed_input_files_are_reported_not_raised(self) -> None:
        self.register_this_machine()
        out = self.base / "run"
        out.mkdir()
        self.cli(
            "begin",
            "--out",
            str(out),
            "--tier",
            "control",
            "--kit",
            "jmh",
            "--arm",
            f"head,commit={COMMIT_B},binary=jvm",
        )
        bad = self.base / "bad.json"
        bad.write_text("{", encoding="utf-8")
        common = ("--run", str(out), "--arm", "head", "--process", "1", "--json")
        status, _, err = self.cli("import-jmh", *common, str(bad))
        self.assertEqual(2, status)
        self.assertIn("bad.json: not JSON", err)
        bad.write_text(json.dumps([{"benchmark": "a.B.c", "forks": 1, "mode": "avgt"}]), encoding="utf-8")
        status, _, err = self.cli("import-jmh", *common, str(bad))
        self.assertEqual(2, status)
        self.assertIn("malformed input (KeyError", err)
        self.assertEqual(2, self.cli("import-jmh", *common, str(self.base / "absent.json"))[0])
        self.assertEqual([], bench_ledger.load_measurements(out))


class CaptureDetailTest(unittest.TestCase):

    def capture(self, directory: str, runner: FakeRunner | None = None, data: Path | None = None, **overrides):
        return bench_ledger.capture_environment(
            root=make_root(Path(directory), **overrides), runner=runner or FakeRunner(), now=NOW, data=data
        )

    def test_the_model_is_the_first_processors_and_cores_are_counted_per_block(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cpu = self.capture(directory)["cpu"]
        self.assertEqual(("Test CPU 9000", 1, 3, 5), (cpu["model"], cpu["sockets"], cpu["cores"], cpu["threads"]))

    def test_a_processor_list_without_core_identity_says_so(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cpu = self.capture(directory, **{"proc/cpuinfo": "processor\t: 0\nBogoMIPS\t: 50\n\nprocessor\t: 1\n"})[
                "cpu"
            ]
        missing = {"unavailable": "/proc/cpuinfo: no physical id"}
        self.assertEqual(
            ({"unavailable": "/proc/cpuinfo: no model name"}, missing, missing, 2),
            (cpu["model"], cpu["sockets"], cpu["cores"], cpu["threads"]),
        )

    def test_the_operating_system_name_may_contain_an_equals_sign(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            capture = self.capture(directory, **{"etc/os-release": 'ID=x\nPRETTY_NAME="Distro 1 (kernel=lts)"\n'})
        self.assertEqual("Distro 1 (kernel=lts)", capture["os"])

    def test_missing_frequency_and_power_sources_name_what_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            empty = Path(directory) / "empty"
            empty.mkdir()
            capture = bench_ledger.capture_environment(root=empty, runner=FakeRunner(), now=NOW)
            self.assertEqual(
                {"unavailable": "/sys/devices/system/cpu/cpufreq: No such file or directory"}, capture["cpufreq"]
            )
            self.assertEqual({"unavailable": "/sys/class/powercap: No such file or directory"}, capture["powercap"])
            self.assertEqual(capture["cpufreq"], bench_ledger.fingerprint_fields(capture)["governors"])
            (empty / "sys/devices/system/cpu/cpufreq").mkdir(parents=True)
            (empty / "sys/devices/system/cpu/cpufreq/boost").write_text("1\n", encoding="utf-8")
            capture = bench_ledger.capture_environment(root=empty, runner=FakeRunner(), now=NOW)
            self.assertEqual({"unavailable": "/sys/devices/system/cpu/cpufreq: no policy"}, capture["cpufreq"])
            self.assertEqual("1", capture["turbo"]["cpufreq_boost"])

    def test_a_power_zone_that_cannot_be_listed_is_passed_over(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            capture = self.capture(directory, **{"sys/class/powercap/a-file-not-a-zone": "x\n"})
        self.assertEqual({"intel-rapl:0/constraint_0_power_limit_uw": "50000000"}, capture["powercap"])

    def test_the_filesystem_is_the_innermost_and_latest_mount_over_the_data_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory) / "data" / "db"
            data.mkdir(parents=True)
            mounts = (
                "malformed line without the separator\n"
                "25 1 259:2 / / rw - ext4 /dev/root rw\n"
                f"90 25 0:50 / {directory}/data rw - xfs /dev/sdb1 rw\n"
                f"91 90 0:51 / {directory}/data rw - btrfs /dev/sdc1 rw\n"
                f"92 25 0:52 / {directory}/data/db/deeper rw - tmpfs none rw\n"
                f"95 91 0:54 / {directory}/data/pool - zfs tank rw\n"
                "93 25 0:53 / - \n"
                "94 25 - ext2 /dev/sdd rw\n"
            )
            expected = {data: "btrfs", Path(directory) / "data" / "pool" / "db": "zfs", Path(directory): "ext4"}
            for index, (path, filesystem) in enumerate(expected.items()):
                capture = self.capture(f"{directory}/root-{index}", data=path, **{"proc/self/mountinfo": mounts})
                self.assertEqual(filesystem, capture["data_filesystem"], path)

    def test_a_data_path_that_cannot_be_resolved_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            os.symlink(f"{directory}/b", f"{directory}/a")
            os.symlink(f"{directory}/a", f"{directory}/b")
            capture = self.capture(f"{directory}/root", data=Path(directory) / "a" / "db")
        self.assertRegex(capture["data_filesystem"]["unavailable"], "^benchmark data path: (Runtime|OS)Error$")

    def test_memory_is_fingerprinted_in_whole_gibibytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            for index, (kilobytes, expected) in enumerate(
                ((32527200, 31), (16777216, 16), (16252929, 16), (16252927, 15))
            ):
                capture = self.capture(f"{directory}/{index}", **{"proc/meminfo": f"MemTotal:       {kilobytes} kB\n"})
                self.assertEqual(expected, bench_ledger.fingerprint_fields(capture)["memory_gib"], kilobytes)

    def test_the_fingerprint_keeps_characters_outside_ascii_as_they_are(self) -> None:
        environment = {
            "kernel_release": "k",
            "os": "o",
            "architecture": "a",
            "firmware": {"version": "f"},
            "product_name": "Zenbook – é",
            "cpu": {"model": "m", "threads": 2},
            "cpufreq": {"unavailable": "none"},
            "turbo": {},
            "memory_total_kb": {"unavailable": "none"},
        }
        canonical = (
            '{"architecture":"a","cpu_model":"m","cpufreq_drivers":{"unavailable":"none"},"firmware_version":"f",'
            '"governors":{"unavailable":"none"},"kernel_release":"k","logical_cpus":2,'
            '"memory_gib":{"unavailable":"none"},"os":"o","product_name":"Zenbook – é","turbo":{}}'
        )
        self.assertEqual(hashlib.sha256(canonical.encode("utf-8")).hexdigest(), bench_ledger.fingerprint(environment))

    def test_a_runtime_is_asked_for_its_version_on_standard_error(self) -> None:
        # The launchers here are small scripts run by the real runner, not the fake one.
        with tempfile.TemporaryDirectory() as directory:
            launchers = {}
            for name, body in (
                ("jdk", "echo 'not the version'; echo 'openjdk 25.0.5' >&2"),
                ("broken", "echo 'openjdk 25.0.5' >&2; exit 3"),
                ("silent", "echo 'only on standard output'"),
            ):
                launcher = Path(directory) / name
                launcher.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
                launcher.chmod(0o755)
                launchers[name] = str(launcher)
            launchers["absent"] = str(Path(directory) / "no-such-java")
            runtimes = bench_ledger.capture_environment(javas=launchers)["runtimes"]
        self.assertEqual(
            {
                "jdk": "openjdk 25.0.5",
                "broken": {"unavailable": "broken: exit status 3"},
                "silent": {"unavailable": "silent: no output"},
                "absent": {"unavailable": "no-such-java: command not found"},
            },
            runtimes,
        )


class ConditionsDetailTest(unittest.TestCase):

    def test_text_and_tables_outside_the_entries_are_not_entries(self) -> None:
        text = (
            "# Conditions\n\n| Machine | a table before the first entry |\n|---|---|\n\n"
            + condition_entry("2026-11-23", "laptop-1", "new baseline", "1" * 64)
            + "| Note | rows the record does not define are left alone |\n| Note | even twice |\n| Machine |\n"
        )
        (entry,) = bench_ledger.parse_conditions(text)
        self.assertEqual(("stated", ["1" * 64]), (entry["rows"]["Machine"], entry["fingerprints"]))
        self.assertEqual(set(bench_ledger.CONDITION_ROWS), set(entry["rows"]))

    def test_two_entries_may_share_a_date(self) -> None:
        text = condition_entry("2026-11-23", "laptop-1", "new baseline", "1" * 64) + condition_entry(
            "2026-11-23", "laptop-1", "continues", "2" * 64
        )
        self.assertEqual(
            ["new baseline", "continues"], [entry["kind"] for entry in bench_ledger.parse_conditions(text)]
        )

    def test_messages_name_the_line_the_fingerprint_and_the_reason(self) -> None:
        with self.assertRaisesRegex(LedgerError, "^conditions line 3: an entry heading is"):
            bench_ledger.parse_conditions("# Conditions\n\n## 2026-11-23 laptop\n")
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(LedgerError, "conditions record .*missing.md: No such file or directory$"):
                bench_ledger.conditions_id(Path(directory) / "missing.md", "1" * 64)

    def test_the_default_record_is_the_one_of_this_checkout(self) -> None:
        with mock.patch.dict(os.environ):
            os.environ.pop(bench_ledger.CONDITIONS_ENV, None)
            path = bench_ledger.default_conditions_path()
            self.assertEqual(("CONDITIONS.md", "bench", "docs"), (path.name, path.parent.name, path.parent.parent.name))
            self.assertEqual(Path(bench_ledger.__file__).resolve().parents[4], path.parents[2])
            os.environ[bench_ledger.CONDITIONS_ENV] = "/elsewhere/CONDITIONS.md"
            self.assertEqual(Path("/elsewhere/CONDITIONS.md"), bench_ledger.default_conditions_path())


class RowDetailTest(unittest.TestCase):

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.environment = fake_environment(Path(directory.name))

    def test_the_smallest_valid_values_are_accepted(self) -> None:
        run = run_row(self.environment)
        run["plan"].update(min_pairs=1, max_pairs=1)
        nightly = run_row(self.environment, tier="nightly", night="2026-11-23")
        nightly["plan"]["min_processes"] = 1
        for row in (run, nightly, verdict_row(pairs=1), verdict_row(threshold=0.0), measurement_row(process=1, pair=1)):
            bench_ledger.validate_row(row)

    def test_values_just_outside_are_refused(self) -> None:
        def with_plan(**changes: Any) -> dict[str, Any]:
            row = run_row(self.environment)
            row["plan"].update(changes)
            return row

        cases = {
            "plan 'floor' must be a positive number": with_plan(floor=0.0),
            "plan 'quantile' must be a positive number": with_plan(quantile=-0.99),
            "plan 'min_pairs' must be a positive integer": with_plan(min_pairs=0),
            "plan 'max_pairs' must be a positive integer": with_plan(max_pairs=0),
            "'arms' must be an object": dict(run_row(self.environment), arms=["head"]),
            "'arms' must be an object$": dict(run_row(self.environment), arms={}),
            "'allocation' must be null or hold": {k: v for k, v in verdict_row().items() if k != "allocation"},
            "'allocation' must be null or hold baseline_bytes, candidate_bytes, delta_bytes and pass": verdict_row(
                allocation=True
            ),
            "'reject_reason' must say why": measurement_row(accepted=False, reject_reason=""),
        }
        for message, row in cases.items():
            with self.subTest(message=message):
                with self.assertRaisesRegex(LedgerError, message):
                    bench_ledger.validate_row(row)


class LedgerFileDetailTest(unittest.TestCase):

    def test_appending_nothing_creates_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "deep" / "er" / "ledger.jsonl"
            bench_ledger.append_rows(ledger, [])
            self.assertFalse(ledger.parent.exists())
            bench_ledger.append_rows(ledger, [measurement_row()])
            self.assertEqual(
                1, len(bench_ledger.read_ledger(ledger)), "missing directories are created on the first row"
            )

    def test_a_short_write_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.jsonl"
            with mock.patch.object(bench_ledger.os, "write", return_value=5):
                with self.assertRaisesRegex(LedgerError, "ledger.jsonl: short write \\(5 of \\d+ bytes\\)"):
                    bench_ledger.append_rows(ledger, [measurement_row()])

    def test_rows_are_written_one_object_per_line_and_read_back_equal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "ledger.jsonl"
            rows = [measurement_row(benchmark="größe"), measurement_row(pair=2, counters={"groupDense": 2})]
            bench_ledger.append_rows(ledger, rows)
            lines = ledger.read_text(encoding="utf-8").splitlines()
            self.assertEqual(rows, [json.loads(line) for line in lines])
            self.assertEqual(rows, bench_ledger.read_ledger(ledger))


class ImporterDetailTest(unittest.TestCase):

    def test_a_result_that_is_not_a_list_of_rows_is_refused(self) -> None:
        for document in ({"result": "0.5"}, {"result": 5}, {"results": {"q1": 0.5}}):
            with self.subTest(document=document):
                with self.assertRaisesRegex(LedgerError, "no 'result' array"):
                    bench_ledger.clickbench_rows(document, arm="head", pair=None, position=None, process=1)

    def test_a_process_with_one_try_per_query_has_no_hot_figure_to_miss(self) -> None:
        rows, skipped = bench_ledger.clickbench_rows(
            {"result": [[0.039], [0.012]]}, arm="head", pair=None, position=None, process=1
        )
        self.assertEqual([], skipped)
        self.assertEqual({"cold_seconds"}, {row["metric"] for row in rows})

    def test_every_query_of_an_isolated_run_can_be_the_one_measured(self) -> None:
        for query in (1, 2, 3):
            document = {"result": [[0.5] if index == query else [None] for index in (1, 2, 3)]}
            rows, _ = bench_ledger.jsonbench_rows(document, query=query, attempt=1, arm="head", process=1)
            self.assertEqual([(f"q{query}", 0.5)], [(row["benchmark"], row["value"]) for row in rows])

    def test_a_jmh_benchmark_is_named_by_class_and_method(self) -> None:
        for name in ("io.sirix.benchmark.LeafBenchmark.warmLookup", "LeafBenchmark.warmLookup"):
            result = dict(jmh_result("warmLookup", 1.0), benchmark=name)
            rows = bench_ledger.jmh_rows([result], arm="baseline", pair=1, position=1, process=1)
            self.assertEqual("LeafBenchmark.warmLookup", rows[0]["benchmark"])


class CommandLineDetailTest(CommandLineCase):
    """More of the command line: what it requires and what it reports."""

    REQUIRED = {
        "capture": ["--out", "DIR"],
        "begin": ["--out", "DIR", "--tier", "gate", "--kit", "jmh", "--arm", "head"],
        "record": [
            "--run",
            "DIR",
            "--process",
            "1",
            "--arm",
            "head",
            "--benchmark",
            "b",
            "--metric",
            "m",
            "--unit",
            "s",
            "--value",
            "1",
        ],
        "import-jmh": ["--run", "DIR", "--process", "1", "--arm", "head", "--json", "FILE"],
        "import-clickbench": ["--run", "DIR", "--process", "1", "--arm", "head", "--json", "FILE"],
        "import-jsonbench": ["--run", "DIR", "--process", "1", "--json", "FILE", "--query", "1", "--attempt", "1"],
        "reject": ["--run", "DIR", "--reason", "why"],
        "stop": ["--run", "DIR"],
        "judge": ["--run", "DIR"],
        "commit": ["--run", "DIR"],
        "report": ["--run", "DIR"],
    }

    def test_every_required_option_is_required(self) -> None:
        with self.assertRaises(SystemExit) as usage, contextlib.redirect_stderr(io.StringIO()):
            bench_ledger.main([])
        self.assertEqual(2, usage.exception.code)
        for command, arguments in self.REQUIRED.items():
            for index in range(0, len(arguments), 2):
                missing = arguments[index]
                with self.subTest(command=command, missing=missing):
                    err = io.StringIO()
                    with self.assertRaises(SystemExit) as usage, contextlib.redirect_stderr(err):
                        bench_ledger.main([command, *arguments[:index], *arguments[index + 2 :]])
                    self.assertEqual(2, usage.exception.code)
                    self.assertIn(f"the following arguments are required: {missing}", err.getvalue())

    def open_gate(self) -> Path:
        self.register_this_machine()
        out = self.base / "run"
        out.mkdir()
        arms = [
            f"baseline,commit={COMMIT_A},binary=jvm,runtime=jdk",
            f"candidate,commit={COMMIT_B},binary=jvm,runtime=jdk",
        ]
        self.assertEqual(
            0,
            self.cli("begin", "--out", str(out), "--tier", "gate", "--kit", "jmh", "--arm", arms[0], "--arm", arms[1])[
                0
            ],
        )
        return out

    def test_a_gate_without_a_regression_exits_with_status_zero(self) -> None:
        out = self.open_gate()
        for pair, deviation in enumerate(QUIET, start=1):
            for position, (arm, value) in enumerate((("baseline", 24.0), ("candidate", 24.0 * math.exp(deviation))), 1):
                status, text, _ = self.cli(
                    "record",
                    "--run",
                    str(out),
                    "--benchmark",
                    "scan",
                    "--param",
                    "filter=a=b",
                    "--metric",
                    "ns_per_op",
                    "--unit",
                    "ns/op",
                    "--arm",
                    arm,
                    "--pair",
                    str(pair),
                    "--position",
                    str(position),
                    "--process",
                    str(pair),
                    "--value",
                    repr(value),
                )
                self.assertEqual((0, "recorded 1 measurement(s)\n"), (status, text))
        status, text, _ = self.cli("judge", "--run", str(out))
        self.assertEqual(0, status)
        self.assertIn("scan{filter=a=b} ns_per_op", text)
        self.assertEqual((0, text), self.cli("report", "--run", str(out))[:2])
        script = subprocess.run(
            [sys.executable, "-B", str(Path(bench_ledger.__file__)), "judge", "--run", str(out)],
            check=False,
            capture_output=True,
            text=True,
            env=dict(os.environ),
        )
        self.assertEqual((2, True), (script.returncode, "verdicts are computed once" in script.stderr))

    def test_a_rejected_run_is_committed_with_its_reason(self) -> None:
        out = self.open_gate()
        self.cli(
            "record",
            "--run",
            str(out),
            "--benchmark",
            "scan",
            "--metric",
            "ns_per_op",
            "--unit",
            "ns/op",
            "--arm",
            "baseline",
            "--pair",
            "1",
            "--position",
            "1",
            "--process",
            "1",
            "--value",
            "24",
        )
        status, text, _ = self.cli("reject", "--run", str(out), "--reason", "wrong answers")
        self.assertEqual((0, "rejected 1 measurement(s): wrong answers\n"), (status, text))
        self.assertEqual(0, self.cli("commit", "--run", str(out))[0])
        self.assertEqual(["wrong answers"], [row["reject_reason"] for row in bench_ledger.read_ledger(self.ledger)[1:]])

    def test_errors_name_what_was_wrong(self) -> None:
        out = self.base / "plain"
        out.mkdir()
        status, _, err = self.cli("capture", "--out", str(self.base / "absent"))
        self.assertEqual((2, True), (status, "output directory does not exist" in err))
        self.register_this_machine()
        self.cli(
            "begin",
            "--out",
            str(out),
            "--tier",
            "control",
            "--kit",
            "jmh",
            "--arm",
            f"head,commit={COMMIT_B},binary=jvm",
        )
        status, _, err = self.cli(
            "import-jmh", "--run", str(out), "--arm", "head", "--process", "1", "--json", str(self.base / "absent.json")
        )
        self.assertEqual((2, True), (status, "absent.json: No such file or directory" in err))
        status, _, err = self.cli("stop", "--run", str(self.base / "absent"))
        self.assertEqual((2, True), (status, "holds no opened run (run.json): No such file or directory" in err))


if __name__ == "__main__":
    unittest.main()
