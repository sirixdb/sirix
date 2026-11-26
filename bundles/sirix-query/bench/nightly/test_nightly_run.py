#!/usr/bin/env python3
"""Behaviour tests of the nightly recorder (nightly_run.py): the manifest, the JMH command lines,
the process-major order, the ledger rows and what a short or failed night leaves behind."""

from __future__ import annotations

import contextlib
import copy
import datetime
import io
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Callable, Sequence  # Callable is named in a quoted annotation
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import bench_ledger  # noqa: E402
import nightly_run  # noqa: E402
from nightly_run import NightRecorded, RunError  # noqa: E402
from test_bench_ledger import NOW, FakeRunner, condition_entry, fake_environment, make_root  # noqa: E402

COMMIT = "c" * 40
RUNNER = "kind=hosted,provider=github,label=ubuntu-24.04,image=ubuntu24/20251119.1.0"
MANIFEST = {
    "schema": 1,
    "warmup_iterations": 2,
    "measurement_iterations": 3,
    "iteration_seconds": 1,
    "allocation_warmup_iterations": 2,
    "allocation_iterations": 3,
    "allocation_heap": "3g",
    "timed_jvm_args": ["-Xms1g", "-Xmx2g", "-XX:+UseSerialGC"],
    "benchmarks": [
        {"benchmark": "AlphaBenchmark.encode", "params": {"shape": "WIDE"}, "batch": 1000},
        {"benchmark": "BetaBenchmark.lookup", "params": {}, "batch": 50000},
    ],
}


def value_after(command: Sequence[str], flag: str) -> str:
    return command[list(command).index(flag) + 1]


class FakeJmh:
    """Writes the JSON a real JMH invocation would write for each command, and records the calls."""

    def __init__(self, fail: set[tuple[int, str, str]] | None = None) -> None:
        self.calls: list[list[str]] = []
        self.fail = fail or set()
        self.process = 1
        self.seen: dict[tuple[str, str], int] = {}

    def __call__(self, command: Sequence[str], log: Path) -> int:
        self.calls.append(list(command))
        output = Path(value_after(command, "-rff"))
        leg = "alloc" if value_after(command, "-prof") == "gc" else "timed"
        name = re.sub(r"\\\.", ".", command[command.index("-jar") + 2]).lstrip(".").rstrip("$")
        # The n-th call of one (benchmark, leg) is that benchmark's n-th process.
        process = self.seen[(name, leg)] = self.seen.get((name, leg), 0) + 1
        if (process, name, leg) in self.fail:
            return 1
        params = {command[i + 1].split("=")[0]: command[i + 1].split("=")[1] for i, a in enumerate(command) if a == "-p"}
        jvm_args = value_after(command, "-jvmArgs").split()
        result: dict[str, Any] = {
            "benchmark": f"io.sirix.benchmark.{name}",
            "forks": 1,
            "params": params,
            "jvmArgs": jvm_args,
        }
        if leg == "timed":
            wobble = 1 + 0.001 * process
            result |= {
                "mode": "avgt",
                "primaryMetric": {"score": 1000.0 * wobble, "scoreUnit": "ns/op", "rawData": [[999.0, 1001.0]]},
                "secondaryMetrics": {"cpu.time.norm": {"score": 990.0 * wobble, "scoreUnit": "ns/op"}},
            }
        else:
            batch = int(value_after(command, "-bs"))
            result |= {
                "mode": "ss",
                "measurementBatchSize": batch,
                "primaryMetric": {"score": 12.0, "scoreUnit": "ms/op"},
                "secondaryMetrics": {"gc.alloc.rate.norm": {"score": 64.0 * batch + 5000, "scoreUnit": "B/op"}},
            }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps([result]), encoding="utf-8")
        return 0


class NightlyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.base = Path(directory.name)
        self.manifest = nightly_run.parse_manifest(MANIFEST)
        self.ledger = self.base / "data" / "ledger.jsonl"
        self.environment_root = make_root(self.base)
        runner_record = bench_ledger.parse_runner(RUNNER)
        fingerprint = bench_ledger.fingerprint(fake_environment(self.base), runner_record)
        self.conditions = self.base / "CONDITIONS.md"
        self.conditions.write_text(
            condition_entry("2026-11-26", "gh-ubuntu-24-04-test", "new baseline", fingerprint), encoding="utf-8"
        )
        self.clock_now = 0.0

    def clock(self) -> float:
        return self.clock_now

    def record(self, night: str = "2026-11-27", jmh: FakeJmh | None = None, **options: Any) -> nightly_run.Summary:
        jmh = jmh or FakeJmh()
        self.jmh = jmh
        settings: dict[str, Any] = {
            "jar": self.base / "jmh.jar",
            "java": "java",
            "ledger": self.ledger,
            "night": night,
            "commit": COMMIT,
            "out": self.base / f"out-{night}",
            "runner_spec": RUNNER,
            "conditions": self.conditions,
            "execute": jmh,
            "clock": self.clock,
            "log": lambda _line: None,
            "capture": {"root": self.environment_root, "runner": FakeRunner(), "now": NOW},
        }
        settings.update(options)
        return nightly_run.run_night(self.manifest, **settings)

    def rows(self) -> list[dict[str, Any]]:
        return bench_ledger.read_ledger(self.ledger)


class ManifestTest(unittest.TestCase):

    def test_the_shipped_manifest_parses_and_names_its_series(self) -> None:
        manifest = nightly_run.load_manifest(nightly_run.DEFAULT_MANIFEST)
        self.assertGreaterEqual(len(manifest.entries), 10)
        self.assertEqual(len(manifest.entries), len({(e.benchmark, e.params) for e in manifest.entries}))

    def test_every_field_is_checked_before_anything_reaches_a_command_line(self) -> None:
        def broken(**changes: Any) -> dict[str, Any]:
            document = copy.deepcopy(MANIFEST)
            document.update(changes)
            return document

        entry = {"benchmark": "AlphaBenchmark.encode", "params": {}, "batch": 10}
        cases = {
            "schema": broken(schema=2),
            "heap": broken(allocation_heap="3 g; rm"),
            "warmup": broken(warmup_iterations=0),
            "iterations are integers": broken(measurement_iterations="3"),
            "boolean is not a count": broken(iteration_seconds=True),
            "jvm option": broken(timed_jvm_args=["-jar", "evil.jar"]),
            "empty set": broken(benchmarks=[]),
            "benchmark name": broken(benchmarks=[{**entry, "benchmark": "Alpha.encode; rm -rf /"}]),
            "param value": broken(benchmarks=[{**entry, "params": {"shape": "A,B"}}]),
            "param name": broken(benchmarks=[{**entry, "params": {"sha pe": "A"}}]),
            "batch": broken(benchmarks=[{**entry, "batch": 0}]),
            "duplicate": broken(benchmarks=[entry, dict(entry)]),
            "not an object": [],
        }
        for label, document in cases.items():
            with self.subTest(label), self.assertRaises(RunError):
                nightly_run.parse_manifest(document)

    def test_an_unreadable_manifest_is_a_run_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "none.json"
            with self.assertRaises(RunError):
                nightly_run.load_manifest(missing)
            bad = Path(directory) / "bad.json"
            bad.write_text("{", encoding="utf-8")
            with self.assertRaises(RunError):
                nightly_run.load_manifest(bad)


class CommandTest(unittest.TestCase):

    def setUp(self) -> None:
        self.manifest = nightly_run.parse_manifest(MANIFEST)
        self.entry = self.manifest.entries[0]
        self.jar = Path("/jars/jmh.jar")

    def test_the_include_pattern_selects_exactly_one_method(self) -> None:
        pattern = re.compile(self.entry.include)
        self.assertTrue(pattern.search("io.sirix.benchmark.AlphaBenchmark.encode"))
        self.assertFalse(pattern.search("io.sirix.benchmark.AlphaBenchmark.encodeAll"))
        self.assertFalse(pattern.search("io.sirix.benchmark.XAlphaBenchmark.encode"))
        self.assertFalse(pattern.search("io.sirix.benchmark.AlphaBenchmark.decode"))

    def test_a_timed_process_is_one_fork_with_the_cpu_profiler_and_nanoseconds(self) -> None:
        command = nightly_run.timed_command(self.manifest, self.entry, "java", self.jar, Path("/o/out.json"))
        self.assertEqual("1", value_after(command, "-f"))
        self.assertEqual("avgt", value_after(command, "-bm"))
        self.assertEqual("ns", value_after(command, "-tu"))
        self.assertEqual("io.sirix.benchmark.CpuTimeProfiler", value_after(command, "-prof"))
        self.assertEqual("shape=WIDE", value_after(command, "-p"))
        self.assertEqual(["-p"], [arg for arg in command if arg == "-p"])
        self.assertIn("-XX:+UseSerialGC", value_after(command, "-jvmArgs").split())
        self.assertNotIn("gc", command)

    def test_an_allocation_process_has_the_conv_302_settings(self) -> None:
        command = nightly_run.allocation_command(self.manifest, self.entry, "java", self.jar, Path("/o/a.json"))
        self.assertEqual("ss", value_after(command, "-bm"))
        self.assertEqual("1000", value_after(command, "-bs"))
        self.assertEqual("gc", value_after(command, "-prof"))
        flags = value_after(command, "-jvmArgs").split()
        for required in ("-XX:+UseEpsilonGC", "-XX:-DoEscapeAnalysis", "-XX:-UseJVMCICompiler", "-Xmx3g"):
            self.assertIn(required, flags)
        # A JVM without JVMCI must ignore the flag instead of refusing to start; the ledger needs it present.
        self.assertLess(flags.index("-XX:+IgnoreUnrecognizedVMOptions"), flags.index("-XX:-UseJVMCICompiler"))


class RunNightTest(NightlyTestCase):

    def test_a_night_records_twenty_processes_of_cpu_wall_and_allocation_for_every_entry(self) -> None:
        summary = self.record()
        self.assertTrue(summary.complete)
        self.assertEqual(20, summary.processes_completed)
        rows = self.rows()
        runs = [row for row in rows if row["kind"] == "run"]
        self.assertEqual(1, len(runs))
        run = runs[0]
        self.assertEqual(("nightly", "2026-11-27", "jmh-nightly"), (run["tier"], run["night"], run["kit"]))
        self.assertEqual(COMMIT, run["arms"]["head"]["commit"])
        self.assertEqual("gh-ubuntu-24-04-test", run["conditions"])
        self.assertEqual("hosted", run["runner"]["kind"])
        self.assertEqual("ubuntu24/20251119.1.0", run["runner"]["image"])
        counts: dict[tuple[str, str], int] = {}
        for row in rows:
            if row["kind"] == "measurement":
                self.assertTrue(row["accepted"])
                self.assertEqual("head", row["arm"])
                counts[(row["benchmark"], row["metric"])] = counts.get((row["benchmark"], row["metric"]), 0) + 1
        self.assertEqual(
            {
                (benchmark, metric): 20
                for benchmark in ("AlphaBenchmark.encode", "BetaBenchmark.lookup")
                for metric in ("cpu_ns_per_op", "ns_per_op", "alloc_bytes_per_op")
            },
            counts,
        )

    def test_the_recorded_figures_are_the_ones_jmh_wrote(self) -> None:
        self.record()
        values = {
            (row["benchmark"], row["metric"], row["process"]): row["value"]
            for row in self.rows()
            if row["kind"] == "measurement"
        }
        self.assertAlmostEqual(1000.0 * 1.001, values[("AlphaBenchmark.encode", "ns_per_op", 1)])
        self.assertAlmostEqual(990.0 * 1.003, values[("AlphaBenchmark.encode", "cpu_ns_per_op", 3)])
        # The importer divides the batch's bytes by the batch: (64 * batch + 5000) / batch.
        self.assertAlmostEqual(64.0 + 5000 / 1000, values[("AlphaBenchmark.encode", "alloc_bytes_per_op", 1)])
        self.assertAlmostEqual(64.0 + 5000 / 50000, values[("BetaBenchmark.lookup", "alloc_bytes_per_op", 20)])

    def test_processes_are_measured_one_at_a_time_across_the_whole_set(self) -> None:
        self.record()
        names = [re.sub(r"\\\.", ".", call[call.index("-jar") + 2]) for call in self.jmh.calls]
        # Per process: Alpha timed, Alpha alloc, Beta timed, Beta alloc; never twenty Alphas in a row.
        self.assertEqual(4 * 20, len(names))
        for start in range(0, len(names), 4):
            self.assertEqual(
                ["Alpha"] * 2 + ["Beta"] * 2,
                ["Alpha" if name.lstrip(".").startswith("Alpha") else "Beta" for name in names[start : start + 4]],
            )

    def test_a_failed_process_leaves_its_series_one_short_and_is_still_recorded(self) -> None:
        summary = self.record(jmh=FakeJmh(fail={(3, "BetaBenchmark.lookup", "timed")}))
        self.assertFalse(summary.complete)
        self.assertEqual(1, len(summary.failures))
        self.assertIn("process 3 BetaBenchmark.lookup", summary.failures[0])
        joined = " ".join(summary.series_short)
        self.assertIn("BetaBenchmark.lookup", joined)
        self.assertIn("19/20", joined)
        self.assertNotIn("AlphaBenchmark", joined)
        run_rows = [row for row in self.rows() if row["kind"] == "run"]
        self.assertEqual(1, len(run_rows))  # the rows that exist were appended, as CONV-303 asks of an aborted run

    def test_a_time_budget_stops_every_series_at_the_same_process(self) -> None:
        def slow(command: Sequence[str], log: Path) -> int:
            self.clock_now += 10.0  # each JMH process takes 10 s; a process of the set takes 40 s
            return fake(command, log)

        fake = FakeJmh()
        summary = self.record(execute=slow, budget_seconds=400.0)
        self.assertTrue(summary.stopped_for_budget)
        self.assertFalse(summary.complete)
        self.assertLess(summary.processes_completed, 20)
        counts = {
            (row["benchmark"], row["metric"]): 0 for row in self.rows() if row["kind"] == "measurement"
        }
        for row in self.rows():
            if row["kind"] == "measurement":
                counts[(row["benchmark"], row["metric"])] += 1
        self.assertEqual({summary.processes_completed}, set(counts.values()))

    def test_a_set_that_cannot_fit_the_budget_is_dropped_after_one_process(self) -> None:
        def slow(command: Sequence[str], log: Path) -> int:
            self.clock_now += 100.0
            return fake(command, log)

        fake = FakeJmh()
        summary = self.record(execute=slow, budget_seconds=1000.0)
        self.assertEqual(1, summary.processes_completed)
        self.assertTrue(summary.stopped_for_budget)

    def test_a_night_cannot_be_recorded_twice(self) -> None:
        self.record()
        before = self.ledger.read_text(encoding="utf-8")
        with self.assertRaises(NightRecorded):
            self.record(out=self.base / "again")
        self.assertEqual(before, self.ledger.read_text(encoding="utf-8"))
        self.record(night="2026-11-28")  # another night is fine
        self.assertEqual({"2026-11-27", "2026-11-28"}, {r["night"] for r in self.rows() if r["kind"] == "run"})

    def test_an_environment_with_no_conditions_entry_measures_nothing(self) -> None:
        self.conditions.write_text("", encoding="utf-8")
        jmh = FakeJmh()
        with self.assertRaises(bench_ledger.LedgerError) as caught:
            self.record(jmh=jmh)
        self.assertIn("fingerprint", str(caught.exception))
        self.assertEqual([], jmh.calls)
        self.assertFalse(self.ledger.exists())

    def test_the_inputs_are_checked_before_the_first_process(self) -> None:
        for options in (
            {"night": "2026-02-30"},
            {"night": "2026-2-3"},
            {"commit": "abc"},
            {"commit": "C" * 40},
            {"processes": 19},
        ):
            jmh = FakeJmh()
            with self.subTest(options), self.assertRaises(RunError):
                self.record(jmh=jmh, **options)
            self.assertEqual([], jmh.calls)

    def test_a_summary_file_states_what_happened(self) -> None:
        self.record()
        summary = json.loads((self.base / "out-2026-11-27" / "summary.json").read_text(encoding="utf-8"))
        self.assertTrue(summary["complete"])
        self.assertEqual(20, summary["processes_completed"])
        self.assertEqual([], summary["failures"])


class CommandLineTest(NightlyTestCase):

    def run_main(self, *arguments: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = nightly_run.main(list(arguments))
        return code, out.getvalue(), err.getvalue()

    def test_a_missing_ledger_directory_or_bad_night_is_exit_two(self) -> None:
        manifest = self.base / "manifest.json"
        manifest.write_text(json.dumps(MANIFEST), encoding="utf-8")
        code, _out, err = self.run_main(
            "--jar", "j.jar", "--java", "java", "--ledger", str(self.ledger), "--night", "2026-13-01",
            "--out", str(self.base / "o"), "--commit", COMMIT, "--manifest", str(manifest),
        )  # fmt: skip
        self.assertEqual(2, code)
        self.assertIn("--night", err)

    def test_a_recorded_night_is_exit_five(self) -> None:
        self.record()
        manifest = self.base / "manifest.json"
        manifest.write_text(json.dumps(MANIFEST), encoding="utf-8")
        code, _out, err = self.run_main(
            "--jar", "j.jar", "--java", "java", "--ledger", str(self.ledger), "--night", "2026-11-27",
            "--out", str(self.base / "o2"), "--commit", COMMIT, "--manifest", str(manifest),
        )  # fmt: skip
        self.assertEqual(5, code)
        self.assertIn("already in", err)


class BudgetTest(NightlyTestCase):
    """The time budget: the whole night is dropped when it cannot fit, and a long night stops evenly."""

    def run_with(self, seconds_per_call: "Callable[[int], float]", budget: float) -> nightly_run.Summary:
        calls = {"count": 0}
        fake = FakeJmh()

        def timed(command: Sequence[str], log: Path) -> int:
            process = calls["count"] // 4 + 1  # two entries, a timed and an allocation call each
            calls["count"] += 1
            self.clock_now += seconds_per_call(process)
            return fake(command, log)

        return self.record(execute=timed, budget_seconds=budget)

    def test_a_night_that_fits_the_budget_exactly_runs_all_twenty_processes(self) -> None:
        summary = self.run_with(lambda _process: 10.0, budget=800.0)  # 40 s a process, 20 processes
        self.assertEqual(20, summary.processes_completed)
        self.assertFalse(summary.stopped_for_budget)
        self.assertTrue(summary.complete)

    def test_a_night_one_second_over_the_budget_is_dropped_after_its_first_process(self) -> None:
        summary = self.run_with(lambda _process: 10.0, budget=799.0)
        self.assertEqual(1, summary.processes_completed)
        self.assertTrue(summary.stopped_for_budget)
        self.assertFalse(summary.complete)

    def test_a_night_that_slows_down_stops_before_the_process_that_would_cross_the_budget(self) -> None:
        # The first process takes 40 s and passes the projection (800 s); every later one takes 120 s.
        slower = lambda process: 10.0 if process == 1 else 30.0  # noqa: E731
        on_the_line = self.run_with(slower, budget=1000.0)  # process 9 ends at 1000 s exactly: it may start
        self.assertEqual((9, True), (on_the_line.processes_completed, on_the_line.stopped_for_budget))
        self.setUp()
        over = self.run_with(slower, budget=999.0)
        self.assertEqual(8, over.processes_completed)

    def test_every_series_of_a_stopped_night_has_the_same_number_of_processes(self) -> None:
        summary = self.run_with(lambda process: 10.0 if process == 1 else 30.0, budget=1000.0)
        per_series: dict[tuple[str, str], int] = {}
        for row in self.rows():
            if row["kind"] == "measurement":
                key = (row["benchmark"], row["metric"])
                per_series[key] = per_series.get(key, 0) + 1
        self.assertEqual({summary.processes_completed}, set(per_series.values()))
        self.assertTrue(all("9/20" in item for item in summary.series_short))

    def test_without_a_budget_the_night_always_runs_to_the_end(self) -> None:
        def slow(command: Sequence[str], log: Path) -> int:
            self.clock_now += 10_000.0
            return fake(command, log)

        fake = FakeJmh()
        summary = self.record(execute=slow)
        self.assertEqual((20, False), (summary.processes_completed, summary.stopped_for_budget))


class AllocationOptOutTest(NightlyTestCase):
    """An entry with `"allocation": false` is timed but has no allocation leg and no allocation series."""

    def setUp(self) -> None:
        super().setUp()
        document = copy.deepcopy(MANIFEST)
        document["benchmarks"][1]["allocation"] = False
        self.manifest = nightly_run.parse_manifest(document)

    def test_the_entry_is_measured_for_time_only(self) -> None:
        summary = self.record()
        self.assertTrue(summary.complete)
        self.assertEqual(3 * 20, len(self.jmh.calls))  # Alpha: timed + alloc, Beta: timed
        metrics = {(r["benchmark"], r["metric"]) for r in self.rows() if r["kind"] == "measurement"}
        self.assertNotIn(("BetaBenchmark.lookup", "alloc_bytes_per_op"), metrics)
        self.assertIn(("BetaBenchmark.lookup", "cpu_ns_per_op"), metrics)
        self.assertIn(("AlphaBenchmark.encode", "alloc_bytes_per_op"), metrics)

    def test_a_missing_allocation_series_is_still_noticed_for_the_entries_that_have_one(self) -> None:
        every = {(process, "AlphaBenchmark.encode", "alloc") for process in range(1, 21)}
        summary = self.record(jmh=FakeJmh(fail=every))
        self.assertIn("1 series have no accepted measurement at all", summary.series_short)

    def test_the_flag_must_be_a_boolean(self) -> None:
        document = copy.deepcopy(MANIFEST)
        document["benchmarks"][0]["allocation"] = "no"
        with self.assertRaises(RunError):
            nightly_run.parse_manifest(document)

    def test_the_shipped_manifest_opts_out_only_the_two_lookups_whose_allocation_is_unresolvable(self) -> None:
        manifest = nightly_run.load_manifest(nightly_run.DEFAULT_MANIFEST)
        self.assertEqual(
            ["HotIndirectPageLookupBenchmark.lookupFound", "HotIndirectPageLookupBenchmark.lookupMissingEquality"],
            sorted(entry.benchmark for entry in manifest.entries if not entry.allocation),
        )


class FailureTest(NightlyTestCase):

    def test_a_failure_says_which_process_and_how_it_ended(self) -> None:
        summary = self.record(jmh=FakeJmh(fail={(2, "AlphaBenchmark.encode", "alloc")}))
        self.assertEqual(["process 2 AlphaBenchmark.encode{'shape': 'WIDE'} alloc: exit 1"], summary.failures)

    def test_a_series_that_never_measured_is_named_as_missing(self) -> None:
        every_process = {(process, "BetaBenchmark.lookup", "alloc") for process in range(1, 21)}
        summary = self.record(jmh=FakeJmh(fail=every_process))
        self.assertIn("1 series have no accepted measurement at all", summary.series_short)
        self.assertFalse(summary.complete)

    def test_a_result_file_that_is_not_a_result_is_a_failure_and_not_a_crash(self) -> None:
        fake = FakeJmh()

        def garbage(command: Sequence[str], log: Path) -> int:
            code = fake(command, log)
            if "AlphaBenchmark" in command[command.index("-jar") + 2] and value_after(command, "-prof") == "gc" \
                    and fake.seen[("AlphaBenchmark.encode", "alloc")] == 4:
                Path(value_after(command, "-rff")).write_text("{", encoding="utf-8")
            return code

        summary = self.record(execute=garbage)
        self.assertEqual(1, len(summary.failures))
        self.assertIn("process 4 AlphaBenchmark.encode", summary.failures[0])

    def test_a_result_that_breaks_a_ledger_rule_is_a_failure(self) -> None:
        fake = FakeJmh()

        def two_forks(command: Sequence[str], log: Path) -> int:
            code = fake(command, log)
            if "BetaBenchmark" in command[command.index("-jar") + 2] and value_after(command, "-prof") != "gc" \
                    and fake.seen[("BetaBenchmark.lookup", "timed")] == 1:
                path = Path(value_after(command, "-rff"))
                document = json.loads(path.read_text(encoding="utf-8"))
                document[0]["forks"] = 2
                path.write_text(json.dumps(document), encoding="utf-8")
            return code

        summary = self.record(execute=two_forks)
        self.assertEqual(1, len(summary.failures))
        self.assertIn("forks", summary.failures[0])


class ProcessTest(unittest.TestCase):

    def test_a_command_is_run_with_its_output_in_the_log(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "deep" / "er" / "run.log"
            self.assertEqual(0, nightly_run.execute_process([sys.executable, "-c", "print('out'); import sys; print('err', file=sys.stderr)"], log))
            self.assertEqual({"out", "err"}, set(log.read_text(encoding="utf-8").split()))
            self.assertEqual(3, nightly_run.execute_process([sys.executable, "-c", "raise SystemExit(3)"], log))

    def test_a_command_that_cannot_start_is_reported_in_the_log(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "run.log"
            self.assertEqual(124, nightly_run.execute_process(["/nonexistent/java", "-version"], log))
            self.assertIn("cannot start /nonexistent/java", log.read_text(encoding="utf-8"))

    def test_the_commit_defaults_to_the_head_of_the_checkout(self) -> None:
        self.assertRegex(nightly_run._git_head(), "^[0-9a-f]{40}$")  # noqa: SLF001
        failing = subprocess.CalledProcessError(128, "git")
        with mock.patch.object(nightly_run.subprocess, "run", side_effect=failing), self.assertRaises(RunError):
            nightly_run._git_head()  # noqa: SLF001
        with mock.patch.object(nightly_run.subprocess, "run", side_effect=FileNotFoundError("git")), self.assertRaises(RunError):
            nightly_run._git_head()  # noqa: SLF001


class MainTest(unittest.TestCase):

    def summary(self, **changes: Any) -> nightly_run.Summary:
        values: dict[str, Any] = {
            "night": "2026-11-27", "commit": COMMIT, "run": "20261127T201700Z-0a1b2c3d", "processes_wanted": 20,
            "processes_completed": 20, "failures": [], "seconds": 3600.4, "series_short": [], "stopped_for_budget": False,
        }  # fmt: skip
        values.update(changes)
        return nightly_run.Summary(**values)

    def run_main(self, summary: nightly_run.Summary, *extra: str) -> tuple[int, str, dict[str, Any]]:
        out = io.StringIO()
        with mock.patch.object(nightly_run, "run_night", return_value=summary) as run, mock.patch.object(
            nightly_run, "load_manifest", return_value=nightly_run.parse_manifest(MANIFEST)
        ), contextlib.redirect_stdout(out):
            code = nightly_run.main(["--jar", "j.jar", "--java", "java", "--ledger", "l.jsonl", "--out", "out", *extra])
        return code, out.getvalue(), run.call_args.kwargs

    def test_a_complete_night_is_exit_zero_and_prints_one_line(self) -> None:
        code, text, kwargs = self.run_main(self.summary(), "--commit", COMMIT, "--night", "2026-11-27")
        self.assertEqual(0, code)
        self.assertEqual(f"night 2026-11-27 commit {COMMIT[:12]}: 20/20 process(es) in 3600s, 0 failure(s)\n", text)
        self.assertEqual((COMMIT, "2026-11-27"), (kwargs["commit"], kwargs["night"]))

    def test_a_short_night_is_exit_four_and_names_the_short_series(self) -> None:
        short = self.summary(processes_completed=11, series_short=["Alpha cpu_ns_per_op: 11/20"], failures=["x", "y"])
        code, text, _ = self.run_main(short, "--commit", COMMIT)
        self.assertEqual(4, code)
        self.assertIn("11/20 process(es)", text)
        self.assertIn("2 failure(s)", text)
        self.assertIn("short: Alpha cpu_ns_per_op: 11/20", text)

    def test_the_options_reach_the_recorder_and_the_defaults_are_today_and_head(self) -> None:
        with mock.patch.object(nightly_run, "_git_head", return_value=COMMIT):
            code, _text, kwargs = self.run_main(
                self.summary(), "--runner", RUNNER, "--processes", "25", "--budget-seconds", "99.5", "--conditions", "c.md"
            )
        self.assertEqual(0, code)
        self.assertEqual(RUNNER, kwargs["runner_spec"])
        self.assertEqual((25, 99.5, Path("c.md")), (kwargs["processes"], kwargs["budget_seconds"], kwargs["conditions"]))
        self.assertEqual(COMMIT, kwargs["commit"])
        self.assertEqual(datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"), kwargs["night"])
        self.assertEqual((Path("j.jar"), "java", Path("l.jsonl"), Path("out")), (kwargs["jar"], kwargs["java"], kwargs["ledger"], kwargs["out"]))

    def test_a_refused_run_is_exit_two_and_a_missing_argument_is_a_usage_error(self) -> None:
        with mock.patch.object(nightly_run, "load_manifest", side_effect=RunError("no manifest")), contextlib.redirect_stderr(
            io.StringIO()
        ) as err:
            code = nightly_run.main(["--jar", "j", "--java", "java", "--ledger", "l", "--out", "o", "--commit", COMMIT])
        self.assertEqual((2, True), (code, "no manifest" in err.getvalue()))
        for missing in ("--jar", "--java", "--ledger", "--out"):
            arguments = {"--jar": "j", "--java": "java", "--ledger": "l", "--out": "o"}
            arguments.pop(missing)
            flat = [item for pair in arguments.items() for item in pair]
            with self.subTest(missing), self.assertRaises(SystemExit) as caught, contextlib.redirect_stderr(io.StringIO()):
                nightly_run.main(flat)
            self.assertEqual(2, caught.exception.code)


if __name__ == "__main__":
    unittest.main()
