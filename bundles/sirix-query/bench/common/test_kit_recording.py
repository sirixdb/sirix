#!/usr/bin/env python3
"""The kits' run record, end to end: the real kit scripts driven with stub engines (CONV-302, CONV-303).

Nothing here is timed and no database is opened. The stub engines write the result files the real
runners write; the ClickBench script runs from a copy whose rig lock is a stub, so the shared
benchmark lock of the machine is never taken.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

import bench_ledger
from test_bench_ledger import condition_entry

COMMON = Path(__file__).resolve().parent
BENCH = COMMON.parent
COMMIT_A = "a" * 40
COMMIT_B = "b" * 40

STUB_EVICT = "#!/usr/bin/env python3\n"

STUB_RIG_LOCK = textwrap.dedent(
    """\
    import os
    import sys
    if sys.argv[1] == "--check":
        sys.exit(0)
    os.environ["CB_RIG_HOST_LOCK_FD"] = "99"
    os.execvp(sys.argv[2], sys.argv[2:])
    """
)

# One ClickBench process: two queries, three tries each. The candidate's deviations repeat every ten calls.
STUB_CLICKBENCH = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import json
    import math
    import os
    import sys
    from pathlib import Path
    FACTOR = {factor}
    DEVIATIONS = [0.004, -0.003, 0.001, -0.002, 0.0, 0.003, -0.004, 0.002, -0.001, 0.0]
    counter = Path(__file__).with_suffix(".calls")
    call = int(counter.read_text()) if counter.exists() else 0
    counter.write_text(str(call + 1))
    if call + 1 == int(os.environ.get("STUB_FAIL_CALL", "0")):
        sys.exit("stub engine: failing on request")
    scale = FACTOR * math.exp(DEVIATIONS[call % 10] if FACTOR != 1.0 or {deviate} else 0.0)
    out = sys.argv[sys.argv.index("--json") + 1]
    rows = [[round(base * scale, 9), round(base * scale / 10, 9), round(base * scale / 9, 9)] for base in (0.1, 0.2)]
    if os.environ.get("STUB_FAIL_HOT") == "1":
        rows[0][-1] = None
    Path(out).write_text(json.dumps({{"result": rows}}))
    print("# served: predicateCounts=2 groupDense=1")
    """
)

# One isolated JSONBench process: the requested query's single timing among five rows.
STUB_JSONBENCH = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import json
    import os
    import sys
    from pathlib import Path
    arguments = sys.argv[1:]
    if "--dump" in arguments:
        sys.exit(0)
    counter = Path(__file__).with_suffix(".calls")
    call = int(counter.read_text()) + 1 if counter.exists() else 1
    counter.write_text(str(call))
    if os.environ.get("STUB_FAIL_CALL") == str(call):
        sys.exit("stub engine: failing on request")
    if os.environ.get("STUB_INTERRUPT_CALL") == str(call):
        import signal
        os.kill(os.getppid(), signal.SIGTERM)
        sys.exit("stub engine: interrupted on request")
    query = int(arguments[arguments.index("--queries") + 1])
    if os.environ.get("STUB_FAIL_QUERY") == str(query):
        sys.exit("stub engine: failing on request")
    rows = [[None] for _ in range(5)]
    rows[query - 1] = [0.1 * query]
    Path(arguments[arguments.index("--json") + 1]).write_text(json.dumps({"result": rows}))
    print(f"# served: groupDense={query}")
    """
)


def executable(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


class KitTestCase(unittest.TestCase):

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.base = Path(directory.name)
        self.ledger = self.base / "ledger.jsonl"
        self.conditions = self.base / "CONDITIONS.md"
        value = bench_ledger.fingerprint(bench_ledger.capture_environment())
        self.conditions.write_text(
            condition_entry("2026-11-23", "this-machine", "new baseline", value), encoding="utf-8"
        )
        self.database = self.base / "db"
        self.database.mkdir()
        (self.database / "data.bin").write_bytes(b"x" * 4096)
        self.environment = {key: value for key, value in os.environ.items() if not key.startswith("SIRIX_BENCH_")}
        self.environment.update(
            {
                "COOL_MAX_C": "999",
                "BENCH_EVICT_PY": str(executable(self.base / "evict.py", STUB_EVICT)),
                bench_ledger.CONDITIONS_ENV: str(self.conditions),
            }
        )

    def run_kit(self, command: list[str], *, ledger: bool, **environment: str) -> subprocess.CompletedProcess[str]:
        variables = dict(self.environment, **environment)
        if ledger:
            variables[bench_ledger.LEDGER_ENV] = str(self.ledger)
        return subprocess.run(
            command, check=False, text=True, capture_output=True, timeout=180, env=variables, stdin=subprocess.DEVNULL
        )

    def rows(self) -> list[dict]:
        return bench_ledger.read_ledger(self.ledger)


class JsonBenchKitTest(KitTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.reference = self.base / "ref"
        self.reference.mkdir()
        (self.reference / "q1.tsv").write_text("", encoding="utf-8")
        lines = ["PROTOCOL jsonbench-isolated-v1", "ROUNDS 2", "TRIES 2"]
        lines += [f"ROUND {round_number} Q{query} 1 1" for round_number in (1, 2) for query in range(1, 6)]
        (self.reference / "baseline.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.engine = executable(self.base / "sirix-jsonbench", STUB_JSONBENCH)
        self.out = self.base / "results"

    def kit(self, *extra: str, ledger: bool, **environment: str) -> subprocess.CompletedProcess[str]:
        return self.run_kit(
            [
                "bash",
                str(BENCH / "jsonbench" / "run-benchmark.sh"),
                "1m",
                str(self.database),
                str(self.reference),
                "--bin",
                str(self.engine),
                "--rounds",
                "2",
                "--tries",
                "2",
                "--out",
                str(self.out),
                *extra,
            ],
            ledger=ledger,
            **environment,
        )

    def test_every_isolated_process_becomes_a_ledger_row(self) -> None:
        completed = self.kit("--commit", COMMIT_B, ledger=True, SKIP_DIFF="1", SIRIX_BIN_KIND="native-pgo")
        self.assertEqual(0, completed.returncode, completed.stderr + completed.stdout)
        rows = self.rows()
        run, measurements = rows[0], rows[1:]
        self.assertEqual(
            ("run", "control", "jsonbench", "this-machine"), (run["kind"], run["tier"], run["kit"], run["conditions"])
        )
        self.assertEqual(
            {
                "head": {
                    "commit": COMMIT_B,
                    "dirty": False,
                    "binary": "native-pgo",
                    "runtime": "unavailable: the kit does not know which runtime this arm runs on",
                }
            },
            run["arms"],
        )
        self.assertEqual(json.loads((self.out / "environment.json").read_text(encoding="utf-8")), run["environment"])
        self.assertNotIn("unavailable", json.dumps(run["environment"]["data_filesystem"]))
        self.assertEqual(20, len(measurements))
        self.assertEqual([{"tier": "1m"}] * 20, [row["params"] for row in measurements])
        second = [row for row in measurements if row["benchmark"] == "q2"]
        self.assertEqual(
            [("cold_seconds", 1), ("hot_seconds", 2), ("cold_seconds", 3), ("hot_seconds", 4)],
            [(row["metric"], row["process"]) for row in second],
        )
        self.assertEqual(
            {(0.2, "head", None, True, "s")},
            {(row["value"], row["arm"], row["pair"], row["accepted"], row["unit"]) for row in second},
        )
        self.assertEqual([{"groupDense": 2}] * 4, [row["counters"] for row in second])
        self.assertEqual({"q1", "q2", "q3", "q4", "q5"}, {row["benchmark"] for row in measurements})

    def test_timings_of_wrong_answers_are_recorded_as_rejected(self) -> None:
        completed = self.kit("--commit", COMMIT_B, ledger=True)
        self.assertNotEqual(0, completed.returncode)
        self.assertIn("the differential did not pass", completed.stderr)
        measurements = self.rows()[1:]
        self.assertEqual(20, len(measurements))
        self.assertEqual(
            {(False, "the differential against the ClickHouse reference failed")},
            {(row["accepted"], row["reject_reason"]) for row in measurements},
        )

    def test_a_run_that_dies_still_records_the_processes_it_measured(self) -> None:
        completed = self.kit("--commit", COMMIT_B, ledger=True, SKIP_DIFF="1", STUB_FAIL_QUERY="3")
        self.assertNotEqual(0, completed.returncode)
        self.assertIn("the native runner failed", completed.stderr)
        rows = self.rows()
        self.assertEqual(["run"] + ["measurement"] * 4, [row["kind"] for row in rows])
        self.assertEqual({("q1", True), ("q2", True)}, {(row["benchmark"], row["accepted"]) for row in rows[1:]})

    def test_completed_attempts_survive_failure_or_interrupt_within_a_query(self) -> None:
        for variable in ("STUB_FAIL_CALL", "STUB_INTERRUPT_CALL"):
            with self.subTest(variable=variable):
                completed = self.kit("--commit", COMMIT_B, ledger=True, SKIP_DIFF="1", **{variable: "2"})
                self.assertNotEqual(0, completed.returncode)
                rows = self.rows()
                self.assertEqual(["run", "measurement"], [row["kind"] for row in rows])
                self.assertEqual(
                    ("q1", "cold_seconds", 1, {"tier": "1m"}, 0.1),
                    tuple(rows[1][key] for key in ("benchmark", "metric", "process", "params", "value")),
                )
                shutil.rmtree(self.out)
                self.ledger.unlink()
                self.engine.with_suffix(".calls").unlink()

    def test_without_a_ledger_the_kit_runs_and_captures_its_environment(self) -> None:
        completed = self.kit(ledger=False, SKIP_DIFF="1")
        self.assertEqual(0, completed.returncode, completed.stderr + completed.stdout)
        self.assertIn("this run goes to no results ledger and is not a protocol result", completed.stdout)
        self.assertIn("kernel_release", json.loads((self.out / "environment.json").read_text(encoding="utf-8")))
        self.assertFalse(self.ledger.exists())
        self.assertFalse((self.out / "run.json").exists())

    def test_a_ledger_run_is_refused_before_anything_is_measured(self) -> None:
        missing_commit = self.kit(ledger=True, SKIP_DIFF="1")
        self.assertNotEqual(0, missing_commit.returncode)
        self.assertIn("a results ledger needs the commit the image was built from", missing_commit.stderr)
        self.conditions.write_text(
            condition_entry("2026-11-23", "elsewhere", "new baseline", "5" * 64), encoding="utf-8"
        )
        unregistered = self.kit("--commit", COMMIT_B, ledger=True, SKIP_DIFF="1")
        self.assertNotEqual(0, unregistered.returncode)
        self.assertIn("holds this environment's fingerprint", unregistered.stderr)
        self.assertIn("the run was not opened for the results ledger", unregistered.stderr)
        self.assertFalse(self.ledger.exists())
        self.assertFalse((self.out / "round-1-parts").exists(), "no process was run")


class ClickBenchColdRoundsTest(KitTestCase):

    def setUp(self) -> None:
        super().setUp()
        kit = self.base / "bench" / "clickbench"
        (kit / "rig").mkdir(parents=True)
        shutil.copy(BENCH / "clickbench" / "cold-rounds.sh", kit / "cold-rounds.sh")
        (kit / "rig" / "rig_lock.py").write_text(STUB_RIG_LOCK, encoding="utf-8")
        (self.base / "bench" / "common").symlink_to(COMMON, target_is_directory=True)
        self.script = kit / "cold-rounds.sh"
        self.out = self.base / "cold-results"
        self.baseline = executable(self.base / "baseline.py", STUB_CLICKBENCH.format(factor=1.0, deviate=False))

    def candidate(self, factor: float) -> Path:
        return executable(self.base / "candidate.py", STUB_CLICKBENCH.format(factor=factor, deviate=True))

    def kit(self, *arguments: str, ledger: bool = True, **environment: str) -> subprocess.CompletedProcess[str]:
        return self.run_kit(
            ["bash", str(self.script), str(self.database), "--out", str(self.out), "--dataset", "hits-100m", *arguments],
            ledger=ledger,
            **environment,
        )

    def paired(self, candidate: Path, *extra: str, **environment: str) -> subprocess.CompletedProcess[str]:
        return self.kit(
            "--arm",
            f"baseline={self.baseline}",
            "--arm",
            f"candidate={candidate}",
            "--arm-commit",
            f"baseline={COMMIT_A}",
            "--arm-commit",
            f"candidate={COMMIT_B}",
            "--arm-kind",
            "candidate=native-pgo",
            *extra,
            **environment,
        )

    def calls(self, stub: Path) -> int:
        counter = stub.with_suffix(".calls")
        return int(counter.read_text()) if counter.exists() else 0

    def test_a_failed_hot_try_never_reaches_the_ledger_as_a_hot_figure(self) -> None:
        completed = self.kit(
            "--arm",
            f"shipped={self.baseline}",
            "--arm-commit",
            f"shipped={COMMIT_A}",
            "--rounds",
            "1",
            STUB_FAIL_HOT="1",
        )
        self.assertNotEqual(0, completed.returncode)
        self.assertIn("q00 hot_seconds contains a failed try", completed.stderr)
        self.assertEqual(
            [
                ("q00", "cold_seconds"),
                ("q01", "cold_seconds"),
                ("q01", "hot_seconds"),
                ("suite", "cold_seconds"),
            ],
            [(row["benchmark"], row["metric"]) for row in self.rows()[1:]],
        )

    def test_a_gate_run_stops_on_precision_and_reports_no_regression(self) -> None:
        candidate = self.candidate(1.0)
        completed = self.paired(candidate, SIRIX_BENCH_TIER="gate", SIRIX_BENCH_JUDGE="suite q00")
        self.assertEqual(0, completed.returncode, completed.stderr + completed.stdout)
        self.assertEqual(
            (10, 10), (self.calls(self.baseline), self.calls(candidate)), "ten rounds, not the limit of twenty"
        )
        self.assertIn("stop rule: precision reached after 10 round(s)", completed.stdout)
        self.assertIn("10 evicted round(s) per arm", completed.stdout)
        self.assertIn("outcome: no regression in 4 judged benchmark(s)", completed.stdout)
        self.assertIn("measured outside the plan's judged set: q01", completed.stdout)
        rows = self.rows()
        self.assertEqual(["run"] + ["measurement"] * 120 + ["verdict"] * 4, [row["kind"] for row in rows])
        run = rows[0]
        self.assertEqual([{"dataset": "hits-100m"}] * 124, [row["params"] for row in rows[1:]])
        self.assertEqual(
            ("gate", "clickbench-cold", ["suite", "q00"], 10, 20),
            (run["tier"], run["kit"], run["plan"]["benchmarks"], run["plan"]["min_pairs"], run["plan"]["max_pairs"]),
        )
        self.assertEqual(
            (COMMIT_A, "native", COMMIT_B, "native-pgo"),
            (
                run["arms"]["baseline"]["commit"],
                run["arms"]["baseline"]["binary"],
                run["arms"]["candidate"]["commit"],
                run["arms"]["candidate"]["binary"],
            ),
        )
        third = [
            row
            for row in rows[1:121]
            if row["pair"] == 3 and row["benchmark"] == "suite" and row["metric"] == "cold_seconds"
        ]
        self.assertEqual(
            [("baseline", 1, 3), ("candidate", 2, 3)], [(row["arm"], row["position"], row["process"]) for row in third]
        )
        self.assertAlmostEqual(0.3, third[0]["value"], places=9)
        self.assertEqual({"predicateCounts": 2, "groupDense": 1}, third[0]["counters"])
        self.assertEqual(
            {("suite", "cold_seconds"), ("suite", "hot_seconds"), ("q00", "cold_seconds"), ("q00", "hot_seconds")},
            {(row["benchmark"], row["metric"]) for row in rows[121:]},
        )
        self.assertEqual({(10, "unchanged")}, {(row["pairs"], row["verdict"]) for row in rows[121:]})

    def test_a_gate_run_with_a_slower_candidate_fails(self) -> None:
        completed = self.paired(self.candidate(1.06), SIRIX_BENCH_TIER="gate")
        self.assertEqual(1, completed.returncode, completed.stderr + completed.stdout)
        self.assertIn("outcome: FAIL - 6 regression(s), 0 failed allocation check(s)", completed.stdout)
        self.assertEqual({"regression"}, {row["verdict"] for row in self.rows() if row["kind"] == "verdict"})

    def test_a_minimum_raised_by_the_plan_is_honoured(self) -> None:
        candidate = self.candidate(1.0)
        completed = self.paired(candidate, SIRIX_BENCH_TIER="gate", SIRIX_BENCH_MIN_PAIRS="12")
        self.assertEqual(0, completed.returncode, completed.stderr + completed.stdout)
        self.assertEqual(12, self.calls(candidate))

    def test_a_gate_run_takes_no_round_count(self) -> None:
        completed = self.paired(self.candidate(1.0), "--rounds", "12", SIRIX_BENCH_TIER="gate")
        self.assertNotEqual(0, completed.returncode)
        self.assertIn("a gate run's pair count is set by the stop rule", completed.stderr)
        self.assertEqual(0, self.calls(self.baseline))

    def test_a_gate_run_that_dies_records_its_rounds_and_has_no_verdict(self) -> None:
        candidate = self.candidate(1.0)
        completed = self.paired(candidate, SIRIX_BENCH_TIER="gate", STUB_FAIL_CALL="3")
        self.assertNotEqual(0, completed.returncode)
        self.assertIn("arm 'baseline' failed in round 3", completed.stderr)
        rows = self.rows()
        self.assertEqual(["run"] + ["measurement"] * 24, [row["kind"] for row in rows])
        self.assertEqual({1, 2}, {row["pair"] for row in rows[1:]})
        self.assertNotIn("outcome:", completed.stdout)

    def test_a_control_run_records_its_rounds_without_a_verdict(self) -> None:
        completed = self.paired(self.candidate(1.0), "--rounds", "2")
        self.assertEqual(0, completed.returncode, completed.stderr + completed.stdout)
        rows = self.rows()
        self.assertEqual(["run"] + ["measurement"] * 24, [row["kind"] for row in rows])
        self.assertEqual("control", rows[0]["tier"])
        self.assertNotIn("outcome:", completed.stdout)

    def test_a_single_arm_is_recorded_as_head(self) -> None:
        completed = self.kit(
            "--arm", f"shipped={self.baseline}", "--arm-commit", f"shipped={COMMIT_A}", "--rounds", "2"
        )
        self.assertEqual(0, completed.returncode, completed.stderr + completed.stdout)
        rows = self.rows()
        self.assertEqual(["head"], list(rows[0]["arms"]))
        self.assertEqual({("head", None, None)}, {(row["arm"], row["pair"], row["position"]) for row in rows[1:]})
        self.assertEqual([1, 2], sorted({row["process"] for row in rows[1:]}))

    def test_arms_the_ledger_cannot_name_are_refused_before_any_process_runs(self) -> None:
        other = executable(self.base / "new.py", STUB_CLICKBENCH.format(factor=1.0, deviate=True))
        unnamed = self.kit("--arm", f"old={self.baseline}", "--arm", f"new={other}", "--rounds", "2")
        self.assertNotEqual(0, unnamed.returncode)
        self.assertIn(
            "the results ledger records one single arm, or the two arms baseline and candidate", unnamed.stderr
        )
        shutil.rmtree(self.out)
        uncommitted = self.kit(
            "--arm",
            f"baseline={self.baseline}",
            "--arm",
            f"candidate={other}",
            "--arm-commit",
            f"baseline={COMMIT_A}",
            "--rounds",
            "2",
        )
        self.assertNotEqual(0, uncommitted.returncode)
        self.assertIn("pass --arm-commit candidate=<40-hex>", uncommitted.stderr)
        self.assertEqual((0, 0), (self.calls(self.baseline), self.calls(other)))
        self.assertFalse(self.ledger.exists())

    def test_a_ledger_requires_a_dataset_name_before_any_engine_runs(self) -> None:
        for dataset in (None, "", str(self.database)):
            with self.subTest(dataset=dataset):
                args = ["bash", str(self.script), str(self.database), "--out", str(self.out),
                        "--arm", f"shipped={self.baseline}", "--arm-commit", f"shipped={COMMIT_A}"]
                if dataset is not None:
                    args += ["--dataset", dataset]
                completed = self.run_kit(args, ledger=True)
                self.assertNotEqual(0, completed.returncode)
                self.assertIn("--dataset", completed.stderr)
                self.assertEqual(0, self.calls(self.baseline))
                self.assertFalse(self.ledger.exists())

    def test_without_a_ledger_any_arms_run_as_before(self) -> None:
        other = executable(self.base / "new.py", STUB_CLICKBENCH.format(factor=1.0, deviate=True))
        completed = self.kit("--arm", f"old={self.baseline}", "--arm", f"new={other}", "--rounds", "2", ledger=False)
        self.assertEqual(0, completed.returncode, completed.stderr + completed.stdout)
        self.assertIn("not a protocol result", completed.stdout)
        self.assertIn("2 evicted round(s) per arm", completed.stdout)
        self.assertTrue((self.out / "environment.json").exists())
        self.assertFalse(self.ledger.exists())


if __name__ == "__main__":
    unittest.main()
