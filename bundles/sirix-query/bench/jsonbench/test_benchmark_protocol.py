from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


HERE = Path(__file__).resolve().parent


class BenchmarkProtocolTest(unittest.TestCase):
    def test_harness_rejects_a_missing_option_value(self) -> None:
        completed = subprocess.run(
            ["bash", str(HERE / "run-benchmark.sh"), "1m", "database", "reference", "--tries"],
            check=False,
            text=True,
            capture_output=True,
            timeout=2,
        )
        self.assertNotEqual(0, completed.returncode)
        self.assertIn("--tries requires a value", completed.stderr)

    def test_summary_selects_a_complete_round(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            out = root / "results"
            out.mkdir()
            (out / "round-1.json").write_text(json.dumps({"result": [[1.0, 1.5], [10.0, 10.5]]}))
            (out / "round-2.json").write_text(json.dumps({"result": [[10.0, 10.5], [1.0, 1.5]]}))
            baseline = root / "baseline.txt"
            baseline.write_text(textwrap.dedent("""\
                PROTOCOL jsonbench-isolated-v1
                ROUNDS 2
                TRIES 2
                ROUND 1 Q1 1 1
                ROUND 1 Q2 1 1
                ROUND 2 Q1 1 1
                ROUND 2 Q2 1 1
            """))

            completed = subprocess.run(
                [sys.executable, str(HERE / "summarize-results.py"), "1m", str(out), "2", "2",
                 str(baseline), "PASS"],
                check=True,
                text=True,
                capture_output=True,
            )

            self.assertIn("best complete suite round", completed.stdout)
            self.assertIn("Σ |    11000 ms", completed.stdout)
            self.assertNotIn("Σ |     2000 ms", completed.stdout)

    def test_summary_refuses_legacy_and_mismatched_baselines(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            out = root / "results"
            out.mkdir()
            (out / "round-1.json").write_text(json.dumps({"result": [[1.0, 1.5]]}))
            baseline = root / "baseline.txt"

            missing = subprocess.run(
                [sys.executable, str(HERE / "summarize-results.py"), "1m", str(out), "1", "2",
                 str(baseline), "PASS"],
                check=False,
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(0, missing.returncode)
            self.assertIn("missing measured ClickHouse baseline", missing.stderr)

            baseline.write_text("Q1 1 1\n")
            legacy = subprocess.run(
                [sys.executable, str(HERE / "summarize-results.py"), "1m", str(out), "1", "2",
                 str(baseline), "PASS"],
                check=False,
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(0, legacy.returncode)
            self.assertIn("legacy or malformed baseline protocol", legacy.stderr)

            baseline.write_text(textwrap.dedent("""\
                PROTOCOL jsonbench-isolated-v1
                ROUNDS 1
                TRIES 4
                ROUND 1 Q1 1 1
            """))
            mismatch = subprocess.run(
                [sys.executable, str(HERE / "summarize-results.py"), "1m", str(out), "1", "2",
                 str(baseline), "PASS"],
                check=False,
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(0, mismatch.returncode)
            self.assertIn("baseline uses 1 round(s)/4 tries", mismatch.stderr)

            baseline.write_text(textwrap.dedent("""\
                PROTOCOL jsonbench-isolated-v1
                ROUNDS 1
                TRIES 2
                ROUND 1 Q1 1 1
            """))
            for invalid_timing in (-1.0, float("nan")):
                with self.subTest(invalid_timing=invalid_timing):
                    (out / "round-1.json").write_text(
                        json.dumps({"result": [[invalid_timing, 1.5]]})
                    )
                    invalid = subprocess.run(
                        [sys.executable, str(HERE / "summarize-results.py"), "1m", str(out), "1", "2",
                         str(baseline), "PASS"],
                        check=False,
                        text=True,
                        capture_output=True,
                    )
                    self.assertNotEqual(0, invalid.returncode)
                    self.assertIn("invalid timing for Q1", invalid.stderr)

    def test_clickhouse_baseline_uses_the_same_complete_round_protocol(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            corpus = root / "corpus.ndjson"
            corpus.write_text("{}\n")
            events = root / "events.log"
            fake_evictor = root / "evict.py"
            fake_evictor.write_text(textwrap.dedent("""\
                #!/usr/bin/env python3
                import os
                from pathlib import Path
                with Path(os.environ["BENCH_PROTOCOL_EVENTS"]).open("a") as output:
                    output.write("E\\n")
            """))
            fake_evictor.chmod(0o755)
            fake_clickhouse = root / "clickhouse.py"
            fake_clickhouse.write_text(textwrap.dedent("""\
                #!/usr/bin/env python3
                import os
                from pathlib import Path
                import sys

                args = sys.argv[1:]
                query = args[args.index("--query") + 1]
                if "--time" in args:
                    if "SETTINGS session_timezone='UTC'" not in query:
                        print("timed query omitted UTC semantics", file=sys.stderr)
                        raise SystemExit(8)
                    if os.environ.get("FAIL_TIMED_QUERY") == "1":
                        print("deliberate timed-query failure", file=sys.stderr)
                        raise SystemExit(9)
                    with Path(os.environ["BENCH_PROTOCOL_EVENTS"]).open("a") as output:
                        output.write("C\\n")
                    print("0.125", file=sys.stderr)
                elif query.lstrip().startswith("INSERT INTO bluesky"):
                    with Path(os.environ["BENCH_PROTOCOL_EVENTS"]).open("a") as output:
                        output.write("I\\n")
                elif query.strip() == "SELECT count() FROM bluesky":
                    print("10")
            """))
            fake_clickhouse.chmod(0o755)
            fake_sync = root / "sync.py"
            fake_sync.write_text(textwrap.dedent("""\
                #!/usr/bin/env python3
                import os
                from pathlib import Path
                import time

                with Path(os.environ["BENCH_PROTOCOL_EVENTS"]).open("a") as output:
                    output.write("S\\n")
                if os.environ.get("FAIL_SYNC") == "1":
                    raise SystemExit(7)
                time.sleep(float(os.environ.get("SYNC_DELAY_SECONDS", "0")))
            """))
            fake_sync.chmod(0o755)
            environment = os.environ.copy()
            for inherited_name in ("FAIL_SYNC", "FAIL_TIMED_QUERY", "FORCE", "SKIP_BASELINE"):
                environment.pop(inherited_name, None)
            environment.update({
                "BENCH_DURABILITY_SYNC_BIN": str(fake_sync),
                "BENCH_EVICT_PY": str(fake_evictor),
                "BENCH_PROTOCOL_EVENTS": str(events),
                "COOL_TIMEOUT_S": "0",
                "ROUNDS": "2",
                "SYNC_DELAY_SECONDS": "0.2",
                "TRIES": "2",
            })

            completed = subprocess.run(
                ["bash", str(HERE / "clickhouse-setup.sh"), "1m", str(corpus),
                 str(fake_clickhouse), str(root)],
                check=True,
                text=True,
                capture_output=True,
                env=environment,
            )

            load_line = next(line for line in completed.stdout.splitlines() if "load took " in line)
            load_seconds = float(load_line.rsplit(" ", maxsplit=1)[1].removesuffix("s"))
            self.assertGreaterEqual(load_seconds, 0.2)
            self.assertRegex(load_line, r"load took [0-9]+\.[0-9]{3}s$")
            expected = ["I", "S"]
            for _round in range(2):
                for _query in range(5):
                    expected.extend(["E", "C", "C"])
            self.assertEqual(expected, events.read_text().splitlines())
            baseline = (root / "ch-ref-1m" / "baseline.txt").read_text().splitlines()
            self.assertEqual("PROTOCOL jsonbench-isolated-v1", baseline[0])
            self.assertEqual("ROUNDS 2", baseline[1])
            self.assertEqual("TRIES 2", baseline[2])
            self.assertEqual(10, sum(line.startswith("ROUND ") for line in baseline))

            failed_timing_environment = environment.copy()
            failed_timing_environment.update({"FAIL_TIMED_QUERY": "1", "ROUNDS": "1", "TRIES": "1"})
            failed_timing = subprocess.run(
                ["bash", str(HERE / "clickhouse-setup.sh"), "1m", str(corpus),
                 str(fake_clickhouse), str(root)],
                check=False,
                text=True,
                capture_output=True,
                env=failed_timing_environment,
            )
            self.assertNotEqual(0, failed_timing.returncode)
            self.assertIn("cold Q1 exited with status 9", failed_timing.stderr)

            sync_failure_root = root / "sync-failure"
            sync_failure_root.mkdir()
            sync_failure_events = root / "sync-failure-events.log"
            failed_sync_environment = environment.copy()
            failed_sync_environment.update({
                "BENCH_PROTOCOL_EVENTS": str(sync_failure_events),
                "FAIL_SYNC": "1",
                "SKIP_BASELINE": "1",
            })
            failed_sync = subprocess.run(
                ["bash", str(HERE / "clickhouse-setup.sh"), "1m", str(corpus),
                 str(fake_clickhouse), str(sync_failure_root)],
                check=False,
                text=True,
                capture_output=True,
                env=failed_sync_environment,
            )
            self.assertNotEqual(0, failed_sync.returncode)
            self.assertIn("durability sync failed after ClickHouse ingestion", failed_sync.stderr)
            self.assertEqual(["I", "S"], sync_failure_events.read_text().splitlines())

    def test_harness_isolates_every_query_and_measures_fresh_process_tries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "database"
            database.mkdir()
            (database / "page").write_bytes(b"page")
            reference = root / "reference"
            reference.mkdir()
            for query in range(1, 6):
                (reference / f"q{query}.tsv").write_text("")
            (reference / "baseline.txt").write_text(
                "PROTOCOL jsonbench-isolated-v1\nROUNDS 1\nTRIES 2\n"
                + "".join(f"ROUND 1 Q{query} 1 1\n" for query in range(1, 6))
            )
            events = root / "events.log"
            fake_evictor = root / "evict.py"
            fake_evictor.write_text(textwrap.dedent("""\
                #!/usr/bin/env python3
                import os
                from pathlib import Path
                with Path(os.environ["BENCH_PROTOCOL_EVENTS"]).open("a") as output:
                    output.write("E\\n")
            """))
            fake_evictor.chmod(0o755)
            fake_runner = root / "runner.py"
            fake_runner.write_text(textwrap.dedent("""\
                #!/usr/bin/env python3
                import json
                import os
                from pathlib import Path
                import sys

                args = sys.argv[1:]
                query = int(args[args.index("--queries") + 1])
                tries = int(args[args.index("--tries") + 1])
                if tries != 1:
                    raise SystemExit("timed process received more than one try")
                output_path = Path(args[args.index("--json") + 1])
                rows = [[None] for _ in range(5)]
                rows[query - 1] = [float(query)]
                output_path.write_text(json.dumps({"result": rows}))
                with Path(os.environ["BENCH_PROTOCOL_EVENTS"]).open("a") as output:
                    output.write(f"Q{query}\\n")
                print("# served: fake")
            """))
            fake_runner.chmod(0o755)
            output = root / "output"
            environment = os.environ.copy()
            environment.update({
                "BENCH_EVICT_PY": str(fake_evictor),
                "BENCH_PROTOCOL_EVENTS": str(events),
                "OUT": str(output),
                "ROUNDS": "1",
                "TRIES": "2",
                "SKIP_DIFF": "1",
                "COOL_TIMEOUT_S": "0",
            })

            subprocess.run(
                ["bash", str(HERE / "run-benchmark.sh"), "1m", str(database), str(reference),
                 "--bin", str(fake_runner)],
                check=True,
                text=True,
                capture_output=True,
                env=environment,
            )

            expected = []
            for query in range(1, 6):
                expected.extend(["E", f"Q{query}", f"Q{query}"])
            self.assertEqual(expected, events.read_text().splitlines())
            merged = json.loads((output / "round-1.json").read_text())
            self.assertEqual([[float(query), float(query)] for query in range(1, 6)], merged["result"])


if __name__ == "__main__":
    unittest.main()
