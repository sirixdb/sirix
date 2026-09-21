#!/usr/bin/env python3
"""Fast structural tests for the SH1 benchmark kit."""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
JAVA_ROOT = HERE.parent.parent / "src/main/java/io/sirix/query/bench/bitemporal"


class ProtocolTest(unittest.TestCase):
    def test_sirix_resources_disable_unmeasured_diff_sidecars(self) -> None:
        loader = (JAVA_ROOT / "BitemporalSirixLoadMain.java").read_text()
        readme = (HERE / "README.md").read_text()
        self.assertEqual(3, loader.count(".storeDiffs(false)"))
        self.assertIn("ResourceConfiguration.storeDiffs(false)", readme)
        self.assertIn("diff-sidecar storage is off", readme)

    def test_both_adapters_define_exactly_twelve_queries(self) -> None:
        sirix = (JAVA_ROOT / "BitemporalQueries.java").read_text()
        xtdb = (HERE / "xtdb.clj").read_text()
        self.assertEqual(12, sirix.count("add(queries,"))
        self.assertEqual(12, len(re.findall(r"\{:q (?:[1-9]|1[0-2]) :columns", xtdb)))
        self.assertEqual(7, sirix.count("true,\n        List.of("))

    def test_pinned_xtdb_runtime(self) -> None:
        pom = (HERE / "pom.xml").read_text()
        wrapper = (HERE / "run-xtdb.sh").read_text()
        adapter = (HERE / "xtdb.clj").read_text()
        self.assertRegex(
            pom,
            r"<artifactId>xtdb-core</artifactId>\s*<version>2\.1\.0</version>",
        )
        self.assertIn("/usr/lib/jvm/java-21-openjdk-amd64/bin/java", wrapper)
        self.assertIn("/var/tmp/sirix-bitemporal", wrapper)
        self.assertEqual(4, adapter.count("call-with-close-suppressed"))
        self.assertIn(".addSuppressed", adapter)
        self.assertNotIn("(with-open [node", adapter)

    def test_comparator_writes_manifest_only_for_identical_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = root / "oracle"
            sirix = root / "sirix"
            xtdb = root / "xtdb"
            for directory in (oracle, sirix, xtdb):
                directory.mkdir()
                for query in range(1, 13):
                    (directory / f"q{query}.tsv").write_text(f"{query}\n")
            output = root / "exactness.json"
            subprocess.run(
                [
                    "python3",
                    str(HERE / "compare-results.py"),
                    "--oracle",
                    str(oracle),
                    "--sirix",
                    str(sirix),
                    "--xtdb",
                    str(xtdb),
                    "--out",
                    str(output),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            manifest = json.loads(output.read_text())
            self.assertEqual("PASS", manifest["status"])
            self.assertEqual(12, len(manifest["queries"]))

            (xtdb / "q7.tsv").write_text("wrong\n")
            failed = subprocess.run(
                [
                    "python3",
                    str(HERE / "compare-results.py"),
                    "--oracle",
                    str(oracle),
                    "--sirix",
                    str(sirix),
                    "--xtdb",
                    str(xtdb),
                    "--out",
                    str(output),
                ],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(0, failed.returncode)
            self.assertIn("mismatch q7.tsv row 1", failed.stderr)


if __name__ == "__main__":
    unittest.main()
