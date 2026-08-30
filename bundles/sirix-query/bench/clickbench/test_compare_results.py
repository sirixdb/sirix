#!/usr/bin/env python3
"""Focused regressions for ClickBench result encoding and the strong gate."""

from __future__ import annotations

import contextlib
import decimal
import importlib.util
import io
import json
import math
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
MODULE_PATH = HERE / "compare-results.py"
SPEC = importlib.util.spec_from_file_location("clickbench_compare_results",
                                              MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
compare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compare)

DUCK_MODULE_PATH = HERE / "duckdb_reference.py"
DUCK_SPEC = importlib.util.spec_from_file_location(
    "clickbench_duckdb_reference", DUCK_MODULE_PATH)
assert DUCK_SPEC is not None and DUCK_SPEC.loader is not None
duck = importlib.util.module_from_spec(DUCK_SPEC)
DUCK_SPEC.loader.exec_module(duck)


class StrongReferenceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.shapes = compare.read_query_shapes(HERE / "queries.sql")

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_reference(self, index: int, entries: list[dict]) -> Path:
        path = self.directory / f"q{index:02d}.full.jsonl"
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            for entry in entries:
                handle.write(json.dumps(entry, separators=(",", ":")))
                handle.write("\n")
        return path

    @staticmethod
    def mark(directory: Path) -> None:
        (directory / compare.RESULT_FORMAT_MARKER).write_text(
            compare.RESULT_FORMAT_VERSION + "\n", encoding="utf-8")

    @staticmethod
    def exact_token(value: object) -> list:
        if value is None:
            return ["null"]
        if isinstance(value, bool):
            return ["bool", value]
        if isinstance(value, int):
            return ["int", value]
        if isinstance(value, float):
            return ["float64", (0.0 if value == 0.0 else value).hex()]
        return ["string", value]

    @staticmethod
    def bounded(candidate_id: str, candidate: list[list], duck: list[list],
                matches: list[tuple[list, tuple, int]],
                hidden: list[tuple] | None = None) -> object:
        width = len(duck[0]) if duck else (len(candidate[0]) if candidate else 0)
        exact_duck = [[StrongReferenceTest.exact_token(value) for value in row]
                      for row in duck]
        exact_hidden = (None if hidden is None else [
            tuple(StrongReferenceTest.exact_token(value) for value in key)
            for key in hidden
        ])
        exact_matches = [
            (row, tuple(StrongReferenceTest.exact_token(value) for value in key),
             multiplicity)
            for row, key, multiplicity in matches
        ]
        return compare.BoundedReference(candidate_id, candidate, duck,
                                        exact_duck, width, hidden,
                                        exact_hidden, exact_matches)

    @staticmethod
    def q08_full_rows() -> list[list[int]]:
        # Nine strict keys, then two real RegionIDs tied at the LIMIT boundary.
        return [[region, 11 - region] for region in range(1, 10)] + [
            [100, 1], [101, 1]
        ]

    def test_fabricated_observable_boundary_member_is_rejected(self) -> None:
        full = self.q08_full_rows()
        reference = self.write_reference(8, [{"row": row} for row in full])
        duck = full[:10]
        fabricated = [row[:] for row in duck]
        fabricated[-1] = [999_999, 1]

        weak = compare.judge(8, fabricated, duck, self.shapes[8],
                             ("sirix", "duckdb"))
        self.assertEqual(compare.TIE, weak.status)

        strong = compare.judge(8, fabricated, duck, self.shapes[8],
                               ("sirix", "duckdb"), reference)
        self.assertEqual(compare.MISMATCH, strong.status)
        self.assertIn("not a multiplicity-respecting member", strong.detail)

    def test_real_members_of_an_observable_boundary_tie_are_verified(self) -> None:
        full = self.q08_full_rows()
        reference = self.write_reference(8, [{"row": row} for row in full])
        duck = full[:10]
        sirix = full[:9] + [full[10]]

        verdict = compare.judge(8, sirix, duck, self.shapes[8],
                                ("sirix", "duckdb"), reference)
        self.assertEqual(compare.TIE, verdict.status)
        self.assertFalse(verdict.unverifiable)
        self.assertIn("STRONGLY VERIFIED", verdict.detail)

    def test_bounded_q08_rejects_fabrication_and_accepts_real_tie(self) -> None:
        full = self.q08_full_rows()
        duck = full[:10]
        real_tie = full[:9] + [full[10]]
        matches = [(row, (row[1],), 1) for row in full]
        valid_reference = self.bounded("vectorized", real_tie, duck, matches)
        valid = compare.judge(8, real_tie, duck, self.shapes[8],
                              ("sirix", "duckdb"),
                              bounded_reference=valid_reference)
        self.assertEqual(compare.TIE, valid.status)
        self.assertIn("STRONGLY VERIFIED", valid.detail)

        fabricated = [row[:] for row in duck]
        fabricated[-1] = [999_999, 1]
        invalid_reference = self.bounded("vectorized", fabricated, duck, matches)
        invalid = compare.judge(8, fabricated, duck, self.shapes[8],
                                ("sirix", "duckdb"),
                                bounded_reference=invalid_reference)
        self.assertEqual(compare.MISMATCH, invalid.status)
        self.assertIn("bounded exact relation", invalid.detail)

    def test_q17_accepts_only_real_multiplicity_respecting_group_rows(self) -> None:
        full = [[user, f"phrase-{user}", 1] for user in range(12)]
        reference = self.write_reference(17, [{"row": row} for row in full])
        duck = full[:10]
        other_legal_subset = full[2:12]

        legal = compare.judge(17, other_legal_subset, duck, self.shapes[17],
                              ("sirix", "duckdb"), reference)
        self.assertEqual(compare.TIE, legal.status)
        self.assertFalse(legal.unverifiable)

        fabricated = [[90_000 + i, "fabricated", 777] for i in range(10)]
        invalid = compare.judge(17, fabricated, duck, self.shapes[17],
                                ("sirix", "duckdb"), reference)
        self.assertEqual(compare.MISMATCH, invalid.status)
        self.assertIn("not a multiplicity-respecting member", invalid.detail)

    def test_q17_duplicate_cannot_reuse_one_full_reference_row(self) -> None:
        full = [[user, f"phrase-{user}", 1] for user in range(10)]
        reference = self.write_reference(17, [{"row": row} for row in full])
        duck = full[:]
        duplicated = full[:9] + [full[0]]

        verdict = compare.judge(17, duplicated, duck, self.shapes[17],
                                ("sirix", "duckdb"), reference)
        self.assertEqual(compare.MISMATCH, verdict.status)

    def test_bounded_q17_subset_is_exact_and_multiplicity_respecting(self) -> None:
        full = [[user, f"phrase-{user}", 1] for user in range(12)]
        duck = full[:10]
        legal_subset = full[2:12]
        matches = [(row, (), 1) for row in full]
        reference = self.bounded("generic", legal_subset, duck, matches)
        legal = compare.judge(17, legal_subset, duck, self.shapes[17],
                              bounded_reference=reference)
        self.assertEqual(compare.TIE, legal.status)

        duplicated = full[2:11] + [full[2]]
        duplicate_reference = self.bounded("generic", duplicated, duck, matches)
        invalid = compare.judge(17, duplicated, duck, self.shapes[17],
                                bounded_reference=duplicate_reference)
        self.assertEqual(compare.MISMATCH, invalid.status)

    def test_q24_hidden_event_time_rejects_arbitrary_projected_values(self) -> None:
        entries: list[dict] = []
        for minute in range(9):
            entries.append({
                "row": [f"phrase-{minute}"],
                "key": [f"2013-07-01T00:{minute:02d}:00"],
            })
        boundary = "2013-07-01T00:09:00"
        entries.extend((
            {"row": ["boundary-a"], "key": [boundary]},
            {"row": ["boundary-b"], "key": [boundary]},
        ))
        reference = self.write_reference(24, entries)
        duck = [entry["row"] for entry in entries[:10]]
        sirix = [entry["row"] for entry in entries[:9]] + [["boundary-b"]]

        legal = compare.judge(24, sirix, duck, self.shapes[24],
                              ("sirix", "duckdb"), reference)
        self.assertEqual(compare.TIE, legal.status)
        self.assertFalse(legal.unverifiable)

        arbitrary = [row[:] for row in duck]
        arbitrary[-1] = ["fabricated"]
        invalid = compare.judge(24, arbitrary, duck, self.shapes[24],
                                ("sirix", "duckdb"), reference)
        self.assertEqual(compare.MISMATCH, invalid.status)

    def test_bounded_q24_binds_projected_rows_to_hidden_window_keys(self) -> None:
        keys = [(f"2013-07-01T00:{minute:02d}:00",) for minute in range(9)]
        keys.extend((("2013-07-01T00:09:00",),) * 2)
        full = [[f"phrase-{position}"] for position in range(11)]
        duck = full[:10]
        legal = full[:9] + [full[10]]
        matches = [(row, key, 1) for row, key in zip(full, keys)]
        reference = self.bounded("vectorized", legal, duck, matches, keys[:10])
        verdict = compare.judge(24, legal, duck, self.shapes[24],
                                bounded_reference=reference)
        self.assertEqual(compare.TIE, verdict.status)

        fabricated = [row[:] for row in duck]
        fabricated[-1] = ["fabricated"]
        invalid_reference = self.bounded("vectorized", fabricated, duck,
                                         matches, keys[:10])
        invalid = compare.judge(24, fabricated, duck, self.shapes[24],
                                bounded_reference=invalid_reference)
        self.assertEqual(compare.MISMATCH, invalid.status)

    def test_bounded_oracle_is_bound_to_candidate_bytes_semantics(self) -> None:
        full = self.q08_full_rows()
        duck = full[:10]
        reference = self.bounded(
            "vectorized", duck, duck,
            [(row, (row[1],), 1) for row in full])
        changed = [row[:] for row in duck]
        changed[0][0] += 1
        verdict = compare.judge(8, changed, duck, self.shapes[8],
                                bounded_reference=reference)
        self.assertEqual(compare.MISMATCH, verdict.status)
        self.assertIn("not bound", verdict.detail)

    def test_bounded_float_boundary_uses_lossless_key_if_payload_is_tampered(self) -> None:
        # The current result encoding cannot produce this rounded collision,
        # but the independent raw key token keeps a modified sidecar fail-closed.
        duck = [[1, 1.23457, 100_001]]
        candidate = [[2, 1.23457, 100_001]]
        exact_duck = [[self.exact_token(1),
                       ["float64", float(1.234574).hex()],
                       self.exact_token(100_001)]]
        matches = [
            ([1, 1.23457, 100_001],
             (["float64", float(1.234574).hex()],), 1),
            ([2, 1.23457, 100_001],
             (["float64", float(1.234573).hex()],), 1),
        ]
        reference = compare.BoundedReference(
            "vectorized", candidate, duck, exact_duck, 3, None, None, matches)
        verdict = compare.judge(27, candidate, duck, self.shapes[27],
                                bounded_reference=reference)
        self.assertEqual(compare.MISMATCH, verdict.status)
        self.assertIn("bounded exact relation", verdict.detail)

    def test_bounded_v3_sidecar_parser_preserves_exact_float_tokens(self) -> None:
        raw_key = ["float64", float(1.234574).hex()]
        payload = {
            "format": compare.BOUNDED_REFERENCE_FORMAT,
            "query_index": 27,
            "candidate_id": "vectorized",
            "candidate_rows": [[1, 1.23457, 100_001]],
            "duckdb_rows": [[1, 1.23457, 100_001]],
            "duckdb_exact_rows": [[self.exact_token(1), raw_key,
                                     self.exact_token(100_001)]],
            "row_width": 3,
            "hidden_window_keys": None,
            "hidden_window_exact_keys": None,
            "reference_matches": [{
                "row": [1, 1.23457, 100_001],
                "exact_row": [self.exact_token(1), raw_key,
                              self.exact_token(100_001)],
                "multiplicity": 1,
            }],
        }
        path = self.directory / "q27.oracle-vectorized.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        parsed = compare.read_bounded_reference(
            path, 27, "vectorized", self.shapes[27])
        self.assertEqual((raw_key,), parsed.reference_matches[0][1])

    def test_q26_carries_the_complete_hidden_composite_key(self) -> None:
        shape = self.shapes[26]
        self.assertEqual([False, False], shape.key_descending)
        self.assertEqual("EventTime, SearchPhrase", shape.key_text)
        entries = []
        for index in range(11):
            phrase = f"phrase-{index:02d}"
            event_time = f"2013-07-01T00:{index:02d}:00"
            entries.append({"row": [phrase],
                            "key": [event_time, phrase]})
        reference = self.write_reference(26, entries)
        duck = [entry["row"] for entry in entries[:10]]

        valid = compare.judge(26, duck, duck, shape,
                              ("sirix", "duckdb"), reference)
        self.assertEqual(compare.MATCH, valid.status)

        malformed = self.write_reference(26, [
            {"row": entry["row"], "key": entry["key"][:1]}
            for entry in entries
        ])
        with self.assertRaisesRegex(ValueError, "expected 2"):
            compare.inspect_full_reference(malformed, shape)

    def test_visible_key_must_be_at_the_exact_window_position(self) -> None:
        full = self.q08_full_rows()
        reference = self.write_reference(8, [{"row": row} for row in full])
        duck = full[:10]
        wrong_order = [row[:] for row in duck]
        wrong_order[0], wrong_order[1] = wrong_order[1], wrong_order[0]

        verdict = compare.judge(8, wrong_order, duck, self.shapes[8],
                                ("sirix", "duckdb"), reference)
        self.assertEqual(compare.MISMATCH, verdict.status)
        self.assertIn("exact legal window key", verdict.detail)

    def test_reachable_but_unparseable_schema_fails_closed(self) -> None:
        schema = self.directory / "ClickBenchSchema.java"
        schema.write_text("final class ClickBenchSchema {}", encoding="utf-8")
        message = compare.schema_drift(schema)
        self.assertIsNotNone(message)
        self.assertIn("cannot locate COLUMN_TABLE", message)

    def test_fractional_comparison_is_exact_not_six_digit_or_tolerance_based(self) -> None:
        left = 1920.0001
        right = 1920.0008
        self.assertNotEqual(format(left, ".6g"), repr(left))
        self.assertEqual(format(left, ".6g"), format(right, ".6g"))
        self.assertFalse(compare.values_equal(left, right))

        verdict = compare.judge(3, [[left]], [[right]], self.shapes[3])
        self.assertEqual(compare.MISMATCH, verdict.status)

    def test_one_ulp_is_a_mismatch_but_integral_float_spelling_is_not(self) -> None:
        value = 1.234574
        self.assertFalse(compare.values_equal(value, math.nextafter(value,
                                                                    math.inf)))
        self.assertTrue(compare.values_equal(1666, 1666.0))
        self.assertTrue(compare.values_equal(10**18, float(10**18)))
        self.assertFalse(compare.values_equal(10**18 + 1, float(10**18)))
        self.assertEqual(([], []), compare.multiset_difference(
            [[{"nested": [1666]}]], [[{"nested": [1666.0]}]]))

    def test_bounded_oracle_rejects_wrong_avg_at_the_same_group_identity(self) -> None:
        duck_row = [7, 100, 9, 1920.0008, 5]
        candidate_row = [7, 100, 9, 1920.0001, 5]
        reference = self.bounded(
            "vectorized", [candidate_row], [duck_row],
            [(duck_row, (9,), 1)])

        verdict = compare.judge(9, [candidate_row], [duck_row],
                                self.shapes[9],
                                bounded_reference=reference)
        self.assertEqual(compare.MISMATCH, verdict.status)

    def test_duckdb_writer_keeps_shortest_round_trip_float_and_rejects_decimal(self) -> None:
        values = [1920.0001, 1920.0008, math.nextafter(1.0, math.inf),
                  float.fromhex("0x0.0000000000001p-1022")]
        encodings = []
        for value in values:
            encoded = json.dumps(duck.canon(value), separators=(",", ":"))
            encodings.append(encoded)
            self.assertEqual(value.hex(), float(json.loads(encoded)).hex())
        self.assertEqual(len(values), len(set(encodings)))
        self.assertEqual(7, duck.canon(decimal.Decimal("7.000")))
        with self.assertRaisesRegex(ValueError, "non-integral DuckDB DECIMAL"):
            duck.canon(decimal.Decimal("1.25"))

    def test_result_directory_marker_rejects_legacy_and_wrong_versions(self) -> None:
        legacy = self.directory / "legacy"
        legacy.mkdir()
        (legacy / "q00.jsonl").write_text("[1]\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "non-empty legacy"):
            duck.prepare_result_directory(legacy)

        fresh = self.directory / "fresh"
        duck.prepare_result_directory(fresh)
        self.assertEqual(
            duck.RESULT_FORMAT_VERSION,
            (fresh / duck.RESULT_FORMAT_MARKER).read_text(
                encoding="utf-8").strip())
        duck.prepare_result_directory(fresh)

        wrong = self.directory / "wrong"
        wrong.mkdir()
        (wrong / duck.RESULT_FORMAT_MARKER).write_text("legacy-v1\n",
                                                        encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unsupported result encoding"):
            duck.prepare_result_directory(wrong)

    def test_duckdb_reuse_invalidates_selected_results_and_oracles(self) -> None:
        output = self.directory / "duck-output"
        duck.prepare_result_directory(output)
        selected = ["q00.jsonl", "q00.jsonl.tmp", "q00.full.jsonl",
                    "q00.full.jsonl.tmp", "q00.oracle-old.json",
                    "q00.oracle-old.json.tmp"]
        for name in selected:
            (output / name).write_text("stale", encoding="utf-8")
        (output / "q01.jsonl").write_text("keep", encoding="utf-8")

        duck.invalidate_selected_outputs(output, {0})

        self.assertTrue(all(not (output / name).exists()
                            for name in selected))
        self.assertTrue((output / "q01.jsonl").exists())

    def test_duckdb_jsonl_failure_cleans_partial_and_stale_result(self) -> None:
        output = self.directory / "duck-atomic"
        duck.prepare_result_directory(output)
        result = output / "q03.jsonl"
        result.write_text("[\"stale\"]\n", encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "non-integral DuckDB DECIMAL"):
            duck.write_jsonl(result, [[1], [decimal.Decimal("1.25")]])

        self.assertFalse(result.exists())
        self.assertFalse((output / "q03.jsonl.tmp").exists())

        duck.write_jsonl(result, [[1920.0008]])
        self.assertEqual("[1920.0008]\n", result.read_text(encoding="utf-8"))
        self.assertFalse((output / "q03.jsonl.tmp").exists())

    def test_candidate_reference_rejects_unmarked_legacy_results(self) -> None:
        candidate = self.directory / "candidate"
        candidate.mkdir()
        (candidate / "q00.jsonl").write_text("[1]\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "lacks readable"):
            duck.parse_candidate_references([f"vectorized={candidate}"])
        self.mark(candidate)
        self.assertEqual(candidate, duck.parse_candidate_references(
            [f"vectorized={candidate}"])["vectorized"])

    def test_comparator_rejects_unmarked_legacy_results(self) -> None:
        left = self.directory / "legacy-sirix"
        right = self.directory / "legacy-duckdb"
        left.mkdir()
        right.mkdir()
        error = io.StringIO()
        with contextlib.redirect_stderr(error):
            status = compare.main([str(left), str(right)])
        self.assertEqual(2, status)
        self.assertIn(compare.RESULT_FORMAT_MARKER, error.getvalue())

    def test_strong_cli_requires_every_window_sidecar(self) -> None:
        left = self.directory / "sirix"
        right = self.directory / "duckdb"
        left.mkdir()
        right.mkdir()
        self.mark(left)
        self.mark(right)
        for index, shape in enumerate(self.shapes):
            (left / f"q{index:02d}.jsonl").write_text("", encoding="utf-8")
            (right / f"q{index:02d}.jsonl").write_text("", encoding="utf-8")
            if shape.windowed:
                (right / f"q{index:02d}.full.jsonl").write_text("",
                                                                 encoding="utf-8")

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            status = compare.main(["--strong", str(left), str(right)])
        self.assertEqual(0, status)
        self.assertNotIn("UNVERIFIABLE", output.getvalue())

        (right / "q17.full.jsonl").unlink()
        with contextlib.redirect_stdout(io.StringIO()):
            status = compare.main(["--strong", str(left), str(right)])
        self.assertEqual(2, status)


if __name__ == "__main__":
    unittest.main()
