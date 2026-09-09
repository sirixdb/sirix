"""Provenance is affirmative: only a leg stating the publication regime reaches the board."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from measurement import steering_log_header
from rank import BOARDS
from rank import PUBLICATION_SCOPE

RIG = Path(__file__).resolve().parent


def suite_log(counts, header=''):
    lines = [f'# q{query} try {attempt}: wall=0.500 s cpu=1.000 s gc=0'
             for query in range(43) for attempt in range(1, counts[query]+1)]
    return header+'\n'.join(lines)+'\n'


def rank(*targets):
    return subprocess.run([sys.executable, str(RIG/'rank.py'), *targets], capture_output=True, text=True)


class PublicationProvenanceTest(unittest.TestCase):
    def convert_and_rank(self, source, tag):
        leg = RIG/'legs'/f'query-{tag}.json'
        self.addCleanup(lambda: leg.unlink(missing_ok=True))
        conversion = subprocess.run([sys.executable, str(RIG/'mkleg.py'), tag, str(source)],
                                    capture_output=True, text=True)
        self.assertEqual(conversion.returncode, 0, conversion.stderr)
        return json.loads(leg.read_text()), rank(tag)

    def convert_text_and_rank(self, text, tag):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            log = Path(directory)/'suite.log'
            log.write_text(text)
            return self.convert_and_rank(log, tag)

    def assertRefused(self, ranking, scope):
        self.assertNotEqual(ranking.returncode, 0)
        self.assertIn(f"rig.scope is {scope!r}", ranking.stderr)
        self.assertNotIn('geomean=', ranking.stdout)

    def test_curated_publication_leg_ranks_and_discloses_its_regime(self):
        ranking = rank('SEG6T')
        self.assertEqual(ranking.returncode, 0, ranking.stderr)
        self.assertIn('geomean=', ranking.stdout)
        regime = json.loads((RIG/'legs/query-SEG6T.json').read_text())['rig']['regime']
        self.assertIn(regime, ranking.stdout)

    def test_publication_leg_without_a_regime_refuses(self):
        leg = RIG/'legs'/'query-RIGTESTNOREGIME.json'
        self.addCleanup(lambda: leg.unlink(missing_ok=True))
        document = json.loads((RIG/'legs/query-SEG6T.json').read_text())
        document['rig'] = {'scope': 'publication'}
        leg.write_text(json.dumps(document))
        ranking = rank('RIGTESTNOREGIME')
        self.assertNotEqual(ranking.returncode, 0)
        self.assertIn('rig.regime is missing', ranking.stderr)
        self.assertNotIn('geomean=', ranking.stdout)

    def test_converted_leg_relabelled_publication_still_refuses(self):
        document, _ = self.convert_text_and_rank(suite_log([3]*43), 'RIGTESTRELABEL')
        leg = RIG/'legs'/'query-RIGTESTPROMOTED.json'
        self.addCleanup(lambda: leg.unlink(missing_ok=True))
        document['rig']['scope'] = 'publication'
        leg.write_text(json.dumps(document))
        ranking = rank('RIGTESTPROMOTED')
        self.assertNotEqual(ranking.returncode, 0)
        self.assertIn('rig.regime is missing', ranking.stderr)
        self.assertNotIn('geomean=', ranking.stdout)

    def test_unmarked_three_try_log_no_longer_ranks(self):
        document, ranking = self.convert_text_and_rank(suite_log([3]*43), 'RIGTESTUNSTATED')
        self.assertEqual(document['rig']['scope'], 'unknown')
        self.assertRefused(ranking, 'unknown')

    def test_committed_pre_cap_thermal_log_converts_to_an_unrankable_leg(self):
        source = RIG/'evidence/thermal-20260908/raw/cool-01.txt'
        document, ranking = self.convert_and_rank(source, 'RIGTESTPRECAP')
        self.assertEqual(len([row for row in document['result'] if row]), 43)
        self.assertEqual(document['rig']['scope'], 'unknown')
        self.assertRefused(ranking, 'unknown')

    def test_committed_launcher_validation_log_converts_to_an_unrankable_leg(self):
        source = RIG/'evidence/harness-validation-20260908/100m-suite.log'
        document, ranking = self.convert_and_rank(source, 'RIGTESTLAUNCHER')
        self.assertEqual(document['rig']['scope'], 'unknown')
        self.assertRefused(ranking, 'unknown')

    def test_committed_thermal_export_refuses_direct_ranking(self):
        export = RIG/'evidence/thermal-20260908/query-baseline-01.json'
        self.assertEqual(json.loads(export.read_text())['rig']['scope'], 'diagnostic')
        self.assertRefused(rank(str(export)), 'diagnostic')

    def test_spliced_leg_refuses_direct_ranking(self):
        self.assertRefused(rank('SEG3TB'), 'composed')

    def test_rig_leg_log_converts_to_an_unrankable_leg(self):
        header = steering_log_header(3)
        document, ranking = self.convert_text_and_rank(suite_log([3]*43, header), 'RIGTESTMARKED')
        self.assertEqual(document['rig']['scope'], 'steering')
        self.assertRefused(ranking, 'steering')

    def test_wider_schedule_log_converts_to_an_unrankable_leg(self):
        counts = [9 if query in (2, 3, 6, 21, 42) else 3 for query in range(43)]
        document, ranking = self.convert_text_and_rank(suite_log(counts), 'RIGTESTWIDER')
        self.assertEqual(document['rig']['scope'], 'steering')
        self.assertEqual(document['rig']['widest_recorded_tries'], 9)
        self.assertRefused(ranking, 'steering')


def curated_publication_legs():
    for leg in sorted((RIG/'legs').glob('query-*.json')):
        document = json.loads(leg.read_text())
        if document.get('rig', {}).get('scope') == PUBLICATION_SCOPE:
            yield leg.name, document


class CuratedLegMetadataTest(unittest.TestCase):
    """A curated leg is a ClickBench submission record: every field the run did not observe is null.

    mkleg.py writes ``machine``, ``load_time`` and ``data_size`` null because a log records timings and
    nothing else; a curated leg keeps that contract, so no leg claims hardware the rig never ran on.
    The board's own machine filter is the consumer of ``machine``: none of these legs was collected on
    a c6a.4xlarge, so the C6A board predicate rank.py applies to board entries must not admit one.
    """

    def test_there_are_curated_publication_legs(self):
        self.assertIn('query-SEG7T.json', dict(curated_publication_legs()))

    def test_curated_legs_do_not_claim_hardware_the_rig_never_observed(self):
        for name, document in curated_publication_legs():
            with self.subTest(leg=name):
                self.assertIsNone(document['machine'], f'{name} claims a machine the run did not observe')
                self.assertFalse(BOARDS['C6A'](document), f'{name} would pass the c6a.4xlarge board filter')

    def test_curated_legs_leave_unobserved_load_fields_null(self):
        for name, document in curated_publication_legs():
            with self.subTest(leg=name):
                self.assertIsNone(document['load_time'], f'{name} carries a load_time no leg run observes')
                self.assertIsNone(document['data_size'], f'{name} carries a data_size no leg run observes')

    def test_curated_leg_date_is_corroborated_by_its_measurement_record(self):
        for name, document in curated_publication_legs():
            with self.subTest(leg=name):
                if document['date'] is not None:
                    self.assertIn(document['date'], document['rig'].get('measured', ''),
                                  f'{name} states a date its rig.measured record does not observe')

    def test_seg7t_ranks_at_the_recorded_standing(self):
        ranking = rank('SEG7T')
        self.assertEqual(ranking.returncode, 0, ranking.stderr)
        lines = ranking.stdout.splitlines()
        hot = next(index for index, line in enumerate(lines) if line.startswith('[C6A    ] hot'))
        self.assertIn('SEG7T      geomean=3.940 rank 15   Σln=58.96', lines[hot+1])


if __name__ == '__main__':
    unittest.main()
