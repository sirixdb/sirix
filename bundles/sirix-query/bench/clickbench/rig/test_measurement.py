"""Statistical and provenance regressions; no database or JVM is launched."""
import copy
import json
import math
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import unittest

from measurement import analyze_pairs
from measurement import detection_power
from measurement import read_timings
from measurement import render_report
from measurement import score_result
from measurement import uncertainty


def pairs_with_deltas(deltas):
    pairs = []
    for index, delta in enumerate(deltas):
        pair = {'order': 'AB' if index % 2 == 0 else 'BA'}
        for arm in ('baseline', 'candidate'):
            result = [[1., 1., 2.] for _ in range(43)]
            if arm == 'candidate':
                # Shift one query's contribution by a known amount, including the +.01 offset.
                result[0] = [1., 1.01*math.exp(-delta)-.01, 20.]
            pair[arm] = dict(result=result, rig=dict(scope='steering', complete=True,
                run_id=f'{index}-{arm}', runtime_id=arm, protocol={'cool_below_C': 55}))
        pairs.append(pair)
    return pairs


def divergent_pairs(count):
    """A candidate that removes real wall-clock seconds from the heaviest query while one sub-10 ms
    query gets slower. The +0.01 s offset and the geometric mean over 43 queries punish that trade,
    so seconds and score must be reported together or a lane optimises seconds and loses ln."""
    pairs = []
    for index in range(count):
        pair = {'order': 'AB' if index % 2 == 0 else 'BA'}
        drift = 1+index/1000
        for arm in ('baseline', 'candidate'):
            hot = [1.]*43
            hot[0], hot[1] = (.001, 10.) if arm == 'baseline' else (.1, 8.)
            hot = [value*drift for value in hot]
            pair[arm] = dict(result=[[value*1.6, value, value] for value in hot],
                             rig=dict(scope='steering', complete=True, run_id=f'{index}-{arm}',
                                      runtime_id=arm, protocol={'cool_below_C': 55}))
        pairs.append(pair)
    return pairs


class MeasurementTest(unittest.TestCase):
    def test_cli_replay_reports_the_single_metric_and_rank_refuses_a_steering_leg(self):
        pairs = pairs_with_deltas([.1, .2]*5)
        plan = dict(orders=[pair['order'] for pair in pairs], planned_pairs=10, target_ln=.5,
                    protocol=pairs[0]['baseline']['rig']['protocol'],
                    baseline_runtime={'runtime_id': 'baseline'}, candidate_runtime={'runtime_id': 'candidate'})
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            output = Path(directory)
            (output/'pairs.json').write_text(json.dumps(pairs))
            (output/'plan.json').write_text(json.dumps(plan))
            cli = subprocess.run([sys.executable, str(Path(__file__).parent/'measure.py'), 'analyze', directory],
                                 capture_output=True, text=True)
            self.assertEqual(cli.returncode, 0, cli.stderr)
            self.assertTrue(cli.stdout.startswith('C6A min(try2,try3)'), cli.stdout[:200])
            self.assertEqual(cli.stdout.count('Total suite hot seconds'), 1)
            (output/'leg.json').write_text(json.dumps(pairs[0]['baseline']))
            ranking = subprocess.run([sys.executable, str(Path(__file__).parent/'rank.py'), str(output/'leg.json')],
                                     capture_output=True, text=True)
            self.assertNotEqual(ranking.returncode, 0)
            self.assertIn("rig.scope is 'steering'", ranking.stderr)

    def test_every_query_needs_exactly_three_recorded_tries(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory)/'suite.log'
            lines = [f'# q{q} try {attempt}: wall=0.123 s cpu=1 s\n'
                     for q in range(43) for attempt in (1, 2, 3)]
            path.write_text(''.join(lines))
            self.assertEqual(len(read_timings(path)), 129)
            for bad in (''.join(lines)+'# q2 try 4: wall=0.5 s\n',
                        ''.join(line for line in lines if not line.startswith('# q2 try 3:'))):
                path.write_text(bad)
                with self.assertRaises(ValueError):
                    read_timings(path)

    def test_report_surfaces_the_observed_power_envelope(self):
        pairs = pairs_with_deltas([.1, .2]*5)
        pairs[0]['baseline']['rig']['observed_power'] = dict(
            start={'mmio': '45000000'}, changes=[{'monotonic': 1.}, {'monotonic': 2.}])
        pairs[0]['candidate']['rig']['observed_power'] = dict(start={'mmio': '76000000'}, changes=[])
        result = analyze_pairs(pairs, planned_pairs=10, target=.5)
        power = result['observed_power']
        self.assertEqual(power['legs'], 20)
        self.assertEqual(power['legs_without_a_power_record'], 18)
        self.assertEqual(power['legs_with_a_limit_change'], 1)
        self.assertEqual(power['limit_changes'], 2)
        self.assertEqual(power['distinct_starting_domain_readings'], 2)
        report = render_report(result)
        self.assertIn('Power envelope:', report)
        self.assertIn('moved mid-run on 1 leg(s) (2 change(s))', report)
        self.assertIn('recorded on 2 of 20 legs', report)

    def test_archived_board_controls_contributions_without_changing_paired_effect(self):
        pairs = pairs_with_deltas([.1, .2]*5)
        for pair in pairs:
            for arm in ('baseline', 'candidate'):
                pair[arm]['rig']['protocol']['board_best_hot'] = [1.]*43
        result = analyze_pairs(pairs, planned_pairs=10, target=.5)
        self.assertAlmostEqual(result['baseline_mean_sum_ln'], 0.)
        self.assertAlmostEqual(result['benefit_ln'], .15)
        with self.assertRaises(ValueError):
            score_result([[1., 1., 2.]]*43, [0.]*42)

    def test_historical_c6a_golden_score(self):
        document = json.loads((Path(__file__).parent/'legs/query-SEG6T.json').read_text())
        score = score_result(document['result'])
        self.assertAlmostEqual(score['sum_ln'], 63.442089, places=5)
        self.assertAlmostEqual(math.log(score['geomean'])*43, score['sum_ln'], places=10)

    def test_only_second_and_third_tries_define_hot(self):
        result = [[.001, 3., 2.] for _ in range(43)]
        self.assertEqual(score_result(result)['hot_seconds'], 86.)
        for invalid in ([*result, [1., 2., 3.]], result[:-1], [[1., 2., 3., .01]]*43,
                        [[1., None, 3.]]*43, [[1., float('nan'), 3.]]*43):
            with self.assertRaises(ValueError):
                score_result(invalid)

    def test_pair_uncertainty_and_contributions(self):
        deltas = [.1, .2, .3, .4, .5, .6, .7, .8, .9, 1.]
        result = analyze_pairs(pairs_with_deltas(deltas), planned_pairs=10, target=.5)
        self.assertAlmostEqual(result['benefit_ln'], statistics.mean(deltas), places=10)
        self.assertAlmostEqual(result['paired_sd_ln'], statistics.stdev(deltas), places=10)
        self.assertAlmostEqual(sum(row['benefit_ln'] for row in result['queries']), result['benefit_ln'])
        self.assertEqual(result['queries'][-1]['query'], 0)
        self.assertEqual(result['required_pairs'] % 2, 0)

    def test_a_faster_candidate_that_scores_worse_is_reported_as_a_regression(self):
        result = analyze_pairs(divergent_pairs(10), planned_pairs=10, target=.5)
        self.assertGreater(result['mean_seconds_saved'], 1.5)
        self.assertLess(result['benefit_ln'], -1.5)
        self.assertEqual(result['queries'][0]['query'], 0)
        self.assertLess(result['queries'][0]['benefit_ln'], -2)
        self.assertEqual(result['queries'][-1]['query'], 1)
        self.assertGreater(result['queries'][-1]['benefit_ln'], 0)
        rendered = {}
        order = []
        for line in render_report(result).splitlines():
            if not line.startswith('| '):
                continue
            cells = [cell.strip() for cell in line.strip('|').split('|')]
            if cells[0].startswith('q'):
                order.append(cells[0])
            else:
                rendered[cells[0]] = cells[-1]
        self.assertEqual((order[0], order[-1]), ('q0', 'q1'))
        self.assertTrue(rendered['Sum-ln'].startswith('-'), rendered)
        self.assertTrue(rendered['Total suite hot seconds'].startswith('+'), rendered)

    def test_complete_matching_independent_protocol_required(self):
        original = pairs_with_deltas([.1, .2]*5)
        alterations = [lambda p: p.pop(),
                       lambda p: p[0]['candidate']['rig'].update(complete=False),
                       lambda p: p[0]['candidate']['rig'].update(scope='diagnostic'),
                       lambda p: p[1]['candidate']['rig'].update(protocol={'cool_below_C': 65}),
                       lambda p: p[1]['candidate']['rig'].update(runtime_id='third-build'),
                       lambda p: p[1]['candidate']['rig'].update(run_id='0-candidate'),
                       lambda p: p[0].update(order='BA')]
        for change in alterations:
            pairs = copy.deepcopy(original)
            change(pairs)
            with self.assertRaises(ValueError):
                analyze_pairs(pairs, planned_pairs=10, target=.5)

    def test_small_samples_and_rounded_constant_data_are_unresolved(self):
        for differences in ([.5], [.5, .6], [.5]*10, [.5+index*1e-15 for index in range(10)]):
            self.assertEqual(uncertainty(differences, .5)['resolution'], 'UNRESOLVED')
        result = uncertainty([(-1)**i for i in range(10)], .5)
        self.assertEqual(result['resolution'], 'UNRESOLVED')
        self.assertGreater(result['required_pairs'], 10)

    def test_large_noncentral_t_tail_remains_finite(self):
        self.assertAlmostEqual(detection_power(10, 1000, 1), 1.)
        self.assertGreater(detection_power(100, .5, 1), detection_power(10, .5, 1))

    def test_whole_pair_cancellation_preserves_covariance(self):
        pairs = pairs_with_deltas([.1, .2, .3, .4, .5]*2)
        for pair, delta in zip(pairs, [.1, .2, .3, .4, .5]*2):
            pair['candidate']['result'][1] = [1., 1.01*math.exp(delta)-.01, 20.]
        result = analyze_pairs(pairs, planned_pairs=10, target=.5)
        self.assertGreater(max(row['paired_delta_sd'] for row in result['queries']), .1)
        self.assertLess(result['paired_sd_ln'], 1e-12)
        self.assertEqual(result['resolution'], 'UNRESOLVED')

    def test_partial_and_duplicate_raw_logs_refuse_scoring(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory)/'suite.log'
            good = ''.join(f'# q{q} try {attempt}: wall=0.123 s cpu=1 s\n'
                           for q in range(43) for attempt in (1, 2, 3))
            path.write_text(good)
            self.assertEqual(len(read_timings(path)), 129)
            for bad in ('\n'.join(good.splitlines()[:-1]), good+good.splitlines()[0]+'\n'):
                path.write_text(bad)
                with self.assertRaises(ValueError):
                    read_timings(path)


if __name__ == '__main__':
    unittest.main()
