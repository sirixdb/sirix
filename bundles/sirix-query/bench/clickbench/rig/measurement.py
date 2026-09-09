"""Strict C6A hot scoring and uncertainty for prespecified paired measurements."""
import json
import math
from pathlib import Path
import re
import statistics

from scipy.optimize import brentq
from scipy.stats import chi2
from scipy.stats import nct
from scipy.stats import t

import rank
from rank import STEERING_LOG_MARKER

TIMING = re.compile(r'^# q(\d+) try (\d+): wall=([0-9.]+) s', re.MULTILINE)
BASE = rank.baseline([row for row in rank.ALL if rank.BOARDS['C6A'](row)])['hot']
MIN_PAIRS = 10
TRIES = 3


def steering_log_header(tries):
    """The provenance line every rig log carries, so no converted leg can reach the published board."""
    return (f'{STEERING_LOG_MARKER} rig protocol: {tries} tries per query; '
            'steering measurement, never a publication leg\n')


def read_timings(path, queries=range(43)):
    expected = {(q, attempt) for q in queries for attempt in range(1, TRIES+1)}
    found = {}
    for query, attempt, wall in TIMING.findall(Path(path).read_text()):
        key = int(query), int(attempt)
        value = float(wall)
        if key not in expected or key in found or not math.isfinite(value) or value < 0:
            raise ValueError(f'invalid, duplicate or unexpected timing {key} in {path}')
        found[key] = value
    if found.keys() != expected:
        raise ValueError(f'incomplete leg {path}: missing {sorted(expected-found.keys())}')
    return found


def score_result(result, base=None):
    if base is None:
        base = BASE
    if (not isinstance(base, list) or len(base) != 43
            or any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in base)):
        raise ValueError('the recorded board requires 43 finite nonnegative hot bests')
    if not isinstance(result, list) or len(result) != 43:
        raise ValueError('a measurement requires all 43 queries')
    for row in result:
        if not isinstance(row, list) or len(row) != 3:
            raise ValueError('every scored query requires exactly three tries')
        if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in row):
            raise ValueError('timings must be finite nonnegative numbers, without missing answers')
    geomean, contributions = rank.score(result, 'hot', base)
    hot = [rank.sel(row, 'hot') for row in result]
    return dict(sum_ln=sum(row[0] for row in contributions), geomean=geomean,
                hot_seconds=sum(hot), hot=hot, ln=[row[0] for row in contributions])


def detection_power(count, effect, sd):
    if count < 2 or effect <= 0 or sd <= 0 or not math.isfinite(effect+sd):
        raise ValueError('power requires at least two pairs and finite positive effect and SD')
    cutoff = t.ppf(.975, count-1)
    shift = effect*math.sqrt(count)/sd
    # Symmetry avoids a SciPy negative-x CDF NaN for vanishing lower tails.
    power = float(nct.sf(cutoff, count-1, shift)+nct.sf(cutoff, count-1, -shift))
    if not math.isfinite(power) or not 0 <= power <= 1:
        raise ArithmeticError('noncentral-t power evaluation failed; no resolution claim is possible')
    return power


def required_pairs(effect, sd, limit=1_000_000):
    low = high = 2
    while detection_power(high, effect, sd) < .8:
        high *= 2
        if high > limit:
            return None
    while low < high:
        middle = (low+high)//2
        if detection_power(middle, effect, sd) >= .8:
            high = middle
        else:
            low = middle+1
    # The collection plan balances AB/BA in blocks of two pairs.
    return 2*math.ceil(max(MIN_PAIRS, low)/2)


def uncertainty(differences, target):
    if not math.isfinite(target) or target <= 0:
        raise ValueError('requested effect must be finite and positive')
    if not differences or any(not math.isfinite(value) for value in differences):
        raise ValueError('paired differences must be nonempty and finite')
    count = len(differences)
    mean = statistics.mean(differences)
    answer = dict(pairs=count, benefit_ln=mean, target_ln=target, confidence=.95, planning_power=.8,
                  ci95=None, half_width_ln=None, detectable_ln_80pct=None, required_pairs=None,
                  conservative_required_pairs=None, resolution='UNRESOLVED')
    if count < 2:
        answer['reason'] = 'one pair cannot estimate measurement noise'
        return answer
    sd = statistics.stdev(differences)
    answer['paired_sd_ln'] = sd
    if sd <= 1e-12:
        answer['reason'] = 'zero observed spread cannot establish a noise floor from rounded timings'
        return answer
    half = float(t.ppf(.975, count-1)*sd/math.sqrt(count))
    upper_effect = 1.
    while detection_power(count, upper_effect, 1) < .8:
        upper_effect *= 2
    detectable = sd*brentq(lambda standardized: detection_power(count, standardized, 1)-.8, 1e-9, upper_effect)
    upper_sd = float(sd*math.sqrt((count-1)/chi2.ppf(.05, count-1)))
    answer.update(ci95=[mean-half, mean+half], half_width_ln=half, detectable_ln_80pct=detectable,
                  required_pairs=required_pairs(target, sd), conservative_required_pairs=required_pairs(target, upper_sd),
                  paired_sd_upper95_ln=upper_sd, interval_excludes_zero=mean-half > 0 or mean+half < 0)
    if count < MIN_PAIRS:
        answer['reason'] = f'fewer than {MIN_PAIRS} pairs: noise and power estimates are provisional'
    elif detectable > target:
        answer['reason'] = 'requested effect is smaller than the estimated 80%-power resolution at this repetition count'
    else:
        answer.update(resolution='TARGET RESOLVED UNDER THE NOISE MODEL', reason='actual change remains subject to its confidence interval')
    answer['assumptions'] = 'Prespecified count, independent approximately normal whole-pair differences, stable conditions. Planning estimates are not guarantees; do not extend until significant.'
    return answer


def analyze_pairs(pairs, *, planned_pairs, target):
    if planned_pairs < 2 or len(pairs) != planned_pairs:
        raise ValueError('the complete prespecified paired series is required; partial series are unscorable')
    if planned_pairs % 2 or [pair['order'] for pair in pairs].count('AB') != planned_pairs//2:
        raise ValueError('the series must contain balanced AB/BA pairs')
    protocol = None
    run_ids = set()
    arm_runtimes = {}
    scored = []
    envelopes = []
    for pair in pairs:
        if pair['order'] not in ('AB', 'BA'):
            raise ValueError('invalid paired run order')
        arms = []
        for name in ('baseline', 'candidate'):
            document = pair[name]
            metadata = document['rig']
            if metadata.get('scope') != 'steering':
                raise ValueError('only complete steering legs enter this report; diagnostic timings cannot score')
            if metadata.get('complete') is not True:
                raise ValueError('failed or incomplete legs cannot enter a paired report')
            runtime_id = metadata['runtime_id']
            if not isinstance(runtime_id, str) or not runtime_id:
                raise ValueError('each leg must identify its frozen runtime')
            if name in arm_runtimes and arm_runtimes[name] != runtime_id:
                raise ValueError('a measured arm changed runtimes during the paired series')
            arm_runtimes[name] = runtime_id
            run_id = metadata['run_id']
            if run_id in run_ids:
                raise ValueError('a leg was reused; repeated files are not independent repetitions')
            run_ids.add(run_id)
            if protocol is None:
                protocol = metadata['protocol']
            elif protocol != metadata['protocol']:
                raise ValueError('recorded protocols differ; do not pool these legs')
            envelopes.append(metadata.get('observed_power'))
            arms.append(score_result(document['result'], metadata['protocol'].get('board_best_hot')))
        scored.append(arms)
    result = summarize_scores(scored, pairs, target)
    result['observed_power'] = summarize_envelopes(envelopes)
    result.update(protocol=protocol, scope='steering', arm_runtimes=arm_runtimes)
    return result


def summarize_envelopes(envelopes):
    """The pinned limits are gated; the platform-managed ones are only observed, so report them."""
    legs = len(envelopes)
    unrecorded = sum(1 for envelope in envelopes if not isinstance(envelope, dict))
    changes = sum(len(envelope.get('changes') or []) for envelope in envelopes if isinstance(envelope, dict))
    legs_that_moved = sum(1 for envelope in envelopes
                          if isinstance(envelope, dict) and envelope.get('changes'))
    distinct = sorted({json.dumps(envelope.get('start'), sort_keys=True) for envelope in envelopes
                       if isinstance(envelope, dict) and envelope.get('start')})
    return dict(legs=legs, legs_without_a_power_record=unrecorded, legs_with_a_limit_change=legs_that_moved,
                limit_changes=changes, distinct_starting_domain_readings=len(distinct))


def summarize_scores(scored, pairs, target):
    differences = [a['sum_ln']-b['sum_ln'] for a, b in scored]
    result = uncertainty(differences, target)
    result.update(paired_differences_ln=differences,
                  baseline_mean_sum_ln=statistics.mean(a['sum_ln'] for a, _ in scored),
                  candidate_mean_sum_ln=statistics.mean(b['sum_ln'] for _, b in scored),
                  baseline_mean_hot_seconds=statistics.mean(a['hot_seconds'] for a, _ in scored),
                  candidate_mean_hot_seconds=statistics.mean(b['hot_seconds'] for _, b in scored),
                  mean_seconds_saved=statistics.mean(a['hot_seconds']-b['hot_seconds'] for a, b in scored))
    query_rows = []
    for q in range(43):
        query_rows.append(dict(query=q,
            baseline_hot_s=statistics.mean(a['hot'][q] for a, _ in scored),
            candidate_hot_s=statistics.mean(b['hot'][q] for _, b in scored),
            baseline_ln=statistics.mean(a['ln'][q] for a, _ in scored),
            candidate_ln=statistics.mean(b['ln'][q] for _, b in scored),
            benefit_ln=statistics.mean(a['ln'][q]-b['ln'][q] for a, b in scored),
            paired_delta_sd=statistics.stdev(a['ln'][q]-b['ln'][q] for a, b in scored)))
    result['queries'] = sorted(query_rows, key=lambda row: row['benefit_ln'])
    if not math.isclose(sum(row['benefit_ln'] for row in query_rows), result['benefit_ln'], abs_tol=1e-10):
        raise ArithmeticError('query contributions do not sum to the reported suite effect')
    result['order_means_ln'] = {
        order: statistics.mean(delta for pair, delta in zip(pairs, differences) if pair['order'] == order)
        for order in ('AB', 'BA')}
    return result


def render_report(result):
    lines = ['C6A min(try2,try3)', '',
              f"{result['resolution']}: {result['reason']}", '',
              f"Prespecified paired repetitions: {result['pairs']}. Target effect: {result['target_ln']:.3f} ln."]
    if result['ci95'] is None:
        lines.append(f"Descriptive mean benefit {result['benefit_ln']:+.3f} ln; a confidence interval is unavailable.")
    else:
        low, high = result['ci95']
        lines.append(f"Observed benefit {result['benefit_ln']:+.3f} ± {result['half_width_ln']:.3f} ln "
                     f"(nominal 95% paired t interval [{low:+.3f}, {high:+.3f}]). Positive means the candidate improved.")
        lines.append(f"Estimated 80%-power detectable effect: {result['detectable_ln_80pct']:.3f} ln. "
                     f"Estimated required pairs for the target: {result['required_pairs']}; "
                     f"using the upper 95% noise bound: {result['conservative_required_pairs']}.")
    if 'baseline_mean_sum_ln' in result:
        lines += ['', '| Measure (mean per leg) | Baseline | Candidate | Benefit |',
                  '|---|---:|---:|---:|',
                  f"| Sum-ln | {result['baseline_mean_sum_ln']:.6f} | {result['candidate_mean_sum_ln']:.6f} | {result['benefit_ln']:+.6f} ln |",
                  f"| Total suite hot seconds | {result['baseline_mean_hot_seconds']:.6f} | {result['candidate_mean_hot_seconds']:.6f} | {result['mean_seconds_saved']:+.6f} s |", '',
                  'Per-query means, sorted with score regressions first. The ln columns are means of per-leg contributions, not scores computed from averaged times.', '',
                  '| Query | Baseline hot s | Candidate hot s | Baseline ln | Candidate ln | Benefit ln |',
                  '|---|---:|---:|---:|---:|---:|']
        for row in result['queries']:
            lines.append(f"| q{row['query']} | {row['baseline_hot_s']:.6f} | {row['candidate_hot_s']:.6f} | "
                         f"{row['baseline_ln']:.6f} | {row['candidate_ln']:.6f} | {row['benefit_ln']:+.6f} |")
    if 'observed_power' in result:
        power = result['observed_power']
        lines += ['', f"Power envelope: the pinned MSR limits were verified on every leg. The platform-managed "
                      f"limits, which the rig does not set, were recorded on "
                      f"{power['legs']-power['legs_without_a_power_record']} of {power['legs']} legs, showed "
                      f"{power['distinct_starting_domain_readings']} distinct starting reading(s), and moved "
                      f"mid-run on {power['legs_with_a_limit_change']} leg(s) ({power['limit_changes']} change(s)). "
                      'The effective envelope is the lower of the pinned and platform-managed limits, so these are '
                      'the conditions this effect was measured under, not a validity verdict. Both arms of a pair '
                      'run back to back and AB/BA order is balanced, which mitigates slow drift; drift unrelated to '
                      'the candidate still enlarges the paired SD and pushes toward UNRESOLVED, the conservative '
                      'direction. That is not a proof of no bias for a change correlated with the arms.']
    lines += ['', 'These are steering measurements. Preserve protocol labels; a leg must never be mixed with full-suite history or published board results unless its provenance says so.', '',
              result.get('assumptions', 'More independent repetitions are required to estimate noise.'), '']
    return '\n'.join(lines)
