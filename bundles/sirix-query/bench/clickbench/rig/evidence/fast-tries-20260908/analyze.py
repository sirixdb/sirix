"""Analyze the fixed fast33 cohort; never a full-suite score or causal experiment."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import statistics as stats
import sys

HERE = Path(__file__).resolve().parent
ROOT = next(parent for parent in HERE.parents if (parent/'bundles/sirix-query/bench/clickbench/rig/rank.py').is_file())
RIG = ROOT/'bundles/sirix-query/bench/clickbench/rig'
sys.path[:0] = [str(HERE/'analysis-packages'), str(RIG)]
from measurement import BASE
from measurement import uncertainty

PATTERN = re.compile(r'^# q(\d+) try (\d+): wall=([\d.]+) s cpu=([\d.]+) s util=([\d.]+)/\d+ gc=(\d+) pauses ([\d.]+) s', re.M)


def describe(values):
    return dict(n=len(values), mean=stats.mean(values), sample_sd=stats.stdev(values),
                min=min(values), max=max(values), spread=max(values)-min(values))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw-root', type=Path, default=HERE)
    parser.add_argument('--out', type=Path, default=HERE/'fast33-study-final')
    args = parser.parse_args()
    plan = json.loads((args.raw_root/'fast-tries-plan.json').read_text())
    queries = plan['queries']
    count, tries = plan['planned_processes'], plan['tries_per_query']
    legs, raw, resources = [], [], []
    expected = {(q, attempt) for q in queries for attempt in range(1, tries+1)}
    for index in range(1, count+1):
        path = args.raw_root/f'fast33-{index:02}'
        verdict = json.loads((path/'verdict.json').read_text())
        assert verdict['exit_code'] == 0 and not verdict['issues'], path
        protocol = json.loads((path/'protocol.json').read_text())
        assert protocol['queries'] == queries and protocol['tries'] == tries, path
        assert protocol['power_uw'] == 50000000 and protocol['cache'] == 'natural; no eviction', path
        content = (path/'suite100m.log').read_text()
        rows = PATTERN.findall(content)
        assert len(rows) == len(expected) and {(int(q), int(a)) for q, a, *_ in rows} == expected, path
        found = {(int(q), int(a)): float(wall) for q, a, wall, *_ in rows}
        assert all(math.isfinite(value) and value > 0 for value in found.values()), path
        resource = {(int(q), int(a)): [float(cpu), float(util), int(gc), float(pause)]
                    for q, a, _, cpu, util, gc, pause in rows}
        samples = [json.loads(line) for line in (path/'telemetry.jsonl').read_text().splitlines()]
        assert samples and all(int(s['values'][f'/sys/class/powercap/intel-rapl:0/constraint_{i}_power_limit_uw']) == 50000000
                               for s in samples for i in (0, 1)), path
        raw.append([[found[q, a] for a in range(1, tries+1)] for q in queries])
        resources.append([[resource[q, a] for a in range(1, tries+1)] for q in queries])
        cooling = [json.loads(line) for line in (path/'cooling.jsonl').read_text().splitlines()]
        legs.append(dict(tag=path.name, duration_s=verdict['duration_s'],
                         cooldown_s=cooling[-1]['time']-cooling[0]['time'],
                         log_sha256=hashlib.sha256(content.encode()).hexdigest()))
    curves = []
    for k in plan['checkpoints']:
        for estimator in ('minimum', 'median', 'tail_median'):
            hot = []
            for leg in raw:
                selected = []
                for attempts in leg:
                    values = attempts[1:k]
                    if estimator == 'tail_median':
                        values = values[len(values)//2:]
                    selected.append(min(values) if estimator == 'minimum' else stats.median(values))
                hot.append(selected)
            ln = [[math.log((.01+value)/(.01+BASE[q])) for q, value in zip(queries, leg)] for leg in hot]
            totals = [sum(leg) for leg in ln]
            delta = [a-b for a, b in zip(totals[::2], totals[1::2])]
            query_rows = [dict(query=q, hot=describe([leg[j] for leg in hot]),
                               ln=describe([leg[j] for leg in ln])) for j, q in enumerate(queries)]
            curves.append(dict(tries=k, estimator=estimator, component_ln=describe(totals),
                               selected_seconds=describe([sum(leg) for leg in hot]),
                               warm_try_execution_cost_s=describe([sum(sum(a[1:k]) for a in leg) for leg in raw]),
                               queries=query_rows, adjacent_null_pairs=uncertainty(delta, .5)))
    attempt_rows = []
    for j, q in enumerate(queries):
        for attempt in range(tries):
            wall = [leg[j][attempt] for leg in raw]
            attempt_rows.append(dict(query=q, attempt=attempt+1, **describe(wall),
                                     gc_affected_legs=sum(leg[j][attempt][2] > 0 for leg in resources)))
    result = dict(plan=plan, scope='five-query component only; never a 43-query C6A score', legs=legs,
                  raw_tries=raw, resources=resources, curves=curves, attempts=attempt_rows,
                  caveat='Chronological unchanged-code null pairs; nominal normal-model intervals and power. Multiple estimators and prefix lengths are descriptive. All preceding selected queries had 33 tries, so prefixes do not simulate a global k-try protocol. No full-suite resolution claim.')
    out = args.out
    out.mkdir(exist_ok=False)
    (out/'summary.json').write_text(json.dumps(result, indent=2, allow_nan=False)+'\n')
    with (out/'attempts.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(attempt_rows[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(attempt_rows)
    text = [result['scope'], result['caveat'], '',
            '| Tries | Estimator | Component ln SD | Range ln | AA MDE80 ln | AA 95% halfwidth | Warm execution s |',
            '|---:|---|---:|---:|---:|---:|---:|']
    for curve in curves:
        pair = curve['adjacent_null_pairs']
        text.append(f"| {curve['tries']} | {curve['estimator']} | {curve['component_ln']['sample_sd']:.6f} | {curve['component_ln']['spread']:.6f} | {pair['detectable_ln_80pct']:.6f} | {pair['half_width_ln']:.6f} | {curve['warm_try_execution_cost_s']['mean']:.6f} |")
    text += ['', 'Minimum at k=3 is the historical min(try2,try3) projection for these five queries, recorded separately. All other rows are alternative internal estimators.', '']
    (out/'report.md').write_text('\n'.join(text))
    print('\n'.join(text))


if __name__ == '__main__':
    main()
