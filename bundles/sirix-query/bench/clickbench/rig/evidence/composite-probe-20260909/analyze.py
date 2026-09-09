"""Replay the complete paired study and its prespecified four-query contrast; no JVM."""
import csv
import json
from pathlib import Path
import statistics
import sys

RIG = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RIG))

from measure import validate_plan
from measurement import analyze_pairs
from measurement import score_result
from measurement import uncertainty

FAMILY = (16, 18, 31, 32)


def summarize(differences, seconds, target):
    result = uncertainty(differences, target)
    mean = statistics.mean(differences)
    observed = uncertainty(differences, abs(mean)) if mean else None
    result.update(paired_differences_ln=differences,
                  mean_seconds_saved=statistics.mean(seconds),
                  observed_magnitude_resolution=observed)
    result['resolved_positive'] = bool(
        observed and mean > 0 and observed['ci95'] and observed['ci95'][0] > 0
        and observed['resolution'] != 'UNRESOLVED')
    return result


def main(directory):
    path = Path(directory)
    plan = json.loads((path / 'plan.json').read_text())
    pairs = json.loads((path / 'pairs.json').read_text())
    validate_plan(pairs, plan)
    suite = analyze_pairs(pairs, planned_pairs=plan['planned_pairs'], target=plan['target_ln'])
    scores = [(score_result(pair['baseline']['result'], plan['protocol']['board_best_hot']),
               score_result(pair['candidate']['result'], plan['protocol']['board_best_hot'])) for pair in pairs]
    family = summarize(
        [sum(a['ln'][q] - b['ln'][q] for q in FAMILY) for a, b in scores],
        [sum(a['hot'][q] - b['hot'][q] for q in FAMILY) for a, b in scores], plan['target_ln'])
    whole = summarize([a['sum_ln'] - b['sum_ln'] for a, b in scores],
                      [a['hot_seconds'] - b['hot_seconds'] for a, b in scores], plan['target_ln'])
    queries = []
    for query in range(43):
        differences = [a['ln'][query] - b['ln'][query] for a, b in scores]
        seconds = [a['hot'][query] - b['hot'][query] for a, b in scores]
        row = summarize(differences, seconds, plan['target_ln'])
        row.update(query=query, baseline_hot_s=statistics.mean(a['hot'][query] for a, b in scores),
                   candidate_hot_s=statistics.mean(b['hot'][query] for a, b in scores),
                   delta_ln=-row['benefit_ln'], delta_s=-row['mean_seconds_saved'])
        queries.append(row)
    queries.sort(key=lambda row: row['delta_ln'], reverse=True)
    summary = dict(primary_queries=FAMILY, family=family, whole_suite=whole, queries=queries,
                   observed_power=suite['observed_power'],
                   sign='benefit = baseline minus candidate; positive improves. Delta = candidate minus baseline; negative improves.',
                   per_query_intervals='exploratory nominal 95 percent; no correction for 43 comparisons')
    (path / 'study-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    fields = ['query', 'baseline_hot_s', 'candidate_hot_s', 'delta_s', 'delta_ln']
    with (path / 'query-deltas.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(queries)
    print(json.dumps(dict(family=family, whole_suite=whole), indent=2))
    print('Delta = candidate minus baseline; negative improves; largest regression first.')
    for row in queries:
        if row['query'] in FAMILY:
            print(f'q{row["query"]}: {row["baseline_hot_s"]:.6f} -> {row["candidate_hot_s"]:.6f} s; '
                  f'delta {row["delta_s"]:+.6f} s, {row["delta_ln"]:+.6f} ln')


if __name__ == '__main__':
    main(sys.argv[1])
