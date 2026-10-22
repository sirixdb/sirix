#!/usr/bin/env python3
"""Paired fork-block bootstrap; raw within-fork samples stay together (5000 fixed-seed draws)."""
import csv
import json
import random
import statistics
import sys
from collections import defaultdict
from pathlib import Path

runs = Path(sys.argv[1])
values = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
paths = sorted(runs.glob('p*.log'))
if not paths:
    raise ValueError('No benchmark forks found')
labels = {path.stem for path in paths}
for label in labels:
    pair, variant = label.split('-', 1)
    if variant not in {'baseline', 'candidate'} or not {f'{pair}-baseline', f'{pair}-candidate'} <= labels:
        raise ValueError(f'Incomplete or unknown fork pair: {label}')
unsupported = set()
measured_by_run = {}
for path in paths:
    lines = path.read_text().splitlines()
    if not any(line.startswith(f'END,{path.stem},') for line in lines):
        raise ValueError(f'{path}: incomplete benchmark fork')
    expected_samples = None
    for line in lines:
        if line.startswith(f'RUN,{path.stem},'):
            expected_samples = int(line.rsplit('samples=', 1)[1])
    if expected_samples is None:
        raise ValueError(f'{path}: missing run metadata')
    seen = set()
    measured = defaultdict(int)
    for line in lines:
        if line.startswith(('UNSUPPORTED,', 'UNSUPPORTED_SCENARIO,')):
            marker, label, scenario, *_ = line.split(',')
            if label.endswith('-baseline'):
                pair = label.split('-', 1)[0]
                unsupported.add((pair, scenario, None if marker == 'UNSUPPORTED_SCENARIO' else 'revisionCopy'))
        if not line.startswith('SAMPLE,'):
            continue
        row = next(csv.reader([line]))
        _, label, scenario, iteration, metric, nanos, allocated, *_ = row
        if label != path.stem:
            raise ValueError(f'{path}: mislabeled sample {label}')
        identity = (scenario, metric, int(iteration))
        if identity in seen or not 0 <= int(iteration) < expected_samples:
            raise ValueError(f'{path}: duplicate/out-of-range sample {identity}')
        seen.add(identity)
        measured[(scenario, metric)] += 1
        pair, variant = label.split('-', 1)
        if int(nanos) >= 0:
            values[(scenario, metric)][pair][variant].append((int(nanos), int(allocated)))
    if path.stem.endswith('-candidate') and not measured:
        raise ValueError(f'{path}: candidate produced no measurements')
    if any(count != expected_samples for count in measured.values()):
        raise ValueError(f'{path}: incomplete metric samples: {dict(measured)}')
    measured_by_run[path.stem] = set(measured)

expected_metrics = set().union(*(metrics for label, metrics in measured_by_run.items()
                                if label.endswith('-candidate')))
for label, measured in measured_by_run.items():
    pair, variant = label.split('-', 1)
    missing = expected_metrics - measured
    if variant == 'candidate' and missing:
        raise ValueError(f'{label}: candidate metrics missing: {missing}')
    if variant == 'baseline' and any((pair, scenario, metric) not in unsupported
                                    and (pair, scenario, None) not in unsupported
                                    for scenario, metric in missing):
        raise ValueError(f'{label}: baseline metrics missing without unsupported marker: {missing}')

rng = random.Random(0x51A1_2026)
report = []
for (scenario, metric), blocks in sorted(values.items()):
    pairs = sorted(pair for pair, variants in blocks.items() if {'baseline', 'candidate'} <= variants.keys())
    if not pairs and all('baseline' not in variants for variants in blocks.values()):
        if not all((pair, scenario, metric) in unsupported or (pair, scenario, None) in unsupported for pair in blocks):
            raise ValueError(f'{scenario}/{metric}: baseline data missing without an explicit unsupported result')
        candidate = [sample for variants in blocks.values() for sample in variants['candidate']]
        report.append(dict(scenario=scenario, metric=metric, status='baseline_unsupported',
                           pairs=0, candidate_median_ms=statistics.median(sample[0] for sample in candidate) / 1e6,
                           candidate_thread_allocated_median=statistics.median(sample[1] for sample in candidate)))
        continue
    if len(pairs) < 6:
        raise ValueError(f'{scenario}/{metric}: only {len(pairs)} complete matched forks')
    if len(pairs) != len(labels) // 2:
        raise ValueError(f'{scenario}/{metric}: mixed successful and unsupported/missing fork pairs')
    if any(len(blocks[pair]['baseline']) != len(blocks[pair]['candidate']) for pair in pairs):
        raise ValueError(f'{scenario}/{metric}: matched forks have different successful sample counts')
    baseline = [sample[0] for pair in pairs for sample in blocks[pair]['baseline']]
    candidate = [sample[0] for pair in pairs for sample in blocks[pair]['candidate']]
    ratios = []
    for _ in range(5000):
        selection = rng.choices(pairs, k=len(pairs))
        a = [sample[0] for pair in selection for sample in blocks[pair]['baseline']]
        b = [sample[0] for pair in selection for sample in blocks[pair]['candidate']]
        ratios.append(statistics.median(b) / statistics.median(a))
    ratios.sort()
    lower, upper = ratios[124], ratios[4874]
    entry = dict(scenario=scenario, metric=metric, pairs=len(pairs), draws=5000,
                 baseline_median_ms=statistics.median(baseline) / 1e6,
                 candidate_median_ms=statistics.median(candidate) / 1e6,
                 ratio=statistics.median(candidate) / statistics.median(baseline),
                 paired_95_lower=lower, paired_95_upper=upper,
                 confirmed_regression=lower > 1.05,
                 baseline_thread_allocated_median=statistics.median(sample[1] for pair in pairs for sample in blocks[pair]['baseline']),
                 candidate_thread_allocated_median=statistics.median(sample[1] for pair in pairs for sample in blocks[pair]['candidate']))
    report.append(entry)
print(json.dumps(report, indent=2))
