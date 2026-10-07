#!/usr/bin/env python3
"""Describe candidate-only sparse samples; deliberately no comparison or regression verdict."""
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

paths = sorted(Path(sys.argv[1]).glob('p*-candidate.log'))
if len(paths) != 6:
    raise ValueError(f'Expected six candidate sparse forks, found {len(paths)}')
values = defaultdict(list)
expected_metrics = {'bulkAppendCommit', 'publicCompactDiff', 'publicMaterializedDiff',
                    'replayRead', 'unchangedPublicDiff', 'revisionCopy'}
for path in paths:
    lines = path.read_text().splitlines()
    if not any(line.startswith(f'END,{path.stem},') for line in lines):
        raise ValueError(f'{path}: incomplete fork')
    metadata = next(line for line in lines if line.startswith(f'RUN,{path.stem},'))
    if 'identity=true' not in metadata or int(metadata.rsplit('samples=', 1)[1]) != 9:
        raise ValueError(f'{path}: wrong variant or sample count')
    seen = set()
    for line in lines:
        if not line.startswith('SAMPLE,'):
            continue
        _, label, scenario, iteration, metric, nanos, allocated, *_ = next(csv.reader([line]))
        identity = (metric, int(iteration))
        if label != path.stem or scenario != 'sparse' or identity in seen:
            raise ValueError(f'{path}: wrong or duplicate sample')
        seen.add(identity)
        if int(nanos) <= 0 or int(allocated) < 0:
            raise ValueError(f'{path}: unsuccessful sample')
        values[metric].append((int(nanos), int(allocated)))
    if seen != {(metric, iteration) for metric in expected_metrics for iteration in range(9)}:
        raise ValueError(f'{path}: incomplete or unexpected metrics')
print(json.dumps([dict(scenario='sparse', metric=metric, status='baseline_source_does_not_complete',
                       forks=6, samples=len(samples),
                       candidate_median_ms=statistics.median(sample[0] for sample in samples) / 1e6,
                       candidate_thread_allocated_median=statistics.median(sample[1] for sample in samples))
                  for metric, samples in sorted(values.items())], indent=2))
