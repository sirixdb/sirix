#!/usr/bin/env python3
"""Summarize untimed diagnostic work separately from latency acceptance samples."""
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

lines = Path(sys.argv[1]).read_text().splitlines()
if not any(line.startswith('END,') for line in lines):
    raise ValueError('Incomplete diagnostic fork')
if not any(line.startswith('JVM,') and '-Dsirix.replay.workDiag=true' in line for line in lines):
    raise ValueError('Replay work diagnostics were not enabled')
columns = next(csv.reader([next(line for line in lines if line.startswith('COLUMNS,'))]))[7:]
values = defaultdict(lambda: defaultdict(list))
for line in lines:
    if not line.startswith('SAMPLE,'):
        continue
    row = next(csv.reader([line]))
    if len(row[7:]) != len(columns):
        raise ValueError('Wrong diagnostic column count')
    for name, value in zip(columns, row[7:]):
        if int(value) < 0:
            raise ValueError('Unavailable diagnostic counter')
        values[(row[2], row[4])][name].append(int(value))
print(json.dumps([dict(scenario=scenario, metric=metric,
                       counters={name: dict(min=min(samples), max=max(samples))
                                 for name, samples in counters.items()})
                  for (scenario, metric), counters in sorted(values.items())], indent=2))
