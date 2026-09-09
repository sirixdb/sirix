"""Attribute hot JFR samples to approximate full-suite query windows.

Input samples.tsv is emitted by the archived ReadSamples.java. Inclusive categories
can overlap; worker and merge acquisition partition the lookup category exactly.
"""
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import re
import sys

QUERIES = (16, 18, 31, 32)


def main(directory):
    path = Path(directory)
    windows = []
    for line in (path / 'diagnostic/query-boundaries.jsonl').read_text().splitlines():
        boundary = json.loads(line)
        match = re.match(r'# q(\d+) try (\d+): wall=([\d.]+)', boundary['line'])
        query, attempt, wall = int(match[1]), int(match[2]), float(match[3])
        if query in QUERIES and attempt in (2, 3):
            windows.append(dict(query=query, attempt=attempt, wall_s=wall,
                                start=boundary['received_epoch'] - wall,
                                end=boundary['received_epoch'], samples=Counter(),
                                leaves=Counter(), inclusive=Counter()))
    if len(windows) != 2 * len(QUERIES):
        raise ValueError('expected both hot tries of all four target queries')
    for line in (path / 'samples.tsv').read_text().splitlines():
        timestamp, thread, stack = line.split('\t')
        epoch = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).timestamp()
        for window in windows:
            if not window['start'] <= epoch <= window['end']:
                continue
            counts = window['samples']
            counts['total'] += 1
            frames = [frame.split(':')[0] for frame in stack.split(';')]
            window['leaves'][frames[0]] += 1
            window['inclusive'].update(set(frames))
            for category, needle in (
                    ('lookup', 'NumericGroupAggTable.acquire'),
                    ('table', 'NumericGroupAggTable'),
                    ('topk', 'GroupTopKSelector'),
                    ('rehash', 'NumericGroupAggTable.rehash'),
                    ('spill_copy', 'GroupTableSpill.spillStripes'),
                    ('string_merge', 'SegmentValueMerge.mergeRange')):
                if needle in stack:
                    counts[category] += 1
            if 'NumericGroupAggTable.acquire' in stack:
                category = 'lookup_merge' if 'mergeStripes' in stack or 'mergePartition' in stack else 'lookup_worker'
                counts[category] += 1
            break
    pending = []
    for line in (path / 'diagnostic/suite.log').read_text().splitlines():
        if 'groupAgg pass done' in line:
            pending.append(line)
        match = re.match(r'# q(\d+) try (\d+):', line)
        if match:
            for window in windows:
                if (window['query'], window['attempt']) == (int(match[1]), int(match[2])):
                    window['passes'] = len(pending)
                    for field in ('scanMs', 'mergeMs', 'sharedRehashes'):
                        window[field] = sum(int(re.search(field + r'=(\d+)', item)[1]) for item in pending)
            pending = []
    rows = []
    for query in QUERIES:
        hot = [window for window in windows if window['query'] == query]
        counts = sum((window['samples'] for window in hot), Counter())
        if not counts['total']:
            raise ValueError(f'no hot samples for q{query}')
        row = dict(query=query, samples=dict(counts),
                   percent={key: 100 * value / counts['total'] for key, value in counts.items() if key != 'total'},
                   wall_s=sum(window['wall_s'] for window in hot) / 2,
                   scan_ms=sum(window['scanMs'] for window in hot) / 2,
                   merge_ms=sum(window['mergeMs'] for window in hot) / 2,
                   passes=[window['passes'] for window in hot],
                   leaves=sum((window['leaves'] for window in hot), Counter()).most_common(30),
                   inclusive=sum((window['inclusive'] for window in hot), Counter()).most_common(40))
        rows.append(row)
        print(f'q{query}: {row["wall_s"]:.3f}s, passes={row["passes"]}, samples={counts["total"]}, '
              + ', '.join(f'{key}={value:.1f}%' for key, value in row['percent'].items()))
    (path / 'profile-summary.json').write_text(json.dumps(dict(queries=rows, windows=windows), indent=2) + '\n')


if __name__ == '__main__':
    main(sys.argv[1])
