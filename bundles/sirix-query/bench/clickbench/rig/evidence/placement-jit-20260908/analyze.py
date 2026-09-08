"""Diagnostic JFR/perf alignment; approximate receipt windows, never score inference."""
import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import re
import statistics as stats


def seconds(value):
    match = re.fullmatch(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?', value)
    if not match:
        raise ValueError(value)
    return 3600*float(match[1] or 0)+60*float(match[2] or 0)+float(match[3] or 0)


def epoch(value):
    return datetime.fromisoformat(value).timestamp()


def method(value):
    return value['type']['name'].replace('/', '.')+'.'+value['name']


def describe(values):
    return dict(n=len(values), mean=stats.mean(values), median=stats.median(values),
                min=min(values), max=max(values)) if values else None


def analyze(path):
    events = json.loads((path/'events.json').read_text())['recording']['events']
    comps, pauses, allocation, main_tids = [], [], [], set()
    gc_causes = Counter()
    explicit = []
    for event in events:
        kind, v = event['type'], event['values']
        if kind == 'jdk.Compilation':
            start = epoch(v['startTime'])
            duration = seconds(v['duration'])
            comps.append(dict(start=start, end=start+duration, duration=duration,
                              compiler=v['compiler'], method=method(v['method'])))
        elif kind == 'jdk.GCPhasePause':
            start, duration = epoch(v['startTime']), seconds(v['duration'])
            pauses.append(dict(start=start, end=start+duration, duration=duration, gc_id=v['gcId']))
        elif kind == 'jdk.GarbageCollection':
            gc_causes[v['cause']] += 1
        elif kind == 'jdk.SystemGC':
            explicit.append(dict(start=epoch(v['startTime']), duration=seconds(v['duration'])))
        elif kind == 'jdk.ExecutionSample':
            if v.get('sampledThread', {}).get('javaName') == 'main':
                main_tids.add(v['sampledThread']['osThreadId'])
        elif kind == 'jdk.ObjectAllocationSample':
            frames = (v.get('stackTrace') or {}).get('frames', [])
            stack = [method(frame['method']) for frame in frames]
            allocation.append(dict(weight=v['weight'], object_class=v['objectClass']['name'],
                                   site=next((name for name in stack if name.startswith(('io.sirix.', 'io.brackit.'))), stack[0] if stack else 'no stack')))
    perf_path = path/'perf-command.json'
    perf = json.loads(perf_path.read_text()) if perf_path.exists() else None
    assert len(main_tids) == 1, (path, main_tids)
    if perf is not None:
        assert main_tids == {perf['main_tid']}, (path, main_tids, perf['main_tid'])
    samples = []
    cpu_path = path/'main-cpu.samples'
    if cpu_path.exists():
        for line in cpu_path.read_text().splitlines():
            match = re.match(r'\s*(\d+)\s+\[(\d+)\]\s+([\d.]+):', line)
            if match:
                assert int(match[1]) in main_tids
                samples.append((float(match[3]), int(match[2])))
    bounds = [json.loads(line) for line in (path/'query-boundaries.jsonl').read_text().splitlines()]
    rows = []
    for bound in bounds:
        m = re.match(r'# q(\d+) try (\d+): wall=([\d.]+) s.* gc=(\d+) pauses ([\d.]+) s', bound['line'])
        q, attempt, wall, count, collection_s = int(m[1]), int(m[2]), float(m[3]), int(m[4]), float(m[5])
        end = bound['received_epoch']
        start = end-wall
        mono_end = bound['received_monotonic']
        cpus = Counter(cpu for timestamp, cpu in samples if mono_end-wall <= timestamp <= mono_end)
        core_types = {'P' if cpu < 12 else 'E' for cpu in cpus}
        overlap = [event for event in comps if event['start'] < end and event['end'] > start]
        rows.append(dict(query=q, attempt=attempt, wall_s=wall, collection_count=count,
                         mxbean_collection_s=collection_s, approximate_start=start, received_end=end,
                         gc_pause_overlap_s=sum(max(0, min(end, event['end'])-max(start, event['start'])) for event in pauses),
                         compiling=Counter(event['compiler'] for event in overlap),
                         c2_methods=[event['method'] for event in overlap if event['compiler']=='c2'],
                         perf_cpu_samples=dict(cpus), sample_count=sum(cpus.values()),
                         core_type=next(iter(core_types)) if len(core_types)==1 else 'mixed' if core_types else 'unsampled'))
    selected_hot, cold = [], []
    for q in sorted({r['query'] for r in rows}):
        qs = [r for r in rows if r['query']==q]
        cold.append(qs[0])
        selected_hot.append(min((r for r in qs if r['attempt'] in (2,3)), key=lambda r:r['wall_s']))
    q6 = [r for r in rows if r['query']==6 and r['attempt']>=2]
    classes, sites = Counter(), Counter()
    for event in allocation:
        classes[event['object_class']] += event['weight']
        sites[event['site']] += event['weight']
    return dict(tag=path.name, scope='diagnostic-only; no calibration, no scored effect',
                caveat='Query windows inferred from light output receipt minus rounded wall; scheduling and pipe latency remain. CPU groups are this host topology (P0..11/E12..19), not measured frequencies. Allocation weights are JFR sampling estimates. Within-process tries are dependent.',
                original_verdict=json.loads((path/'verdict.json').read_text()),
                native_main_tid=next(iter(main_tids)), jfr_main_tids=sorted(main_tids),
                cpu_samples=len(samples), compilation_count=len(comps), c2_count=sum(c['compiler']=='c2' for c in comps),
                total_gc_pause_s=sum(g['duration'] for g in pauses), gc_causes=dict(gc_causes),
                explicit_gc_calls=explicit,
                selected_hot_gc_pause_overlap_s=sum(r['gc_pause_overlap_s'] for r in selected_hot),
                selected_hot_gc_affected_queries=[r['query'] for r in selected_hot if r['gc_pause_overlap_s']>.0005],
                cold_gc_pause_overlap_s=sum(r['gc_pause_overlap_s'] for r in cold),
                q6_core_groups={kind:describe([r['wall_s'] for r in q6 if r['core_type']==kind]) for kind in ('P','E','mixed','unsampled')},
                allocation_weight_by_class=classes.most_common(15), allocation_weight_by_site=sites.most_common(20),
                queries=rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('paths', type=Path, nargs='+')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    results = [analyze(path) for path in args.paths]
    args.out.write_text(json.dumps(results, indent=2, allow_nan=False)+'\n')
    for result in results:
        print(json.dumps({k:v for k,v in result.items() if k not in ('queries','original_verdict','allocation_weight_by_site','allocation_weight_by_class')}, indent=2))
        print('q6', [(r['attempt'],r['wall_s'],r['core_type'],r['sample_count'],r['compiling'].get('c2',0)) for r in result['queries'] if r['query']==6])


if __name__ == '__main__':
    main()
