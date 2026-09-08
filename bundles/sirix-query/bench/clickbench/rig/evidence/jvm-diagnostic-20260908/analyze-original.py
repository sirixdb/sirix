"""JFR and passive-memory observations; approximate receive-time query windows, never scores."""
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import re
import statistics
import sys

path=Path(__file__).resolve().parent/(sys.argv[1] if len(sys.argv)>1 else 'jvm50-01')
def epoch(text):return datetime.fromisoformat(text.replace('Z','+00:00')).timestamp()
def duration(text):
    m=re.fullmatch(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?',text)
    if not m:raise ValueError(text)
    return float(m[1] or 0)*3600+float(m[2] or 0)*60+float(m[3] or 0)
def method(value):return value['type']['name'].replace('/','.')+'.'+value['name']
all_events=json.loads((path/'jvm-events.json').read_text())['recording']['events']
events=[]
for event in all_events:
    value=event['values'];start=epoch(value['startTime'])
    events.append(dict(type=event['type'],start=start,end=start+duration(value.get('duration','PT0S')),value=value))
comp=[e for e in events if e['type']=='jdk.Compilation']
gc=[e for e in events if e['type']=='jdk.GCPhasePause']
starts=[e for e in events if e['type']=='jdk.ThreadStart']
main=[]
for line in (path/'main-samples.tsv').read_text().splitlines():
    timestamp,stack=line.split('\t',1);main.append((epoch(timestamp),stack.split(';')))
boundaries=[json.loads(line) for line in (path/'query-boundaries.jsonl').read_text().splitlines()]
rows=[]
for index,boundary in enumerate(boundaries):
    match=re.match(r'# q(\d+) try (\d+): wall=([0-9.]+) s',boundary['line']);q,tr=int(match[1]),int(match[2]);wall=float(match[3])
    end=boundary['received_epoch'];start=end-wall
    overlapping=[e for e in comp if e['start']<end and e['end']>start]
    c2=[e for e in overlapping if e['value']['compiler']=='c2']
    sample=[stack for t,stack in main if start<=t<=end]
    compile_samples=sum('io.brackit.query.Query.<init>' in stack for stack in sample)
    compiler=Counter(e['value']['compiler'] for e in overlapping)
    worker_starts=[e for e in starts if start<=e['start']<=end and e['value']['thread'].get('javaName')=='sirix-vec-exec']
    gc_overlap=sum(max(0,min(e['end'],end)-max(e['start'],start)) for e in gc)
    rows.append(dict(query=q,try_number=tr,wall_s=wall,approx_start_epoch=start,received_end_epoch=end,
                     compilations_overlapping=dict(compiler),c2_methods=[method(e['value']['method']) for e in c2],
                     compiler_elapsed_overlap_s=sum(max(0,min(e['end'],end)-max(e['start'],start)) for e in overlapping),
                     gc_pause_overlap_s=gc_overlap,vec_thread_starts=len(worker_starts),
                     main_samples=len(sample),main_samples_in_query_compiler=compile_samples,
                     main_top_methods=Counter(stack[0] for stack in sample).most_common(6)))
telemetry=[json.loads(line) for line in (path/'telemetry.jsonl').read_text().splitlines()]
mem=dict(min_available_kb=min(r['meminfo']['MemAvailable'] for r in telemetry),
         max_jvm_rss_kb=max((r.get('process') or {}).get('status',{}).get('VmRSS',0) for r in telemetry),
         max_jvm_swap_kb=max((r.get('process') or {}).get('status',{}).get('VmSwap',0) for r in telemetry),
         max_observer_snapshot_s=max(r['snapshot_duration_s'] for r in telemetry))
mem['host_vmstat_deltas']={key:telemetry[-1]['vmstat'].get(key,0)-telemetry[0]['vmstat'].get(key,0) for key in
                         ('pswpin','pswpout','pgmajfault','pgscan_direct','pgscan_kswapd','pgsteal_direct','pgsteal_kswapd','allocstall_normal','compact_stall')}
for category in ('memory','io','cpu'):
    for scope in ('some','full'):
        def total(row):
            for line in row['pressure'][category].splitlines():
                if line.startswith(scope+' '):return int(re.search(r'total=(\d+)',line)[1])
            return None
        first,last=total(telemetry[0]),total(telemetry[-1])
        mem[f'{category}_{scope}_pressure_seconds']=(last-first)/1e6 if first is not None else None
live=[r['process'] for r in telemetry if r.get('process')]
mem['process_io_final_minus_first']={key:live[-1]['io'].get(key,0)-live[0]['io'].get(key,0) for key in ('read_bytes','write_bytes','rchar','wchar')}
thread_max={}
for row in live:
    for tid,thread in row['threads'].items():
        if tid not in thread_max or thread_max[tid]['cpu_s']<thread['cpu_s']:thread_max[tid]=thread
mem['sampled_compiler_cpu_s_lower_bound']=sum(t['cpu_s'] for t in thread_max.values() if 'Compiler' in t['name'])
summary=dict(scope='diagnostic-only; profiling overhead and observer teardown error retained',
             timing_caveat='Query windows infer start from resource-line receive time minus rounded wall. Inline observer work can delay receipt by up to its snapshot duration; short-window event overlaps are approximate, not exact phase measurements.',
             verdict=json.loads((path/'verdict.json').read_text()),memory=mem,
             compilation_count=len(comp),compilation_count_by_compiler=dict(Counter(e['value']['compiler'] for e in comp)),
             compiler_elapsed_seconds=sum(e['end']-e['start'] for e in comp),
             gc_pause_count=len(gc),gc_pause_seconds=sum(e['end']-e['start'] for e in gc),
             vec_thread_starts=sum(e['value']['thread'].get('javaName')=='sirix-vec-exec' for e in starts),
             total_main_samples=len(main),query_compiler_main_samples=sum('io.brackit.query.Query.<init>' in stack for _,stack in main),
             query_windows=rows)
(path/'analysis.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({k:v for k,v in summary.items() if k!='query_windows'},indent=2))
for row in rows:
    if row['query'] in (2,3,6,21,39,42):print(json.dumps(row))
