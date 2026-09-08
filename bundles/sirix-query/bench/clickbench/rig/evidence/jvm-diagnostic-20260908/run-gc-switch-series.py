import json,os,re,subprocess,sys,time
from pathlib import Path
w=Path(__file__).resolve().parent
plan=json.loads((w/'gc-switch-plan.json').read_text())
expected={(q,t) for q in range(43) for t in (1,2,3)}
for index,order in enumerate(plan['orders'],1):
 for arm in order.split('-'):
  tag=f'gcswitch-{index:02}-{arm}'
  print('START',tag,time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),flush=True)
  with (w/(tag+'-controller.log')).open('x') as log:
   subprocess.run([sys.executable,str(w/'gc-switch-probe.py'),tag,'--gc-refresh','true' if arm=='on' else 'false','--expected-pl1-uw','50000000','--expected-pl2-uw','50000000'],stdout=log,stderr=subprocess.STDOUT,check=True)
  p=w/tag
  verdict=json.loads((p/'verdict.json').read_text())
  assert verdict['exit_code']==0 and not verdict['issues'],verdict
  rows=re.findall(r'^# q(\d+) try (\d+): wall=([0-9.]+) s',(p/'suite100m.log').read_text(),re.M)
  assert len(rows)==129 and {(int(q),int(t)) for q,t,_ in rows}==expected,p
  print('COMPLETE',tag,time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),flush=True)
print('GC_SWITCH_SERIES_COMPLETE',flush=True)
