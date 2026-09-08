"""Diagnostic protocol experiment using the unchanged aa4d81d54 runtime command."""
from pathlib import Path
import argparse
import fcntl
import glob
import json
import os
import subprocess
import threading
import time
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[5] / "bundles/sirix-query/bench/clickbench/rig"))
from rig_lock import RigLease, require_no_benchmark, other_java_processes

ROOT = Path('/home/johannes/.treehouse/sirix-cdde48/3/sirix')
WORK = ROOT / 'bundles/sirix-query/build/diagnostics/rig-trust'
parser=argparse.ArgumentParser()
parser.add_argument('tag')
parser.add_argument('--gc-refresh', choices=('true','false'), required=True)
parser.add_argument('--expected-pl1-uw', type=int)
parser.add_argument('--expected-pl2-uw', type=int)
args=parser.parse_args()
out=WORK/args.tag
out.mkdir()
cmd=None
with (WORK/'baseline/telemetry.jsonl').open() as f:
    for line in f:
        row=json.loads(line)
        for data in row['java'].values():
            argv=data['cmdline'].rstrip('\0').split('\0')
            if any(a.endswith('ClickBenchRunMain') for a in argv):
                cmd=argv
                break
        if cmd:
            break
assert cmd
idx=next(i for i,a in enumerate(cmd) if a.endswith('ClickBenchRunMain'))
(out/'db').symlink_to('/home/johannes/IdeaProjects/sirix/bundles/sirix-query/build/diagnostics/clickbench-seg100m-20260905-2328/db', target_is_directory=True)
cmd[idx+1]=str(out/'db')
cmd[cmd.index('--tries')+1]='3'
cmd.insert(1, '-Dsirix.projection.groupPasses.refreshBudgetByGc='+args.gc_refresh)
(out/'protocol.json').write_text(json.dumps({'scope':'GC-switch full-suite steering experiment; never published','queries':list(range(43)),'tries':3,'power_uw':50000000,'cache':'natural; no eviction','source':'unchanged aa4d81d54 compiled runtime','gc_refresh':args.gc_refresh,'context':'full43 preceding query/heap context; fresh executor each try'},indent=2)+'\n')
(out/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')

def snap():
    return {'time':time.time(),'values':{p:Path(p).read_text().strip() for pattern in ('/sys/class/powercap/intel-rapl*/constraint_*power_limit_uw','/sys/class/thermal/thermal_zone*/temp','/sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq','/sys/devices/system/cpu/cpu*/thermal_throttle/*throttle_count') for p in glob.glob(pattern)}, 'load':Path('/proc/loadavg').read_text().strip()}

os.environ['CB_RIG_WORK']='/home/johannes/IdeaProjects/sirix/bundles/sirix-query/build/diagnostics/rig'
with RigLease(timeout=0) as lease:
    require_no_benchmark()
    assert not other_java_processes(), other_java_processes()
    with (out/'cooling.jsonl').open('x') as stream:
        start=time.monotonic()
        stable=0
        while stable<3:
            row=snap()
            stream.write(json.dumps(row)+'\n');stream.flush()
            temp=int(row['values']['/sys/class/thermal/thermal_zone12/temp'])/1000
            actual_pl1=int(row['values']['/sys/class/powercap/intel-rapl:0/constraint_0_power_limit_uw'])
            if args.expected_pl1_uw is not None and actual_pl1!=args.expected_pl1_uw:
                raise SystemExit(f'power regime mismatch: expected {args.expected_pl1_uw}, observed {actual_pl1}; no benchmark launched')
            actual_pl2=int(row['values']['/sys/class/powercap/intel-rapl:0/constraint_1_power_limit_uw'])
            if args.expected_pl2_uw is not None and actual_pl2!=args.expected_pl2_uw:
                raise SystemExit(f'power regime mismatch: expected PL2 {args.expected_pl2_uw}, observed {actual_pl2}; no benchmark launched')
            stable=stable+1 if temp<55 else 0
            launch_temp=int(Path('/sys/class/thermal/thermal_zone12/temp').read_text())/1000
            if stable>=3 and launch_temp>=55:
                stable=0
            print('cool gate',temp,'C; consecutive <55C:',stable,flush=True)
            if time.monotonic()-start>900:
                raise SystemExit('cool start unavailable within 900s; no benchmark launched')
            if stable<3:
                time.sleep(5)
    before=row
    before['last_checked_launch_temp_C']=launch_temp
    (out/'start.json').write_text(json.dumps(before,indent=2)+'\n')
    (out/'start.txt').write_text(str(before['time'])+'\n')
    done=threading.Event()
    issues=[]
    def observe():
        try:
            next_census=0
            with (out/'telemetry.jsonl').open('x') as stream:
                while not done.is_set():
                    row=snap()
                    assert all(int(row['values'][f'/sys/class/powercap/intel-rapl:0/constraint_{i}_power_limit_uw'])==50000000 for i in (0,1)), 'power changed'
                    stream.write(json.dumps(row)+'\n');stream.flush()
                    if time.monotonic()>=next_census:
                        assert not other_java_processes(exclude=proc.pid if 'proc' in globals() else None), 'other Java workload appeared'
                        next_census=time.monotonic()+5
                    done.wait(.5)
        except Exception as failure:
            issues.append(str(failure))
            if 'proc' in globals() and proc.poll() is None: proc.terminate()
    thread=threading.Thread(target=observe)
    try:
        with (out/'suite100m.log').open('x') as log, (out/'query-boundaries.jsonl').open('x') as boundaries:
            lease.verify(); require_no_benchmark()
            proc=subprocess.Popen(cmd,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,env=lease.child_environment(),pass_fds=lease.pass_fds)
            thread.start()
            for line in proc.stdout:
                log.write(line);log.flush()
                if line.startswith('# q') and ' try ' in line:
                    row={'received_epoch':time.time(),'line':line.strip()}
                    boundaries.write(json.dumps(row)+'\n');boundaries.flush()
                    print(line.strip(),flush=True)
            code=proc.wait()
    finally:
        if 'proc' in locals() and proc.poll() is None:
            proc.terminate();code=proc.wait()
        done.set()
        if thread.ident is not None: thread.join()
    after=snap()
    (out/'end.json').write_text(json.dumps(after,indent=2)+'\n')
    deltas={p:int(v)-int(before['values'][p]) for p,v in after['values'].items() if p.endswith('throttle_count')}
    verdict={'exit_code':code,'issues':issues,'throttled':any(v>0 for v in deltas.values()),'throttle_deltas':deltas,'duration_s':after['time']-before['time']}
    (out/'verdict.json').write_text(json.dumps(verdict,indent=2)+'\n')
    print(json.dumps(verdict),flush=True)
    assert code==0 and not issues, verdict
