"""Read-only query execution under an inherited rig lease and recorded protocol."""
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import uuid

from measurement import read_timings
from measurement import steering_log_header
from rig_lock import require_no_benchmark
from rig_lock import other_java_processes
from rig_lock import wait_for_quiet_java
from runtime import command
from runtime import file_hash
from runtime import validate_environment
from runtime import validate_output_location
from runtime import validate_runtime
from runtime import verify_scored_runtime

POWER_PATHS = [Path(f'/sys/class/powercap/intel-rapl:0/constraint_{i}_power_limit_uw') for i in (0, 1)]
POWER_DOMAIN_GLOB = 'sys/class/powercap/intel-rapl*/constraint_*_power_limit_uw'
# PF_EXITING: the task-flag bit the kernel sets on entry to do_exit, before any teardown that
# can cost the observer its access to the task's counters.
PF_EXITING = 0x4


def temperature_paths():
    paths = [path.parent/'temp' for path in Path('/sys/class/thermal').glob('thermal_zone*/type')
             if path.read_text().strip() == 'x86_pkg_temp']
    if not paths:
        raise RuntimeError('no identified x86 package-temperature sensor; cannot apply this rig protocol')
    return sorted(paths)


def machine_settings():
    settings = {}
    patterns = ('/sys/devices/system/cpu/cpufreq/policy*/scaling_driver',
                '/sys/devices/system/cpu/cpufreq/policy*/scaling_governor',
                '/sys/devices/system/cpu/cpufreq/policy*/scaling_min_freq',
                '/sys/devices/system/cpu/cpufreq/policy*/scaling_max_freq',
                '/sys/devices/system/cpu/cpufreq/policy*/energy_performance_preference',
                '/sys/devices/system/cpu/intel_pstate/no_turbo')
    for pattern in patterns:
        for path in Path('/').glob(pattern.lstrip('/')):
            settings[str(path)] = path.read_text().strip()
    return settings


def task_fields(path):
    """A task's /proc/PID/stat, past the parenthesised command name a JVM's own may contain."""
    return (path/'stat').read_text().rsplit(')', 1)[1].split()


def leaving(fields):
    """Whether the kernel already reports this task as on its way out: inside do_exit (PF_EXITING),
    or exited and awaiting its parent (Z, X). Its counters are then gone rather than withheld."""
    return fields[0] in ('Z', 'X') or bool(int(fields[6]) & PF_EXITING)


def process_snapshot(process):
    """Read live counters, distinguishing a child on its way out from a live denial."""
    path = Path(f'/proc/{process.pid}')
    try:
        fields = task_fields(path)
        if leaving(fields):
            return None
        return dict(cpu_s=(int(fields[11])+int(fields[12]))/os.sysconf('SC_CLK_TCK'),
                    minor_faults=int(fields[7]), major_faults=int(fields[9]),
                    rss_bytes=int(fields[21])*os.sysconf('SC_PAGE_SIZE'),
                    io={key: int(value) for key, value in
                        (line.split(':', 1) for line in (path/'io').read_text().splitlines())})
    except (FileNotFoundError, ProcessLookupError):
        return None
    except PermissionError:
        # Linux revokes /proc/PID/io the moment a task detaches its address space, which happens
        # inside do_exit and long before the child is reaped -- so process.poll() is still None
        # here, the main thread being busy draining the child's stdout. Ask the kernel what it now
        # says about the task instead: a child that is leaving or already gone costs this one
        # sample, not the leg. A live child withholding its counters is still a failed regime.
        try:
            if leaving(task_fields(path)):
                return None
        except (FileNotFoundError, ProcessLookupError):
            return None
        raise


def power_domains():
    """Every RAPL limit the platform exposes, including domains the rig never sets."""
    return {str(path): path.read_text().strip() for path in sorted(Path('/').glob(POWER_DOMAIN_GLOB))}


def snapshot(sensors, process=None):
    values = {str(path): int(path.read_text()) for path in POWER_PATHS}
    temperatures = {str(path): int(path.read_text())/1000 for path in sensors}
    row = dict(epoch=time.time(), monotonic=time.monotonic(), power_limits_uw=values,
               power_domains_uw=power_domains(),
               package_C=temperatures, load=Path('/proc/loadavg').read_text().strip())
    row['reported_kHz'] = {str(path): int(path.read_text()) for path in Path('/sys/devices/system/cpu').glob('cpu*/cpufreq/scaling_cur_freq')}
    row['cpu0_throttle_counts'] = {path.name: int(path.read_text()) for path in Path('/sys/devices/system/cpu/cpu0/thermal_throttle').glob('*throttle_count')}
    if process is not None:
        row['process'] = process_snapshot(process)
    return row


def check_power(row, expected):
    if any(value != expected for value in row['power_limits_uw'].values()):
        raise RuntimeError(f"power regime changed: expected both limits {expected} uW, observed {row['power_limits_uw']}")


def cool_gate(output, sensors, protocol):
    consecutive = 0
    deadline = time.monotonic()+900
    with (output/'cooling.jsonl').open('x') as log:
        while True:
            row = snapshot(sensors)
            check_power(row, protocol['power_limit_uw'])
            consecutive = consecutive+1 if max(row['package_C'].values()) < protocol['cool_below_C'] else 0
            # Recheck after the full snapshot; sampling itself can coincide with a thermal burst.
            row['launch_recheck_C'] = {str(path): int(path.read_text())/1000 for path in sensors}
            if consecutive >= 3 and max(row['launch_recheck_C'].values()) >= protocol['cool_below_C']:
                consecutive = 0
            row['consecutive'] = consecutive
            log.write(json.dumps(row)+'\n')
            log.flush()
            if consecutive >= 3:
                return row
            if time.monotonic() >= deadline:
                raise RuntimeError('cool start unavailable within 900 seconds; no query JVM launched')
            time.sleep(5)


def run_part(runtime, database, output, queries, protocol, lease, *, diagnostic=False, tries=3):
    source = runtime.get('source_worktree')
    output = validate_output_location(output, source) if source else Path(output)
    output.mkdir(exist_ok=False, parents=True)
    validate_runtime(runtime, allow_diagnostics=diagnostic)
    validate_environment()
    if not diagnostic:
        verify_scored_runtime(runtime)
    lease.verify()
    require_no_benchmark()
    wait_for_quiet_java()
    sensors = temperature_paths()
    before = cool_gate(output, sensors, protocol)
    if machine_settings() != protocol['cpu_policy']:
        raise RuntimeError('CPU policy changed after the collection plan was recorded')
    lease.verify()
    require_no_benchmark()
    argv = command(runtime, database, queries=queries, tries=tries)
    (output/'command.json').write_text(json.dumps(argv, indent=2)+'\n')
    (output/'start.json').write_text(json.dumps(before, indent=2)+'\n')
    issues = []
    domain_changes = []
    done = threading.Event()
    started = time.monotonic()
    process = None
    observer = None
    code = None
    try:
        with (output/'suite.log').open('x') as log, (output/'query-boundaries.jsonl').open('x') as boundaries:
            log.write(steering_log_header(tries))
            log.flush()
            # The frozen manifest already records the campaign identity this runtime was
            # classified and validated at. Handing the JVM that conclusion, rather than the pointers
            # behind it, is what keeps the measured process from classifying itself differently.
            process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                       env=lease.child_environment(runtime.get('campaign_classification')),
                                       pass_fds=lease.pass_fds)

            def observe():
                try:
                    next_policy_check = 0
                    domains = before['power_domains_uw']
                    with (output/'telemetry.jsonl').open('x') as telemetry:
                        while not done.is_set():
                            row = snapshot(sensors, process)
                            telemetry.write(json.dumps(row)+'\n')
                            telemetry.flush()
                            check_power(row, protocol['power_limit_uw'])
                            if row['power_domains_uw'] != domains:
                                # Platform-managed limits are observed context, not a validity gate:
                                # record the move, which may explain variance, and keep measuring.
                                changed = {path: [domains.get(path), value]
                                           for path, value in row['power_domains_uw'].items()
                                           if domains.get(path) != value}
                                domain_changes.append(dict(monotonic=row['monotonic'], changed=changed))
                                domains = row['power_domains_uw']
                            if time.monotonic() >= next_policy_check:
                                live = other_java_processes(exclude=process.pid)
                                if live:
                                    raise RuntimeError(f'other Java workloads appeared during measurement: {live}')
                                if machine_settings() != protocol['cpu_policy']:
                                    raise RuntimeError('CPU policy changed during measurement')
                                next_policy_check = time.monotonic()+5
                            done.wait(.5)
                except Exception as failure:
                    issues.append(str(failure))
                    if process.poll() is None:
                        process.terminate()

            observer = threading.Thread(target=observe, name='rig-telemetry')
            observer.start()
            for line in process.stdout:
                received_epoch = time.time()
                received_monotonic = time.monotonic()
                log.write(line)
                log.flush()
                if line.startswith('# q') and ' try ' in line:
                    # Keep pipe draining independent of /proc and sysfs sampling.
                    # Receipt time is an approximate boundary, not a JVM event timestamp.
                    row = dict(received_epoch=received_epoch, received_monotonic=received_monotonic,
                               line=line.rstrip())
                    boundaries.write(json.dumps(row)+'\n')
                    boundaries.flush()
            code = process.wait()
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            code = process.wait()
        done.set()
        if observer is not None:
            observer.join()
        verdict = dict(exit_code=code, issues=issues, duration_s=time.monotonic()-started,
                       power_domain_changes=domain_changes)
        (output/'verdict.json').write_text(json.dumps(verdict, indent=2)+'\n')
    after = snapshot(sensors)
    (output/'end.json').write_text(json.dumps(after, indent=2)+'\n')
    check_power(after, protocol['power_limit_uw'])
    if machine_settings() != protocol['cpu_policy']:
        raise RuntimeError('CPU policy changed during this part; the leg is unscorable')
    if code != 0 or issues:
        raise RuntimeError(f'query process failed or its regime changed: {verdict}; logs retained in {output}')
    power = dict(gated_limits_uw=protocol['power_limit_uw'], start=before['power_domains_uw'],
                 end=after['power_domains_uw'], changes=domain_changes)
    if domain_changes:
        print(f'NOTE: {len(domain_changes)} platform power-limit change(s) observed during this run; '
              f'the leg stands and the readings are retained in {output}/telemetry.jsonl', flush=True)
    return None if diagnostic else (read_timings(output/'suite.log', queries), power)


def run_leg(runtime, database, output, protocol, lease):
    source = runtime.get('source_worktree')
    output = validate_output_location(output, source) if source else Path(output)
    verify_scored_runtime(runtime)
    output.mkdir(exist_ok=False, parents=True)
    result = [[None]*3 for _ in range(43)]
    timings, power = run_part(runtime, database, output/'suite', range(43), protocol, lease)
    for (query, attempt), seconds in timings.items():
        result[query][attempt-1] = seconds
    document = dict(result=result, rig=dict(scope='steering', complete=True, protocol=protocol,
                    run_id=str(uuid.uuid4()), runtime_id=runtime['runtime_id'], source_commit=runtime.get('source_commit'),
                    runtime_manifest=runtime['manifest_path'],
                    suite_log_sha256=file_hash(output/'suite/suite.log'),
                    observed_power=power, output=str(output.resolve())))
    (output/'leg.json').write_text(json.dumps(document, indent=2)+'\n')
    return document
