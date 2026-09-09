#!/usr/bin/env python3
"""Compile a tiny probe and verify the production Java/Python flock interoperation.

Uses an already prepared runtime and fresh local output; never launches a query,
loads a database, or touches the shared production lock files.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import selectors
import subprocess
import sys

from rig_lock import HOST_FD
from rig_lock import RigLease
from runtime import verify_runtime


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    runtime = json.loads(args.runtime.read_text())
    verify_runtime(runtime)
    output = args.out.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).with_name('RigLockProbe.java')
    classpath = os.pathsep.join(runtime['classpath'])
    subprocess.run([str(Path(runtime['java']).with_name('javac')), '-cp', classpath,
                    '-d', str(output), str(source)], check=True)
    base = [runtime['java'], '-Xms32m', '-Xmx64m', '--enable-native-access=ALL-UNNAMED',
            '-cp', str(output)+os.pathsep+classpath, 'io.sirix.query.bench.clickbench.RigLockProbe']
    lock = output/'lease'
    lock.touch()
    children = []

    def start(*, shared=False, descriptor=None, close=False, should_fail=False):
        argv = base+[str(lock), str(not shared).lower(), 'none' if descriptor is None else str(descriptor),
                     'close' if close else 'hold']
        child = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                 pass_fds=() if descriptor is None else (descriptor,))
        children.append(child)
        if should_fail:
            stdout, stderr = child.communicate(timeout=10)
            assert child.returncode != 0 and 'READY' not in stdout, (stdout, stderr)
            return child
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            if not selector.select(10):
                raise AssertionError('Java lease probe did not become ready')
        line = child.stdout.readline().strip()
        if line != 'READY':
            raise AssertionError(f'Java probe failed: {line}; {child.stderr.read()}')
        return child

    def available(shared=False):
        with lock.open('r+') as stream:
            try:
                fcntl.flock(stream.fileno(), (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False

    def stop(child):
        child.terminate()
        child.communicate(timeout=10)

    try:
        child = start()
        assert not available(), 'Java exclusive lease did not exclude Python'
        start(should_fail=True)
        stop(child)
        assert available(), 'Java process exit did not release ownership'
        child = start(shared=True)
        assert available(shared=True) and not available(), 'shared Java lease mode is wrong'
        stop(child)
        with lock.open('r+') as unlocked:
            start(descriptor=unlocked.fileno(), should_fail=True)
        with RigLease(timeout=0, paths=[(HOST_FD, lock)]) as lease:
            start(should_fail=True)
            child = start(descriptor=lease.pass_fds[0])
        assert not available(), 'Java did not retain its inherited lease after parent close'
        stop(child)
        assert available(), 'inherited child exit did not release its lease'
        with RigLease(timeout=0, paths=[(HOST_FD, lock)]) as lease:
            child = start(descriptor=lease.pass_fds[0], close=True)
            assert not available(), 'Java close improperly unlocked its parent lease'
            stop(child)
        assert available() and lock.exists(), 'lease file must remain harmless after all owners exit'
    finally:
        for child in children:
            if child.poll() is None:
                stop(child)
    print('PASS: native Java/Python exclusive/shared, rejection, inheritance, close-only and exit release')


if __name__ == '__main__':
    main()
