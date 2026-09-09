"""Linux process-owned benchmark leases, shared by every rig entry point.

Never unlink a lock file. An empty existing file is harmless: only flock ownership
matters. Children inherit the descriptors, including after an abrupt parent exit.
"""
import argparse
import fcntl
import math
import os
from pathlib import Path
import stat
import subprocess
import sys
import time

from runtime import CAMPAIGN_DIRECTORY
from runtime import RIG_WORK
from runtime import campaign_pointers

HOST_FD = 'CB_RIG_HOST_LOCK_FD'
LEGACY_FD = 'CB_RIG_LEGACY_LOCK_FD'


def lock_paths():
    paths = [(HOST_FD, Path('/tmp')/f'sirix-clickbench-{os.getuid()}.lock')]
    work = os.environ.get(RIG_WORK)
    if work:
        paths.append((LEGACY_FD, Path(work).resolve()/'leg.lock'))
    return paths


def verify_descriptor(fd, path, exclusive=True):
    if fd < 3:
        raise ValueError('a rig lease must use a descriptor above stderr')
    own = os.fstat(fd)
    named = os.stat(path, follow_symlinks=False)
    if not stat.S_ISREG(own.st_mode) or (own.st_dev, own.st_ino) != (named.st_dev, named.st_ino):
        raise ValueError(f'rig descriptor does not name the current regular lock file: {path}')
    for line in Path(f'/proc/self/fdinfo/{fd}').read_text().splitlines():
        fields = line.split()
        if len(fields) > 4 and fields[0] == 'lock:' and fields[2] == 'FLOCK':
            if fields[4] == 'WRITE' or (not exclusive and fields[4] == 'READ'):
                return
    raise ValueError(f'descriptor has no sufficient kernel flock lease: {path}')


class RigLease:
    def __init__(self, *, exclusive=True, timeout=120, paths=None):
        if sys.platform != 'linux':
            raise ValueError('the 100M measurement rig requires Linux flock ownership')
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError('lock timeout must be nonnegative')
        self.exclusive = exclusive
        self.timeout = timeout
        self.paths = list(lock_paths() if paths is None else paths)
        self.descriptors = {}

    def __enter__(self):
        deadline = time.monotonic()+self.timeout
        try:
            for key, path in self.paths:
                inherited = os.environ.get(key)
                if inherited is not None:
                    original = int(inherited)
                    verify_descriptor(original, path, self.exclusive)
                    self.descriptors[key] = os.dup(original)
                    continue
                fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
                self.descriptors[key] = fd
                while True:
                    try:
                        fcntl.flock(fd, (fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
                        verify_descriptor(fd, path, self.exclusive)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError(f'rig busy: a live process owns {path}; inspect lslocks, never delete the file')
                        time.sleep(min(.1, max(0, deadline-time.monotonic())))
            return self
        except BaseException:
            self.close()
            raise

    def child_environment(self):
        """Pin the campaign pointer this process resolved into every child, so a rig-launched JVM
        classifies its database exactly as the launcher did instead of re-resolving from whatever
        the operator's shell happened to hold."""
        env = os.environ.copy()
        for key in (HOST_FD, LEGACY_FD):
            env.pop(key, None)
        consulted = campaign_pointers()
        if consulted:
            env[CAMPAIGN_DIRECTORY] = consulted[0][0]
        env.update({key: str(fd) for key, fd in self.descriptors.items()})
        return env

    @property
    def pass_fds(self):
        return tuple(self.descriptors.values())

    def verify(self):
        for key, path in self.paths:
            verify_descriptor(self.descriptors[key], path, self.exclusive)

    def close(self):
        for fd in self.descriptors.values():
            # No LOCK_UN: an inherited descriptor shares its lock with the parent.
            os.close(fd)
        self.descriptors.clear()

    def __exit__(self, exc_type, exc, traceback):
        self.close()


def benchmark_processes():
    """Exact argv tokens avoid matching shell prompts containing a class name."""
    mains = {b'io.sirix.query.bench.clickbench.ClickBenchRunMain',
             b'io.sirix.query.bench.clickbench.ClickBenchLoadMain'}
    found = []
    for process in Path('/proc').iterdir():
        if not process.name.isdigit() or int(process.name) == os.getpid():
            continue
        try:
            argv = (process/'cmdline').read_bytes().split(b'\0')
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if mains.intersection(argv):
            found.append(int(process.name))
    return found


def require_no_benchmark():
    live = benchmark_processes()
    if live:
        raise RuntimeError(f'benchmark JVMs are already alive: {live}; the rig must be exclusive')


def other_java_processes(exclude=None):
    found = []
    for process in Path('/proc').iterdir():
        if not process.name.isdigit() or int(process.name) == exclude:
            continue
        try:
            if (process/'comm').read_text().strip() == 'java':
                found.append(int(process.name))
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
    return found


def wait_for_quiet_java(timeout=30):
    deadline = time.monotonic()+timeout
    while True:
        live = other_java_processes()
        if not live:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f'quiet measurement window unavailable; other Java processes are alive: {live}')
        time.sleep(.5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='verify an inherited lease without acquiring one')
    parser.add_argument('--timeout', type=float, default=120)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.check:
        for key, path in lock_paths():
            if key not in os.environ:
                raise ValueError(f'missing inherited lease: {key}')
            verify_descriptor(int(os.environ[key]), path)
        return 0
    command = args.command
    if command and command[0] == '--':
        command = command[1:]
    if not command:
        parser.error('provide a command after --, or use --check')
    with RigLease(timeout=args.timeout) as lease:
        require_no_benchmark()
        process = subprocess.Popen(command, env=lease.child_environment(), pass_fds=lease.pass_fds)
        try:
            return process.wait()
        except BaseException:
            process.terminate()
            process.wait()
            raise


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError) as failure:
        print(f'ABORT: {failure}', file=sys.stderr)
        sys.exit(2)
