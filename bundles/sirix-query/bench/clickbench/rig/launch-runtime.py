#!/usr/bin/env python3
"""Direct locked JVM launch for the historical cold-round driver (never Gradle timing)."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

from rig_lock import RigLease
from rig_lock import require_no_benchmark
from rig_lock import wait_for_quiet_java
from runtime import command
from runtime import validate_environment
from runtime import verify_runtime


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', required=True)
    parser.add_argument('--db', required=True)
    parser.add_argument('--json', required=True)
    parser.add_argument('--queries', default='')
    parser.add_argument('--tries', type=int, default=3)
    args = parser.parse_args()
    validate_environment()
    output = Path(args.json).resolve()
    if output.exists():
        raise ValueError('refusing to overwrite an existing round result')
    runtime = json.loads(Path(args.runtime).read_text())
    verify_runtime(runtime)
    queries = None
    if args.queries:
        queries = []
        for token in args.queries.split(','):
            if '-' in token:
                start, end = map(int, token.split('-'))
                if start > end:
                    raise ValueError('query ranges must be ascending')
                queries.extend(range(start, end+1))
            else:
                queries.append(int(token))
    argv = command(runtime, args.db, queries=queries, tries=args.tries)+['--json', str(output)]
    with RigLease() as lease:
        require_no_benchmark()
        wait_for_quiet_java()
        process = subprocess.Popen(argv, env=lease.child_environment(), pass_fds=lease.pass_fds)
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
