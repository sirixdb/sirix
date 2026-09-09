#!/usr/bin/env python3
"""Fixed paired rig run with untimed answer dumps and a byte-equality stop gate.

The underlying measure.compare owns builds, leases, cooling, collection, analysis and scratch
cleanup. This experiment adds --dump equally to both arms, after the JVM's timed query execution,
and checks every completed leg against the first baseline before permitting another launch.
"""
import hashlib
import json
from pathlib import Path
import sys

RIG = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RIG))

import measure
import runner

MAX_LEGS = 24
MIN_AVAILABLE_BYTES = 26 << 30


def answers(directory):
    files = [directory / '.clickbench-result-format']
    files.extend(directory / f'q{query:02}.jsonl' for query in range(43))
    return {path.name: path.read_bytes() for path in files}


def main():
    original_command = runner.command
    original_leg = measure.run_leg
    original_protocol = measure.protocol_for
    reference = None
    current_output = None
    launches = 0

    def protocol(args):
        if args.pairs != 12 or args.effect_ln != 2.0 or args.seed != 0:
            raise ValueError('this experiment prespecifies 12 pairs, effect-ln 2.0, seed 0')
        value = original_protocol(args)
        value['answer_validation'] = dict(
            dump='after timed query, on final try, in both arms',
            comparison='all 43 lossless JSONL files byte-identical to first baseline',
            controller_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            minimum_available_bytes=MIN_AVAILABLE_BYTES, maximum_legs=MAX_LEGS)
        return value

    def command(runtime, database, **kwargs):
        nonlocal launches
        if current_output is None or launches >= MAX_LEGS:
            raise RuntimeError('no current leg or the authorized 24-leg allowance is exhausted')
        memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
        available = int(memory['MemAvailable'].split()[0]) * 1024
        if available < MIN_AVAILABLE_BYTES:
            raise RuntimeError(f'busy box: MemAvailable={available} below {MIN_AVAILABLE_BYTES}')
        argv = original_command(runtime, database, **kwargs)
        launches += 1
        (current_output / 'launch-allowance.json').write_text(json.dumps(dict(
            leg=launches, maximum=MAX_LEGS, available_bytes=available), indent=2) + '\n')
        return argv + ['--dump', str(current_output / 'answers')]

    def run_leg(runtime, database, output, protocol, lease):
        nonlocal current_output, reference
        current_output = Path(output)
        leg = original_leg(runtime, database, output, protocol, lease)
        observed = answers(current_output / 'answers')
        if reference is None:
            if not current_output.name.endswith('-baseline'):
                raise RuntimeError('the first leg must establish the current baseline answers')
            reference = observed
        mismatches = [name for name, data in observed.items() if data != reference[name]]
        (current_output / 'answer-equality.json').write_text(json.dumps(dict(
            byte_identical=not mismatches, mismatches=mismatches,
            sha256={name: hashlib.sha256(data).hexdigest() for name, data in observed.items()}), indent=2) + '\n')
        if mismatches:
            raise RuntimeError(f'answer mismatch: {mismatches}; stop without a performance claim')
        print(f'Correctness: all 43 answers byte-identical ({current_output.name})', flush=True)
        return leg

    measure.protocol_for = protocol
    runner.command = command
    measure.run_leg = run_leg
    return measure.main()


if __name__ == '__main__':
    sys.exit(main())
