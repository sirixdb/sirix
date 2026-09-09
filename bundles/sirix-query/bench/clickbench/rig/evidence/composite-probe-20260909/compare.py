"""Fixed ten-pair rig comparison, with all-43 byte equality before accepting each leg.

The rig owns freezing, exclusive leases, cooldown, telemetry, scoring, and scratch
cleanup. This study adds untimed answer dumps equally to both arms and stops on
any mismatch against the freshly profiled, landed baseline.
"""
import hashlib
import json
from pathlib import Path
import sys

RIG = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RIG))

import measure
import runner

PAIRS = 10
TARGET_LN = 0.25
MIN_AVAILABLE_BYTES = 26 << 30


def answers(directory):
    files = [directory / '.clickbench-result-format']
    files.extend(directory / f'q{query:02}.jsonl' for query in range(43))
    return {path.name: path.read_bytes() for path in files}


def main():
    original_command = runner.command
    original_leg = measure.run_leg
    original_protocol = measure.protocol_for
    reference = answers(Path(__file__).resolve().parent / 'baseline-answers')
    current_output = None
    launches = 0

    def protocol(args):
        if args.pairs != PAIRS or args.effect_ln != TARGET_LN or args.seed != 0:
            raise ValueError('prespecified plan: 10 pairs, effect-ln 0.25, seed 0')
        if args.baseline_jvm_arg or args.candidate_jvm_arg:
            raise ValueError('comparison uses the unchanged canonical JVM envelope')
        value = original_protocol(args)
        value['answer_validation'] = dict(
            dump='after timed query, on final try, equally in both arms',
            comparison='all 43 lossless JSONL files byte-identical to landed baseline profile',
            reference_sha256={name: hashlib.sha256(data).hexdigest() for name, data in reference.items()},
            controller_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            minimum_available_bytes=MIN_AVAILABLE_BYTES, maximum_legs=PAIRS * 2,
            profiling_jvms_already_used=1, authorized_maximum_pairs=12,
            primary_queries=[16, 18, 31, 32], primary_target_ln=TARGET_LN)
        return value

    def command(runtime, database, **kwargs):
        nonlocal launches
        if current_output is None or launches >= PAIRS * 2:
            raise RuntimeError('no current leg or fixed study allowance exhausted')
        memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
        available = int(memory['MemAvailable'].split()[0]) * 1024
        if available < MIN_AVAILABLE_BYTES:
            raise RuntimeError(f'busy box: MemAvailable={available} below {MIN_AVAILABLE_BYTES}')
        argv = original_command(runtime, database, **kwargs)
        launches += 1
        (current_output / 'launch-allowance.json').write_text(json.dumps(dict(
            leg=launches, maximum=PAIRS * 2, available_bytes=available), indent=2) + '\n')
        return argv + ['--dump', str(current_output / 'answers')]

    def run_leg(runtime, database, output, protocol, lease):
        nonlocal current_output
        current_output = Path(output)
        leg = original_leg(runtime, database, output, protocol, lease)
        observed = answers(current_output / 'answers')
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
