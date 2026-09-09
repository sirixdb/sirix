#!/usr/bin/env python3
"""One command for a fixed paired ClickBench comparison with uncertainty."""
import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import random
import secrets
import shlex
import subprocess
import sys
import time

from measurement import analyze_pairs
from measurement import BASE
from measurement import render_report
from rig_lock import RigLease
from rig_lock import require_no_benchmark
from runner import machine_settings
from runner import run_leg
from runner import run_part
from runtime import prepare_revision
from runtime import prepare_current
from runtime import remove_source_worktree
from runtime import verify_runtime
from runtime import verify_scored_runtime
from runtime import verify_shared_dependencies
from runtime import verify_shared_harness
from runtime import validate_environment
from runtime import campaign_pointers
from runtime import classify_target
from runtime import file_hash
from runtime import CAMPAIGN_DIRECTORY
from runtime import RIG
from runtime import ROOT


def write_json(path, document):
    with Path(path).open('x') as stream:
        json.dump(document, stream, indent=2, allow_nan=False)
        stream.write('\n')


def paired_orders(count, seed):
    if count < 2 or count % 2:
        raise ValueError('use an even number of pairs, at least two, to balance AB/BA order')
    generator = random.Random(seed)
    orders = []
    for _ in range(count//2):
        orders.extend(['AB', 'BA'] if generator.getrandbits(1) else ['BA', 'AB'])
    return orders


def protocol_for(args):
    if not args.db:
        raise ValueError('provide --db, or make a campaign pointer resolve; the command never loads a database')
    database = Path(args.db).resolve(strict=True)
    if not database.is_dir():
        raise ValueError('the database must already exist as a directory')
    if args.power_uw <= 0 or not math.isfinite(args.cool_below) or not 0 < args.cool_below < 100:
        raise ValueError('power must be positive and cooldown temperature must be between 0 and 100 C')
    info = database.stat()
    return dict(queries='all 43 in one JVM, inherited OS page cache', tries=3,
                power_limit_uw=args.power_uw, cool_below_C=args.cool_below,
                cool_consecutive=3, cool_sample_seconds=5, launch_recheck=True,
                cpu_policy=machine_settings(), database=str(database), database_device=info.st_dev,
                database_inode=info.st_ino, score='C6A hot min(try2,try3), +0.01 offset, unchanged board snapshot',
                board_best_hot=BASE, board_sha256=file_hash(RIG/'board/data.generated.js'),
                harness_sha256={name: file_hash(RIG/name) for name in
                                ('measure.py', 'measurement.py', 'rank.py', 'runner.py', 'runtime.py',
                                 'rig_lock.py')})


def runtime_for(args, arm, output, classification):
    manifest = getattr(args, arm+'_runtime')
    if manifest:
        if getattr(args, arm+'_jvm_arg'):
            raise ValueError('prepared manifests already fix the JVM arguments; prepare a separate runtime for an ablation')
        runtime = json.loads(Path(manifest).read_text())
        verify_runtime(runtime)
        return runtime
    return prepare_revision(getattr(args, arm), output/('runtime-'+arm), getattr(args, arm+'_jvm_arg'),
                            classification)


def owned_scratch(args, output):
    """The checkouts this comparison prepares, derived from where it puts them rather than from a
    runtime document, so a preparation that failed halfway is still accounted for."""
    paths = []
    for arm in ('baseline', 'candidate'):
        if getattr(args, arm+'_runtime'):
            continue
        source = (output/('runtime-'+arm)/'source').resolve()
        if source.is_dir():
            paths.append(source)
    return paths


def cleanup_commands(paths):
    return '\n'.join(f'git -C {shlex.quote(str(ROOT))} worktree remove --force {shlex.quote(str(source))}'
                      for source in paths)


def release_scratch(paths):
    for source in paths:
        try:
            remove_source_worktree(ROOT, source)
        except subprocess.CalledProcessError as failure:
            print(f'Could not release the scratch checkout {source}: {failure.stderr.strip()}\n'
                  f'Reclaim it with:\n{cleanup_commands([source])}', file=sys.stderr, flush=True)
        else:
            print(f'Released scratch checkout {source}', flush=True)


def save_report(output, pairs, plan):
    validate_plan(pairs, plan)
    report = analyze_pairs(pairs, planned_pairs=plan['planned_pairs'], target=plan['target_ln'])
    report['plan_sha256'] = hashlib.sha256((output/'plan.json').read_bytes()).hexdigest()
    write_json(output/'report.json', report)
    (output/'report.md').write_text(render_report(report))
    with (output/'queries.csv').open('x') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(report['queries'][0]))
        writer.writeheader()
        writer.writerows(report['queries'])
    print(render_report(report))
    print(f'Full evidence: {output}')
    return report


def compare(args):
    if not math.isfinite(args.effect_ln) or args.effect_ln <= 0:
        raise ValueError('effect size must be finite and positive')
    protocol = protocol_for(args)
    classification = classify_target(args.db, declared=args.declare_envelope)
    seed = args.seed if args.seed is not None else secrets.randbits(32)
    orders = paired_orders(args.pairs, seed)
    output = Path(args.out).resolve()
    output.mkdir(exist_ok=False, parents=True)
    try:
        with RigLease(timeout=args.lock_timeout) as lease:
            require_no_benchmark()
            baseline = runtime_for(args, 'baseline', output, classification)
            candidate = runtime_for(args, 'candidate', output, classification)
            verify_scored_runtime(baseline)
            verify_scored_runtime(candidate)
            if (baseline['java_version'] != candidate['java_version']
                    or baseline['jdk_sha256'] != candidate['jdk_sha256']):
                raise ValueError('paired runtimes use different JDKs; this protocol requires the same JDK')
            if baseline.get('envelope') != candidate.get('envelope'):
                raise ValueError('paired runtimes declare different JVM envelopes; both arms must measure '
                                 'at the same envelope for their difference to mean anything')
            verify_shared_dependencies(baseline, candidate)
            verify_shared_harness(baseline, candidate)
            plan = dict(planned_epoch=time.time(), planned_pairs=args.pairs, orders=orders, seed=seed,
                        target_ln=args.effect_ln, protocol=protocol,
                        baseline_runtime=baseline, candidate_runtime=candidate,
                        stopping_rule='complete this fixed plan; never extend until significant; failures abort without replacing observations')
            write_json(output/'plan.json', plan)
            pairs = []
            for index, order in enumerate(orders):
                pair = {'order': order}
                for name in (('baseline', 'candidate') if order == 'AB' else ('candidate', 'baseline')):
                    print(f'Pair {index+1}/{args.pairs}: {name}', flush=True)
                    runtime = baseline if name == 'baseline' else candidate
                    verify_runtime(runtime)
                    pair[name] = run_leg(runtime, args.db or protocol['database'], output/f'pair-{index+1:04}-{name}', protocol, lease)
                pairs.append(pair)
                write_json(output/f'pair-{index+1:04}.json', pair)
            write_json(output/'pairs.json', pairs)
            report = save_report(output, pairs, plan)
    except BaseException:
        scratch = owned_scratch(args, output)
        if scratch:
            print(f'The scratch checkouts are kept for diagnosis. Nothing else under {output} is '
                  f'disposable. Reclaim them with:\n{cleanup_commands(scratch)}', file=sys.stderr, flush=True)
        raise
    release_scratch(owned_scratch(args, output))
    return 3 if report['resolution'] == 'UNRESOLVED' else 0


def analyze(args):
    source = Path(args.input).resolve()
    plan = json.loads((source/'plan.json').read_text())
    pairs = json.loads((source/'pairs.json').read_text())
    validate_plan(pairs, plan)
    report = analyze_pairs(pairs, planned_pairs=plan['planned_pairs'], target=plan['target_ln'])
    print(render_report(report))
    return 3 if report['resolution'] == 'UNRESOLVED' else 0


def validate_plan(pairs, plan):
    if [pair['order'] for pair in pairs] != plan['orders']:
        raise ValueError('the observed order does not match the prespecified plan')
    for pair in pairs:
        for arm in ('baseline', 'candidate'):
            metadata = pair[arm]['rig']
            if (metadata['runtime_id'] != plan[arm+'_runtime']['runtime_id']
                    or metadata['protocol'] != plan['protocol']):
                raise ValueError('an observation differs from its planned runtime or protocol')


def prepare(args):
    output = Path(args.out).resolve()
    flags = shlex.split(args.jvm_args)
    with RigLease(timeout=args.lock_timeout):
        require_no_benchmark()
        classification = classify_target(args.db, declared=args.declare_envelope)
        runtime = (prepare_revision(args.revision, output, flags, classification) if args.revision
                   else prepare_current(output, flags, classification))
    print(json.dumps(runtime, indent=2))
    print(f"Prepared manifest: {output/'frozen/runtime.json'}")


def run(args):
    protocol = protocol_for(args)
    if args.diagnostic:
        queries = [int(value) for value in args.queries.split(',')]
    elif (args.tries != 3 or args.queries != ','.join(map(str, range(43)))
            or args.diagnostic_arg or args.diagnostic_args):
        raise ValueError('a scored leg requires all 43 queries and three tries; use --diagnostic for profiling/subsets')
    output = Path(args.out).resolve()
    output.mkdir(exist_ok=False, parents=True)
    with RigLease(timeout=args.lock_timeout) as lease:
        require_no_benchmark()
        if args.runtime:
            runtime = json.loads(Path(args.runtime).read_text())
            if args.jvm_args or args.declare_envelope:
                raise ValueError('runtime manifests already fix the JVM flags and the classified envelope; '
                                 'use --diagnostic-arg for a diagnostic overlay')
            verify_runtime(runtime)
        else:
            runtime = prepare_current(output/'runtime', shlex.split(args.jvm_args),
                                      classify_target(args.db, declared=args.declare_envelope))
        if not args.diagnostic:
            verify_scored_runtime(runtime)
        plan = dict(runtime=runtime, protocol=protocol, diagnostic=args.diagnostic)
        overlay = (runtime['jvm_args']+args.diagnostic_arg+shlex.split(args.diagnostic_args)
                   if args.diagnostic else None)
        if overlay is not None:
            plan['diagnostic_settings'] = dict(
                queries=queries, tries=args.tries, jvm_args=overlay,
                note='what this run executed; the protocol block records the scored-leg defaults it does not use')
        write_json(output/'plan.json', plan)
        if args.diagnostic:
            run_part(dict(runtime, jvm_args=overlay), args.db, output/'diagnostic', queries, protocol, lease,
                     diagnostic=True, tries=args.tries)
            print(f'Diagnostic only, no score or confidence claim: {output}')
        else:
            run_leg(runtime, args.db, output/'leg', protocol, lease)
            print(f'One complete leg, no effect estimate: {output}. Use compare for paired uncertainty.')


def common_options(parser, *, collection=True):
    parser.add_argument('--out', required=True, help='fresh output directory; existing evidence is never overwritten')
    parser.add_argument('--lock-timeout', type=float, default=120)
    if collection:
        placed = next((entry[0] for entry in campaign_pointers() if entry[2] == 'resolved'), None)
        parser.add_argument('--db', default=str(Path(placed)/'db') if placed else None,
                            help='existing database directory; defaults to the campaign database the '
                                 'rig resolves, pointer file before $CB100M_DIR')
        parser.add_argument('--power-uw', type=int, default=50_000_000, help='verify both power limits; never changes them')
        parser.add_argument('--cool-below', type=float, default=55)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    comparison = commands.add_parser('compare', help='build/freeze both revisions, collect a fixed paired plan, report uncertainty')
    common_options(comparison)
    for arm in ('baseline', 'candidate'):
        group = comparison.add_mutually_exclusive_group(required=True)
        group.add_argument('--'+arm, help='git revision, built in an isolated worktree before measuring')
        group.add_argument('--'+arm+'-runtime', help='prepared runtime.json manifest')
        comparison.add_argument('--'+arm+'-jvm-arg', action='append', default=[], help='additional JVM argument; use = before a leading dash')
    comparison.add_argument('--pairs', type=int, default=10, help='fixed even count, balanced AB/BA; do not extend until significant')
    comparison.add_argument('--effect-ln', type=float, default=.5, help='positive target for 80%%-power resolution planning')
    comparison.add_argument('--seed', type=int)
    comparison.set_defaults(function=compare)
    preparation = commands.add_parser('prepare', help='freeze a runtime without querying; current worktree by default')
    common_options(preparation, collection=False)
    preparation.add_argument('--revision')
    preparation.add_argument('--db', help='the database this runtime will measure; only the campaign 100M '
                                          'database pins the campaign envelope, anything else declares its own')
    single = commands.add_parser('run', help='one full leg or an explicitly unscored diagnostic run')
    common_options(single)
    single.add_argument('--runtime')
    single.add_argument('--diagnostic', action='store_true')
    single.add_argument('--queries', default=','.join(map(str, range(43))))
    single.add_argument('--tries', type=int, default=3)
    single.add_argument('--diagnostic-arg', action='append', default=[])
    single.add_argument('--diagnostic-args', default='', help='shell-like diagnostic JVM arguments, parsed without shell execution')
    for child in (preparation, single):
        child.add_argument('--jvm-args', default='', help='shell-like argument text, parsed without shell execution')
    preparation.set_defaults(function=prepare)
    single.set_defaults(function=run)
    for child in (comparison, preparation, single):
        child.add_argument('--declare-envelope', action='store_true',
                           help='assert this target is not the campaign 100M database, so its own flags fix the '
                                'envelope; only needed where no campaign pointer resolves, and refused when one '
                                'names this database')
    analysis = commands.add_parser('analyze', help='recompute a completed collection without running a JVM')
    analysis.add_argument('input')
    analysis.set_defaults(function=analyze)
    args = parser.parse_args()
    if args.action != 'analyze':
        validate_environment()
    return args.function(args)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, ArithmeticError, subprocess.CalledProcessError) as failure:
        print(f'ABORT: {failure}. Any incomplete evidence is retained and cannot score.', file=sys.stderr)
        sys.exit(2)
