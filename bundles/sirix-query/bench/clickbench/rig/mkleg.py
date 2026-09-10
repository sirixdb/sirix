"""Export a verified rig leg: python3 mkleg.py TAG RUN/leg/leg.json.

Raw Java/Gradle logs and --json outputs are diagnostic evidence, not scored legs. Only the
runner's complete receipt, bound to a freshly built frozen runtime and its exact suite log,
can be exported. Unobserved submission metadata remains null. Export never grants publication
scope: curation still has to state the observed regime and the limits of a single leg.
"""
import argparse
import json
from pathlib import Path
import re
import sys

from measurement import read_timings
from runtime import file_hash
from runtime import verify_scored_runtime

RIG = Path(__file__).resolve().parent


def submission(receipt):
    receipt = Path(receipt).resolve()
    try:
        document = json.loads(receipt.read_text())
    except json.JSONDecodeError as failure:
        raise ValueError("raw logs lack scored provenance; provide measure.py's complete leg/leg.json receipt") from failure
    metadata = document.get('rig', {})
    if (metadata.get('scope') != 'steering' or metadata.get('complete') is not True
            or not metadata.get('runtime_manifest') or not metadata.get('suite_log_sha256')):
        raise ValueError('scored export requires a complete rig receipt with frozen-runtime and suite-log provenance')
    runtime = json.loads(Path(metadata['runtime_manifest']).read_text())
    verify_scored_runtime(runtime, check_source=False)
    if (metadata.get('runtime_id') != runtime['runtime_id']
            or metadata.get('source_commit') != runtime['source_commit']):
        raise ValueError('leg source/runtime identity does not match the frozen runtime manifest')
    log = receipt.parent/'suite/suite.log'
    if file_hash(log) != metadata['suite_log_sha256']:
        raise ValueError(f'leg suite-log hash mismatch: {log}')
    verdict = json.loads((log.parent/'verdict.json').read_text())
    if verdict.get('exit_code') != 0 or verdict.get('issues') != []:
        raise ValueError('leg lacks a successful, issue-free collection verdict')
    timings = read_timings(log)
    result = [[timings[q, attempt] for attempt in (1, 2, 3)] for q in range(43)]
    if result != document.get('result'):
        raise ValueError('leg timings do not match its recorded suite log')
    return dict(system='SirixDB (segment lane)', date=None, machine=None, cluster_size=1,
                proprietary='no', hardware='cpu', tuned='no',
                tags=['Java', 'document-oriented', 'embedded', 'versioned'],
                load_time=None, data_size=None, concurrent_qps=None, concurrent_error_ratio=None,
                result=result, rig=dict(metadata, source_receipt=str(receipt), source_log=str(log)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('tag')
    parser.add_argument('receipt', help='complete leg/leg.json from measure.py, never a raw log')
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', args.tag):
        raise ValueError('tag must contain only letters, digits, underscores or hyphens')
    document = submission(args.receipt)
    target = RIG/'legs'/f'query-{args.tag}.json'
    with target.open('x') as stream:
        json.dump(document, stream, indent=2, allow_nan=False)
        stream.write('\n')
    print(f'{args.tag}: 43/43 queries with 3 tries; verified steering leg, no publication claim')
    print('hot sum: %.3f s' % sum(min(row[1:]) for row in document['result']))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError) as failure:
        sys.exit(f'ABORT: {failure}')
