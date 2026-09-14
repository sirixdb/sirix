"""Restore a recorded archive without replacing an existing path; retained hashes are authoritative."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import tarfile

from campaign_100m import BASE, EVIDENCE, digest

parser = argparse.ArgumentParser()
parser.add_argument('--original', type=Path, required=True,
                    help='original path recorded in a compaction manifest (file or source directory)')
parser.add_argument('--destination', type=Path, help='optional new path inside this campaign for a rehearsal')
parser.add_argument('--verify-only', action='store_true', help='verify decompressed bytes without writing them')
args = parser.parse_args()
original = args.original.resolve()
destination = (args.destination or original).resolve()
if not destination.is_relative_to(BASE) or destination == BASE:
    parser.error('destination must be inside the campaign')
files = []
for entry in json.loads((EVIDENCE / 'archived-native-images.json').read_text()):
    files.append(dict(original=entry['source'], retained=entry['archive'],
                      original_sha256=entry['sha256'], retained_sha256=entry['archive_sha256'],
                      original_bytes=entry['logical_bytes'], mode=0o755))
for name in ('column-major-profile-compaction.json', 'column-major-secondary-compaction.json',
             'group-repeat-profile-compaction.json', 'dict-count-native-preservation.json',
             'dict-count-v3-native-preservation.json'):
    path = EVIDENCE / name
    if path.exists():
        files.extend(json.loads(path.read_text())['files'])
sources = []
for name in ('owned-artifact-compaction.json', 'column-major-old-build-archives.json',
             'group-repeat-build-archives.json', 'dict-count-build-archives.json',
             'dict-count-v3-build-archives.json'):
    path = EVIDENCE / name
    if path.exists():
        sources.extend(json.loads(path.read_text())['sources'])
matches = [entry for entry in files if Path(entry['original']) == original]
source_matches = [entry for entry in sources if Path(entry['source']) == original]
if len(matches) + len(source_matches) != 1:
    parser.error('original path must identify exactly one recorded artifact')


def check_space(size):
    if shutil.disk_usage(BASE).free - size < 20 * 1024 ** 3:
        raise RuntimeError('restoration would violate the unchanged 20 GiB free-space floor')
    if destination.exists() or destination.is_symlink():
        raise RuntimeError('refusing to overwrite existing destination: ' + str(destination))


if matches:
    entry = matches[0]
    archive = Path(entry['retained'])
    if digest(archive) != entry['retained_sha256']:
        raise RuntimeError('compressed artifact checksum mismatch')
    hasher = hashlib.sha256()
    size = 0
    if not args.verify_only:
        check_space(entry['original_bytes'])
        destination.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(archive, 'rb') as content:
        target = None if args.verify_only else destination.open('xb')
        try:
            for block in iter(lambda: content.read(1024 * 1024), b''):
                hasher.update(block)
                size += len(block)
                if target is not None:
                    target.write(block)
            if target is not None:
                target.flush()
                os.fsync(target.fileno())
        finally:
            if target is not None:
                target.close()
    if size != entry['original_bytes'] or hasher.hexdigest() != entry['original_sha256']:
        raise RuntimeError('restored artifact differs; leave destination for inspection, do not execute it')
    if not args.verify_only:
        destination.chmod(entry.get('mode', 0o644))
else:
    entry = source_matches[0]
    archive = Path(entry['archive'])
    if digest(archive) != entry['archive_sha256']:
        raise RuntimeError('source archive checksum mismatch')
    expected = {member['path']: member for member in entry['members']}
    size = sum(member.get('bytes', 0) for member in entry['members'])
    if not args.verify_only:
        check_space(size)
        destination.mkdir(parents=True)
    with tarfile.open(archive) as content:
        for member in content:
            recorded = expected.pop(member.name)
            relative = Path(member.name).relative_to(original.name)
            if relative.is_absolute() or '..' in relative.parts:
                raise RuntimeError('invalid archive member')
            target = destination / relative
            if 'symlink' in recorded:
                if not member.issym() or member.linkname != recorded['symlink']:
                    raise RuntimeError('symlink archive mismatch')
                if not args.verify_only:
                    if not (target.parent / member.linkname).resolve().is_relative_to(destination):
                        raise RuntimeError('symlink escapes restoration destination')
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.symlink_to(member.linkname)
                continue
            hasher = hashlib.sha256()
            restored_size = 0
            if not args.verify_only:
                target.parent.mkdir(parents=True, exist_ok=True)
            with content.extractfile(member) as stream:
                output = None if args.verify_only else target.open('xb')
                try:
                    for block in iter(lambda: stream.read(1024 * 1024), b''):
                        hasher.update(block)
                        restored_size += len(block)
                        if output is not None:
                            output.write(block)
                finally:
                    if output is not None:
                        output.close()
            if hasher.hexdigest() != recorded['sha256'] or restored_size != recorded['bytes']:
                raise RuntimeError('restored source member differs: ' + member.name)
            if not args.verify_only:
                target.chmod(member.mode & 0o777)
    if expected:
        raise RuntimeError('source archive has missing members')
print(json.dumps(dict(original=str(original), destination=None if args.verify_only else str(destination),
                     verified=True, restored=not args.verify_only, uncompressed_bytes=size)))
