#!/usr/bin/env python3
"""Normalized identity of every entry on an effective Java runtime classpath."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any


_MAGIC = b"SIRIX-HFT-CLASSPATH-V1\0"
_BUILD_IDENTITY_RESOURCE = "META-INF/sirix-hft-build.properties"


def _update_file(digest: Any, path: Path) -> None:
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(64 * 1024), b""):
            digest.update(chunk)


def _entry_digest(path: Path) -> bytes:
    if path.is_file():
        digest = hashlib.sha256(b"F\0")
        _update_file(digest, path)
        return digest.digest()
    if not path.is_dir():
        raise ValueError(f"runtime classpath entry is unreadable: {path}")
    digest = hashlib.sha256(b"D\0")
    files = sorted(
        (
            candidate
            for candidate in path.rglob("*")
            if candidate.is_file()
            and candidate.relative_to(path).as_posix() != _BUILD_IDENTITY_RESOURCE
        ),
        key=lambda candidate: candidate.relative_to(path).as_posix(),
    )
    for file in files:
        name = file.relative_to(path).as_posix().encode("utf-8")
        digest.update(len(name).to_bytes(8, "big"))
        digest.update(name)
        digest.update(file.stat().st_size.to_bytes(8, "big"))
        _update_file(digest, file)
    return digest.digest()


def runtime_classpath_sha256(classpath: str) -> str:
    if not classpath.strip():
        raise ValueError("runtime classpath must not be empty")
    raw_entries = classpath.split(os.pathsep)
    if any(not entry.strip() for entry in raw_entries):
        raise ValueError("runtime classpath contains an empty entry")
    digest = hashlib.sha256(_MAGIC)
    for entry in raw_entries:
        path = Path(entry).resolve()
        digest.update(_entry_digest(path))
    return digest.hexdigest()
