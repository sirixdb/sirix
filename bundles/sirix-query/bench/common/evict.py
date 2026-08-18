#!/usr/bin/env python3
"""Drop the OS page cache for a file or a directory tree, without root.

This is the *cold regime* half of the benchmark protocol used by both the
ClickBench and the JSONBench campaigns: a cold measurement is a fresh process
reading from a cache the previous run cannot have warmed.

    evict.py /path/to/db [/path/to/other ...]
    evict.py --verify /path/to/db          # also report what was actually evicted

Why posix_fadvise and not /proc/sys/vm/drop_caches
--------------------------------------------------
`echo 3 > /proc/sys/vm/drop_caches` needs root, and it drops *everything* --
including the pages of the binary under test, the JDK, and any other engine's
files, which makes interleaved A/B arms depend on the order they ran in.
`posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)` needs no privileges and evicts
exactly the files named. For the files being measured the two are equivalent:
after either one, the next read faults from the device. Verified on the campaign
box by comparing suite timings under both (within run-to-run spread), and
directly by `--verify`, which reports residency measured with mincore(2).

The one thing fadvise will NOT do is evict *dirty* pages -- the kernel keeps
them until writeback. A database that was just loaded or committed therefore has
to be flushed first, so this script calls sync(2) before evicting. Without that,
the first "cold" run after a load silently measures a warm cache.

Exit status: 0 on success, 1 if nothing could be evicted (a wrong path is the
usual cause, and a cold protocol that silently evicts nothing is worse than no
protocol at all).
"""

from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import os
import sys

PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")

_PROT_READ = 0x1
_MAP_SHARED = 0x01
_MAP_FAILED = ctypes.c_void_p(-1).value

# byte -> low bit, so a mincore vector can be counted with bytes.count()
_LOW_BIT = bytes(value & 1 for value in range(256))


def _load_libc():
    """libc with the three calls --verify needs, or None if they are unavailable."""
    try:
        libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
        libc.mmap.restype = ctypes.c_void_p
        libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                              ctypes.c_int, ctypes.c_int, ctypes.c_long]
        libc.munmap.restype = ctypes.c_int
        libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        libc.mincore.restype = ctypes.c_int
        libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t,
                                 ctypes.POINTER(ctypes.c_ubyte)]
        return libc
    except (OSError, AttributeError):
        return None


def resident_bytes(libc, fd: int, size: int) -> int:
    """Bytes of this file currently in the page cache, via mincore(2)."""
    if libc is None or size == 0:
        return 0
    addr = libc.mmap(None, size, _PROT_READ, _MAP_SHARED, fd, 0)
    if addr is None or addr == _MAP_FAILED:
        return 0
    try:
        pages = (size + PAGE_SIZE - 1) // PAGE_SIZE
        vector = (ctypes.c_ubyte * pages)()
        if libc.mincore(ctypes.c_void_p(addr), size, vector) != 0:
            return 0
        resident_pages = bytes(vector).translate(_LOW_BIT).count(1)
        return min(resident_pages * PAGE_SIZE, size)
    finally:
        libc.munmap(ctypes.c_void_p(addr), size)


def iter_files(root: str):
    """Every regular file under root (or root itself if it is a file)."""
    if os.path.isfile(root):
        yield root
        return
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            yield os.path.join(dirpath, name)


def evict(root: str, verify: bool, libc) -> tuple[int, int, int, int]:
    """Evict one path. Returns (files, bytes, resident_before, resident_after)."""
    files = total = before = after = 0
    for path in iter_files(root):
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            continue
        try:
            size = os.fstat(fd).st_size
            if verify:
                before += resident_bytes(libc, fd, size)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            if verify:
                after += resident_bytes(libc, fd, size)
            files += 1
            total += size
        except OSError:
            pass
        finally:
            os.close(fd)
    return files, total, before, after


def mib(value: int) -> str:
    return f"{value / (1 << 20):.1f} MiB"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evict the page cache for the given files/directories "
                    "(posix_fadvise DONTNEED).")
    parser.add_argument("paths", nargs="+", help="files or directory trees to evict")
    parser.add_argument("--verify", action="store_true",
                        help="measure page-cache residency before and after with mincore(2)")
    parser.add_argument("--no-sync", action="store_true",
                        help="skip the sync(2) that flushes dirty pages (they cannot be evicted "
                             "while dirty -- only pass this if nothing has written to the target)")
    parser.add_argument("-q", "--quiet", action="store_true", help="print only the total line")
    args = parser.parse_args()

    for path in args.paths:
        if not os.path.exists(path):
            print(f"evict.py: no such path: {path}", file=sys.stderr)
            return 1

    if not args.no_sync:
        os.sync()

    libc = _load_libc() if args.verify else None
    if args.verify and libc is None:
        print("evict.py: --verify needs libc mincore(2); reporting eviction without residency",
              file=sys.stderr)

    files = total = before = after = 0
    for path in args.paths:
        f, t, b, a = evict(path, args.verify, libc)
        files += f
        total += t
        before += b
        after += a
        if not args.quiet:
            if args.verify:
                print(f"  {path}: {f} files, {mib(t)} on disk, "
                      f"cached {mib(b)} -> {mib(a)}")
            else:
                print(f"  {path}: {f} files, {mib(t)} on disk")

    if files == 0:
        print("evict.py: evicted nothing -- check the path", file=sys.stderr)
        return 1

    if args.verify:
        freed = before - after
        print(f"evicted {files} files, {mib(total)} on disk; "
              f"page cache {mib(before)} -> {mib(after)} (freed {mib(freed)})")
        # Pages another live process still maps cannot be dropped. That is a real
        # warning: it means the next run is not fully cold.
        if before > 0 and after > before * 0.01:
            print(f"WARNING: {mib(after)} still resident -- something is holding these pages "
                  f"(a running engine? a live mmap?). The next run is NOT fully cold.",
                  file=sys.stderr)
    else:
        print(f"evicted {files} files, {mib(total)} on disk")
    return 0


if __name__ == "__main__":
    sys.exit(main())
