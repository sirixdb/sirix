---
kind: row
---
| One `madvise` advice was assumed to mean the same thing on every POSIX platform: `MADV_DONTNEED` discards anonymous pages on Linux but is advisory on Darwin, so a documented discard-to-zeros left the old contents readable and reported success. | Pin a platform syscall's advice per platform, not per family, and make the caller's test assert the observable contract — that the next read returns zeros — so the lane that runs the other platform fails instead of silently reading stale bytes. | [POSIX discard advice](postmortems/2026-11-compact-image-decode-arena.md#review-round-2-unmapping-was-the-wrong-half-of-the-design), `CompactImageReclamationTest` |
