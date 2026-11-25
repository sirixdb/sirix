# Bug: RAM long-name acceptance fixture exceeds the filesystem limit

- Fix / issue: test-phase follow-up to #1463 and TXN-615.
- Symptom: `longResourceNamesAreRetiredAndReusable` fails for JSON and XML at initial resource creation, before exercising retirement.
- Trigger: run the acceptance test in a worktree on a filesystem with a 143-byte filename limit; the fixture requests a 220-character ASCII name. A direct directory-creation probe returns `ENAMETOOLONG`.
- Root cause: the fixture assumes the common 255-byte component limit. The retirement invariant instead concerns a valid name near the filesystem's actual limit: adding a retirement suffix must not make cleanup fail.
- Why tests missed it: earlier validation used filesystems accepting the hard-coded 220-character component.
- Regression test: `LocalDatabaseInMemoryLifetimeTest.longResourceNamesAreRetiredAndReusable` now probes the longest accepted ASCII name up to 220 in its disposable directory before creating the real resource. It still commits, closes, checks footprint removal, reopens, and recreates the name at empty revision 0. On a 143-byte filesystem, the formerly appended UUID suffix would still exceed the limit; on a 255-byte filesystem, the original 220-character case remains. Pre-fix failures are retained in the test-phase evidence directory; run `./gradlew :sirix-core:test --tests 'io.sirix.access.LocalDatabaseInMemoryLifetimeTest.longResourceNamesAreRetiredAndReusable'` for both models after the fixture repair.
- Lesson: derive filesystem-boundary fixtures from a successful local filesystem operation, while retaining a name long enough to reject the original suffix-growth bug. See [Lessons](../LESSONS.md).
