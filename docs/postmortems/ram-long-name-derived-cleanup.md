# Bug: RAM retirement mistakes an impossible derived name for a leftover

- Fix / issue: test-phase follow-up to #1463 and TXN-615.
- Behaviour and invariants: every filesystem-valid IN_MEMORY resource name can be removed or closed, leaving no footprint and permitting recreation with empty state. Retire actual derived siblings before their source. An inconclusive filesystem lookup must not discard cleanup proof; denied traversal or enumeration remains reportable and retryable.
- Error cases and derived tests: use the longest locally supported resource name (up to the existing 220-character fixture), commit JSON/XML content, close or remove the resource, verify the resource directory is absent, and recreate empty state. Existing denied-traversal and derived-sibling permission tests guard against treating inaccessible children as absent.
- Symptom: a legal 143-byte name commits successfully, but final database close throws `SirixIOException` and retains the resource footprint.
- Trigger: the nonexistent sibling's name gains `$derived`, exceeding the filesystem component limit. Its `Files.notExists` result is false.
- Root cause: cleanup treats every inconclusive attribute lookup as a potentially existing derived resource. It attempts to retire an impossible sibling, cannot rename it, and retains the source as ownership proof indefinitely.
- Why tests missed it: the original 220-character fixture failed at creation on this filesystem; adapting it to the actual limit exposes the separate cleanup defect. On a filesystem accepting 255-byte components, its derived name remains legal.
- Regression test: `LocalDatabaseInMemoryLifetimeTest.longResourceNamesAreRetiredAndReusable` exercises both document models; its expected absence and empty recreation follow TXN-615 clauses 3, 5 and 8. Before the product fix, both JSON/XML close cases failed with `SirixIOException` after successful commits (Gradle exit 1). After the fix, all four close/removal cases passed (Gradle exit 0).
- Lesson: see [Lessons](../LESSONS.md): distinguish an inconclusive attribute lookup from a directory entry proven absent by successful enumeration.

## Focused verification

A temporary Gradle `Test` task used the standard test runtime/JVM settings, a 1 GiB
heap and 256 MiB allocator cap. It selected only these six methods, running 16
parameterized cases successfully, with no skipped or failed cases:

- `LocalDatabaseInMemoryLifetimeTest.longResourceNamesAreRetiredAndReusable`
- `LocalDatabaseInMemoryLifetimeTest.derivedLeftoversAreRetiredBeforeSourceNameReuse`
- `LocalDatabaseInMemoryLifetimeTest.removalRetiresDerivedSiblingWhenParentCannotBeListed`
- `LocalDatabaseInMemoryLifetimeTest.missingKnownDirectoryDoesNotBlockRemovalOrRecreation`
- `DatabasesInMemoryLifetimeTest.failedTraversalRemainsPendingUntilRepeatedClose`
- `DatabasesInMemoryLifetimeTest.finalCloseRetainsKnownCleanupWhenDirectoryScanningFails`

The independent oracle is TXN-615: retired RAM resources and their derived state
leave no original footprint, reused names start empty, and cleanup denied by
permissions remains retryable. The unreadable-parent case uses actual POSIX
permissions on a real directory; it rejects silently leaving a derived sibling
behind after removing its source. Permission cases ran as a non-root user.

Six manual source mutants were compiled into isolated classpath overlays and run
through the same 16 methods/cases using the JUnit Platform launcher. Each compiled
successfully; every mutant exited 1 because of behavioral assertion failures or
cleanup exceptions, with no aborted tests. The unmodified overlay exited 0 with
16 passes. Production sources were unchanged by mutation testing.

| Injected wrong behavior | Failing cases | Result |
| --- | ---: | --- |
| Report confirmed missing paths as present | 8 | Killed |
| Always report absence after successful enumeration | 4 | Killed |
| Never report absence after successful enumeration | 4 | Killed |
| Report absence after failed enumeration | 2 | Killed |
| Invert the check before retiring a derived sibling | 10 | Killed |
| Invert the check after retiring a derived sibling | 6 | Killed |

No surviving manual mutants require reviewed dispositions. This was focused manual
mutation testing, not a PIT run or a full repository test run. No linters,
formatters or static analysis were run. Temporary launchers, overlays, databases
and newly generated test outputs were removed after verification.
