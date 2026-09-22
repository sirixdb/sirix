# Transient ingest positions for update diffs

## Measurement before implementation

Baseline: `a11b1a74af42d747ecf0bb0d04d11eefa4377bf6` (`origin/main` on 2026-09-22).
An initially empty array was committed, then grown through `insertSubtreeAsLastChild`
with the root token skipped. Diff storage and path summaries were enabled, using
`FILE_CHANNEL` storage. Every batch was committed. Each emitted sidecar was replayed
through counted read cursors with exactly its emitted tuples, and compared byte for
byte with the committed file. Counters include both left and right sibling moves,
summed across all append commits; setup is excluded.

| Final elements | Batch size | Append commits | Baseline sibling moves | With ingest hints |
|---:|---:|---:|---:|---:|
| 10,000 | 10,000 | 1 | 9,999 | 0 |
| 10,000 | 1,000 | 10 | 54,990 | 0 |
| 20,000 | 20,000 | 1 | 19,999 | 0 |
| 20,000 | 1,000 | 20 | 209,980 | 0 |

The baseline costs `batchSize * commits * (commits + 1) / 2 - commits` moves:
each commit revisits the prefix committed earlier. Doubling the final size at a
fixed batch size nearly quadruples the work. A single append commit stays linear.
The residual is therefore meaningful even though memoization already prevents
quadratic traversal *within* one revision.

The four baseline cases took 1.007, 0.388, 0.122, and 0.508 seconds respectively;
the first Gradle invocation, including compilation, took 28 seconds. These are
runtime observations, not performance assertions or a controlled timing comparison.
The measurement fixture is tagged `heavy`; the ordinary regression uses 2,048
elements in 16 batches (17,392 baseline moves, zero with hints).

## Implementation and lifetime

The streaming shredders share `JsonNodeTrxImpl.adaptForInsert`. That linkage path
already binds the parent and increments its child count. For a bulk append under
an array, the updated count minus one is the exact ordinal. Capture piggybacks on
this existing work, without another cursor move, input pass, or persisted field.
The primitive node-key-to-int map is allocated lazily and holds only the current
commit's known appends, including nested arrays and fused named arrays.

The serializer reads hints only for the new revision. Missing hints use the
existing memoized structural resolver, whose bounds and revision-isolation
contract [`JSON_UPDATE_DIFFS.md`](JSON_UPDATE_DIFFS.md) owns; a left walk can
also stop on a hinted sibling. The ingest map is never modified by
serialization. Old-revision deletion positions always come from the old tree.

Inserting before a sibling, removing a subtree, or moving a subtree discards all
hints in constant time. Subsequent bulk appends may capture fresh positions. Commit
serialization releases the map in `finally`; writer replacement also releases it
on rollback, revert, and intermediate commits. No hints survive into a later
revision. Standalone edits do not produce hints.

Capture is disabled without stored child counts, path summaries, or diff storage,
and on any revision that emits no sidecar. The bootstrap revision of a fresh
resource has no predecessor to diff against, so the default whole-document load
learns no ordinals at all; the gate mirrors the serializer's own condition and is
refreshed wherever the writer is replaced, so the revision number is never read on
an append. Unknown positions always retain the structural fallback. The bulk page
assembler for fresh resources does not use this cursor linkage path; this
optimization targets streaming append ingestion, where the measured residual
occurs.

## Verification

`JsonDiffArrayPositionWorkBudgetTest` counts baseline and hinted cursor moves and
compares both serializations against the actual committed bytes. Its cache probe
also requires the real append commit to allocate no fallback ordinal storage, so
disconnecting the commit-to-serializer handoff fails even when a standalone
hinted replay remains fast. An unhinted standalone tail must make exactly one
sibling move to the preceding ingested element.

Mutation proof: replacing the hinted serialization entry point with the no-hint
path fails the 2,048-element regression on its first batch: 127 sibling moves
against a zero budget. Independently, the real commit reports 128 fallback cache
entries and 4,388 backing bytes against zero. Dropping the predecessor-revision
half of the capture gate fails the 100,000-element head-insert budget on its load,
observed while the transaction still holds the map: `ingestHintBackingBytes` reads
3,145,740 against a zero budget, and the map holds one entry per loaded element.
The source was restored and rebuilt before the successful validation run.

`JsonDiffIngestPositionsTest` captures the actual pending tuples and compares each
real sidecar with the unchanged no-hint serializer. Cases cover nested and fused
arrays, Gson/Jackson ingestion, Dewey IDs, head and middle edits, removal, subtree
move, field replacement, value update, rollback/revert, later revisions, and
disabled child counts/path summaries/diff storage. The existing revision-isolation
test additionally supplies a new ordinal for a node that is deleted at a different
old ordinal, proving that hints do not cross the revision boundary.
Automatic commit boundaries and a pre-commit failure followed by retry are also
covered.

Reproduce the counted experiment (including the heavy fixture):

```bash
./gradlew :sirix-core:test \
  --tests 'io.sirix.budget.JsonDiffArrayPositionWorkBudgetTest' \
  --tests 'io.sirix.diff.JsonDiffIngestPositionsTest' \
  --tests 'io.sirix.diff.JsonDiffSerializerArrayPositionTest' \
  --no-daemon --max-workers=1 -Dorg.gradle.jvmargs=-Xmx768m \
  -PtestHeapMin=256m -PtestHeapMax=2g
```

The required broader work-budget commands remain in `docs/VERIFICATION.md`.

Local validation on 2026-09-22 used one test fork with `-Xmx2g` and a 768 MiB
Gradle JVM, with available memory and disk checked between runs:

- Core budgets, diff tests, property/oracle tests, structural fuzzing, and
  Gson/Jackson/LDJSON shredder suites: 294 tests, zero failures, nine skips;
  Gradle runtime 51 seconds.
- After restoring the mutation and rebuilding, core budgets and diff tests:
  47 tests, zero failures; query work budgets: 16 tests, zero failures.
  Combined Gradle runtime 48 seconds.
- `git diff --check` passed. No work bounds were widened and no wall-clock
  assertions were introduced.
