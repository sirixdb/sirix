# Bug: JSON keys lose their style beyond the container scratch capacity

- Fix / issue: PR #1357, review R1 at starting head `9f786eba`.
- Symptom: the second key in an object nested inside 256 arrays is green instead
  of blue; JSON diff operation keys can also lose their operation colours.
- Trigger: 256 opening arrays, `{"a":1,"b":2}`, then 256 closing arrays.
- Root cause: the renderer increments logical depth beyond its fixed 256-entry
  stack while discarding container types. A later comma cannot recover whether
  its parent is an object or array, so it classifies object keys as values.
- Invariant / specification: every open JSON container retains its type until
  closed, regardless of write boundaries or the stack's initial capacity.
  Openings, closings, commas, key classification, all write overloads, flushes,
  EOF and finish/reuse share this invariant in the common value parser.
- Fix: grow the primitive stack geometrically at the shared opening boundary,
  retain it across finish, and remove the capacity-based context fallback.
  Fixed token/output buffers remain unchanged; scratch is O(maximum depth).
- Why tests missed it: the old style assertion stopped at depth 256. The deeper
  test checked payload alone, which remains correct despite the style error.
- Regression test: `ShellResultWriterTest` checks exact independent ANSI output
  for arrays and mixed parents at depths 255, 256, 257, 513 and 4097, including
  keys after nested values, diff keys, strings containing punctuation, every
  write overload, chunked flushes, incomplete EOF and repeated finish/reuse.
  The oracle is the presentation contract's blue keys/green values and explicit
  diff operation colours, assembled from fixed styled fragments rather than
  inferred by another lexer. Allocation checks measure bounded first growth
  and retained-capacity reuse without loosening the existing allowance.
- Lesson: assert semantic state on both sides of scratch-capacity boundaries,
  not merely payload survival. See [Lessons](../LESSONS.md).

## Verification

This review is separate from the historical 413/159/170 mutation audits; none
of those reports verifies this change, and none has been replaced.

The attempted focused round queued the original-writer regression reproduction,
`ShellResultWriterTest`, `ShellOutputTest`, `ShellXmlDiffColourTest`, the query
work-budget package and `IndexedArrayPositionTest`, followed by unsampled PIT
for `ShellResultWriter` with only `ShellResultWriterTest*` (CI's exact-name
selection). The local setup and original source are retained in
`build/review-json-depth/verification.gradle` and `original-src/`.

**Blocked, not passed:** the required `heavy.sh` runner could not schedule the
command after an extended wait. Both normal JVM slots were occupied; the
supported priority slot also remained blocked by the unchanged 6 GiB available
memory / load-below-12 guards. At the final check, available memory was about
5.3 GiB and load was 14.34. The waiting command was stopped before any JVM,
formatter, test or PIT process began. No before/after test reproduction or new
mutation/allocation/work-budget result is claimed. Disk remained above the
60 GiB floor. The outer test/lint/CI phases must provide those results.

The focused invocation used `--no-daemon --max-workers=2`,
`-Dorg.gradle.jvmargs=-Xmx2g -PtestHeapMin=256m -PtestHeapMax=1g`, and the
worktree-local init script. Unrelated isolated loader, temporal-reader and
translator-startup tasks were excluded to keep this review's scope focused.
Static tracing confirms the reported sequence and ordinary parent-context
transitions; `git diff --check` passes. No engine read/load/query code changed.
