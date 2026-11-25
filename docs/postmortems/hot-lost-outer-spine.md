# Bug: HOT subtree insertion loses its outer spine

- Fix / issue: https://github.com/sirixdb/sirix/issues/1457. The merged fix referenced #1452,
  a number this repository has no issue for; #1457 is the issue this bug was filed as.
- Symptom: descending 2,048/4,096 sequential keys produced structural inner height 2/3
  while ascending produced height 1 with identical four/eight full leaves. Descending
  skewed 50,000-key seeds 5 and 6 produced height 9. Ancestors retained stored height 1
  above taller children. Correct key answers hid the excess lookup work.
- Trigger: CAS writer, sixteen-byte big-endian keys, one posting per key. Descending
  insertion 1,537 of the four-leaf case splits a leaf through a Direction-1 collision
  below an outer root that still has capacity.
- Root cause: `subInsertAt` starts a local spine. Its split correctly materializes a local
  root, but the outer caller neither updates ancestor heights nor absorbs the replacement.
  Later operations see stale metadata and the extra levels remain in committed snapshots.
- Why tests missed it: key/posting oracles detect loss and misrouting, not excess inner
  pages. The invariant validator bounded depth without independently checking stored heights.
  Comparing only with the bulk builder also cannot establish Binna's minimum-height claim.
- Regression tests: `HOTSubInsertSpineTest` independently walks references, checks every
  stored key routes to its actual leaf, and compares every stored height to the walk before
  commit and after cold reopening both historical revisions. It covers ascending controls,
  descending four/eight-leaf cases, nonzero outer slots and skew seeds 5/6. The original run
  failed on every descending case while both ascending controls passed. The constructed
  boundary-child overflow test exercises the second published sub-insertion call site in
  all four versioning modes. Its live two-level fixture is reconstructed after the baseline
  commit, which otherwise normalizes away the intended call-site witness.
  `HOTCommitNormalizerTest` checks dirty-only scope, preserved
  leaf references, exact heights, idempotence, wire height bounds and cycle refusal.
  `HOTOuterHeightMaintenanceTest` covers growing/shrinking maxima, tied siblings, unchanged
  maxima and failure poisoning against an independent reference walk.
- Lesson: local dispatch boundaries are not metadata publication boundaries. Independently
  verify live heights and lookup work, and charge structural normalization to the phase
  whose latency budget allows it; see [Lessons](../LESSONS.md).

A local insertion now updates height metadata on the already-owned outer CoW spine before
returning. Growth propagates maxima without allocation; a decrease resolves the bounded
set of siblings only where the previous maximum may have disappeared. It does not change
routing, leaf packing or insertion's structural dispatch. Height updates never mutate a
committed/shared indirect page. A failure after local publication poisons the transaction.

A stronger boundary-child regression exposed a second consequence of losing the containing
spine: overflowing that local leaf can publish a root whose leading discriminator equals
its parent's, violating I11 before commit. Commit-time normalization cannot defer this
invariant. At that collision, only a direct leaf that is certain to store the mutation
without splitting may use local insertion; other cases decline before publication and use
the existing outer structural-frontier fallback. No new insertion-time folding algorithm
is introduced. The regression requires the fallback and checks all four versioning modes.

Before recursively serializing a dirty HOT root, commit normalizes its dirty indirect blocks
bottom-up. It folds compatible child blocks by composing sparse routing coordinates and
preserves the exact leaf and side-reference objects. Clean subtrees are opaque boundaries:
only their root height may be read, and they are neither normalized nor rewritten. A folded
block keeps the dirty parent's page identity. Copy-on-write history and the existing commit
ownership/serialization flow remain authoritative. This is reference-only folding, without
reconstructing keys or repacking leaves.

The allocation audit treats commit as a per-page hot path. The first commit normalizer
allocated an identity map, scratch arrays, a capturing key supplier and boxed MultiMask
assembly. The revised normalizer reuses session-owned bounded scratch, detects cycles on
its 64-entry ancestor stack, and clears retained references on every success or failure.
Scratch is initialized lazily at the session's first normalizing HOT commit, so insert-only
transactions do not construct it. Storage writers are replaced on commit and rollback;
retaining scratch in that shorter lifecycle still allocated 1,184 B per normalizing commit.
The session now retains it across writer replacement and later transactions, releases it on
close, and confines use to its exclusive writer after pending writes complete. Finally-reset
clears page references, the active mask count and the in-use flag on success and failure. Fold preflight copies discriminator positions into primitive scratch
without populating lazy per-page caches. Direct primitive assembly creates only the accepted
replacement page and owned routing/search storage; the immutable transaction-log envelope
is created only when publishing that output. Clean-root and resident no-fold paths allocate
nothing after workspace initialization. A successful fold replaces two reachable blocks
with one, so normalization does not add pages to commit serialization.

`HOTCommitAllocationBudgetTest` runs in the existing clean allocation JVM. It asserts zero
bytes for clean/no-fold traversal, refused folds and resident-spine height propagation;
SingleMask and MultiMask folds are compared with the independently established page
factories' output cost (240 B for the tested SingleMask graph, 352 B for MultiMask).
A published replacement has a separate pinned bound of one 32-byte PageContainer plus
152 bytes for the existing cache-retirement path. Workspace reuse across session transactions
is checked independently of this output budget; no-fold and steady-state workspace allowance
is exactly zero. Primitive mask round-trips and workspace-overwrite routing tests
check all 32 discriminator columns, scalar/vector layouts and ownership of retained arrays.
The warmed measurement batches avoid one-time test-loop initialization; no zero bound is
widened. The initial measurement JVM removed the established factory's temporary
shuffle-index array; that optimization is not portable. The later
[CI follow-up](hot-ci-boundary-heights.md) records why the comparison factory became
an allocating control independent of the exact production output budget.

Fanout, mask width, parent decision order, sparse partial order and wire representability
must all permit a fold. Otherwise the existing structure remains. The differential oracle
compares every eight-bit key, including absent keys, over 400 deterministic sparse shapes.
It exposed aliasing when a parent prefix and child partials share extra mask requirements
(seed `0x1452`, trial 35, parent mask `0xc7`, child mask `0x2c`, query byte 207); strict unsigned
partial order rejects that composition. Long-key tests distinguish SingleMask's int start
position from MultiMask's u8 extraction positions, including the byte-255/256 boundary.

Metadata is exact when a successful mutation returns and in newly committed snapshots,
assuming valid metadata in the input revision. Private intermediate states during a mutation
or normalization are not published as snapshots. This dirty-only change does not backfill
already-invalid metadata or globally rebuild inflated clean subtrees in older revisions.

### Review follow-up: consolidation after value-only updates

R1 identified a second publication boundary after `dispatchInsert`: periodic
`consolidateLeafParent`. A cold committed height-three root can retain a cleared pending
hint through value-only CoW. Updating a leaf below a two-leaf indirect child makes that
child transaction-owned; a subsequent cadence update in a direct sibling consolidates
their full 32-child parent. Consolidation preserves live height but frees fanout, making
the dirty indirect child foldable at commit. Without re-arming the outer spine, commit
normalizes the parent independently and serializes the skipped root with stale height three
above a structural height-two tree.

The root cause was treating dispatch completion as the only structural publication
boundary. Consolidation now marks its existing owned route after successful registration,
using the same bounded, allocation-free marking helper as structural dispatch. No-work
consolidation and value-only dispatch still do not mark ancestors. Both the unchanged live
height and the later commit shrink must obey the exact-height invariant.

`HOTConstructedShapeIntegrationTest.consolidationAfterValueUpdatesRearmsTheOuterSpine`
constructs this full-parent witness, commits and cold reopens it, then performs the two
value updates with cadence two. It requires actual consolidation, unchanged live height
three, a re-armed root, exact stored heights from an independent reference walk, full blob
routing, and cold revision-one/revision-two history across all four versioning modes.
The resulting revision must have height two. Previous re-arming coverage reached a
structural insertion within dispatch, not this later maintenance publication. The fixture's
shared verification now checks exact stored heights as well as routing in all constructed
scenarios. See the corresponding [lesson](../LESSONS.md).

Earlier timing and allocation tables below describe their original measured source versions,
before this review correction; they are not measurements of the consolidation fix. The
accepted fold graph, 32-byte container and 152-byte cache-retirement budgets, zero workspace
and no-fold bounds remain unchanged. Refs #1452.

Review-fix verification passed on this worktree: 83 focused tests (26 constructed-shape,
including all four consolidation regressions; 49 normalizer/workspace/fold/height tests;
eight HOT historical-read/posting work-budget cases), with no skips or failures. The clean
`dictionaryReadViewAllocationTest` passed all 13 cases, including eight HOT commit allocation
cases, with existing exact bounds unchanged. Error Prone compilation completed with existing
warnings. All JVM commands used the required `heavy.sh` wrapper; available disk remained
above 60 GiB. The first invocation stopped before the scoped tests because its inherited
5 GiB minimum heap exceeded the requested 2 GiB maximum; the successful retry explicitly
set `-PtestHeapMin=128m -PtestHeapMax=2g`.

Scoped PIT removed the new marking call and the regression killed that mutant, reproducing
the pre-fix missing outer hint. The scope produced 15 void-call mutants: four killed, one
surviving unchanged retirement call, and ten uncovered unchanged calls. Only the new
marking call is a changed production line. Individual scope limits and dispositions are
retained in [mutation dispositions](hot-lost-outer-spine-mutations.md#consolidation-publication-review-fix).
Worktree artifacts: `build/review-r1/verification-retry.log`,
`build/review-r1/pit.gradle`, `build/review-r1/pit-report/mutations.xml`, and the two
core test-result directories. No full-suite or new timing gate was run.

| Workload | Original live/committed H | New live H | New committed H | Fixed leaf reference H |
| --- | ---: | ---: | ---: | ---: |
| Ascending 2,048 / 4,096 | 1 / 1 | 1 / 1 | 1 / 1 | 1 / 1 |
| Descending 2,048 / 4,096 | 2 / 3 | 2 / 3 | 1 / 1 | 1 / 1 |
| Descending skew 50,000, seed 5 / 6 | 9 / 9 | 9 / 9 | 3 / 3 | 2 / 2 |

A bounded follow-up walk finds one remaining H=3 to H=2 edge in skew seed 5: both
parent and child have 32 children, so direct folding would require 63 slots. Removing that
level requires repartitioning across blocks. Mean page visits improve from 3.00 to 2.00
for descending 4,096 keys and from 6.686 to 3.192 for skew seed 5; stale-height counts
fall from 2 / 24 to zero. No further skew optimization is attempted here.

Commit also changes the geometry of the captured valid-time frontier stream. Its full posting
and cold historical-revision oracle is retained, and it now requires an observed decrease in
inner-page count at commit rather than requiring a later split-half path that normalization
has removed. The independent lower/upper canonical-half witnesses in `HOTOrderingGuardTest`
retain their mandatory recanonicalization-counter assertions. No work-budget bound is widened.
The 100,000-operation correction stream likewise retains its complete posting and historical
revision checks and both remaining reach assertions. Its lost full-node split-guard witness
is replaced by a constructed 32-child parent with a straddling child, which must decline the
invalid split before publication and preserve every original and inserted key.

No global minimum-height guarantee is added. The remaining skew gap stays under issue #1452.
Bulk-builder SMHP and global reconstruction remain separate follow-ups; leaf minima alone
cannot establish routing equivalence for multi-entry leaves.

An insert-time folding candidate was rejected by two exclusive pinned ABBA JMH comparisons:
its warmed descending insertion cost increased about 18% and 172 B/key because it reached
an additional direct-leaf reconstruction. That result motivated commit-time reference folding.
The final allocation rewrite's auxiliary exclusive CPU-0 ABBA runs used JDK 25 C2,
synchronous compilation and disabled HOT memoization. These short supporting measurements
are distinct from the preregistered insertion gate:

| Measurement | Baseline | Candidate |
| --- | ---: | ---: |
| Descending 4,096 point lookup | 380.40 ns | 336.18 ns (-11.6%) |
| Descending skew 50,000 point lookup | 627.30 ns | 541.35 ns (-13.7%) |
| Descending 4,096 commit only | 10.719 ms | 9.595 ms (-10.5%) |
| Descending skew 50,000 commit only | 25.884 ms | 31.361 ms (+21.2%) |
| Descending 4,096 normalization alone | — | 66.53 µs |
| Descending skew 50,000 normalization alone | — | 1.083 ms |

The 50,000-key commit difference is a material measured cost, not hidden by the lookup
improvement. The isolated normalization measurement does not explain the whole commit
mean difference; the short comparison does not establish its cause. Commit timing excludes
insertion, while its GC profiler includes invocation fixtures. Lookup allocation remains
approximately 64 B/op. The page-output profiler measured 240 B/output for SingleMask in
both arms and 496 to 352 B/output for the tested MultiMask shape. Fixed fixture costs
appear in no-fold profiler means; exact prewarmed ThreadMXBean tests enforce zero bytes.

Before the pending-hint correction, diagnostic captures matched baseline/candidate C2 shapes: `dispatchInsert` has
327 bytecodes and 18,368 main-code bytes, `HOTIndexReader.get` has 55 / 1,040, and
`findChildIndex` has 22 bytecodes with 1,112 then 1,736 main-code bytes as profiles evolve.
The new fold preparation, materialization and page factory are compiled separately from
the per-key dispatcher. The resident no-fold normalizer capture has 567 bytecodes / 1,808
main-code bytes, with its public wrapper 104 / 984. These are profile-specific compiled
shapes, not independent timing scores or proof that every cold branch compiled.

The first fixed final-code insertion gate used 12 ABBA blocks per case, default asynchronous
C2, 30 seconds of real warmup plus 50 JMH warmup batches, and 36 retained measured batches
per process. All cases had zero timed GC. Descending 4,096 passed. Ascending 4,096 had a
-5.43% paired latency change (95% interval -13.56% to +3.47%) but a +0.023438 B/key median
paired allocation difference; descending 50,000 had a +1.78% latency point estimate
(95% interval -5.38% to +9.48%). Both failed a fixed acceptance criterion and were held.
A separately preregistered unchanged-candidate replication was cancelled during quiet-load
admission, before any measurement, when the commit regression was reviewed. The original
samples remain a HOLD and are retained without pooling or reclassification.

JFR execution sampling of 260 warmup/measured 50,000-key commits identifies fold preparation
and partial expansion as added CPU work. Non-normalization Java CPU samples did not increase;
recorded commit FileForce events over 10 ms totaled 901 ms before and 1,477 ms after. The
profiling run's overall means were 33.85 and 31.57 ms, opposite to the short ABBA mean effect.
These separate, thresholded observations do not account for every millisecond or establish a
speedup. They identify avoidable normalization work and variable durable-write waits.

The no-fold page probe's 215 ns came from log lookups, child scans, swizzling and scratch
setup/reset even for a height-one block. The bounded correction adds a transient pending
hint and checks it before that work. Height-one and unchanged normalized/decoded blocks
return immediately. Structural publication re-arms the owned outer spine even when height
stays unchanged; value-only CoW preserves the cleared hint. Successful normalization clears
it. Fold traversal advances past already-normalized absorbed children and full parents skip
futile fold-preflight scans. The hint is never serialized. Targeted tests cover cold reopen,
CoW isolation, re-arming, routing, exact heights and zero-allocation skipped paths.

One combined final gate is preregistered for the corrected candidate: descending commits at
4,096/50,000 keys and the three original insertion cases, 24 ABBA blocks per case. All five
must meet the unchanged latency, allocation and zero-timed-GC criteria. Commit allocation
uses the JVM-wide counter, with an escaping virtual-thread positive control, because commit
work runs off the caller thread. The completed gate is HOLD on allocation; its fixed criteria were not changed.

Before the pending-hint correction, the allocation rewrite compiled with Error Prone. The full core suite completed 15,718
tests with zero failures and 97 skips. Its surrounding 45-minute command then timed out
while running query prerequisites; the saved core XML is complete. Rerunning only the
unfinished tasks passed query budgets (327 tests, five skips), MCP budgets (three tests),
and the isolated prerequisites. The final strengthened cold/fold suite passed all 38 tests;
the earlier full focused spine/height/guard suite passed 73. The isolated allocation JVM
passed nine tests, including all four new HOT zero/output-budget tests. No allocation or
work-budget bound was widened.

That pre-correction scoped PIT generated 590 mutants: 366 killed, one timed out, 69 survived and 154
uncovered. On changed lines, 248 were killed, 17 survived and five were uncovered. Eight
initial survivors were killed by stronger cold-page, swizzle, exact extraction-storage and
boundary-representation assertions, with production code unchanged. Ten remaining
undetected changed-line mutants concern unchanged commit statements moved into
`try/finally`; the others have individual precondition, equivalence or allocation-only
[dispositions](hot-lost-outer-spine-mutations.md). The sole timeout is outside changed lines.
The pending-hint correction passed 78 focused regressions and nine isolated allocation
tests. The final publication-guard rerun passed the focused suite again. Broader current
verification passed 1,731 core HOT/budget tests (two skips), 327 query tests (five skips),
and three MCP budget tests, with no failures. Test-only survivor strengthening then passed
all 24 focused tests. The final expanded PIT scope generated 673 mutants: 429 killed,
one timed out, 82 survived and 161 uncovered. Changed lines had 272 killed, 25 survived
and five uncovered. Five targeted survivors were killed by the committed unchanged-height
spine, height-one skip and retained-child assertions. Every remaining changed-line survivor
or uncovered mutant has an individual disposition in the linked mutation report.

Current CPU-0 C2 diagnostics show dispatchInsert at 327 to 338 bytecodes, with initial
main-code captures of 23,032 to 22,528 bytes and later profile-specific recompilations
of 1,072 to 1,096 bytes. The new spine-marking helper
has 26 bytecodes / 200 main-code bytes. The no-work normalizer wrapper compiles to
133 bytecodes / 320 main-code bytes; its visited recursive body has 597 / 5,032.
Commit has 346 to 448 bytecodes and 3,040 to 9,544 main-code bytes in these profiles.
These are separate shared-admission diagnostics, not timing-gate results. The height-one
probe observed 1.41 ns/op and approximately 0.0028 profiler B/op, while isolated tests
require exactly zero allocation and no writer interactions. This supports the removed
visitor work but is not a paired comparison with the earlier 215 ns probe.

Before the session-workspace correction, the completed 24-block combined gate passed the
latency criterion in all five cases,
with zero timed GC. Descending commits changed by -3.72% at 4,096 keys and -1.93% at
50,000 keys. Allocation held ascending insertion (+0.046875 B/key median paired delta),
4,096-key commit (+1,574.44 B/commit) and 50,000-key commit (+18,137.11 B/commit).
No gate was repeated or reclassified.

Separate JFR and zero-interval JVMTI allocation profiles identify three fold outputs totaling
784 B at 4,096 keys and 41 outputs totaling 13,392 B at 50,000 keys. Additional workspace,
publication containers and cache retirement cost 1,552 B and 5,600 B respectively. These
bytes are beyond the replacement-page graph allowance. The workspace is reused within
normalization, but the fresh storage-writer lifecycle pays its 1,184 B on each measured commit.
The profile counters matched 235 of 240 operations exactly; the five boundary differences
were at most 96 B, and all ascending caller-thread counters matched.

The profiles also exposed a harness classpath asymmetry: baseline has one more unique
classes directory, causing 848 B/commit of extra XML parser resource lookup allocation.
That offset is not a normalization benefit and can also affect elapsed time. Original gate
figures remain unchanged, with this limitation. Ascending insertion's profile difference is
seven 32-byte cache ReservationNodes; HOT page sizes/counts and cache-retirement call
counts match. The original gate's larger mean difference is mostly three baseline JVMs
entering a 24-B/key lower allocation mode. Neither observation waives its failed criterion.
The captain authorized retaining the workspace at session scope and pinned only fold graphs
plus 184 bytes per published replacement. The corrected allocation-only harness uses one
production classes directory per arm, with unique, otherwise identical classpaths and
equal-length production paths. The original timing gate remains unchanged and qualified;
no new timing gate is authorized. Allocation confirmation and scoped validation of session
workspace reuse are recorded below.

The session-ownership follow-up passed 350 core focused/work-budget tests, 327 query tests
(five existing skips), three MCP work-budget tests, 13 isolated core allocation tests and
five isolated query allocation tests. Its PIT scope generated 122 mutants (19 killed,
11 survived, 92 uncovered); all three new session-accessor mutants and both scratch
reference-clear mutants were killed. Ten undetected changed-line mutants are unchanged
commit statements moved into try/finally, individually documented in the mutation report.
No fixed allocation bound was widened. The new combined fixture initially warmed only
1,000 operations and observed one-time allocation beyond its bound; a stack diagnostic
identified initialization/linkage allocation beyond that warmup. Using the established
40,000-operation warmup and bounded reused TIL identities passed the original exact bounds.

The corrected allocation-only profiles retain all 40 operations per arm/size and measure zero
workspace allocation throughout. All output and publication counts match the pinned model:

| Per commit | 4,096 keys | 50,000 keys |
| --- | ---: | ---: |
| Fold outputs | 3 / 784 B | 41 / 13,392 B |
| Published replacements | 2 | 24 |
| Containers (32 B each) | 64 B | 768 B |
| Cache retirement bound (152 B each) | 304 B | 3,648 B |
| Observed cache retirement | 304 B | 3,584 B |
| Workspace | 0 B | 0 B |
| Total pinned bound | 1,152 B | 17,808 B |
| Observed normalization | 1,152 B | 17,744 B |

The larger case is 64 B below the bound: two cache publications use existing map bins and
avoid 32-byte ReservationNodes. No other category changes. Whole-commit direct means are
232,870.8 to 233,757.8 B and 243,010.8 to 260,720.2 B. Native allocation events attribute
other-work deltas of -263.8 B and -22.6 B respectively; these are kept separate rather than
claiming whole-process totals equal an exact subsystem budget. 156/160 operation counters
match native events; four boundary discrepancies are -32, -80, +384 and +96 B. All positive
controls pass, and no measured-operation GC occurs. The corrected arms have 55 unique,
otherwise identical, equal-path-length classpath entries; resource-search allocation differs
by zero bytes. No corrected-classpath timing gate was run or historical result reclassified.

A final warmup-only compiler capture records the session accessor at 27 bytecodes / 160 C2
main-code bytes and commit(PageReference) at 438 / 6,760. Scratch.reset has a C1 capture;
no standalone C2 reset compilation is claimed. Dispatcher, reader, incremental-insert and
indirect-page class files match the frozen candidate byte-for-byte. These profile-specific
shapes support the implementation audit; no latency score or new timing gate was produced.

## Regression protection added afterwards (issue #1457)

Re-measuring this bug on `main` at `31c9c16f1` found it gone: over ascending, descending, mixed and
staged-commit workloads and the 50,000-key skew seeds, no stored height disagreed with the walked
structure and no stored key failed to route to its own leaf, live, committed and cold. The
descending 4,096-key index now commits at height 1 like its ascending control, and the skew seeds at
height 3 against an independent minimal-height reference of 2.

What the repair lacked was anywhere for the next change to trip over it. Three additions close that:
`spec/FMT-disk-format.md` states the requirement as FMT-306 (exact stored heights, spine continuity,
routing independent of the field, and the validator's duty); `HOTInvariantValidator` compares every
walked node's stored height with the structure below it as `I13-stored-height-exact`, so every HOT
test that validates a trie now enforces it; and `HOTLookupShapeWorkBudgetTest` counts the routing
steps of a point lookup, which equal the trie's height, pinning the committed shape at one level for
both insertion orders. `HOTStoredHeightSpineTest` carries the spec's clauses as behaviour,
including that a deliberately wrong stored height misroutes no key and is still rejected.

The remaining excess height on the skew workload is unchanged and permitted by FMT-302; it needs
repartitioning across block boundaries rather than a larger fold.

### Review follow-up: post-commit regression writer ownership

- Symptom: all three `HOTStoredHeightSpineTest` cases reached a closed-writer guard
  after commit instead of checking committed heights, subtree-local insertion or corruption.
- Trigger: retain `transaction.getStorageEngineWriter()` across a `KEEP_OPEN` commit.
- Root cause: commit re-instantiates the storage transaction, closing the predecessor writer;
  the regression fixture treated its borrowed writer as having the node transaction's lifetime.
- Why tests missed it: the new regression cases had not successfully executed their post-commit
  assertions, even though sibling spine tests already reacquired the writer at that boundary.
- Regression test: all four commit boundaries in `HOTStoredHeightSpineTest` now reacquire the
  writer before creating another index writer, loading pages, walking or validating the trie.
  Every original assertion, height bound and subtree-local insertion witness remains intact.
  Focused command: `./gradlew :sirix-core:test --tests io.sirix.index.hot.HOTStoredHeightSpineTest
  -PtestHeapMin=128m -PtestHeapMax=2g`.
- Lesson: reacquire transaction-owned storage handles after commit or rollback before checking
  the next state; see the [lesson fragment](../lessons.d/hot-stored-height-spine-continuity.md).
