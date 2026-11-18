# HOT lost outer spine: mutation dispositions

The pending-hint candidate has two unsampled scopes: changed method names (including structural dispatch and durable decode) and HOTIndirectPage constructors. Final results: **673 mutants: 429 killed, one timed out, 82 survived, 161 uncovered**. Changed lines: **272 killed, 25 survived, five uncovered**. The timeout is an unchanged tryBranchIncremental registration call at line5294; it is not counted as changed-line coverage.

Test-only strengthening killed all five targeted changed-line survivors: the structural-scope condition, the dispatch spine-marking call, the per-ancestor mark call, the height-one skip boundary, and retained dirty-child swizzling. The committed-tree regression requires an actual subtree-local leaf split below an unchanged-height root, then checks re-arming, full-key routing and exact heights. The final focused suite passed all24 tests, with production code unchanged.

Fifteen remaining undetected changed-line mutants are unchanged commit/optional-validation statements moved into new blocks; they remain explicit coverage debt. Other remaining mutants have the individual precondition, equivalence, defensive-boundary or work/allocation dispositions below. No survivor is silently counted as killed.

Raw evidence: pending-strengthened-pit-report/mutations.xml, pending-pit-constructor-report/mutations.xml, pending-final-pit-summary.txt and pending-final-pit-all-mutants.txt in the task evidence directory. Earlier pre-hint and pre-strengthening reports remain separate; their counts are not pooled.

- **SURVIVED — AbstractHOTIndexWriter.dispatchInsert:2583 — NegateConditionalsMutator**: negated conditional. Existing optional post-publication validation, moved under the structural-change branch. Successful valid-input fixtures do not distinguish disabling validation or its scope; unchanged validation coverage debt, not an equivalence claim.

- **SURVIVED — AbstractHOTIndexWriter.dispatchInsert:2586 — NegateConditionalsMutator**: negated conditional. Existing optional post-publication validation, moved under the structural-change branch. Successful valid-input fixtures do not distinguish disabling validation or its scope; unchanged validation coverage debt, not an equivalence claim.

- **SURVIVED — AbstractHOTIndexWriter.dispatchInsert:2586 — NegateConditionalsMutator**: negated conditional. Existing optional post-publication validation, moved under the structural-change branch. Successful valid-input fixtures do not distinguish disabling validation or its scope; unchanged validation coverage debt, not an equivalence claim.

- **SURVIVED — AbstractHOTIndexWriter.dispatchInsert:2585 — VoidMethodCallMutator**: removed call to io/sirix/index/hot/AbstractHOTIndexWriter::validatePublishedStructuralScope. Existing optional post-publication validation, moved under the structural-change branch. Successful valid-input fixtures do not distinguish disabling validation or its scope; unchanged validation coverage debt, not an equivalence claim.

- **SURVIVED — AbstractHOTIndexWriter.dispatchInsert:2586 — VoidMethodCallMutator**: removed call to io/sirix/index/hot/AbstractHOTIndexWriter::validatePublishedStructuralPath. Existing optional post-publication validation, moved under the structural-change branch. Successful valid-input fixtures do not distinguish disabling validation or its scope; unchanged validation coverage debt, not an equivalence claim.

- **SURVIVED — AbstractHOTIndexWriter.refreshOuterHeight:2771 — ConditionalsBoundaryMutator**: changed conditional boundary. Equivalent: the enclosing loop requires newChildHeight != oldChildHeight, so equality cannot reach this shrink comparison.

- **SURVIVED — AbstractHOTIndexWriter.subInsertAt:2684 — BooleanTrueReturnValsMutator**: replaced boolean return with true for io/sirix/index/hot/AbstractHOTIndexWriter::subInsertAt. The delegated mutation still executes. This wrapper receives a resolved bounded route and returns true for valid input. Its defensive unresolved/malformed/depth-overflow false return is outside that caller precondition; not universally equivalent.

- **SURVIVED — AbstractHOTIndexWriter.tryBranchIncremental:5087 — MathMutator**: Replaced integer addition with subtraction. The boundary collision beta equals the containing child MSB. Preflight permits only a certain nonstructural direct-leaf write; structuralValidationScope stays null and this outer-height depth is not consumed. The generic helper retains the correct depth.

- **SURVIVED — AbstractHOTIndexWriter.tryBranchIncremental:4917 — BooleanTrueReturnValsMutator**: replaced boolean return with true for io/sirix/index/hot/AbstractHOTIndexWriter::tryBranchIncremental. The ordinary local insertion executes and succeeds on the established valid bounded route. Defensive invalid-route false return is outside these caller preconditions.

- **SURVIVED — AbstractHOTIndexWriter.tryBranchIncremental:5087 — BooleanTrueReturnValsMutator**: replaced boolean return with true for io/sirix/index/hot/AbstractHOTIndexWriter::tryBranchIncremental. Boundary preflight permits a certain direct in-place leaf write, which executes and succeeds; the return mutation does not remove its side effects.

- **SURVIVED — HOTIncrementalInsert.prepareCompatibleChild:1023 — ConditionalsBoundaryMutator**: changed conditional boundary. Equivalent for accepted compositions: path order requires the first child discriminator after the parent root/path decisions. A child bit cannot equal parentBits[0], so binarySearch cannot return index zero here.

- **SURVIVED — HOTCommitNormalizer.normalize:81 — ConditionalsBoundaryMutator**: changed conditional boundary. Defensive private depth guard. Exact input height is checked against 64 at public entry, and height-one blocks skip recursion; valid visited depth cannot reach the changed boundary. Malformed inconsistent height metadata is not an equivalence claim. The public 64/65 boundary is tested.

- **SURVIVED — HOTCommitNormalizer.normalize:84 — ConditionalsBoundaryMutator**: changed conditional boundary. Equivalent: checks one extra null ancestor slot. Successful children clear their slot and failure/reuse clears scratch; the extra slot cannot equal the current non-null page.

- **SURVIVED — HOTCommitNormalizer.normalize:103 — ConditionalsBoundaryMutator**: changed conditional boundary. Equivalent under exact input metadata and fold-only normalization: a changed child only shrinks, so newHeight+1 < oldHeight+1 <= parentHeight. This growth term remains false; maximum shrink is detected independently by oldHeight+1 == parentHeight.

- **SURVIVED — HOTCommitNormalizer.normalize:121 — ConditionalsBoundaryMutator**: changed conditional boundary. Work-only mutant: allowing a full 32-child parent into the loop performs extra TIL/preflight checks. Every child has at least two entries, so preflight rejects count >32 without output allocation; routing/height stay identical. Not claimed killed or latency-equivalent; the production fast guard and compiled-shape/performance evidence are retained.

- **SURVIVED — HOTCommitNormalizer.normalize:103 — MathMutator**: Replaced integer addition with subtraction. Equivalent under exact input metadata and fold-only normalization: a changed child only shrinks, so newHeight+1 < oldHeight+1 <= parentHeight. This growth term remains false; maximum shrink is detected independently by oldHeight+1 == parentHeight.

- **NO_COVERAGE — HOTCommitNormalizer.normalize:76 — NullReturnValsMutator**: replaced return value with null for io/sirix/index/hot/HOTCommitNormalizer::normalize. Defensive ownership recheck is unreachable for valid single-writer normalization: entry and each recursive caller already checked the same log owner, and concurrent mutation is excluded.

- **SURVIVED — NodeStorageEngineWriter.commit:4336 — ConditionalsBoundaryMutator**: changed conditional boundary. Unchanged log-key/back-reference propagation moved into try/finally. Selected fixtures do not distinguish slot zero and the alternate durable-handle branch; scope debt, not equivalence.

- **NO_COVERAGE — NodeStorageEngineWriter.commit:4338 — ConditionalsBoundaryMutator**: changed conditional boundary. Unchanged original-reference identity/durable-key guards moved into try/finally. This branch is not reached by the selected fixtures; scope debt.

- **SURVIVED — NodeStorageEngineWriter.commit:4336 — NegateConditionalsMutator**: negated conditional. Unchanged log-key/back-reference propagation moved into try/finally. Selected fixtures do not distinguish slot zero and the alternate durable-handle branch; scope debt, not equivalence.

- **NO_COVERAGE — NodeStorageEngineWriter.commit:4338 — NegateConditionalsMutator**: negated conditional. Unchanged original-reference identity/durable-key guards moved into try/finally. This branch is not reached by the selected fixtures; scope debt.

- **NO_COVERAGE — NodeStorageEngineWriter.commit:4338 — NegateConditionalsMutator**: negated conditional. Unchanged original-reference identity/durable-key guards moved into try/finally. This branch is not reached by the selected fixtures; scope debt.

- **SURVIVED — NodeStorageEngineWriter.commit:4326 — VoidMethodCallMutator**: removed call to io/sirix/access/trx/page/NodeStorageEngineWriter::requireNoSpillKeys. Unchanged spill-key safety guard moved into try/finally. Selected HOT fixtures contain no spill keys; scope debt.

- **NO_COVERAGE — NodeStorageEngineWriter.commit:4340 — VoidMethodCallMutator**: removed call to io/sirix/page/PageReference::copyHashFrom. Unchanged back-reference hash propagation moved into try/finally. Branch not covered; scope debt.

- **SURVIVED — NodeStorageEngineWriter.commit:4345 — VoidMethodCallMutator**: removed call to io/sirix/page/interfaces/Page::close. Unchanged resource cleanup moved into try/finally. Routing/value assertions do not distinguish retained resources; scope debt.

- **SURVIVED — NodeStorageEngineWriter.commit:4346 — VoidMethodCallMutator**: removed call to io/sirix/page/interfaces/Page::close. Unchanged resource cleanup moved into try/finally. Same retention coverage debt.

- **SURVIVED — NodeStorageEngineWriter.commit:4349 — VoidMethodCallMutator**: removed call to io/sirix/page/PageReference::setPage. Unchanged swizzle release moved into try/finally. Durable routing still succeeds with a retained heap reference; scope debt.

- **SURVIVED — HOTIndirectPage.<init>:455 — ConditionalsBoundaryMutator**: changed conditional boundary. Public-behavior equivalent: >=1 sets a latent pending flag on a height-one page, but needsNormalization still requires height >1. Copy preserves that unobservable flag; a later height change or structural child update explicitly re-arms it, and decode clears it. Reflection into private state is not the contract.

- **SURVIVED — HOTIndirectPage.<init>:488 — NegateConditionalsMutator**: negated conditional. Allocation-only mutant: allocates a fresh shuffle-index temporary instead of borrowing scratch; retained output/routing is identical. PIT functional scope does not execute the isolated exact-allocation JVM. Separate independent output-budget and zero-allocation tests pass; this mutant is not claimed killed or allocation-equivalent.

- **SURVIVED — HOTIndirectPage.<init>:491 — VoidMethodCallMutator**: removed call to java/util/Arrays::fill. Equivalent routing/output: all used extraction lanes are overwritten; only unused primitive shuffle lanes retain old values, and their extraction masks are zero. Scalar/vector workspace-poisoning tests cover retained output. No reference can be retained by the primitive scratch array.

The wider method scope also contains unchanged-code survivors/uncovered mutants, individually listed in pending-final-pit-all-mutants.txt. No new coverage is claimed for them.

## Consolidation publication review fix

The R1 correction adds one production statement at
`AbstractHOTIndexWriter.consolidateLeafParent:5994`: `markNormalizationSpine(route)`.
The new four-version regression exercises committed height-three input, value-only CoW,
periodic consolidation, live exact heights and cold two-revision routing/height checks.
PIT's `VOID_METHOD_CALLS` operator generated **15 mutants: four killed, one survived,
ten uncovered**, with no timeouts or errors. The sole changed-line mutant was **killed**:
removing `markNormalizationSpine` fails
`HOTConstructedShapeIntegrationTest.consolidationAfterValueUpdatesRearmsTheOuterSpine`
(invocation #4). This removal reproduces the pre-fix failure to re-arm the outer hint.

The other three killed mutants remove child swizzling (`5978`), replacement publication
(`5991`) and fresh-subtree registration (`5993`). These are unchanged statements, not
additional changed-line coverage. Remaining dispositions, all unchanged code:

| Status | Method / line | Removed call | Disposition |
| --- | --- | --- | --- |
| SURVIVED | `consolidateLeafParent:5996` | `releaseOrphanedHOTLeaves` | Routing/height assertions do not prove retirement of detached leaves; retained lifecycle coverage debt, not equivalent. |
| NO_COVERAGE | `consolidateLeafParent:6003` | `markTransactionRollbackOnly` | Successful consolidation does not reach the failure handler; unchanged failure-path scope debt. |
| NO_COVERAGE | `consolidateLeafParent:6009` | `closeUnregisteredFreshChildren` | Successful registration does not reach failed-publication cleanup; unchanged lifecycle scope debt. |
| NO_COVERAGE | `branchAddStrandsExisting:2918` | `addSuppressed` | Unchanged refusal/failure path outside the consolidation correction. |
| NO_COVERAGE | `existingKeyRoutesToSlot:3128` | `requireCandidateSlot` | Unchanged annotated helper outside the consolidation scope. |
| NO_COVERAGE | `existingKeyRoutingVerdict:3142` | `requireCandidateSlot` | Unchanged annotated helper outside the consolidation scope. |
| NO_COVERAGE | `spliceBulkBuiltRoot:1610` | `setPage` | Unchanged bulk-root publication outside the consolidation scope. |
| NO_COVERAGE | `spliceBulkBuiltRoot:1612` | `registerFreshSubtree` | Unchanged bulk-root registration outside the consolidation scope. |
| NO_COVERAGE | `spliceBulkBuiltRoot:1616` | `markTransactionRollbackOnly` | Unchanged bulk-root failure handling outside the consolidation scope. |
| NO_COVERAGE | `spliceBulkBuiltRoot:1617` | `closeLocallyOwnedFreshSubtree` | Unchanged bulk-root failure cleanup outside the consolidation scope. |
| NO_COVERAGE | `spliceBulkBuiltRoot:1619` | `closeFreshHOTSubtree` | Unchanged bulk-root failure cleanup outside the consolidation scope. |

The worktree-local init script excludes other declared method names; annotated/generic
signatures omitted from that selection also emitted the unrelated mutants listed above.
This is a narrow void-call scope for the added statement, not a new all-operator or
whole-class coverage claim. Raw evidence remains in
`build/review-r1/pit-report/mutations.xml`, scope in `build/review-r1/pit.gradle`, and the
successful combined focused run in `build/review-r1/verification-retry.log`. Results do
not supersede or pool the earlier structural, constructor or workspace scopes.

## Session workspace ownership follow-up

The later ownership/reset/commit scope generated **122 mutants: 19 killed, 11 survived,
92 uncovered**. Against the branch diff, **11 were killed, six survived and four were
uncovered**. All three mutations of the new session accessor were killed: lazy-init
condition, closed-session guard, and null return. Both reference-clearing mutations in
Scratch.reset were killed. Resetting the primitive count and releasing the field on close
are asserted directly; the configured default operators do not mutate those assignments.

The ten remaining changed-line mutants below are unchanged commit statements moved into
the original normalization try/finally. They remain coverage debt, not claimed equivalent.
Synthetic lambdas outside the requested methods were also emitted by PIT; none is a changed
line. Their full results remain in session-workspace-pit-report/mutations.xml, with the
changed-line mapping in session-workspace-pit-summary.json. This scope supplements the
structural/constructor scopes above; overlapping results are not added together.

- **SURVIVED — NodeStorageEngineWriter.commit:4331 — ConditionalsBoundaryMutator**: changed conditional boundary. Unchanged log-key/back-reference propagation moved into try/finally. These valid-input fixtures do not distinguish slot zero or disabling the back-reference branch; retained coverage debt.

- **NO_COVERAGE — NodeStorageEngineWriter.commit:4333 — ConditionalsBoundaryMutator**: changed conditional boundary. Unchanged durable back-reference branch, moved into try/finally. The selected fixtures do not reach the distinct back-reference with an unset durable offset; retained uncovered branch debt.

- **SURVIVED — NodeStorageEngineWriter.commit:4331 — NegateConditionalsMutator**: negated conditional. Unchanged log-key/back-reference propagation moved into try/finally. These valid-input fixtures do not distinguish slot zero or disabling the back-reference branch; retained coverage debt.

- **NO_COVERAGE — NodeStorageEngineWriter.commit:4333 — NegateConditionalsMutator**: negated conditional. Unchanged durable back-reference branch, moved into try/finally. The selected fixtures do not reach the distinct back-reference with an unset durable offset; retained uncovered branch debt.

- **NO_COVERAGE — NodeStorageEngineWriter.commit:4333 — NegateConditionalsMutator**: negated conditional. Unchanged durable back-reference branch, moved into try/finally. The selected fixtures do not reach the distinct back-reference with an unset durable offset; retained uncovered branch debt.

- **SURVIVED — NodeStorageEngineWriter.commit:4321 — VoidMethodCallMutator**: removed call to io/sirix/access/trx/page/NodeStorageEngineWriter::requireNoSpillKeys. Existing spill-key rejection moved into try/finally. Valid normalized fixtures do not carry illegal spill keys, so deleting the rejection is not distinguished; retained defensive-validation coverage debt.

- **NO_COVERAGE — NodeStorageEngineWriter.commit:4335 — VoidMethodCallMutator**: removed call to io/sirix/page/PageReference::copyHashFrom. Existing hash propagation on the uncovered durable back-reference branch. No claim of equivalence; retained branch coverage debt.

- **SURVIVED — NodeStorageEngineWriter.commit:4340 — VoidMethodCallMutator**: removed call to io/sirix/page/interfaces/Page::close. Existing complete-page close moved into try/finally. These routing/history tests do not assert the exact release of the complete object at this boundary; retained resource-lifecycle coverage debt.

- **SURVIVED — NodeStorageEngineWriter.commit:4341 — VoidMethodCallMutator**: removed call to io/sirix/page/interfaces/Page::close. Existing modified-page close moved into try/finally. These routing/history tests do not assert the exact release of this object at this boundary; retained resource-lifecycle coverage debt.

- **SURVIVED — NodeStorageEngineWriter.commit:4344 — VoidMethodCallMutator**: removed call to io/sirix/page/PageReference::setPage. Existing post-write reference unswizzling moved into try/finally. Durable routing/history remains correct in the selected fixtures without asserting the cleared in-memory pointer; retained lifecycle coverage debt.
