# Mutation dispositions (Refs #1452)

Final scope: 179 mutants, 164 killed, 10 survived, 5 uncovered. Initial scope: 145 killed; behavior tests killed 19 additional mutants. These are overlapping runs, not additive totals. No default mutation operator, production analyzer rule or allocation bound was weakened.

Every extracted helper and each changed method is included. The three source classes also contain untouched code: name-based method scoping includes both commit overloads. The residuals below distinguish proofs of equivalence/unreachability from inherited debt; neither a completed PIT task nor retained debt is described as complete mutation coverage.

## 40 — NO_COVERAGE — io.sirix.index.hot.HOTCommitNormalizer.normalize:81

- Operator: `NullReturnValsMutator`; instruction indexes 19.
- Mutation: replaced return value with null for io/sirix/index/hot/HOTCommitNormalizer::normalize.
- Descriptor: `(Lio/sirix/page/PageReference;Lio/sirix/index/hot/page/HOTIndirectPage;Lio/sirix/api/StorageEngineWriter;Lio/sirix/index/hot/HOTCommitNormalizer$Scratch;I)Lio/sirix/index/hot/page/HOTIndirectPage;`.
- Disposition: **Unreachable under the normalization contract**. The public entry checks ownership, and recursion enters only a child whose owner was just found in the same exclusive transaction log. No writer mutation or pending flush may race normalization. Losing that owner between the checks violates the entry contract; a changing mock answer would not be a meaningful regression test. The public unowned-root early exit is tested separately.

## 43 — SURVIVED — io.sirix.index.hot.HOTCommitNormalizer.normalizeChildren:146

- Operator: `ConditionalsBoundaryMutator`; instruction indexes 98.
- Mutation: changed conditional boundary.
- Descriptor: `(Lio/sirix/index/hot/page/HOTIndirectPage;Lio/sirix/api/StorageEngineWriter;Lio/sirix/index/hot/HOTCommitNormalizer$Scratch;ILio/sirix/cache/TransactionIntentLog;)Z`.
- Disposition: **Equivalent for exact entry heights**. This changes the defensive newHeight + 1 > parentHeight arm to >=. With exact entry heights and non-increasing structural height under folding, a shrinking child cannot grow past its parent. A shrinking maximum takes the unchanged oldHeight + 1 == parentHeight arm. Thus this arm cannot change the result for supported invariant-preserving input. The defensive check is preserved for stale metadata.

## 47 — SURVIVED — io.sirix.index.hot.HOTCommitNormalizer.normalizeChildren:146

- Operator: `MathMutator`; instruction indexes 95.
- Mutation: Replaced integer addition with subtraction.
- Descriptor: `(Lio/sirix/index/hot/page/HOTIndirectPage;Lio/sirix/api/StorageEngineWriter;Lio/sirix/index/hot/HOTCommitNormalizer$Scratch;ILio/sirix/cache/TransactionIntentLog;)Z`.
- Disposition: **Equivalent for exact entry heights**. The mutated addition is the defensive newHeight + 1 comparison, not the oldHeight + 1 maximum test. Normalization never increases structural height on a sound graph, so the defensive growth arm is false before and after this mutation. Full-parent maximum-shrink and nonmaximum-shrink tests exercise the required repairs; the old-height addition mutants are killed.

## 87 — SURVIVED — io.sirix.index.hot.HOTIncrementalInsert.mergeDiscriminativeBits:1083

- Operator: `ConditionalsBoundaryMutator`; instruction indexes 33.
- Mutation: changed conditional boundary.
- Descriptor: `(IILio/sirix/index/hot/HOTCommitNormalizer$Scratch;)I`.
- Disposition: **Equivalent after path eligibility**. binarySearch < 0 becomes <= 0, differing only for a child column equal to the first parent column. That first column is the root decision on every parent path. isChildBelowParentPath already refuses such a child, and all later child columns are greater than its first. No accepted composition can reach a binarySearch result of zero. Overlapping off-path columns at later positions remain covered by the differential router oracle.

## 136 — SURVIVED — io.sirix.access.trx.page.NodeStorageEngineWriter.commit:4290

- Operator: `ConditionalsBoundaryMutator`; instruction indexes 146.
- Mutation: changed conditional boundary.
- Descriptor: `(Lio/sirix/page/PageReference;)V`.
- Disposition: **Inherited unchanged back-reference debt**. Inherited compatibility-branch coverage debt; not claimed equivalent. These statements are unchanged by this extraction and already appeared in the pre-refactor session-workspace mutation dispositions. Current durable transaction-log handles normally clear the log key before this legacy back-reference branch. The focused suite does not exercise a distinct back-reference with an unset offset at this point. The new closed-CoW-copy tests independently assert offset and hash-presence propagation, including key zero and a present zero hash. This older live-write compatibility branch remains a limitation.

## 137 — NO_COVERAGE — io.sirix.access.trx.page.NodeStorageEngineWriter.commit:4292

- Operator: `ConditionalsBoundaryMutator`; instruction indexes 165.
- Mutation: changed conditional boundary.
- Descriptor: `(Lio/sirix/page/PageReference;)V`.
- Disposition: **Inherited unchanged back-reference debt**. Inherited compatibility-branch coverage debt; not claimed equivalent. These statements are unchanged by this extraction and already appeared in the pre-refactor session-workspace mutation dispositions. Current durable transaction-log handles normally clear the log key before this legacy back-reference branch. The focused suite does not exercise a distinct back-reference with an unset offset at this point. The new closed-CoW-copy tests independently assert offset and hash-presence propagation, including key zero and a present zero hash. This older live-write compatibility branch remains a limitation.

## 144 — SURVIVED — io.sirix.access.trx.page.NodeStorageEngineWriter.commit:4290

- Operator: `NegateConditionalsMutator`; instruction indexes 146.
- Mutation: negated conditional.
- Descriptor: `(Lio/sirix/page/PageReference;)V`.
- Disposition: **Inherited unchanged back-reference debt**. Inherited compatibility-branch coverage debt; not claimed equivalent. These statements are unchanged by this extraction and already appeared in the pre-refactor session-workspace mutation dispositions. Current durable transaction-log handles normally clear the log key before this legacy back-reference branch. The focused suite does not exercise a distinct back-reference with an unset offset at this point. The new closed-CoW-copy tests independently assert offset and hash-presence propagation, including key zero and a present zero hash. This older live-write compatibility branch remains a limitation.

## 146 — NO_COVERAGE — io.sirix.access.trx.page.NodeStorageEngineWriter.commit:4292

- Operator: `NegateConditionalsMutator`; instruction indexes 160.
- Mutation: negated conditional.
- Descriptor: `(Lio/sirix/page/PageReference;)V`.
- Disposition: **Inherited unchanged back-reference debt**. Inherited compatibility-branch coverage debt; not claimed equivalent. These statements are unchanged by this extraction and already appeared in the pre-refactor session-workspace mutation dispositions. Current durable transaction-log handles normally clear the log key before this legacy back-reference branch. The focused suite does not exercise a distinct back-reference with an unset offset at this point. The new closed-CoW-copy tests independently assert offset and hash-presence propagation, including key zero and a present zero hash. This older live-write compatibility branch remains a limitation.

## 147 — NO_COVERAGE — io.sirix.access.trx.page.NodeStorageEngineWriter.commit:4292

- Operator: `NegateConditionalsMutator`; instruction indexes 165.
- Mutation: negated conditional.
- Descriptor: `(Lio/sirix/page/PageReference;)V`.
- Disposition: **Inherited unchanged back-reference debt**. Inherited compatibility-branch coverage debt; not claimed equivalent. These statements are unchanged by this extraction and already appeared in the pre-refactor session-workspace mutation dispositions. Current durable transaction-log handles normally clear the log key before this legacy back-reference branch. The focused suite does not exercise a distinct back-reference with an unset offset at this point. The new closed-CoW-copy tests independently assert offset and hash-presence propagation, including key zero and a present zero hash. This older live-write compatibility branch remains a limitation.

## 153 — NO_COVERAGE — io.sirix.access.trx.page.NodeStorageEngineWriter.commit:4294

- Operator: `VoidMethodCallMutator`; instruction indexes 177.
- Mutation: removed call to io/sirix/page/PageReference::copyHashFrom.
- Descriptor: `(Lio/sirix/page/PageReference;)V`.
- Disposition: **Inherited unchanged back-reference debt**. Inherited compatibility-branch coverage debt; not claimed equivalent. These statements are unchanged by this extraction and already appeared in the pre-refactor session-workspace mutation dispositions. Current durable transaction-log handles normally clear the log key before this legacy back-reference branch. The focused suite does not exercise a distinct back-reference with an unset offset at this point. The new closed-CoW-copy tests independently assert offset and hash-presence propagation, including key zero and a present zero hash. This older live-write compatibility branch remains a limitation.

## 157 — SURVIVED — io.sirix.access.trx.page.NodeStorageEngineWriter.commit:4365

- Operator: `VoidMethodCallMutator`; instruction indexes 5.
- Mutation: removed call to io/sirix/access/trx/page/NodeStorageEngineReader::assertNotClosed.
- Descriptor: `(Ljava/lang/String;Ljava/time/Instant;ZZ)Lio/sirix/page/UberPage;`.
- Disposition: **Unchanged overload outside the extraction**. PIT scopes methods by name, so it also mutates commit(String, Instant, boolean, boolean), which this fix does not change. This is an existing validation call; commitWritePages repeats the validation. No new claim is made about the early rejection/lock ordering of invalid calls to that unchanged overload.

## 158 — SURVIVED — io.sirix.access.trx.page.NodeStorageEngineWriter.commit:4366

- Operator: `VoidMethodCallMutator`; instruction indexes 9.
- Mutation: removed call to io/sirix/access/trx/page/NodeStorageEngineWriter::assertTransactionWritable.
- Descriptor: `(Ljava/lang/String;Ljava/time/Instant;ZZ)Lio/sirix/page/UberPage;`.
- Disposition: **Unchanged overload outside the extraction**. PIT scopes methods by name, so it also mutates commit(String, Instant, boolean, boolean), which this fix does not change. This is an existing validation call; commitWritePages repeats the validation. No new claim is made about the early rejection/lock ordering of invalid calls to that unchanged overload.

## 161 — SURVIVED — io.sirix.access.trx.page.NodeStorageEngineWriter.commit:4375

- Operator: `VoidMethodCallMutator`; instruction indexes 55,41.
- Mutation: removed call to java/util/concurrent/locks/Lock::unlock.
- Descriptor: `(Ljava/lang/String;Ljava/time/Instant;ZZ)Lio/sirix/page/UberPage;`.
- Disposition: **Unchanged overload outside the extraction**. PIT scopes methods by name and includes the untouched commit(String, Instant, boolean, boolean) overload. Deleting unlock is not equivalent: the selected same-thread fixtures do not prove a subsequent cross-thread handoff of the resource commit lock. This is pre-existing concurrency-coverage debt outside commit(PageReference), not a new behavior or an accepted CodeFactor waiver.

## 171 — SURVIVED — io.sirix.access.trx.page.NodeStorageEngineWriter.commitUnloggedReference:4345

- Operator: `VoidMethodCallMutator`; instruction indexes 77.
- Mutation: removed call to io/sirix/access/trx/page/NodeStorageEngineWriter::materializeSpilledPage.
- Descriptor: `(Lio/sirix/page/PageReference;)V`.
- Disposition: **Inherited spill-delegation coverage debt**. Not claimed equivalent. The same materializeSpilledPage call is preserved verbatim at the same early-return boundary. The selected mutation fixtures exercise ordinary durable/unresolved keys here, for which the delegate is a no-op; they do not inject a real spilled reference in this particular unlogged/closed state. The full work-budget lane is green, but is not presented as a kill of this operator. No new spill state machine, fallback or retry was added to manufacture coverage for the refactor.

## 179 — SURVIVED — io.sirix.access.trx.page.NodeStorageEngineWriter.resolveClosedCommitReference:4359

- Operator: `VoidMethodCallMutator`; instruction indexes 48.
- Mutation: removed call to io/sirix/access/trx/page/NodeStorageEngineWriter::materializeSpilledPage.
- Descriptor: `(Lio/sirix/page/PageReference;)V`.
- Disposition: **Inherited spill-delegation coverage debt**. Not claimed equivalent. The same materializeSpilledPage call is preserved verbatim at the same early-return boundary. The selected mutation fixtures exercise ordinary durable/unresolved keys here, for which the delegate is a no-op; they do not inject a real spilled reference in this particular unlogged/closed state. The full work-budget lane is green, but is not presented as a kill of this operator. No new spill state machine, fallback or retry was added to manufacture coverage for the refactor.
