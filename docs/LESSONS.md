# Lessons from failures

Keep entries short and evidence-based: broken assumption, prevention, and fix/test evidence.
For each new bug, add a [write-up](postmortems/TEMPLATE.md), a failing-before regression and a
lesson here. Extend an existing lesson when the same failure mode recurs.

| Failure / broken assumption | Apply next time | Repository evidence |
| --- | --- | --- |
| JSON serialization assumed every result item matched a supported branch; unsupported items could leave the iterator on the same item. | Specify iterator progress and unsupported-type errors; test unsupported items alone and mixed into sequences. | [c9efc3a7b158](https://github.com/sirixdb/sirix/commit/c9efc3a7b158a12af7fc544c03b6113081ffe077), `JsonDBSerializerTest` and `QueryJsonSerializationTest` |
| Query mutation authorization relied on an updating-expression flag; ordinary function calls could mutate too. Assertions comparing absent aggregate headers also became vacuous after storage moved. | Guard the actual mutation boundary; require the witness to exist, assert persisted state is unchanged on refusal, and include an authorized control. Verify the relevant JVM tests actually run in CI. | [36ee410f1f8d](https://github.com/sirixdb/sirix/commit/36ee410f1f8d0ff201bcb80170781f8d86b47550), `QueryMutationAuthorizationTest`, `PermissionCheckingQueryTest` |
| Correct timestamp results alone did not guard temporary allocations on the indexing path. | Pair a randomized independent JDK oracle with an allocation budget and a control that proves the budget detects the allocating implementation; retain precision and overflow cases. | [2c96b9196502](https://github.com/sirixdb/sirix/commit/2c96b91965024aaf9692974d661d39f3d7570d7c), `UtcTimestampParserTest`; [verification layers](VERIFICATION.md) |
