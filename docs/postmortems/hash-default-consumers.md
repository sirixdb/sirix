# Bug: hashless resources lost diff safety and planner size signals

- Fix / issue: the node-hashes-default-off change for Sirix 1.0.
- Symptom: a caller could force the hash shortcut on a resource without stored hashes, causing changed subtrees to compare equal. Query size heuristics also saw every hashless document as empty, disabling large-document optimization routes.
- Trigger: create a resource with `hashKind(NONE)`, edit it across revisions, and invoke a diff with `DiffOptimized.HASHED` and `hashKind(ROLLING)`. Independently, ask the query statistics provider for the size of a populated hashless document.
- Root cause: `DiffFactory.Builder` defaulted to `ROLLING` independently of the resource, and `AbstractDiff` trusted that setting. Stored descendant counts share the node-hash storage gate, but five planner consumers assumed the count was unconditional. Changing the Java default to `NONE` exposed both assumptions on the default path.
- Why tests missed it: public diff callers normally disabled the shortcut themselves. Query result tests could not detect degraded planner decisions because the fallback execution still produced correct values. Some hash-dependent fixtures also compared two absent counts (`0 == 0`).
- Regression test: `DiffFactoryHashKindTest` covers forced and default hash kinds; `JsonDiffHashEquivalenceTest` and `XmlDiffHashEquivalenceTest` compare revision histories with hashes on and off. `DocumentNodeCountTest` requires a positive size signal and tests both the stored-count and node-frontier branches. Removing the resource-authoritative guard or replacing the fallback with the stored count violates those assertions. Run the corresponding `:sirix-core:test` and `:sirix-query:test --tests` filters.
- Lesson: gate an optimization at the resource that makes it valid, and inventory all fields gated by a configuration change. Assert a planner's decision inputs independently of result correctness. See the two hash-default entries in [Lessons](../LESSONS.md).

Mutation evidence: actual PIT kills all three `DocumentNodeCount.estimate` mutants, both
`ResourceConfiguration.defaultHashType` mutants and the resource-authoritative `AbstractDiff`
constructor guard. The builder's new default assignment has no mutation under PIT's default
mutators; its behavior is asserted for both hash kinds.

Four surviving mutations in `DiffFactory.Builder` are outside the changed logic. All affect
the existing revision-argument checks before the new hash-default assignment:

| Method | Line | Mutation | Justification |
|---|---|---|---|
| `Builder` constructor | 226 | boundary `newRev >= 0` | Unchanged argument validation; the focused hash tests pass valid positive revisions. |
| `Builder` constructor | 226 | remove `checkArgument` | Same unchanged validation, outside the hash-kind contract. |
| `Builder` constructor | 228 | boundary `oldRev >= 0` | Unchanged argument validation; the focused hash tests pass valid positive revisions. |
| `Builder` constructor | 228 | remove `checkArgument` | Same unchanged validation, outside the hash-kind contract. |

A wider class-level run also mutates legacy traversal loops and unrelated configuration methods.
Its two memory errors and one timeout are incomplete analysis of those methods, not killed mutants.
The follow-up excludes those unchanged methods and retains the changed constructor and resolver.
