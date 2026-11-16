# Bug: temporal functions threw ClassCastException on stored values

- Fix / issue: [#1429](https://github.com/sirixdb/sirix/issues/1429); spec
  [TEMPORAL_FUNCTIONS_SPEC.md](../TEMPORAL_FUNCTIONS_SPEC.md)
- Symptom: `jn:first(jn:doc('db','res')[0].price)` and every other temporal navigation function
  (`jn:last`, `jn:previous`, `jn:next`, `jn:past`, `jn:future`, `jn:all-times`) failed with
  `ClassCastException: IntNumericJsonDBItem cannot be cast to TemporalJsonDBItem` for numbers,
  strings, booleans, `null` and object field names, although these are stored nodes with a stable
  node key and history. The same calls on the enclosing object worked. Constructed objects, arrays
  and computed strings failed with the same Java exception instead of a query error.
- Trigger: any stored value item as the argument, for example in `sirix-shell`:
  `jn:previous(jn:doc('db','res')[0].price)` after one update of `price`.
- Root cause: the functions cast their argument to `TemporalJsonDBItem`, an interface only the
  object and array items implement. Their signatures accept any `json-item()`, and the value items
  (`*NumericJsonDBItem`, `AtomicStrJsonDBItem`, `AtomicBooleanJsonDBItem`, `AtomicNullJsonDBItem`)
  are JSON items with node identity, so the type check admitted arguments the implementation could
  not handle. The argument domain was never written down, so nothing tied "every stored item has a
  history" to the code. `sdb:item-history`, `jn:first-existing` and `jn:last-existing` already used
  the node key and worked on values.
- Why tests missed it: every temporal-function test navigated objects or arrays; no test enumerated
  the item kinds the signature admits, and no test passed a constructed item. The initial spec also
  generalized same-kind object-field replacement to all replacements. The select-item oracle
  correctly followed separate node keys but did not independently assert that a type change
  creates a new node, so it did not expose the overbroad documentation guarantee.
- Regression test: `TemporalNavigationTest` (`./gradlew :sirix-query:test --tests
  'io.sirix.query.function.jn.temporal.TemporalNavigationTest'`): every function x every value kind
  x array position / field value x revision against an oracle that reads each revision with
  `sdb:select-item`, the shell reproduction, a differential against the enclosing record, field
  names, and the error code for computed items. It fails before the fix with the
  `ClassCastException` and passes after. `typeChangingFieldReplacementStartsANewHistory` pins
  null-to-number replacement to distinct keys and literal values/revisions for both histories,
  including empty `jn:first` and `jn:previous` results on the replacement. The shell reproduction
  pins key preservation for number-to-number replacement; `mixedTypesAtOnePathAcrossRevisions`
  checks that same-kind and type-changing array replacements create new keys.
- Lesson: when a function's signature admits a type, enumerate the implementing item classes in a
  spec and a test matrix; dispatch on the property the operation needs (here: a node key), not on
  an interface only some of the admitted classes implement. Separately assert identity preservation
  or replacement with literal history expectations for each update category before promising
  continuity in the spec. See [LESSONS.md](../LESSONS.md).

## CI follow-up: Sonar maintainability gate

- Symptom / trigger: PR #1431, run 43777408800 completes analysis but fails both
  Sonar checks on new maintainability (rating B); reliability and security pass,
  coverage is 98.1%, and duplication is 0%.
- Root cause: S1452 flags the heterogeneous temporal stream's wildcard return;
  S110 counts the field-name marker as a sixth superclass. S7467 and S6213 flag
  an unused catch binding and a restricted identifier in tests. S5778 finds that
  a null-rejection lambda constructs both its fixture and the tested stream.
- Invariants / scope: past, future and all-times must expose only query items,
  preserving each structural view and primitive representation. All seven
  functions must distinguish field names from values under fused and unfused
  storage without enlarging every stored string. Exception assertions must
  isolate the tested constructor for either null argument; test identifiers must
  obey Java's language conventions.
- Correction: bound the stream to query items and remove caller casts. Scope
  S1452 to this producer boundary because its subtype streams are covariant;
  scope S110 to the intentional, stateless marker subclass to retain the string
  allocation contract. Rename the restricted identifier, use an unnamed catch,
  and construct the axis fixture before the exception assertion.
- Regression checks / oracle: Sonar is the actual failing-before consumer.
  Existing `TemporalNavigationTest` independently reads revision histories and
  checks field names; `TemporalSirixJsonItemStreamTest` checks exhaustion, close
  delegation and rejection of each null argument. These reject changed item
  representations, lost node identity and missing constructor validation. Runtime
  tests cannot prove analyzer policy, so do not add source-text tests. No runtime
  logic changes or new mutation sites are intended.
- Lesson: inspect the actual quality-gate condition even when tests and coverage
  pass; retain necessary representation and allocation choices with narrowly
  justified analyzer exceptions. See [LESSONS.md](../LESSONS.md).

The repaired caller trace is unchanged: `AllTimes`, `Past` and `Future` validate
before lazy iteration, initialize the range once, read items until `null`, and
close the same stream. Structural items still delegate to their own temporal
views; values and field names still use the prefetched axis and item factory.
Both range directions retain include-self handling. The four single-step
functions still share `step` and the same field-name marker; its suppression
changes neither construction nor string methods. The oracle's absent-node catch
still records absence, and its renamed description parameter emits the same
query. Both null-argument assertions now have only the tested constructor inside
the lambda. No branch, helper or parameter becomes unreachable; the three
redundant result casts have been removed.

Local verification passed: 828 temporal-navigation cases and three stream
contract tests, plus all suites in the [Work budgets](../VERIFICATION.md#running-the-layers)
command block. The main query/core/MCP tasks report 1,486 passed, five skipped,
and no failures or errors; their isolated task dependencies also succeeded.
`:sirix-query:spotlessJavaCheck` and `git diff --check` passed. Local evidence:
`build/ci-temporal-sonar-tests.log`, `build/ci-temporal-sonar-budgets.log` and
`build/ci-temporal-sonar/test-results.json`; the same directory holds the
failing-before Sonar gate and issues. These edits change only erased type
contracts, analyzer annotations and test clarity, so there is no new production
logic to mutation-test. Remote Sonar reanalysis remains with the outer executor;
this phase neither uploads analysis nor changes the quality-gate policy.
