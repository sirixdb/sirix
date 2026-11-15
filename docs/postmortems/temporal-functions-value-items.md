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
