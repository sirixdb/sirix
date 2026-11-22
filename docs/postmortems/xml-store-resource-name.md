# Bug: `xn:store` ignored its resource name when creating a database

- Fix / issue: [#1471](https://github.com/sirixdb/sirix/issues/1471), found while writing the #1430
  regressions (PR #1472).
- Symptom: `xn:store('db','products',<a/>)` stored the document as `resource1`, so
  `xn:doc('db','products')` failed with "Resource could not be opened". The same happened with
  `create-new = false()` when the collection did not exist yet. `ShellPrompt` already assumed the
  given name and pointed the prompt at a resource that did not exist.
- Trigger: any `xn:store` call that creates the database.
- Root cause: `Store.execute` read `$res` but its private `create` helper did not take it, and
  called the store overloads without a resource name, although `XmlDBStore` has overloads with one
  and `xn:load` uses them. Only the append-to-existing-collection path passed the name.
- Current naming rules: [README](../../README.md#using-the-cli-native-binaries).
- Why tests missed it: existing `xn:store` tests passed `()` as the name, appended to existing
  collections, or read the document back through the collection rather than by name.
- Regression test: `StoreResourceNameTest.aSingleNodeIsStoredUnderTheGivenResourceNameOnEveryPath`
  failed with `[resource1]` instead of `[products]` before the fix.
  `withoutANameOrForSeveralNodesResourcesKeepTheirAutomaticNames` passes both before and after,
  and pins the unchanged automatic naming.
- Lesson: when sibling functions share a parameter (`xn:load`, `xn:store`, `jn:store`), test the
  parameter on every path through each, reading the result back by that parameter; see
  [Lessons](../LESSONS.md).

## Review follow-up: singleton sequence representations

- Invariant (S1–S3): one node uses `$res` for creation, missing-database creation with
  `false()`, recreation and append, regardless of its sequence representation; absent names
  and multiple nodes retain the existing automatic names on those same paths.
- Trigger: `xn:store('db','products',xn:sort(<a/>))`.
- Root cause: dispatch classified Java `Node` instances as singleton inputs; a sorted singleton
  is a sequence, so both create and append selected bulk operations that ignore `$res`.
- Why tests missed it: singleton tests supplied only bare element constructors, and automatic
  naming tests omitted bulk append and recreation.
- Regression: `StoreResourceNameTest` exercises bare nodes, sorted singletons and lazy filtered
  singletons on all four paths, plus absent-name and multi-node controls. The oracle is the
  S1–S3 resource-name contract and retrieval by name; automatic `resource1` rejects the singleton
  case. Parser lookahead must consume the input once and close its iterator on every path.
- Lesson: test cardinality independently of runtime representation; see [Lessons](../LESSONS.md).
