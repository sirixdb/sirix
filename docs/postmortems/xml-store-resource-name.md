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
- Contract now: a single node is stored as `$res` on every path. Without `$res` a created resource
  keeps its automatic name (`resourceN`) and an appended one is named `resource`, as before. A
  sequence of several nodes keeps automatic names, as in `xn:load` and `jn:store`.
- Why tests missed it: existing `xn:store` tests passed `()` as the name, appended to existing
  collections, or read the document back through the collection rather than by name.
- Regression test: `StoreResourceNameTest.aSingleNodeIsStoredUnderTheGivenResourceNameOnEveryPath`
  failed with `[resource1]` instead of `[products]` before the fix.
  `withoutANameOrForSeveralNodesResourcesKeepTheirAutomaticNames` passes both before and after,
  and pins the unchanged automatic naming.
- Lesson: when sibling functions share a parameter (`xn:load`, `xn:store`, `jn:store`), test the
  parameter on every path through each, reading the result back by that parameter; see
  [Lessons](../LESSONS.md).
