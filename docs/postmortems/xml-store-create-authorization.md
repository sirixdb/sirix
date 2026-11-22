# Bug: `xn:store` did not authorize CREATE at the query function

- Fix / issue: [#1473](https://github.com/sirixdb/sirix/issues/1473), found while fixing #1471
  (PR #1472).
- Symptom: `xn:store` was the only store/load query function that did not call
  `MutationAuthorization.requireCreate(ctx, collection, resource)` before touching the store;
  `xn:load` and `jn:store` both do. With a context store that authorizes mutations but does not
  check inside `create`/`add`, `xn:load` was refused and `xn:store('s','r',<a/>)` created the
  database.
- Impact: none through the REST API. `XmlSessionDBStore` and `AuthCheckingXmlDBCollection` check
  the `CREATE` role in every `create`, `add` and `addAll`. A probe that ran every `xn:store` path
  as a caller without `CREATE` was refused with `Forbidden` and left the store unchanged. The gap
  affected only embedders relying on the query-function check, like the one `xn:load` and
  `jn:store` provide.
- Trigger: `xn:store('fresh','r',<a/>)` with a context whose store implements `MutationAuthorizer`
  and refuses `authorizeCreate`.
- Root cause: the query-function authorization boundary was added to `jn:store`, `jn:load` and
  `xn:load` when ordinary function calls were found to bypass the updating-expression check (see
  the matching lesson), but `xn:store` was missed. REST stayed safe only because its store
  decorators duplicate the check one layer down.
- Why tests missed it: query-module authorization tests exercised `jn:*` and `xn:load` only, and
  `QueryMutationAuthorizationTest` had no `xn:store` case, so neither boundary was pinned for it.
  The first probe for this issue also overstated the impact, because it bypassed the REST
  decorators. A test through the real REST store corrected it.
- Regression tests:
  - `StoreAuthorizationTest.aRefusedCreateLeavesTheStoreUnchangedOnEveryPath` covers new database,
    append, overwrite and node sequence, asserting the refusal is the cause and the database and
    resource directories are unchanged. It failed before the fix: nothing was thrown.
  - `StoreAuthorizationTest.aGrantedCreateStoresAndNamesTheTarget` is the control: a granted create
    stores and authorizes the exact (database, resource) pair. It failed before the fix: no
    authorization call was made.
  - `QueryMutationAuthorizationTest.xmlStoreWithoutCreateRoleIsRefused` and
    `xmlStoreWithCreateRoleStores` pin the REST outcome (403, store unchanged) independently of
    the query-function check.
- Lesson: extended the existing query-mutation-authorization entry in [Lessons](../LESSONS.md):
  apply a boundary to every sibling function, and judge impact through the real deployment layers.
