# Bug: storing a new resource into an existing database dropped the whole database

- Fix / issue: [#1384](https://github.com/sirixdb/sirix/issues/1384).
- Symptom: `jn:store('db','fresh',…)`, `jn:load`, `xn:store` and `xn:load` against an existing
  database removed the whole database and recreated it with only the new resource. Every other
  resource, its history and its indexes were destroyed by a call whose intent was to add one
  resource. Reproduced: storing `added` into a database holding `first` and `other` left `[added]`.
- Trigger: any of the four functions with `$create-new` true, which is the default, against an
  existing database.
- Root cause: `$create-new` chose the store's database `create` path whenever it was true, without
  first asking whether the database existed. That path calls `removeIfExisting` on the database
  before creating it. The flag documented "create a new collection", while the call site named a
  resource, so the database-level meaning of the flag and the resource-level intent of the call
  never met. With `$create-new` false, an existing name was written *into* the resource, so
  store/load also overlapped with `jn:import`/`xn:import`.
- Contract now (`ExistingResourceTarget`, function descriptions):
  - A missing database is created, as before.
  - In an existing database, a new resource name is added, and every other resource keeps its
    content, revisions and indexes.
  - An existing name is replaced alone when `$create-new` is true: its history is discarded and a
    new one starts, as for REST PUT (REST-210). This needs CREATE, MODIFY and DELETE.
  - An existing name is refused with `SIRIXDBF0006` when `$create-new` is false, and nothing is
    written. Writing into an existing resource is `jn:import`/`xn:import`.
  - Several inputs always receive new automatic names, including named `xn:load` calls.
  - No store or load call drops a database; replacing one is `jn:drop-database`/`xn:drop-database`
    followed by a store.
  - A replace first closes this handle's session on the resource, so items an earlier query read
    no longer block it. A session held through another handle still refuses the removal, and
    nothing is changed.
- Why tests missed it: tests stored into fresh locations, or asserted the replacement itself
  (`createAndDeleteRolesCanReplaceAnExistingDatabase`, `StoreResourceNameTest`'s "recreating
  replaces the database"). No test stored a second resource into a populated database and checked
  the first one.
- Regression tests:
  - `function.jn.io.StoreIntoExistingDatabaseTest` and `function.xml.io.StoreIntoExistingDatabaseTest`
    cover add, replace and refuse for store and load under every flag value, plus several items,
    replacing while the query holds a session on the target, `xn:load`'s named multi-input, and the
    MODIFY/DELETE requirements. Each asserts that the other resource's content, revision count and
    path index are unchanged. Before the fix, 8 of the 10 JSON cases failed, with storing `added`
    leaving `[added]`.
  - The same suites cover more properties:
    - A caller racing to create the missing database is seen, not wiped
      (`aDatabaseCreatedByARacingCallerIsKept`). With the database hold removed, the racer's
      resource was lost.
    - Replacing drops the replaced resource's statistics and advances the plan-cache index schema
      version, while another resource's statistics stay the same instances.
    - `jn:load`/`xn:load` return the collection they loaded into.
  - `LoadSourceAcquisitionTest.sourceAcquisitionFailurePreservesExistingData` caught a regression
    during the fix. Routing multi-URI loads into an existing collection through
    `JsonDBCollectionImpl.addAllFromUris` wrote each source as it was opened, so a failing later
    source left a half-loaded database. That method now acquires every source first, as database
    creation does. `acquiredSourcesAreReleasedWhenALaterSourceFails` (Linux, `/proc/self/fd`)
    pins that sources acquired before the failure are released.
  - The REST `QueryMutationAuthorizationTest` and `ContentImportAuthorizationTest` were translated
    from database replacement to resource add, replace and refuse, keeping their authorization
    intent.
- Review follow-up root causes: XML parsers opened URIs only while writing and kept live stored
  subtree readers across replacement; JSON singleton selection depended on the sequence class,
  and its replacement removed the target before source acquisition. Inputs are now retained before
  target preparation, XML replacement snapshots its input, and actual cardinality selects targets.
  `LoadSourceAcquisitionTest` covers XML named/unnamed and existing/missing databases plus JSON
  replacement acquisition failure; both store/load suites cover lazy singletons, XML self-copy,
  and named multi-input loads under all flag values.
- Lesson: see [Lessons](../LESSONS.md). A flag must act at the scope its call names: a function
  given a resource name must not act on the whole database.
