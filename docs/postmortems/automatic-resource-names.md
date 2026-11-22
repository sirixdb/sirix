# Bug: automatic resource names could pick a name that already exists

- Fix / issue: [#1478](https://github.com/sirixdb/sirix/issues/1478), found while fixing #1384.
- Symptom: adding unnamed items to an existing collection could write into an existing resource
  instead of creating a new one. That contradicts #1384's contract that several items are always
  added as new resources. The store layer then required MODIFY, so this was not an authorization
  bypass, but an authorized caller silently changed an unrelated resource.
- Trigger: the generated name was derived from the resource count, not from the names in use.
  - XML `addAll`/`add(parser)` and JSON `add(reader)`: `resource(size + 1)`, so `{resource1,
    resource3}` gave `resource3`.
  - JSON `addAll`/`addAllFromUris`: `resource(size)`, with a single `+"1"` fallback that could also
    exist, so `{resource2, resource21}` gave `resource21`.
  - `xn:load` without `$res`: `count + 1` per input.
  - A name was also decided before the resource was created, so a concurrent creator could take it
    in between, and the "new" item was then written into that caller's resource.
- Root cause: the "new name" decision and the creation were two steps with different authority.
  The count was used as a stand-in for "free", and a name decided outside the creation could go
  stale before it was used.
- Fix: one claim step per item (`writeUnderFreeName` in both collections; the same discipline in
  `xn:load`). Under the candidate's resource hold, the name is claimed by creating it.
  `createResource` is atomic and fails for an existing name, so a taken name is skipped. The item
  is written only into the resource it just created, which needs only CREATE. The first candidate
  is unchanged, so names in the common contiguous case stay as before.
- Why tests missed it: naming tests used empty or contiguously named collections. The REST tests
  asserted the collision itself, as "needs MODIFY" or a write into the colliding resource
  (`generatedAppendCannotOverwriteAnyResolvedTarget`, `xmlAddsRequireModifyForExistingTargets`
  overloads 3/4).
- Regression tests: `json.JsonDBCollectionAutomaticNamesTest` and
  `node.XmlDBCollectionAutomaticNamesTest`. They cover non-contiguous names through `jn:store`,
  `jn:load`, `xn:store`, `xn:load` and the collection API, a single unnamed add, and a
  deterministic concurrent creator that takes the candidate while the caller waits for it. All 10
  failed before the fix. The REST tests were translated to the contract: CREATE suffices for
  automatic names, the existing resource is unchanged, and named adds into an existing resource
  still need MODIFY.
- Lesson: see [Lessons](../LESSONS.md).
