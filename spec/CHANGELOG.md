# Specification changes

## 1.0.0 (unreleased)

- Replace the retired node-hash REST precondition with an opaque conditional-write token.
  The ETag is `<incarnation-hex>.<revision>` for the revision a response serves in every hash mode;
  the id is a 16-digit lowercase hexadecimal, cryptographically secure random 64-bit value,
  regenerated at creation, truncate, whole-resource replacement and backup restore. No global
  allocator is needed; reproduction of a particular old token has probability 2^-64. Conditional
  writes check the target on demand without ancestor propagation. Node hashes are not used:
  the rolling hash is order-insensitive, so reorders and same-depth moves can leave it unchanged.
  Add TXN-311 subtree, insertion-point and resource predicates under the single writer;
  child-position anchors skip the sibling-parent test, and only empty containers use a parent
  stamp. VER-206 stamps value, name, structural-link and XML attribute/namespace membership
  changes, excluding derived hash/count bookkeeping. Membership changes stamp the owning
  element; XML subtree checks include attribute and namespace records of every included element.
  FMT-209 stores the incarnation id in CONF-102 settings and the last-revert revision
  in a revision-root extension with a readOnlyCompat bit. Add CONF-109 `requirePreconditions`
  answering 428, JAPI-111 one stable conditional check, and REST-204/205/211/212 for If-Match,
  ETag, 412/428 responses and root/query scope. Conditional queries refuse cross-resource
  mutation; strict policy is enforced at the shared mutation boundary through every route.
  Successful mutation ETags require exactly one surviving resource head; resource/database
  deletion and multi-resource operations carry none. Record superseded REST-204/205 wording in RETIRED.
  Native/SDK conditional-write entry points are planned open capabilities, not required for 1.0;
  REST reuses the shared Java core check. Nothing here is implemented: every requirement
  states its gap.

- Add the [FMT-304 leaf-lifetime contract](FMT-disk-format.md#fmt-304--hot-mutation-leaf-lifetime)
  for HOT mutations. A mutation never fails because the page cache evicted a leaf it needs: reuse of
  a frame is deferred while any reader or writer may still read it, a leaf that may still be read is
  cooled rather than reclaimed, and a read taken without a hold is validated against the frame's
  version. Deferred reclamation is a separate task, so the requirement is `partial`: in the interim a
  required re-resolution is bounded and raises a distinctly typed leaf-retirement failure at the
  bound, leaving published revisions unchanged and rollback available. Stated as six clauses. The first three are observable and carry the acceptance
  evidence: a mutation never reads memory eviction can reclaim under the read, eviction changes
  neither what a mutation reads nor what it writes (a committed revision is identical in content
  either way), and a leaf that cannot be made safe to read makes optional maintenance decline rather
  than fail the transaction or report the index malformed. The remaining clauses constrain the
  mechanism: a direct caller of the consolidation primitive holds the node's direct leaf children for
  the call, a hold is never released while the mutation may still read that leaf unheld and every
  hold is released by the time the mutation ends (per-attempt release allowed), and a mutation that
  reads no cached leaf neither allocates nor holds for this rule. Verified by poisoned-frame eviction
  sweeps that strike at every engine call and compare the whole index with a model, a clean-JVM
  allocation budget, and a planned concurrent-evictor conformance case.

- Resolve imported timestamp ordering: preserve increasing source timestamps, adjust
  ties/regressions to the preceding effective timestamp plus 1 microsecond, and keep
  each adjusted original timestamp as queryable metadata. Preserve revision order;
  time lookup uses only the effective timestamps. No migration tie-policy question remains.

- Apply the final review decisions: known durable cleanup failures return success;
  unknown durability stops writes and throws CommitOutcomeUnknownException without
  fsync retry. Add revision-token notification replay, queue default 10,000 and close
  default 5 seconds, callback-scoped views and no retry of listener exceptions.
  Reject beta stores with history-preserving export/import guidance; require an
  immutable 1.0 corpus in CI. Truncate fails fast for affected open readers. Backups
  pin startup resource/revision membership; dropping a pinned resource fails fast.

- Classify Java declarations into the decided small stable core, labelled expert tier
  and internal tier. Include query/diff entry points and member-level exclusions.
  Require a default CI signature-compatibility gate from 1.0; no JPMS export lockdown.

- Resolve FMT-302: logical HOT height counts indirect levels and MUST satisfy
  `h <= D <= 8 * L` for distinct discriminative positions D and longest encoded
  key length L. Require validator enforcement; typical measured heights remain
  informative. Record current fixed-depth and undefined-bit validation gaps.

- Resolve timestamp ties and backwards clocks: TXN-310 and VER-201–204 require
  `max(wall clock, previous commit timestamp + 1 microsecond)`, precision-preserving
  persistence and unique timestamp lookup. Record current millisecond-storage gaps
  and the remaining wire-encoding, legacy-store and custom-input integration details.

- Add the P0 CONV, FMT, TXN, VER core, persisted CONF, JAPI/manifest and REST draft
  chapters against baseline `13f65148b117b9c8e1858a7f1510d2f65501bad9`. Preserve TXN
  sample IDs and record implementation gaps separately from decisions. Include the
  decided asynchronous hook dispatcher, bounded close, three-way commit outcome contract,
  no IN_MEMORY marker, current-head revert append, illegal one-child HOT nodes,
  HOT discriminative-bit height bound, unchanged three opt-in/layout choices,
  official work budgets, JSON type-change replacement identity and copied derived backups.
  Trace tooling, runtime fixes, annotations and CI changes are planned follow-up work.

- Adopt the 24 NATIVE requirements for stable native ABI 1.0, with JSON and XML
  parity and C/C++/Rust/Python embedding required together. Add the complete
  declaration header, numeric/layout/symbol manifests and executable header
  consumer checks. Runtime conformance remains pending implementation.
  [I1](https://github.com/sirixdb/sirix/issues/1436),
  [epic](https://github.com/sirixdb/sirix/issues/1435),
  [XML parity](https://github.com/sirixdb/sirix/issues/1447).
