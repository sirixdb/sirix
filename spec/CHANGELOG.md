# Specification changes

## 1.0.0 (unreleased)

- Make the PR mutation lane's unit of work its blocking criterion. New
  [CONV-205](CONV-conventions.md#conv-205--pr-mutation-lane-mutant-scope) restricts mutant
  generation to the PR's own lines through a repository interceptor on PIT's tool classpath,
  applied before any sampling, forbids a per-class mutant limit, requires a census of what the
  restriction removed and a verdict that fails when a censused mutant is on a changed line, and
  bounds coverage collection to once per module. New
  [CONV-206](CONV-conventions.md#conv-206--scheduled-full-scope-mutation-report) moves the
  advisory report on unchanged lines to a scheduled full-scope run that never runs for a pull
  request and never decides an outcome.
  [CONV-202](CONV-conventions.md#conv-202--pr-mutation-verdict-with-dispositions) judges entries
  against the census as well as the report, retires its per-class sampling caveat, requires a
  module killed by its deadline to report every status it had already recorded, and makes a report
  with no mutant complete when the census explains it: the run generated mutants and none was on a
  changed line, which is nothing to judge rather than nothing to measure. Commands, the
  measured cost this replaces and the scheduled report are in
  [Verification](../docs/VERIFICATION.md#pr-mutation-testing).

- Store the commit timestamp as signed epoch microseconds instead of epoch milliseconds.
  VER-201 fixes the unit in the unchanged 64-bit timestamp field, requires truncation toward
  the past and explicit rejection of an out-of-range instant, and requires the public API to
  expose every stored microsecond; VER-202/203 compare point-in-time lookups at that
  resolution, VER-205 reports history timestamps in it through the renamed
  `getHistoryTimestampsMicros` accessor (the millisecond name is gone, so callers break
  loudly instead of silently reinterpreting the unit), FMT-104/106 define the directory and
  tail-log timestamp words in it, and TXN-310's one-microsecond increment becomes
  representable. NATIVE-203 compares transaction timestamps at microseconds, and REST
  timestamp selection and history responses resolve at microseconds. The field width and slot
  geometry are unchanged, so no numeric layout version changes; 1.0 has exactly one format, so
  no millisecond store is read and no conversion path exists. The unit is format-visible and
  must land before the freeze.
- Align [TXN-801–806](TXN-transactions-durability.md#txn-801--pre-commit-hooks) with the
  lifecycle implementation: revision-pinned asynchronous callbacks, bounded best-effort
  notification retention and retryable close ownership. The later bounded-queue decision
  defers replay to follow-up work.
  [CONF-201](CONF-configuration.md#conf-201--dispatcher-queue-and-close-defaults) owns current
  defaults and configuration, and JAPI-201 points to the hook contract.

- Align [CONV-103](CONV-conventions.md#conv-103--specification-before-implementation) with the
  [development strategy's test derivation policy](../docs/DEVELOPMENT.md#2-tests-are-derived-from-the-requirements-not-from-the-code).

- Allow one narrow exception to [CONV-201](CONV-conventions.md#conv-201--mutation-disposition-record)'s
  cost rule: a mutant that produces the same result and differs only at a single boundary input, by
  at most one extra constant-time step, may be recorded as equivalent with that reason. Every other
  cost-only mutant still needs an allocation, work-budget or latency kill, `killed-out-of-lane`
  counterfactual evidence, or removal of the code. The development strategy states the same
  exception in [Development](../docs/DEVELOPMENT.md#4-mutation-testing-as-a-complement).

- Add [TXN-615](TXN-transactions-durability.md#txn-615--in-memory-state-lives-with-the-open-database)
  for IN_MEMORY lifetime, cleanup, reserved retirement names and backup exclusions; FMT-101
  references it. Implementation status and remaining token work are recorded there.
  [JAPI-104](JAPI-java-api.md#japi-104--transaction-threading-boundary) clarifies shared handles
  and sessions with independent transactions.

- Add reviewed mutation dispositions, test associations and local refactoring helpers for the
  PR mutation lane in [CONV-201 to CONV-204](CONV-conventions.md#conv-201--mutation-disposition-record).
  [CONV-103](CONV-conventions.md#conv-103--specification-before-implementation) defines the revised
  survivor-resolution policy. Commands and review workflow are in
  [Verification](../docs/VERIFICATION.md#pr-mutation-testing).

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
  for HOT mutations. That requirement owns the normative clauses, interim retirement failure,
  evidence and remaining deferred-reclamation gap; the consolidation method contract owns its
  caller's retention duty.

- Resolve imported timestamp ordering: preserve increasing source timestamps, adjust
  ties/regressions to the preceding effective timestamp plus 1 microsecond, and keep
  each adjusted original timestamp as queryable metadata. Preserve revision order;
  time lookup uses only the effective timestamps. No migration tie-policy question remains.

- Apply the final review decisions: known durable cleanup failures return success;
  unknown durability stops writes and throws CommitOutcomeUnknownException without
  fsync retry. Reject beta stores with history-preserving export/import guidance; require an
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
