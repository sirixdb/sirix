# Specification changes

## 1.0.0 (unreleased)

- Add FMT-210 for kind-12 flags, no kind-local codec and mixed mode-zero/packed history.
- Add FMT-211 for u16-only slot directories, complete dumps and strict side-reference framing.
- Add FMT-212 for separate u8 key and value lengths and their slot bounds.
- Add FMT-213 for prefixes shared by groups of up to 16 HOT keys.
- Add FMT-214 for posting tags and byte-width frame-of-reference postings in CAS and VALIDTIME leaves.
- Add FMT-215 for same-frame value aliases to earlier inline owners.
- Add FMT-216 for kind-9 projection flags, codec assignments and bounded frame validation.
- Add FMT-217 for writer conformance, with separate selection and inclusive 20% compression acceptance.

- Schedule the nightly trend tier and give it a hosted-runner class
  ([CONV-303](CONV-conventions.md#conv-303--results-ledger-and-conditions-record),
  [CONV-304](CONV-conventions.md#conv-304--nightly-trend-tier)). A run row may name its `runner`
  (`dedicated` or `hosted`, provider, label, image); a hosted runner is fingerprinted by its class
  (label, architecture, CPU model, logical CPUs, memory) instead of its kernel, which the provider
  changes with every image, and a different CPU model is a different condition. The nightly tier
  judges CPU time and allocation per operation on a hosted runner and records wall-clock time as
  informational. A zero allocation level is compared one byte up. Apache Otava is allowed as an
  advisory second opinion; both answers are reported when they disagree, and an issue is opened once
  per confirmed upward change, never from a fork. Nothing public is committed, pushed, deployed or
  opened between 04:00 and 17:30 Europe/Berlin on a weekday, and the publishing code enforces it.
  The ledger of the nightly tier is the one ledger that is committed, on the `nightly-data` branch.
  Making Otava the judge is not decided here.
  Decided by the maintainer on 2026-11-26: a hosted runner is fingerprinted by its class, not by its
  kernel or image.
- Record the derived half of
  [TXN-613](TXN-transactions-durability.md#txn-613--backups-include-derived-resources) as
  implemented: a backup copies each backed-up resource's hidden `$derived` resource, a restore
  verifies those stores before any visible resource and reports them, and a derived store whose
  maintenance does not quiesce is refused rather than copied mid-build. The clause's reference to
  the TXN-614 pinned start snapshot is unchanged and still unimplemented, so the status moves from
  `designed` to `partial` rather than to a conformance claim. Clarify the clause's copied-resource
  identity, frontier and isolated-verification requirements following backup/restore review.
- Record the synchronous and pipelined cleanup-failure implementation and regression evidence under
  [TXN-307](TXN-transactions-durability.md#txn-307--known-durable-commit-survives-cleanup-failure),
  moving its status to `partial` without changing requirement text. The
  [TXN-305](TXN-transactions-durability.md#txn-305--known-pre-commit-point-failure) and
  [TXN-705](TXN-transactions-durability.md#txn-705--unknown-durability-outcome-and-no-fsync-retry)
  notes distinguish the remaining pre-point reuse and uncertainty gaps.
- Add the [MEM](MEM-memory-pool.md) chapter (MEM-101 to MEM-402, all proposed): page frames give
  their budget back across size classes on demand, in 2 MiB batches, with a version bump around
  each OS release and re-charge before new frames are minted; the buffer-manager caches share one
  frame budget with priority eviction up to the shortfall, keeping the old shares only as optional
  caps; the compact-image pool is charged as a reservation of that budget in one account. The
  fixed reserves are unchanged for 1.0. Each class reserves virtual capacity for the whole frame
  budget, with lazy per-slot metadata and initialization rollback on reservation refusal; slot
  exhaustion requests relief only from the requesting class. Transfer regressions independently
  exhaust each consumer's admission ceiling, and release residency is measured before access.
  No product behavior changes with this entry.
- Record the structural validator as evidence for
  [FMT-304](FMT-disk-format.md#fmt-304--hot-mutation-leaf-lifetime). Clause 3 forbids a mutation from
  reporting the index malformed because a leaf was evicted, and the validation that follows a published
  splice now has its own conformance case: `HOTStructuralValidatorLeafRetentionTest` retires every leaf
  nobody holds at the publication boundary and evicts again at every engine call, with released frames
  poisoned. No requirement text changes.

- Require allocation legs to run with escape analysis off, the Graal JIT disabled (HotSpot C2),
  a non-collecting collector and a fixed batch in
  [CONV-302](CONV-conventions.md#conv-302--timed-acceptance-verdict): their whole-byte figure
  must be identical in every process; the importer refuses other legs.
  The allocation acceptance rule is unchanged.

- Say which discriminative key bytes HOT routing can name, and require every index kind to stay
  inside them. New [FMT-305](FMT-disk-format.md#fmt-305--hot-routable-key-bound) forbids a
  persisted indirect node from naming a byte its layout cannot express: a `MULTI_MASK` node may
  name key bytes 0 to 255 only, a builder that would exceed that must narrow the node and leave
  the remaining bits to deeper levels, and reducing a byte position to the layout's width is
  forbidden because it routes on an unrelated key byte. The leading discriminative bit position
  FMT-302 and the range-scan walk-up read must survive the round trip without overflow or be
  recomputed on decode. Every index kind must state a key bound rather than inherit one by
  accident. The decided bound is 4096 stored bytes including chunk and delta suffixes; NAME
  keys past it are truncated with an unambiguous marker and read through an exact candidate
  re-check, without insert failure. Names inside the bound pay no re-check record reads. CAS and
  VALIDTIME retain their existing smaller bounds. The cross-kind conformance sweep remains planned.
- Add [FMT-306](FMT-disk-format.md#fmt-306--hot-stored-height-and-spine-continuity): a HOT
  indirect node's stored height MUST equal the structural height below it, live in the writing
  transaction, in every published revision and after cold decode; a structural change below a node
  MUST be carried back along the whole parent chain, so no reachable ancestor keeps describing the
  subtree it replaced, including when the ancestor's own height does not change, and a mutation that
  cannot complete that propagation MUST fail the transaction. Locating a key MUST NOT depend on a
  stored height, while maintenance MAY read it, so an inexact height is a malformed subtree rather
  than a cost matter, and the invariant validator MUST compare stored heights with the structure it
  walks. Height minimality stays out of scope: FMT-302 remains the only height guarantee, and a
  committed trie MAY exceed the smallest height admissible for its leaf partitioning.
  [#1457](https://github.com/sirixdb/sirix/issues/1457).

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

- Amend [REST-210](REST-api.md#rest-210--resource-creation-and-query-import-boundaries) twice. A PUT
  that creates a resource requires CREATE, and one that replaces an existing resource additionally
  requires MODIFY and DELETE, checked before the resource is removed; a refusal returns 403 and
  preserves content and revisions. The query functions `jn:store`, `jn:load`, `xn:store` and
  `xn:load` instead never replace a resource: they add a free name without dropping the database or
  touching any other resource, and refuse a name that is already taken with `SIRIXDBF0006` before
  anything is written, whatever `$create-new` says. Replacing a resource from a query is reserved
  for a future atomic replace operation. Imports into existing databases retain configured bounded
  transactions and release discarded results, while singleton empty JSON stores preserve their
  empty-resource and configured-index contract.

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

- Replace the fixed timed acceptance bar (paired point estimate at most +1%, lower 95% bound at
  most 0) with [CONV-302](CONV-conventions.md#conv-302--timed-acceptance-verdict): a regression is
  a paired difference above the 4% floor and above the benchmark's own noise threshold, measured by
  randomizing the run's pairs; a benchmark too noisy to judge is reported as unstable; a run stops
  on the precision it reached (10 to 20 pairs, 3%) and never on its result. The allocation rule is
  unchanged. [CONV-303](CONV-conventions.md#conv-303--results-ledger-and-conditions-record) adds
  the results ledger, the per-run environment capture and the dated conditions record, and
  [CONV-304](CONV-conventions.md#conv-304--nightly-trend-tier) the nightly trend tier that reports
  cumulative drift above 2%. The development strategy states the reasons in
  [The benchmark protocol](../docs/DEVELOPMENT.md#the-benchmark-protocol).

- Make 1.0 the only on-disk format under
  [FMT-401](FMT-disk-format.md#fmt-401--10-format-baseline), with strict identity checks under
  FMT-102/CONF-102 and history-preserving export/import under
  [FMT-402](FMT-disk-format.md#fmt-402--history-preserving-export-and-import).
  FMT-401 remains partial: beaconless framed-prefix and bounded historical-root checks
  do not replace the complete envelope scan deferred to `sirix-repair-format-gate`. Explicit
  truncate checks the target and its bounded root window before publishing beacons; absent revisions headers remain
  recoverable with validated data identity and tail-log evidence.

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
  fsync retry.
  The format baseline and export/import policy follow
  [FMT-401–402](FMT-disk-format.md#fmt-401--10-format-baseline).
  Truncate fails fast for affected open readers. Backups
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
