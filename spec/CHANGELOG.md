# Specification changes

## 1.0.0 (unreleased)

- Replace the retired node-hash REST precondition with an opaque conditional-write token.
  The ETag is `<epoch>.<revision>` for the revision a response serves, in every hash mode; a
  conditional write checks on demand whether the target changed, so writes pay nothing. The epoch
  is a per-resource history number, bumped by truncate and by whole-resource replacement, that
  invalidates tokens from an ended history without renumbering revisions. Node hashes are not used
  as preconditions at all: the rolling hash is an order-insensitive weighted sum, so a reorder or a
  same-depth move leaves ancestor hashes unchanged, which a precondition must not miss. Add TXN-311
  conflict semantics (subtree, insertion-point and resource target kinds over per-record
  modification stamps, evaluated under the single writer), VER-206 last-modified stamping,
  FMT-209 persisted epoch and last-revert revision with a readOnlyCompat feature bit and a
  monotonic per-database epoch counter in CONF-103, CONF-109 `requirePreconditions` answering
  428, JAPI-111 one stable conditional check, and REST-204/205/211/212 for If-Match, ETag,
  412/428 responses and root/query scope. Record the superseded REST-204/205 wording in RETIRED.
  Nothing here is implemented: every requirement states its gap.

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
