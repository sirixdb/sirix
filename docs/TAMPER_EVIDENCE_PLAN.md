# Tamper-Evident Ledger Mode — Design Plan

Status: **proposal** (no implementation yet). Reviewed twice against the
codebase (storage-layer fact check, adversarial protocol review); all factual
claims below carry the reviewed semantics.

Scope: per-resource opt-in "ledger mode" that upgrades SirixDB's append-only,
immutable-revision storage into a cryptographically tamper-evident system with
externally anchorable, independently verifiable history.

## 1. Motivation and honest starting point

SirixDB already has the *structural* properties a tamper-evident store needs —
and they are the hard ones to retrofit:

- append-only single-log storage (FineLine-style: the data file *is* the log,
  no WAL, no in-place updates);
- every commit produces an immutable, permanently addressable revision;
- parent pages embed a hash of every referenced child page fragment
  (`PageReference.hashInBytes`, serialized with the reference in
  `SerializationType.writeHash`, verified on read — `verifyChecksumsOnRead`
  already defaults to `true`);
- a per-commit record in the revisions file (32 bytes: offset, timestamp,
  record checksum over the leading 16/24 bytes, and the `RevisionRootPage`'s
  own 64-bit page hash);
- optional per-node subtree hashes (`HashType.ROLLING`/`POSTORDER`).

What it does **not** have is adversarial integrity:

- every hash is **XXH3-64** (`HashAlgorithm.XXH3` is the only enum constant;
  `nodeHashFunction` is XXH3; superblock and record checksums are XXH3) —
  recomputable by anyone, collision-findable, not a security control;
- the page-Merkle coverage has a hole: **older page fragments** are referenced
  by `(revision, key)` *without* hashes (`PageFragmentKeyImpl`), and the
  reconstruction path builds hash-less references, so under the default
  `SLIDING_SNAPSHOT` versioning only the *newest* fragment of each page is
  verified (the code itself carries a TODO for this in
  `NodeStorageEngineReader`);
- nothing chains commits; commit authorship (`CommitCredentials`) is
  unauthenticated metadata; no state ever leaves the machine.

### Threat model and what each phase actually buys

| Adversary | Capability | Detected by |
|---|---|---|
| A. External client | API access only | existing authz; Phase 5 gives verifiable proofs |
| B. Insider, file access, **no keys** | read/write database files | Phase 3 (cannot re-sign) **or** Phases 1–2 + an externally trusted head (Phase 4) |
| C. Insider, files + server + keys | full control of one machine | Phase 4 only, and only for the **anchored prefix** |

Stated plainly: Phases 1–2 alone (hashes + chain, no signatures, no anchor) do
**not** stop adversary B — with no secret in the construction, B rewrites
history and recomputes every hash and chain link forward. Chaining is
tamper-evidence *relative to a trusted head held outside the attacker's
reach*. Phases 1–2 provide: corruption detection, and O(1)-trusted-input
verification once such a head exists. Everything after the last external
anchor is malleable to adversary C (drop, reorder, rewrite, re-sign, or fork
from the anchored prefix); only anchored prefixes are protected against C.
Tamper *prevention* against C is impossible and out of scope.

## 2. Phase 1 — Cryptographic page-Merkle foundation

**Goal:** make the page trie's existing parent-embedded hash structure a
cryptographic Merkle DAG that covers **every byte reachable at every
revision** — including reconstruction inputs and all secondary-index pages.

Two gaps must close, not one:

1. **Hash strength.** Add `SHA_256` (JDK, SHA-NI/ARMv8 intrinsics) and
   optionally `BLAKE3_256` (length-extension-resistant by design, needs a
   dependency) to `HashAlgorithm`. The enum dispatches by hash length, but the
   wire format does **not** compose automatically: `SerializationType`
   hardcodes `PAGE_HASH_BYTES = 8` (write throws on other lengths), and the
   uber-beacon trailer and revisions-record hash field are hardcoded
   `Long.BYTES`. Widening to 32 bytes touches all of these sites and is gated
   per resource by ledger mode plus a superblock layout-version bump — noting
   that `Superblock.validate` currently hard-rejects any version ≠ 0, so the
   migration needs an explicit compatibility branch, not just a new constant.
2. **Fragment coverage.** Extend `PageFragmentKey` from `(revision, key)` to
   `(revision, key, hash)` in ledger mode, so the parent's fragment list is
   itself a Merkle commitment. Without this, a page reconstructed from older
   fragments reads unverified bytes and the transitivity claim below is false
   for every versioning type except `FULL`. Cost: 32 bytes per fragment key
   (bounded by the snapshot window, ≤ N fragments per page).

With both in place: `revHash(N) := pageHash(RevisionRootPage(N))`
transitively commits to the document tree, the path summary, the DeweyID and
valid-time index pages, and **all secondary-index page subtrees**
(CAS/Path/Name pages and the HOT trees hanging off them are written through
the same reference-hash path). Index coverage is a requirement, not an
optimization: an unprotected index page lets an adversary silently omit or
redirect index-served query results while document data verifies clean.
(Whether a committed index correctly reflected the data *at build time* is a
correctness concern for tests, not an integrity concern — the Merkle tree
guarantees the index is exactly the committed bytes.)

**What the hash commits to — plaintext vs. pipeline output.** Today page
hashes cover the serialized payload *after* the byte-handler pipeline
(compression, optionally encryption). That is sound for storage integrity of
as-stored bytes, but it makes proofs non-verifiable for auditors without
decryption keys and couples integrity to pipeline determinism. Decision for
ledger mode: commit to a **canonical deterministic pre-pipeline encoding**
(specified byte order, stable field ordering) for unencrypted resources;
for encrypted resources, hash the ciphertext (plaintext hashes stored beside
ciphertext would enable content-confirmation attacks) and accept that
third-party proof verification then requires key access. Serialization
determinism becomes a hard design requirement, enforced by a differential
test (`stored hashes == recomputed-from-scratch` across randomized update
sequences).

**Write-path cost:** streaming hash over serialized page payloads at commit —
O(bytes written), no node-layout change, zero cost for non-ledger resources.
Budget gate: ≤ 15 % commit-throughput cost at 4 KB documents, measured like
`COMPARISON_POSTGRES.md`. The optional 256-bit *per-node* hash profile
(commit-time, dirty-paths-only via the `TransactionIntentLog`) is deferred to
Phase 5 — it only makes single-node inclusion proofs compact; page-granular
proofs work without it.

## 3. Phase 2 — Chained commit records

**Goal:** the head commits to the entire history, in order, with an exact,
independently reproducible byte specification.

### Protocol specification (normative for implementation)

All hashes below are the resource's ledger hash algorithm; every hash use is
**domain-separated** by a tag that also pins the algorithm and version, and
every variable-length field is length-prefixed (canonical encoding — two
different field splits can never produce the same byte stream):

```
resourceIdentity = databaseId ‖ resourceId ‖ resourceUUID
                   // resourceUUID: generated at resource creation, stored in
                   // the superblock's reserved slot — binds chains to THIS
                   // resource; defeats file-swap/transplant/replay of another
                   // resource's valid chain

chainHash(0)     = H( "SIRIX-LEDGER-GENESIS-v1-SHA256" ‖ resourceIdentity
                      ‖ creationNonce )

revHash(N)       = pageHash( RevisionRootPage(N) )
                   // the RevisionRootPage body already serializes revision
                   // number, commit timestamp, commit message, and author —
                   // revHash therefore commits to all of them

chainHash(N)     = H( "SIRIX-LEDGER-CHAIN-v1-SHA256" ‖ resourceIdentity
                      ‖ revisionNumber(N) ‖ revHash(N) ‖ chainHash(N-1) )
```

- `chainHash(N-1)` is embedded **in the `RevisionRootPage(N)` body** (a page
  cannot contain its own hash, so each root page carries its *parent's* chain
  state — thereby covered by `revHash(N)` and, in Phase 3, by the signature).
- The extended commit record stores full-width (256-bit, never truncated)
  `revHash(N)` and `chainHash(N)` plus, later, the signature. **The record is
  a non-authoritative lookup cache**: its XXH3 checksum is a corruption check
  with no security role, and verifiers MUST recompute `chainHash(N)` from
  page-derived inputs and the parent link — never trust stored copies.
  Verifiers also assert `record slot index == RevisionRootPage.revision ==
  chained revisionNumber` (no slot relocation) and that commit timestamps are
  non-decreasing along the chain.
- The revisions channel is already SYNC write-through, so the chain link
  becomes durable atomically with the commit; per-resource logs keep chains
  independent (aligned with autonomous-commit decentralization — nothing new
  on the commit path's critical section).
- **Async intermediate commits (`KEEP_OPEN_ASYNC_FLUSH`) produce no commit record**
  (they only pre-flush leaf pages; no uber page is written), hence no chain
  link — correct, since those pages become reachable only through the next
  real commit. Sync auto-commits produce full revisions and therefore full
  chain links.
- Open behavior `verifyChainOnOpen = full | anchored | head`. Honest
  semantics: `head` checks only that the head record is internally consistent
  (and signed) — it detects **no** historical tampering; `anchored` walks and
  recomputes the chain back to the newest externally witnessed link;
  `full` recomputes from genesis. Ledger-mode default: `anchored` once
  Phase 4 exists, `full` in the Phases-1+2-only release (there is no anchor
  to stop at yet).
- Deliverable gate: adversarial tests — bit-flip in an old revision **and in
  an old page fragment**, replayed commit, swapped revisions, swapped
  resource files, transplanted chain, truncated tail — every one must fail
  verification against a trusted head.

## 4. Phase 3 — Authenticated commits

**Goal:** bind *who* to each chain link with a secret adversary B lacks.

- Ed25519 (JDK `java.security`, no dependency). The signature covers the
  domain-separated tuple
  `("SIRIX-LEDGER-SIG-v1" ‖ keyId ‖ resourceIdentity ‖ revisionNumber(N)
  ‖ chainHash(N))` — since `chainHash(N)` already commits to `revHash(N)`,
  the parent link, and (via the page) timestamp/author/message, signing it
  transitively signs them all; `keyId` and identity prevent key- and
  resource-confusion. Verification recomputes `chainHash(N)` first (record
  fields are untrusted).
- Key material via a `SignerProvider` SPI: file keystore → env/KMS/HSM
  (enterprise module).
- **Key rotation is not purely in-band** (a compromised current key could
  otherwise rewrite rotation history): rotation events are commits in which
  the *new* key signs the old key's fingerprint and the current head
  `chainHash`, **and** every external anchor (Phase 4) records the active key
  fingerprint, making the key→revision-range binding externally immutable.
  The trusted-key set is external trust state, not in-band data.
- REST layer maps the authenticated principal (Keycloak) to the signing
  identity; embedded users supply a signer in `ResourceConfiguration`.
- Ledger mode **rejects `customCommitTimestamps`** (transaction time only;
  valid time — the bitemporal axis — is user data and unaffected). The
  transaction timestamp remains the local clock and is *not* trusted time;
  trusted time comes from TSA anchoring (Phase 4). Verifiers reject
  non-monotonic timestamps.

## 5. Phase 4 — External anchoring

**Goal:** make anchored prefixes undeniable; bound adversary C's silent
window to the commits since the last anchor.

- `AnchorProvider` SPI, policy per resource (`every N commits` / `every T
  seconds` / manual `sdb:anchor()`), publishing
  `(resourceIdentity, revisionNumber, chainHash, keyFingerprint, signature)`.
- **The external witness is the authority.** On open (and in the verifier),
  the newest anchor is fetched *from the provider*, not from local storage:
  require durable head ≥ anchored revision, recomputed
  `chainHash(anchoredRev)` == witnessed value, and the chain to be an
  append-only extension of the anchored prefix. In-band anchor receipts are
  written back as commits for provenance, but they are a convenience cache —
  an adversary who can tamper storage can delete them, so they must never be
  the source of truth. (This also preserves the clean separation between
  crash-recovery truncation — which never crosses an anchored revision — and
  malicious truncation.)
- Provider trust classes, documented per implementation: an RFC 3161 TSA
  (adds trusted time), a public transparency log (Trillian/CT-style), or
  WORM/object-lock storage **under an independent principal** resist
  adversary C; a second SirixDB instance or an S3 bucket whose credentials C
  controls is a dev/availability tier only and explicitly *not* a trust
  anchor against C. There is always an un-anchored tail (at minimum the
  anchor receipt itself); the anchoring interval is the honesty window.

## 6. Phase 5 — Proofs and independent verification

**Goal:** auditors and clients verify without trusting the server.

- **Inclusion proofs:** page path from `revHash(N)` down to the fragment
  containing the target (page-granular; per-fragment hashes from Phase 1 make
  these sound without whole-chain walks) — `sdb:proof($node)` + REST
  endpoint, verified client-side against an anchored `chainHash`. The
  optional per-node hash profile shrinks proofs to node granularity.
- **Consistency proofs:** chain segment export proving revision M..N is an
  append-only extension (auditors retain the highest anchored
  `(revision, chainHash)` they have seen and reject non-extensions).
- **Offline verifier:** `sirix verify` in `sirix-kotlin-cli` (native-image
  friendly): full or anchored-prefix re-hash including fragment chains, chain
  recomputation from genesis or anchor, signature checks against the external
  key-fingerprint record, anchor comparison. Wired into `verification.yml`
  as a CI gate.
- Client libraries (python/ts) get proof-verification helpers (pure crypto,
  no engine code).

## 7. Phase 6 — Ops hardening

- Backup/restore (`docs/BACKUP.md`): a backup is valid iff its chain verifies
  and its head chains into an externally anchored prefix.
- Multi-resource databases: an optional database-level checkpoint chain over
  a Merkle map of per-resource heads (each entry bound to its
  `resourceIdentity`), so one anchor covers all resources.
- Documentation: threat model + invariants into `docs/formal-verification.md`;
  the README's "tamper detection" claim upgraded honestly only when Phases
  1–4 ship.

## 8. Sequencing, risk, and effort

| Phase | Blast radius | Risk | Depends on |
|---|---|---|---|
| 1 Crypto page-Merkle + fragment hashes | reference/fragment wire format, write/verify path, superblock compat branch | perf regressions (mitigated: O(bytes written), opt-in); format migration | — |
| 2 Chain | commit-record format, commit path | spec discipline (canonical encoding) | 1 |
| 3 Signatures | commit record, config, key mgmt | key-management UX | 2 |
| 4 Anchoring | new SPI, open-path check | provider trust classes must be respected by operators | 2 (3 strongly recommended — see threat table) |
| 5 Proofs/verifier | additive APIs + CLI | none | 1–2 |
| 6 Ops | docs/tooling | none | 4 |

Phases 1–2 are the engine work. Honest framing of the cut line: after 1–2 the
system detects corruption and is verifiable against a trusted head; **a
key-less insider (B) is resisted only once signatures (3) or an external
anchor (4) exist, and a key-holding one (C) only by anchoring (4), for
anchored prefixes**. Recommended cut: 1+2 behind `ledgerMode=true` in one
release; 3+4+5 in the next.

## 9. Non-goals

- Tamper *prevention* against an adversary with unrestricted machine + key +
  witness access (impossible; out of scope).
- Semantic re-verification of index *contents* against document data (the
  page-Merkle tree guarantees index pages are exactly the committed bytes;
  build-time correctness is covered by tests).
- Confidentiality (already served by the existing encryption pipeline;
  orthogonal — but see the Phase 1 ciphertext-vs-plaintext commitment
  decision for where they touch).
- Distributed consensus / BFT replication (anchoring covers the integrity
  need without it).
