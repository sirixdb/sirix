# Tamper-Evident Ledger Mode — Design Plan

Status: **proposal** (no implementation yet). Reviewed three times against the
codebase (storage-layer fact check; adversarial protocol review; cross-cutting
review covering availability, erasure, configuration integrity, proof
semantics, and the open-core boundary); all factual claims below carry the
reviewed semantics.

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

**The on/off switch is attacker-writable.** `ResourceConfiguration` is plain
JSON on disk (`ressetting.obj`), covered by no page hash and no signature, and
it holds `ledgerMode`, `hashAlgorithm`, `hashKind` and
`verifyChecksumsOnRead`. Adversary B therefore never needs to forge a hash:
setting `ledgerMode=false` means no verification path is entered at all.
**Phases 1–3 alone are defeated by a text edit.** The only durable fix is to
make ledger-mode-ness part of externally anchored state (Phase 4, §5) and to
have the verifier refuse to open a resource as non-ledger once any anchor
exists for it. Until Phase 4 ships, this limitation is stated, not solved.

### What is *not* proven, in any phase

Proofs attest that **stored bytes were committed** — nothing more. Users will
assume more, so these are explicit limitations rather than parentheticals:

- **Query completeness.** No proof says a result set is complete. A writer
  with legitimate credentials can commit a deliberately skewed secondary
  index; every later index-served query returns wrong answers, all
  cryptographically verified. No production ledger system (immudb, Azure SQL
  ledger, Oracle blockchain tables) solves this either.
- **Absence.** "This record was never in the database" is not provable here;
  it needs an authenticated dictionary over keys (a sparse Merkle tree) —
  a separate project.
- **Index build-time correctness.** The Merkle tree guarantees an index page
  is exactly the committed bytes, not that those bytes were a correct index of
  the data when built. That stays a correctness concern for tests.
- **Trusted time.** The transaction timestamp is the local clock (Phase 3);
  only TSA anchoring (Phase 4) supplies time an auditor can rely on.

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
ciphertext would enable content-confirmation attacks). Serialization
determinism becomes a hard design requirement **for the canonical encoding**,
enforced by a differential test (`stored hashes == recomputed-from-scratch`
across randomized update sequences).

The two branches have different verifier stories, and only the first can use
that test. `Encryptor` is Tink `StreamingAead`, which draws a fresh random
salt per stream: identical plaintext encrypts to different ciphertext on every
write, so a ciphertext hash is a function of the *encryption event*, not of
the data, and can never be re-derived from plaintext. For encrypted resources
the verifier checks stored-bytes-against-stored-hash plus the chain, and a key
holder then decrypts to inspect — sound, but "third-party verification
requires key access" understates it: even with the key, verification is
*decrypt and look*, never *recompute*. The differential test is scoped to
unencrypted resources accordingly.

Two further caveats on the ciphertext branch. First, the encryption key is
stored in cleartext inside the resource directory (`encryptionKey.json`, read
via `InsecureSecretKeyAccess`), so against adversary B — defined as having
file access — the confidentiality and integrity boundaries coincide on disk;
the content-confirmation rationale holds only once key storage leaves the
resource directory (a key SPI alongside `SignerProvider`, §11). Second,
ciphertext hashing is what makes **crypto-shredding** (§8) a consequence of
this design rather than an add-on: destroy the key and every hash and chain
link still verifies while the plaintext is gone.

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

algorithmId      = canonical name of the resource's ledger hash algorithm
                   // e.g. "SHA256" / "BLAKE3-256"; MUST be derived from the
                   // algorithm actually in use, never a fixed literal —
                   // otherwise the tag does not pin the algorithm at all

chainHash(0)     = H( "SIRIX-LEDGER-GENESIS-v1-" ‖ algorithmId
                      ‖ resourceIdentity ‖ creationNonce )
                   // creationNonce: 32 random bytes drawn at resource
                   // creation, stored beside resourceUUID in the superblock.
                   // A critical field: any change re-roots the chain and
                   // fails every link (fail-closed by construction)

revHash(N)       = pageHash( RevisionRootPage(N) )
                   // the RevisionRootPage body already serializes revision
                   // number, commit timestamp, commit message, and author —
                   // revHash therefore commits to all of them

chainHash(N)     = H( "SIRIX-LEDGER-CHAIN-v1-" ‖ algorithmId
                      ‖ resourceIdentity ‖ revisionNumber(N) ‖ revHash(N)
                      ‖ chainHash(N-1) )
```

- **Precondition: one writer per resource.** The chain is a total order and
  is well-defined only because SirixDB has exactly one write transaction per
  resource at a time. Pin it with a test rather than leave it implicit: any
  future multi-writer or decentralized commit path turns the chain into a DAG
  and invalidates this section.

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
  Phase 4 exists; **`head` in the Phases-1+2-only release**, with `full`
  offered as an explicit `sirix verify` / CI action rather than an open-time
  default. `full` on open is O(entire history) of page reads on every open,
  unbounded on a long-lived resource, and it turns one flipped bit in
  revision 3 into a total denial of service — verification must never be the
  thing that makes the database unavailable. A locally persisted "verified
  through revision K" watermark can bound `full` for corruption checks but
  has no security value: B can write the watermark too.
- **Verification failure must be reportable, not only fatal.** Every
  verifier entry point (open, `sirix verify`, REST) produces a structured
  report — first divergent revision, expected vs. recomputed
  `revHash`/`chainHash`, signature and anchor status — and open-time policy
  decides whether to refuse, open read-only with the report attached, or
  proceed. Regulated operators need "prove the tamper happened"; a dead
  process cannot supply that.
- Deliverable gate: adversarial tests — bit-flip in an old revision **and in
  an old page fragment**, replayed commit, swapped revisions, swapped
  resource files, transplanted chain, truncated tail, **edited
  `ressetting.obj`** — every one must fail verification against a trusted
  head. These are regression cases, not the coverage argument. The coverage
  argument is a **byte-range fuzzer** over every file in the resource
  directory asserting, for each mutation, either detection with a correct
  report or a clean refusal — never a silent success, never an unhandled
  exception. A fixed attack list covers the attacks its author thought of;
  the fuzzer does not depend on the author's imagination.

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
- Key material via a `SignerProvider` SPI: file keystore (core) → env/KMS/HSM
  (sirix-enterprise, see §11).
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
  `(resourceIdentity, revisionNumber, chainHash, keyFingerprint, signature,
  ledgerParams)`, where `ledgerParams` = (ledger mode on, `algorithmId`, key
  policy). The anchor is the only place ledger-mode-ness can live that
  adversary B cannot edit (§1): on open, a resource for which any anchor
  exists MUST be opened in ledger mode with those parameters, whatever
  `ressetting.obj` says.
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

Phase 5 is three features with different dependencies and is split
accordingly; conflating them delays the auditor-facing half by a release.

- **5a — Consistency proofs (depends on Phase 2 only).** Chain segment
  export proving revision M..N is an append-only extension; auditors retain
  the highest anchored `(revision, chainHash)` they have seen and reject
  non-extensions. A pure chain property — none of the Phase 1 Merkle work is
  needed. Caveat, so it is not overclaimed: a chain over an XXH3 `revHash`
  (5a shipped before Phase 1) is sound for *history structure* and *commit
  metadata* — order, authorship, timestamps, messages — but hollow for
  *data*, since a page with a colliding XXH3 is cheap to construct. That is a
  real intermediate product ("provable commit history") and must be described
  as exactly that, never as verifiable data.
- **5b — Inclusion proofs (depends on Phase 1).** Page path from `revHash(N)`
  down to the fragment containing the target (page-granular; per-fragment
  hashes from Phase 1 make these sound without whole-chain walks) —
  `sdb:proof($node)` + REST endpoint, verified client-side against an
  anchored `chainHash`. The optional per-node hash profile shrinks proofs to
  node granularity.
- **5c — Verifiable diffs (depends on Phase 1; the differentiator).** The
  diff engine already decides subtree equality by comparing subtree hashes
  (`AbstractDiff`, `DiffType.SAMEHASH`). Once those hashes are cryptographic,
  the same traversal yields a proof of the form "between revisions N and M
  exactly these subtrees changed, and every sibling on the path is
  unchanged" — the *negative* statement ("nothing else changed") that
  row-chained ledgers cannot make without rehashing the whole table, and the
  one an auditor actually asks for. Nearly free given Phase 1 and the existing
  diff; it is the strongest differentiator in this design and ships with 5b.
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
- Documentation: threat model + invariants into `docs/formal-verification.md`.
  The README and `SECURITY.md` now describe the existing hashes as
  non-cryptographic change detection; they are upgraded to a tamper-evidence
  claim only when Phases 1–4 ship.

## 8. Erasure and redaction (cross-cutting)

The target market — finance, healthcare, regulated records — is also the
market with erasure obligations (GDPR Art. 17, HIPAA amendment and erasure,
right-to-be-forgotten regimes). Ledger mode makes the conflict sharper, not
softer: it adds a cryptographic commitment to data that may later have to be
destroyed, on top of copy-on-write storage that physically retains every old
revision. "Not addressed" is not a survivable answer here, so the plan takes a
position.

Two mechanisms, chosen per resource:

1. **Crypto-shredding (encrypted resources) — preferred.** Because ledger
   mode hashes ciphertext (§2), destroying the key erases the plaintext while
   every page hash, chain link, signature and anchor still verifies
   byte-for-byte. Granularity follows key granularity: today one key per
   resource, so this is resource-level erasure. Per-subject erasure needs
   per-subject keys (envelope encryption keyed by a subject identifier), which
   is a key-management feature rather than a storage-format change and
   belongs in the enterprise key SPI (§11). Preferred because it is a
   consequence of a decision already made and needs no new proof machinery.
2. **Salted value commitments (unencrypted resources).** Store
   `H(salt ‖ value)` as the committed leaf and keep `(salt, value)` beside it;
   erasure deletes `(salt, value)` and leaves the commitment. The chain
   verifies unchanged, and a redacted leaf is *visibly* redacted — the
   commitment is present, the preimage is not — which is itself an auditable
   fact. Costs: a 32-byte salt per committed value, a canonical-encoding rule
   for which values are committed individually, and a verifier that
   understands the redacted state. Deferred until a customer needs erasure on
   an unencrypted ledger resource.

Either way the erasure event is itself a commit (who, when, which key or which
leaves, under what authority), so the ledger records *that* data was erased
without recording *what* it was. Operators must understand that the fact of
erasure is permanent and provable; that is the point.

Non-goal: making old revisions physically disappear from the data file
without a rewrite. Compaction already never touches reachable bytes; erasing
reachable bytes is exactly what the two mechanisms above avoid.

## 9. Sequencing, risk, and effort

| Phase | Blast radius | Risk | Depends on |
|---|---|---|---|
| 1 Crypto page-Merkle + fragment hashes | reference/fragment wire format, write/verify path, superblock compat branch | perf regressions (mitigated: O(bytes written), opt-in); format migration | — |
| 2 Chain | commit-record format, commit path | spec discipline (canonical encoding) | 1 |
| 3 Signatures | commit record, config, key mgmt | key-management UX | 2 |
| 4 Anchoring | new SPI, open-path check | provider trust classes must be respected by operators | 2 (3 strongly recommended — see threat table) |
| 5a Consistency proofs + verifier | additive APIs + CLI | overclaim risk if shipped before 1 (§6) | 2 |
| 5b Inclusion proofs | additive APIs | none | 1 |
| 5c Verifiable diffs | additive APIs | none | 1 |
| 6 Ops | docs/tooling | none | 4 |

Phases 1–2 are the engine work. Honest framing of the cut line: after 1–2 the
system detects corruption and is verifiable against a trusted head; **a
key-less insider (B) is resisted only once signatures (3) or an external
anchor (4) exist, and a key-holding one (C) only by anchoring (4), for
anchored prefixes; and the ledger on/off switch stays attacker-writable until
4 (§1)**. Recommended cut: 1+2 behind `ledgerMode=true` in one release, with
the 5a verifier alongside so the release can actually be checked; 3+4+5b+5c
in the next.

## 10. Non-goals

- Tamper *prevention* against an adversary with unrestricted machine + key +
  witness access (impossible; out of scope).
- Semantic re-verification of index *contents* against document data, query
  completeness, and absence proofs (see "What is *not* proven" in §1 — stated
  limitations users must be told about, not silent gaps).
- Confidentiality (already served by the existing encryption pipeline;
  orthogonal — but see the Phase 1 ciphertext-vs-plaintext commitment
  decision for where they touch).
- Distributed consensus / BFT replication (anchoring covers the integrity
  need without it).

## 11. Open-core boundary (sirix-enterprise)

Ledger mode is a candidate for the sirix-enterprise extension layer
(`ROADMAP.md`), but one property of this feature fixes where the line can go:
**a tamper-evidence system is only worth anything if a third party can verify
it without trusting the vendor.** A closed verifier is "trust us" — the exact
thing the feature exists to remove. So the specification and the verifier are
open by necessity, not by choice. Phase 1 also changes the on-disk format
(`SerializationType`, `PageFragmentKey`, superblock version), and a format
change cannot live in an extension module.

The resulting cut follows the standard open-core pattern — **the primitive is
open, the operationalization is paid**:

| Open core (sirix-core / sirix-query / sirix-kotlin-cli) | sirix-enterprise |
|---|---|
| Cryptographic page-Merkle DAG, chained commit records, the public byte specification (Phases 1–2) | `SignerProvider` implementations for KMS / HSM / Vault / PKCS#11 |
| `SignerProvider` SPI with a file-keystore Ed25519 implementation (Phase 3) — so the open ledger is meaningful, not a stub | Production `AnchorProvider`s: RFC 3161 TSA, S3 Object Lock, Azure immutable blob, Trillian/Rekor-style transparency logs |
| `AnchorProvider` SPI with a local-directory implementation (dev/availability tier only; explicitly *not* a trust anchor against adversary C, per the Phase 4 trust classes) | Anchoring policy scheduler, anchor monitoring, alerting on verification failure |
| Proof generation (`sdb:proof`), consistency-proof export, offline `sirix verify`, client-side proof verification (Phase 5) | Key rotation workflow and trusted-key-set management |
| Adversarial and fuzz test suites | Signed compliance reports, scheduled verification jobs, GUI audit views |
| | Redaction / crypto-shredding workflow for erasure requests (GDPR, HIPAA) |
| | Database-level checkpoint chain, verified backup/restore (Phase 6) |

Invariant for the split: **the open build must be a complete, honest,
end-to-end ledger** — sign with a file key, anchor to a directory, verify from
genesis. What is paid is production-grade trust anchors and the compliance
workflow around them. An open build that can write hashes but not verify them
would be worse than no ledger mode at all.

Design consequences for Phases 1–5:

- `SignerProvider` and `AnchorProvider` are core SPIs discovered via
  `ServiceLoader` (as `StorageProvider` already is), never referenced by
  concrete class from core.
- Anchor records must carry the ledger parameters (algorithm, mode, key
  policy) in addition to `(resourceIdentity, revisionNumber, chainHash,
  keyFingerprint)`, and the verifier must refuse to open a resource as
  non-ledger when an anchor exists for it. `ResourceConfiguration` is plain
  JSON on disk (`ressetting.obj`), covered by no hash and no signature; without
  this rule an adversary with file access disables every verification path by
  editing one flag. The external anchor is the only place that state can live.
- Verification failure must be able to produce a *report* (first divergent
  revision, expected vs. recomputed hashes, signature and anchor status)
  rather than only an exception; the open verifier emits the report, the
  enterprise layer signs, schedules, and distributes it.

Positioning: sell the compliance mapping (SEC 17a-4, 21 CFR Part 11, SOC 2
audit-trail controls, ISO 27001 integrity requirements), not the cryptography.
The hash algorithm is a commodity; the regulatory workflow, the KMS
integrations, and the support behind them are the product. The open
`AnchorProvider` implementations are a few hundred lines each and will be
reimplemented by others under the BSD license — the defensible paid surface is
the part with ongoing maintenance burden, not the part with clever code.

Sequencing: define the SPIs during Phase 1 (they shape the commit path), ship
the open implementations with Phases 3–5, and build the first paid provider
only when a customer names the KMS or anchor service they need. Do not build
enterprise integrations ahead of demand.
