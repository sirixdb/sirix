# Tamper-Evident Ledger Mode — Design Plan

Status: **proposal** (no implementation yet)
Scope: per-resource opt-in "ledger mode" that upgrades SirixDB's append-only,
immutable-revision storage into a cryptographically tamper-evident system with
externally anchorable, independently verifiable history.

## 1. Motivation and honest starting point

SirixDB already has the *structural* properties a tamper-evident store needs —
and they are the hard ones to retrofit:

- append-only single-log storage (FineLine-style: the data file *is* the log,
  no WAL, no in-place updates);
- every commit produces an immutable, permanently addressable revision;
- an optional per-node subtree hash tree (`HashType.ROLLING`/`POSTORDER`),
  i.e. a Merkle-tree *shape* over every document;
- page checksums, superblock checksums, checksummed 32-byte commit records.

What it does **not** have is adversarial integrity. Every hash in the system
is XXH3 (64-bit, non-cryptographic — `ResourceConfiguration.nodeHashFunction`,
`io.sirix.io.HashAlgorithm.XXH3`), nothing chains commits together
cryptographically, commit authorship (`CommitCredentials`) is unauthenticated
metadata, and no state ever leaves the machine. Anyone with file access can
rewrite a revision, recompute the hashes, and leave no trace. The README's
"tamper detection" claim currently covers corruption and bugs, not adversaries.

Threat model targeted by this plan:

| Adversary | Capability | Goal of this plan |
|---|---|---|
| A. External client | API access only | already handled (authz) + verifiable query proofs |
| B. Insider with file access | read/write the database files | every modification of committed history is detectable |
| C. Insider with file + server + key access | full control of one machine | detectable once the head has been anchored externally |

"Tamper-**proof**" in the strict sense is impossible for C without external
state; the design goal is tamper-**evidence** with a minimal trusted base: one
externally witnessed hash.

## 2. Phase 1 — Cryptographic page-Merkle foundation

**Goal:** turn the page trie's *existing* parent-embedded child hashes into a
cryptographic Merkle DAG covering everything reachable from the revision root.

SirixDB already stores a hash of every referenced page fragment in the parent
page (`PageReference.hashInBytes`, set from the payload hash on write in
`FileChannelWriter`, verified on read under `verifyChecksumsOnRead`, with the
`RevisionRootPage`'s own hash stored in the commit record). This is a Merkle
tree in shape — uber page → revision root → indirect pages → fragments —
covering document data, the path summary, **and every secondary-index page
subtree**. It is merely built from XXH3-64.

- Add `SHA_256` (JDK built-in, SHA-NI/ARMv8 intrinsics) and optionally
  `BLAKE3_256` to `io.sirix.io.HashAlgorithm`; the enum already dispatches by
  hash length, so on-disk auto-detection composes.
- Ledger mode switches the **page-reference hash** to the cryptographic
  algorithm and makes `verifyChecksumsOnRead` mandatory. `revHash(N)` becomes
  the RevisionRootPage's page hash — the commit record already carries a
  64-bit slot for exactly this value; the extended layout widens it.
- **Secondary indexes are covered by construction.** This is required, not
  optional: an unprotected index page lets an adversary silently omit or
  redirect index-served query results while all document data verifies clean.
  Rebuildability is no defense unless every query rebuilds and compares.
- Cost: a streaming hash over already-serialized page payloads at commit —
  O(bytes written), no node-layout change, zero cost for non-ledger resources.
- **Optional add-on (may ship with Phase 5 instead):** 256-bit per-node
  hashes computed once per commit over dirty paths (the
  `TransactionIntentLog` knows every modified node), for *compact* semantic
  inclusion proofs of a single node. Page-granular proofs (the page path from
  root to fragment) work without it and are the default.
- Deliverable gate: differential test `stored hashes == recomputed-from-
  scratch` across randomized update sequences; commit-overhead benchmark
  (target: ≤ 15 % commit throughput cost at 4 KB documents, measured like
  `COMPARISON_POSTGRES.md`).

## 3. Phase 2 — Chained commit records

**Goal:** the head commits to the entire history, in order.

Per committed revision *N* define:

```
revHash(N)   = pageHash( RevisionRootPage(N) )   // Phase 1: transitively
               // commits to document data, path summary, and ALL
               // secondary-index page subtrees via parent-embedded hashes
chainHash(N) = H( chainHash(N-1) ‖ revHash(N) ‖ commitTimestamp(N)
                  ‖ authorId(N) ‖ H(commitMessage(N)) )
```

- Storage: extend the per-commit record in the revisions file
  (`FileChannelWriter`, today 32 bytes: offset + timestamp + checksum) to a
  versioned 96–128-byte layout carrying `revHash`, `chainHash`, and later the
  signature. The revisions channel is already SYNC write-through, so the chain
  link becomes durable atomically with the commit — the single-log design
  (FineLine) makes the commit record the natural chaining unit, and
  per-resource logs keep chains independent (aligned with autonomous-commit
  decentralization; no cross-resource coordination on the commit path).
- Also embed `chainHash(N-1)` in `RevisionRootPage` for self-containment (it
  must be the *parent's* chain hash there, since `revHash(N)` is the hash of
  the RevisionRootPage itself — a page cannot contain its own hash).
- Secondary indexes (HOT/RBTree/path/CAS) are covered **by construction**
  through the page-Merkle tree (Phase 1) — their page subtrees hang off the
  revision root like all other pages.
- Open behavior: `verifyChainOnOpen = none | heads | full` (default `heads`).
- Deliverable gate: adversarial tests — bit-flip in an old revision, replayed
  commit, swapped revisions, truncated tail — every one must fail verification.

## 4. Phase 3 — Authenticated commits

**Goal:** bind *who* to each chain link.

- Ed25519 (JDK `java.security` since 15, no dependency) signature over
  `chainHash(N)`, stored in the extended commit record.
- Key material via a `SignerProvider` SPI: file keystore → env/KMS/HSM
  (enterprise module). Key-rotation events are written **in-band** as commits,
  so the chain itself witnesses key changes.
- REST layer maps the authenticated principal (Keycloak) to the signing
  identity; embedded users supply a signer in `ResourceConfiguration`.
- Ledger mode **rejects `customCommitTimestamps`** for transaction time
  (valid time — the bitemporal axis — is user data and unaffected).

## 5. Phase 4 — External anchoring

**Goal:** make the head undeniable, bounding what adversary C can silently do
to the window since the last anchor.

- `AnchorProvider` SPI, policy per resource (`every N commits` / `every T
  seconds` / manual `sdb:anchor()`), publishing
  `(databaseId, resource, revision, chainHash, signature)`.
- Reference implementations, in order: (1) second SirixDB instance (dogfood),
  (2) S3 object-lock/WORM bucket (enterprise S3 module exists), (3) RFC 3161
  TSA (adds trusted time), (4) transparency log (Trillian/CT-style).
- Anchor receipts are written back as in-band commits (the chain witnesses its
  own anchoring). On open: if the durable head is *behind* the newest local
  anchor receipt → tampering/truncation alarm; this also cleanly separates the
  legitimate crash-recovery truncation path (never crosses an anchored
  revision) from malicious truncation.

## 6. Phase 5 — Proofs and independent verification

**Goal:** auditors and clients verify without trusting the server.

- **Inclusion proofs:** Merkle audit path from any node to `revHash(N)` —
  `sdb:proof($node)` (query function) + REST endpoint; verified client-side
  against an anchored `chainHash`.
- **Consistency proofs:** chain segment export proving revision M..N is an
  append-only extension (auditors keep the last head they saw).
- **Offline verifier:** `sirix verify` subcommand in `sirix-kotlin-cli`
  (native-image friendly): full or incremental re-hash, chain walk, signature
  checks, anchor comparison. Wire into the existing `verification.yml` deep
  workflow as a CI gate.
- Client libraries (python/ts) get proof-verification helpers (pure crypto, no
  engine code).

## 7. Phase 6 — Ops hardening

- Backup/restore (`docs/BACKUP.md`): a backup is valid iff its chain verifies
  and its head is ≤ the anchored head — verification becomes O(1) per backup.
- Multi-resource databases: an optional database-level checkpoint chain over
  a Merkle map of per-resource heads, so one anchor covers all resources.
- Documentation: threat model + invariants added to
  `docs/formal-verification.md`; README claim upgraded honestly only when
  Phases 1–4 ship.

## 8. Sequencing, risk, and effort

| Phase | Blast radius | Risk | Depends on |
|---|---|---|---|
| 1 Crypto page-Merkle | page-reference hash width, write/verify path | perf regressions (mitigated: O(bytes written), opt-in) | — |
| 2 Chain | commit record format, commit path | format migration (mitigated: superblock versioning) | 1 |
| 3 Signatures | commit record, config | key management UX | 2 |
| 4 Anchoring | new SPI, background task | none on engine | 2 (3 recommended) |
| 5 Proofs/verifier | additive APIs + CLI | none | 1–2 |
| 6 Ops | docs/tooling | none | 4 |

Phases 1–2 are the engine work and carry the value: after them the system is
tamper-evident *given a trusted head*. Phases 3–5 make it auditable by third
parties. Recommended cut: ship 1+2 behind `ledgerMode=true` in one release,
3+4+5 in the next.

## 9. Non-goals

- Tamper *prevention* against an adversary with unrestricted machine + key +
  witness access (impossible; out of scope).
- Semantic re-verification of index *contents* against document data (the
  page-Merkle tree guarantees index pages are exactly the committed bytes;
  whether the committed index correctly reflected the data at build time is a
  correctness concern covered by tests, not an integrity concern).
- Confidentiality (already served by the existing encryption pipeline;
  orthogonal).
- Distributed consensus / BFT replication (anchoring covers the integrity
  need without it).
