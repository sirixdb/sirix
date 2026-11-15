# Erasure preparation: what node hashes remember

Status: **notes**, kept with the code. They record one decision taken for 1.0 and the consequence a
caller has to weigh. They are not legal advice.

SirixDB keeps every revision. That is the point of it, and it is also why removing a value is not a
matter of overwriting it in place: the value, or something derived from it, can survive in more than
one structure. This page covers one of those structures — the optional per-node subtree hash — and
states what is and is not settled.

## The decision

**Node hashes are off by default since 1.0.** A resource created without an explicit
`hashKind(...)` uses `HashType.NONE`:

| Entry point | Default |
|---|---|
| `ResourceConfiguration.Builder` (Java API) | `NONE` (`ResourceConfiguration.DEFAULT_HASH_TYPE`) |
| `BasicJsonDBStore` / `BasicXmlDBStore`, `jn:store` / `xn:store` options | `NONE` (resolved from the same constant) |
| Kotlin CLI (`create-resource`, `create`) | `NONE` |
| REST (`?hashType=`) | `NONE` (unchanged — the REST server never defaulted to hashes) |

Hashing stays available as an explicit, per-resource choice: `hashKind(HashType.ROLLING)` or
`POSTORDER`, the `hashType` store option, `?hashType=ROLLING` on REST resource creation, or
`-Dsirix.hashType=ROLLING` to restore the old default for a whole JVM.

An **existing** resource is not affected and needs no migration. The hash kind is persisted in the
resource settings and read back on open, so a store created with rolling hashes keeps them and keeps
maintaining them.

## Why off is the default

A leaf's own hash is computed over its key, its parent's key, its kind, its neighbours, its
field-name key and its **raw value**. A container's *stored* hash is its own hash plus 77081 times
the sum of its children's hashes. So every ancestor up to the document root carries a term that
depends on the leaf's value, in every revision in which that value existed.

Everything in that computation except the value itself is known to whoever holds the store.
A guess can therefore be checked: rebuild the hash with a candidate value and compare. For a date of
birth, a postcode, a diagnosis code or a yes/no field, the candidate set is small enough to
enumerate. This was reproduced against the engine — five guesses to confirm a date of birth — during
the 1.0 erasure/format design review.

Hashes off is the only default under which a value removed from a store leaves no such confirmable
residue. It also removes the stored hash and descendant count of every container record and the hash
maintenance work on every commit.

**Stored versus derived.** What `HashType.NONE` removes is the *stored* hash. A container
(`ObjectNode`, `ArrayNode`) then reads back `0`. A primitive leaf, and an XML element, instead
*derive* a hash from the record's own content when none is stored (`StringNode#getHash`,
`ElementNode#getHash`), so `getHash()` and `sdb:hash` still answer non-zero there. That derivation
is no residue: it is computed on demand from the value the caller already holds, it is written
nowhere, and it aggregates nothing. The oracle in the section above needs the *container's* stored
aggregate, which is exactly what is gone.

## What you give up by turning them on — and what you do not

Turning hashes **on** is a legitimate choice for a resource that holds no data anybody may ask you
to erase. It buys:

* `sdb:hash` as a subtree aggregate, and with it hash-based change detection across
  `jn:all-times`;
* stored descendant counts, and with them `sdb:descendant-count`, which answers `0` on a resource
  that keeps no hashes (the count shares the hash field's storage gate);
* the hash-optimized diff route, which can skip a subtree whose stored hash is unchanged;
* the REST `ETag` / `If-Match` optimistic-concurrency mechanism, which is only offered when
  `hashType != NONE`.

What you do **not** give up by leaving them off is diff *correctness*. The hash route is an
optimization: without hashes the diff compares the nodes instead, and reports exactly the same
differences. This is enforced by differential tests that run the same edit history on a hashed and a
hashless resource and require byte-identical diffs
(`JsonDiffHashEquivalenceTest`, `XmlDiffHashEquivalenceTest`), and the diff refuses the hash
shortcut on a resource that stores no hashes even when a caller asks for it
(`DiffFactoryHashKindTest`).

These hashes never were a security control: they are XXH3-64, recomputable by anyone. See the
[storage integrity limits](DISK_FORMAT.md#3-integrity) and the
[tamper-evident ledger proposal](TAMPER_EVIDENCE_PLAN.md).

## What this page does not settle

Hashes off removes one residue. It is not erasure, and the following are **not** implemented:

* **Blinded hashes** — a hash salted with a secret that is destroyed with the data, which would let
  a resource keep node hashes *and* be erasable. Not in 1.0.
* **An eraser** — removing a subtree from every revision by rewriting the pages that hold it.
* **Retention** — dropping whole old revisions and reclaiming their space.
* **Page-checksum residue** — a record page's parent stores the XXH3-64 of that page's bytes, which
  is the same kind of oracle for anyone able to reproduce the page. Record pages now carry random
  salts by default; destroying those salts during a future erasure is separate from disabling node
  hashes, and no eraser exists yet.
* **Other copies** — REST multipart uploads, backups and replicas are outside this page.

Until those exist, the honest statement is: a value written to a SirixDB store stays recoverable from
that store's history. Choosing `HashType.NONE` means a *future* erasure of it will not be defeated by
the node hashes.
