# Retired specification IDs

No adopted requirement IDs have been retired. TXN sample IDs retain their subjects.
Future retirements record the ID, former subject, replacement (if any), version and
reason under CONV-101; an ID is never reused.

## Superseded wording, IDs retained

These IDs keep their subject and are not retired or reusable; only their mechanism changed
while both chapters are still drafts for adoption. The former wording is recorded so a reader
of an earlier draft can see what was replaced and why.

| ID | Subject | Superseded wording | Replaced by | Version | Reason |
|---|---|---|---|---|---|
| REST-204 | optimistic preconditions on update and delete | "Where hash preconditions apply, update/delete MUST accept If-Match and the legacy request ETag form, stripping surrounding quotes and comparing the numeric node hash under writer ownership. Malformed, missing or stale required values MUST return 400 without committing. For HashType.NONE no hash precondition is available." | REST-204 token wording; REST-211, REST-212, TXN-311 | 1.0 | node hashing defaults to NONE for 1.0 (CONF-104), so a bare-hash precondition protects nothing by default. The replacement is an opaque `<incarnation-hex>.<revision>` token with a cryptographically secure random 64-bit incarnation id (FMT-209), checked against per-record modification stamps; hashes are not used as preconditions in any hash mode, because the rolling hash is order-insensitive (TXN-311 rationale). Stale tokens now map to 412 and a missing required token to 428 |
| REST-205 | ETag output | "HEAD MUST return the selected node's hash in ETag when hashing is available, with no serialized body. GET ETag behavior MUST match the schema's selected single-revision cases; disabled hashing MUST NOT invent a concurrency token." | REST-205 token wording | 1.0 | the ETag combines the served revision with its resource incarnation id, so it has the same form in every hash mode and the empty-ETag case for HashType.NONE disappears; the node hash is no longer an entity tag at all |
| CONV-103 | specification before implementation | "Changed logic MUST receive mutation testing; each surviving or uncovered mutant needs either a stronger test or an explicit equivalent/unreachable justification." | CONV-103 three-resolution wording; CONV-201 to CONV-203 | 1.0 | a free-text justification in a PR could not be read by CI, so the PR mutation lane could only pass with every changed-line mutant detected. Equivalence is now a machine-checked per-mutant record; an "unreachable" mutant shows code without observable effect, which is removed rather than justified, and internal-peeking tests no longer count as resolution |
