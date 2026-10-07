# Sparse document identity growth

The first authoritative-delta shadow run exposed source corruption before replay ran.
After reserving frontier 1,000,000,000,000 and appending, the source later observed a
NUMBER_VALUE at the old array identity 1. All 24 versioning/hash/Dewey combinations
failed in the source fixture. Evidence: delta-shadow-1.log and delta-shadow-1-results/.

KeyedTrieWriter grew its indirect tree only when pageKey equaled the next capacity.
That assumes consecutive allocation and fails when an explicit identity or reserved
frontier skips a boundary. An out-of-range reference offset could then be narrowed by
a compact reference delegate. Growth now repeats until the key fits; invalid keys and
out-of-range offsets are rejected before narrowing.

SparseDocumentIdentityTest is independent of replay. It checks three allocation jumps,
through 2^52, then verifies every cold-reopened revision's identities, kinds, values,
ordered links, counts and allocation frontier across all 24 configurations. The test
also checks that later sparse identities are absent from earlier revisions.

Validation: delta-shadow-2.log and delta-shadow-2-results/ — 126/126 passing invocations:
24 sparse source cases, 10 existing keyed-trie integration cases, 85 snapshot/import and
shadow cases, and seven public-diff budgets. Formatting applied; git diff --check passed.
This change does not touch the parallel worker's tombstone or legacy replay allocator fix.
Full final suites and paired latency acceptance remain pending for the replay branch.
