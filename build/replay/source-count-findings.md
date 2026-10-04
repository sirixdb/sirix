# Gate-2 blocker: invalid descendant counts in committed R16 source

Confirmed 2026-10-05 01:07 Berlin on branch fm/sirix-replay-identity-delta.
Production JsonResourceCopy and the public diff implementation are unchanged.

## Reproducer and evidence

The source alone performs the report's R16 sequence:

1. Insert `[0]` and commit revision 1.
2. At array key 1, append `[{"x":1},{}]` with SkipRootToken.YES.
3. Move field key 4 under later-created object key 5.
4. Remove the now-empty transient object key 3; reserve frontier 1,000,000; commit revision 2.

The final physical graph is `0 -> 1 -> [2, 5 -> 4]`, serialized as `[0,{"x":1}]`.
Array key 1 has three descendants; object key 5 has one descendant.

Run through the required memory/slot limiter and private Maven repository:

```sh
build/replay/run.sh :sirix-core:test --tests 'io.sirix.access.trx.node.json.JsonIdentityImportTest'
```

The first run (import-1.log) failed 16 of 29 invocations at graph validation.
The second diagnostic (import-2.log) compiled successfully and ran 37 invocations:
21 passed, 16 failed, zero skipped. It first compares every staged document field with
the immutable committed source, then invokes the graph invariant validator directly
on that SOURCE reader before the importer's target validation. All field comparisons
pass. The source validation fails in all four versioning types and both Dewey modes:

| Hash mode | Failing source key | Stored / independently counted descendants | Invocations |
| --- | --- | --- | --- |
| ROLLING | 1 | 2 / 3 | 8 |
| POSTORDER | 5 | 0 / 1 | 8 |

Child counts and final child links agree in these failures. The exception is raised
from JsonIdentityImportTest's `JsonReplayGraphValidator.validate(reader)` call, where
`reader` is source revision 2, not the target transaction.

Retained raw evidence:

- build/replay/import-1.log and import-1-results/*.xml
- build/replay/import-2.log and import-2-results/*.xml
- build/replay/baseline.log and baseline-results/*.xml

The second run's passing invocations cover:

- 8 HashType.NONE R16 snapshots, cold reopened, with exact keys, gaps, frontier,
  revision mapping, Dewey IDs and path-summary/statistics comparison;
- 4 all-payload/collision invocations, each internally testing both Dewey modes,
  including overflow Unicode strings and surviving BB after colliding Aa is removed;
- 4 cold-reopened primitive-index invocations, one per versioning type, covering NAME,
  unrestricted/selective PATH and unrestricted/selective CAS exact posting keys;
- 4 injected staging-failure rollback/retry checkpoints;
- 1 dirty-writer refusal preserving the caller's changes.

## Likely source repair sites (not changed here)

`JsonNodeTrxImpl.insertSubtreeInternal` selects the last inserted root after a
SkipRootToken.YES append and calls `adaptHashesInPostorderTraversal()` only from that
selected root. Earlier inserted sibling roots do not receive that repair when
per-insert adaptation is disabled. The existing sibling boundary is captured only
for presentation-diff collection, not for hash/count repair.

`AbstractNodeHashing.postorderAdd` updates hashes without maintaining descendant
counts. The POSTORDER move leaves object key 5's stored count at zero despite its
live field child. These are inspection findings; the test above establishes the
committed-source defect independently of this proposed diagnosis.

Source fixes must preserve bounded append work and the source append+commit latency
bar. Recomputing the entire unchanged prefix is not an acceptable shortcut. Repairing
only the imported target would also violate the exact source/copy identity oracle.

## Handoff state

The same gate-2 obstacle was confirmed twice. Per the worker brief's repeated-obstacle
rule, implementation stops for firstmate steering after preserving a local WIP commit.
No daemon/pipeline problem exists; no no-mistakes run has started. The diagnostic JVM
has finished. No background job from this lane remains.

The typed manifest, snapshot reader and staged import seam are WIP, NOT production
routing. Authoritative page deltas, generated/shrinkable streams, continuation/index
lifecycle coverage, projection/valid-time verification, bounded delta validation,
new work counters, full core/query suites, formatting and latency acceptance remain.
The prerequisite tombstone fix has not yet been rebased. No push or merge occurred.
