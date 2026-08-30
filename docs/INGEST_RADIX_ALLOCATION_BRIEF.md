# Next brief — allocator #2: the global value-dictionary radix

Written 2026-08-31 after D3/D4 of the ingestion-HFT campaign, so the next session inherits the
profile instead of reconstructing it. **Not implemented. Deliberately deferred by the lead**: high
risk, a redesign, and it was on a collision course with the P2 work that was stopped.

## The measurement (already done — do not re-derive, only re-confirm)

1M ClickBench load, `-Xms4g -Xmx8g`, G1. JFR `settings=profile`, `ObjectAllocationSample`,
aggregated by NEAREST `io.sirix` frame. Sampled weight totalled 14.43 GB against the GC log's
15.28 GB of eden — the two agree, so the fractions are trustworthy.

| site | GB | dominant object |
|---|---|---|
| `ValueDictionaryRadixNode.<init>` | 1.48 | `long[]` 1.26, `byte[]` 0.20 |
| `GlobalValueDictionaryRadix.replaceRadixChild` | 1.09 | `long[]` 1.05 |
| `GlobalValueDictionaryRadix$RadixPlan.write` | 0.89 | `long[]` 0.76, `byte[]` 0.11 |
| `GlobalValueDictionaryRadix$KeyCursor.next` | 0.39 | |
| `GlobalValueDictionaryWriter.<init>` | 0.32 | |
| `GlobalValueDictionaryRadix.append` | 0.26 | |
| `GlobalValueDictionaryWriter.insertAt` | 0.22 | |
| `GlobalValueDictionaryProbeFront.prepareInsertionStorage` | 0.17 | |
| **total** | **≈ 4.8** | overwhelmingly `long[]`, all on the `main` thread |

**The share is now much larger than 33 %.** That figure was against the pre-D4 total. D4 removed
5.39 GB, taking eden from 13.54 → 8.06 GB, so on the post-D4 baseline this is roughly **55–60 % of
everything a load still allocates** — the largest remaining item by a wide margin. Re-measure rather
than quoting that estimate.

## The shape

A per-node child/edge `long[]` that is **copied to grow on every child insertion**
(`replaceRadixChild` is the copy). Cost is quadratic in fan-out per node and is paid again for every
rebuild, which is why it dominates a load that builds the dictionary once.

## The fix to evaluate

An arena-backed node layout: children in one growable off-heap segment addressed by offset, nodes as
offsets rather than objects. That removes both the per-node array and the copy-to-grow.

## The one thing to check BEFORE designing anything

**What depends on node identity and on minted id order.** P2 mints `id == rank`, so any change to
insertion or traversal order changes the ids on disk. In D4 the equivalent check — confirming the PCR
and name memos are value-equality maps before writing a fallback that returns a fresh instance — is
what kept a bounded fallback from being a data bug that only appears at scale. Do the same here
first: enumerate every consumer of node identity and of id order, and write down which are contracts
and which are conveniences. If that check is skipped, the failure surfaces at 100M, not at 1M.

Also confirm P2's status before starting: its segment-0 gate FAILED on 2026-08-31 (see
`p2-gate-failed-on-two-measured-design-errors`), and this structure is P2's, so the brief may need
re-scoping to whatever survives.

## Method that worked, reuse it

- **JFR for attribution, one exact diag-gated counter for the number, GC log for the systemic total.**
  All three, because each alone misleads.
- **Aggregate JFR by the nearest `io.sirix` frame, never the immediate frame.** Inlining puts the
  allocation on a non-allocating line — in D3 the top frame was `peekNonWhitespaceIsColon`, a
  `return c == ':';`, and the real allocator was `intern`.
- JFR `settings=profile` costs ~45 % on the load. **Profile and time in separate runs.**
- Expect the pause numbers not to move. D4 cut allocation 40.5 % and total GC pause went 180.0 →
  186.6 ms, i.e. unchanged. **Allocation rate is the deliverable**, not pause — the 100M evidence is
  the 763-second concurrent mark, not any 1M pause figure.
- Timing discipline: this rig's noise floor reached ±16 % with cores at 400–1093 MHz and other
  agents active. Alternate arms, run both orders, check `free`/`loadavg` around every measurement,
  and **say "inside the noise" rather than claiming a number** — that was accepted as the right call
  for D4.

## Acceptance to demand

Exact allocated bytes at the site collapsing on the **same denominator** in both arms; the GC log's
eden total moving by the same amount; `StorageProfile` byte-identical between arms; the 43 query
dumps byte-identical (`ClickBenchRunMain --dump` + `diff -r`); a kill switch; and the correctness
suite named, not gestured at.

## Related memories

`ingest-allocation-profile-1m`, `char-slice-intern-removes-a-string-per-key`,
`skip-optimisations-need-an-expiry`, `thermal-throttling-invalidates-timings`,
`p2-gate-failed-on-two-measured-design-errors`.
