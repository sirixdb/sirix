# Generic numeric grouping experiment

**Completed outcome: UNRESOLVED.** [The result](RESULT.md) records all 12 pairs, byte-identical
answers for all 43 queries in every leg, and a suite benefit interval spanning zero. The requested
aggregation family also has no resolved benefit. All 24 authorized benchmark JVMs have been used;
validation agents need Firstmate authorization for any additional 100M run.

The candidate adds a bounded, worker-local handle cache for numeric group keys. Existing leaf
zone bounds enable direct addressing only when their range fits 4096 entries and at least two
rows per possible key. Wide ranges use the existing hash table. Growth invalidates cached handles;
zero and missing keys retain their separate accumulators. No data prepass or stored format changes
are involved. The ordinary numeric aggregate fold is also separated from distinct/string-length
processing, shared by numeric, string, composite and packed-key sliced group scans.

The baseline is campaign commit `b815d459d`. The predeclared target is **2.0 ln**, with **12 paired
comparisons** and seed 0. `planning.json` records the rig's answer using the retained historical
null-pair calibration: 10 pairs, or 12 with the conservative noise bound. The historical power
regimes varied; these are conditional planning estimates, not an effect claim or a promised
resolution for this candidate. No collection may be extended after seeing its result.

`compare.py` delegates the paired collection to `measure.py`. Both arms use the same untimed
`--dump` output option. Every completed leg must have all 43 answer files byte-identical to the
first baseline leg; any missing file or mismatch aborts before another leg and prevents a report.
The controller checks MemAvailable >= 26 GiB after the rig's cooldown and lease checks, immediately
before each launch, and caps the collection at 24 JVMs. The pinned JVM envelope, power policy,
three tries, hot minimum, balanced ordering and score arithmetic remain the rig's.

The added dump option is recorded in the protocol and each actual command. This comparison must
be interpreted within that common protocol; the old publication leg is context, not a paired arm.

Invocation from this worktree, with the existing read-only campaign directory supplied through
`CB100M_DIR`:

```sh
build/rig-python/bin/python3 \
  bundles/sirix-query/bench/clickbench/rig/evidence/aggregate-generic-20260909/compare.py compare \
  --baseline b815d459d --candidate CANDIDATE_COMMIT \
  --pairs 12 --effect-ln 2.0 --seed 0 \
  --out bundles/sirix-query/build/diagnostics/aggregate-generic-paired
```

Focused validation before collection passed 34 core tests and 204 query tests, covering group-table
growth, overflow, sparse and missing values, exact composite-key identity, ordering and top-K,
distinct aggregation, and the 43-query synthetic ClickBench smoke suite. This does not replace
the required 100M baseline byte comparison.
