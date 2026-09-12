# Defer descriptor error-label formatting

`RowGroupDescriptor.validateCanonicalSchema` previously allocated column-qualified
error labels on valid reads. The change passes literal families and primitive
column indices to validation helpers and formats labels only when a check fails.
All validation conditions, error messages and wire bytes remain unchanged.

The allocation regression fails on the original validator at 35,446,400 bytes
and passes a 1,024-byte bound for 20,000 warmed validations after the change.
Exact message tests and existing codec/integrity regressions pass. Production
integration passes 60 tests and all five 1M queries. Its 1,213 retained main
sources match production base `2d1700d18cd9dc8205d02acd60d6caa5f566c56d`;
the descriptor override matches the native-measured bytecode exactly.

The native attribution experiment compares frozen bounded-reader v7/v10 builds
on the existing canonical 100M database (99,999,968 cleaned rows). All five
queries match ClickHouse references at 1M and 100M on both images, with identical
projection routes. Two rounds reverse execution order. Every timed try has a
fresh process; each block starts at verified mincore-zero and has two subsequent
OS-hot tries. Catalog work stays inside the query timer.

Adjusted hot ratios `(after + .010)/(before + .010)`, selecting the smaller hot
try, are Q1 **0.9224 / 0.8955** and Q4 **0.8102 / 0.8225**. The query gate passes
389 continuous guard samples; builds pass 235/236. Native images use the same
retained baseline PGO, compiler options and native library.

These gains measure the isolated formatter change in the experimental bounded
reader context. They do not establish the same gain on the production reader,
a complete five-query score, fresh-PGO acceptance or a ClickHouse win. The bounded
reader as a whole remains slower than the historical eager path. Mixed JVM Q1
first-try evidence is retained. Production integration contains the formatter
and tests; it does not include the experimental reader changes.

`q1-q4-attempts.jsonl` preserves every timed attempt. `native-outcome.json` records
the protocol, hashes, exact checks, guards and limitations. `production-proof.json`
ties the patch to the committed-source classpath and integration gates.
`retained-files.json` maps the larger immutable local evidence; no retained
attempt or dataset was discarded. The JSONBench rank-one campaign remains open.
