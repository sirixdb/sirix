# Pre-existing sparse SUM ordering defect

Observed on unmodified campaign baseline `b815d459d1218ab9081f360257ae6f16eff9f476` and on the partial-group prototype. Both tests FAILED the same interpreter-equivalence assertion. Their wrong result is byte-identical: SHA-256 `a66ecfd207d469cf2284d7691b0896755a9d65653b7ed6ad4beb6ad3d2c49293`.

This is Firstmate's decision branch 2(b), not a passing correctness test. No engine fix is included in this performance lever. The regression remains explicitly disabled as a known baseline failure in `SparseSumOrderingOriginTest`, with this report as its authority. The baseline/prototype byte comparison establishes unchanged behavior on this case; it does not establish correct SUM ordering.

## Exact reproduction

The archived `origin/SparseSumOrderingOriginTest.java.txt` is the identical executable test source installed in both checkouts. It retains the original 24,000-row fixture, the two preceding COUNT-ordered queries, and the failing query/assertion. Only the prototype-only activation-counter checks were removed so it can compile against the unchanged baseline API; neither the fixture, failing query, answer assertion nor production code changed. The archive records that executed source before the known-defect annotation was added to the permanent test.

The fixture has `id = row % 11000`, `bucket = id % 101`, and `value = row % 31 - 15`, with `value` absent exactly when `id % 7 == 0`. It creates a numeric projection over these three fields. The failing query is:

```xquery
subsequence(for $r in jn:doc('partial','records')[]
  let $id := $r.id, $bucket := $r.bucket
  group by $id, $bucket
  let $s := sum($r.value)
  order by $s descending
  return {"id":$id,"bucket":$bucket,"s":$s,"lo":min($r.value),"hi":max($r.value)},1,17)
```

The interpreter starts with `{ "id":30, "bucket":30, "s":30, "lo":5, "hi":15 }`. Both baseline and prototype instead start with `{ "id":0, "bucket":0, "s":0, "lo":null, "hi":null }`, followed by the other all-missing operand groups. The complete expected and actual outputs, exact queries and Gradle failure logs are retained under `origin/`; `origin.json` records their hashes.

To reproduce with an unmodified baseline checkout: copy the archived Java source to `bundles/sirix-query/src/test/java/io/sirix/query/scan/SparseSumOrderingOriginTest.java`, set `SIRIX_ORIGIN_EVIDENCE` to a writable evidence directory, then run `./gradlew --no-daemon --console=plain :sirix-query:test --tests io.sirix.query.scan.SparseSumOrderingOriginTest`. The test is expected to fail. This is a small synthetic integration test and opens no campaign database.

The observed baseline run used a clean nested checkout of the exact commit; `git diff b815d459d -- bundles/sirix-core/src/main bundles/sirix-query/src/main` was empty, and the only worktree addition was the reproduction test. The same source hash was verified in both arms before execution.

## What the campaign cannot detect

The campaign's 43-answer equality gate does not exercise this sparse-SUM ordering case. In particular, q31 and q32 compute `SUM(IsRefresh)` but sort on row count, and `IsRefresh` is never missing. Consequently, byte-identical answers on all 43 queries cannot establish that SUM ordering is correct. The disabled regression and this defect report must remain visible independently of any benchmark equality or performance result.
