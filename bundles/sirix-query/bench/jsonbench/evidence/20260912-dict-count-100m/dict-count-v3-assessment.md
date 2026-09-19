# Dictionary-count assessment: rejected

Both fresh-PGO 100M comparisons completed on the retained 99,999,968-row cleaned
corpus. All five canonical queries matched ClickHouse exactly, every expected
Sirix route check passed, and both measurement guards passed (565 and 498 samples).

The v3 binary `00503db691552199ac6596785e538c69e55cad0cf350ff916cd04056df5c50f3`
was paired against accepted group-repeat binary
`d30dc2cf56dd0caebc7c11a11e8929be2944042f5e9675365d654431b23f9afb`.
After/before hot scores were **1.002024 and 1.010685**: the complete workload
does not improve. Cold scores were 0.893380 and 0.991168, with substantial
first-round cold variation. The separate ClickHouse pairing yielded hot scores
9.734459 and 9.333631 and cold scores 9.936381 and 10.273468. Rank one is false.
Keep d685cae276ae7055311a64e4e4b62b92d443f9a8 as the accepted implementation.

The actual canonical compiler-path JDI counterfactual confirms that v3 executes
the local dictionary-count loop; v1 did not. Corrected eligibility therefore
fixes the experimental route, but does not establish a workload speedup. Q1's
improvement is offset by other queries. Code layout or PGO effects are possible
explanations, not established causes. Preserve v1, the failed v2 test, v3, both
native builds/profiles, every raw attempt, exact result and route/guard evidence.

## Next bounded experiment

Start from the accepted production implementation, removing the unaccepted
histogram. Classify neutral transform metadata once per composite-group kernel
invocation, and use the existing no-transform loop for those keys. Preserve
missing-to-literal substitution using the existing exact identity lanes. Keep
nonzero offsets, substring casts, div/mod and real conditions on the general
loop. This is a generic grouping mechanism for count, distinct and min/max
shapes, with no dataset prepass, global cache, or additional index.

Add direct identity/ownership tests and compiler-driven differential coverage;
prove positive and negative routes with diagnostic-only JDI before native work.
Run the established guarded focused tests, all-five-query 1M steering, then
fresh native PGO and paired 100M only when conservative projected space fits.
The existing cumulative 4 GiB allowance, ordinary 66 GiB guard, permanent 20 GiB
floor, balanced/balance_power policy, exact identity and route gates remain.
The one-off 65 GiB cleanup exception is exhausted. The independent column-major
space decision remains open; no new database/rebuild/migration is authorized.

Before new builds, losslessly archive the now-inactive v3 expanded checkout and
native executables under the normal guard, with member/decompressed hashes and
restoration mapping. Existing databases and the accepted timed binary stay in
place. Recheck peak allocation with margin; no build starts merely because the
current free-space sample passes.
