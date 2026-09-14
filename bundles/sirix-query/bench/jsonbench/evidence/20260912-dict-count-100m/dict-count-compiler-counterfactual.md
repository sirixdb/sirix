# Dictionary-count compiler-path counterfactual

This is diagnostic evidence, excluded from every ranked latency. JDI breakpoints observe the existing JVM bytecode without production counters or source instrumentation. Each breakpoint is disabled after its first observation; the observation proves at least one execution, not a total invocation count. All target queries and fixture setup ran inside the campaign's continuous fail-closed guard.

## Canonical Q1

The identical canonical Q1 ran against `db-1m-retention-repaired`, using each candidate's freshly frozen runtime and all 1,000,000 cleaned rows. Both dumps matched the retained ClickHouse reference exactly and both reported the expected group-aggregate route. Route counters alone would have missed the eligibility defect.

| Runtime | Actual compiler annotations | Histogram entry | General-loop entry |
| --- | --- | --- | --- |
| v1 | offsets `[0]`, substring `[0,0]`, divmod null, condition columns `[-1,-1]` | not observed | observed |
| v3 | same annotations | observed | not observed |

The v1 null-only eligibility check excluded `fn:string` because its compiler annotations contain neutral arrays. V3 accepts absent or zero-valued arrays while preserving missing-to-empty substitution, exact byte identity, and the existing fallback for actual transforms. The v1 and v3 guards passed with seven and six samples respectively.

Raw evidence is in `diagnostic-dict-count-v1-compiler-q1/` and `diagnostic-dict-count-v3-compiler-q1/`, particularly `observation.txt`, `result.json`, `launch.txt`, dumps and query logs. The results contain source hashes and frozen runtime manifests. Source archives and restoration maps retain the now-archived v1 expanded build.

## Failed v2 assertion and baseline control

V2's added test incorrectly asserted projection serving for `xs:integer(substring($e.commit.collection, 1, 1)) + 0`. The test first checked differential equality, which passed; its route assertion failed with expected 1, actual 0. The failed source snapshot, complete test XML, build log and guard verdict are retained. V2 was never measured natively.

The accepted `d685cae276ae7055311a64e4e4b62b92d443f9a8` implementation's frozen runtime and v3 then ran all three expressions over the same 240-row fixture, repeating collection strings `10`, `11`, `20`, `21`. For each expression, both runtimes matched the interpreter and these pinned answers:

| Expression | Baseline route | V3 route | Exact answer |
| --- | --- | --- | --- |
| substring integer cast `+ 0` (the exact failed expression) | fallback | fallback | `{"event":1,"count":120} {"event":2,"count":120}` |
| bare substring integer cast | projection | projection | `{"event":1,"count":120} {"event":2,"count":120}` |
| substring integer cast `+ 3` | fallback | fallback | `{"event":4,"count":120} {"event":5,"count":120}` |

This is the existing compiler contract: `GroupAggregateDetectionStage.integerOfSubstring` recognizes the cast call directly; arithmetic around that call is not the supported shifted-dereference form. V3's regression now asserts the supported bare cast and the unchanged arithmetic fallback, retaining exact result checks. It does not weaken an expectation for a route changed by the candidate.

The candidate control's JDI trace observed count-only, one dictionary key, offsets `[0]`, substring `[1,1]`, divmod null and condition columns null. It observed the general-loop entry and never the histogram entry. Thus the nonzero substring transform demonstrably avoids the new shortcut, while its computed numeric identities still merge the four raw strings into two correct groups. The control guard passed with ten samples.

Raw control evidence is in `diagnostic-dict-count-v3-transform-control-attempt2/`; `result.json` retains complete query texts, answers, route deltas and both runtime manifests. Attempt 1 only encountered a diagnostic-helper compilation error (`SirixVectorizedExecutor` is not `AutoCloseable`); its source snapshot and failure log remain. The helper was corrected to use the repository's explicit `close()` pattern before the successful control. No production source changed for that correction.

## Staged result, not completion

V3 passed 83 focused tests across ten suites, including real compiler fixtures, exact missing/string identities, forced fingerprint collisions, lazy/eager identity proof, partition ownership, high cardinality fallback and repeated-group regressions. Its complete 1M JVM gate retained 60 paired attempts, twenty verified cold blocks and 289 clean telemetry samples. Hot after/before ratios were 0.9191260551696799 and 0.9673513454277921. These are steering results; fresh native 100M PGO and paired full-size evidence are still required.

V1 remains rejected: its 100M hot before/after ratios were 1.05365813434394 and 1.0588675448132023, and hot ratios against ClickHouse were 9.695843699765692 and 9.977471817449691. The positive v3 diagnostic does not reclassify any v1 attempt as a shortcut measurement.
