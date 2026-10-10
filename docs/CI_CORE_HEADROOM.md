# Core CI headroom

## Measurement and plan (2026-10-05)

Baseline: [main run 37338714391, Test sirix-core](https://github.com/sirixdb/sirix/actions/runs/37338714391/job/111861076345),
commit `bb2881586062dbac0112a692e3ea5c54ad80db37`. Measurements come from the
GitHub job's step timestamps and full `gh-axi` log, rather than local laptop timing.

| Phase | Time |
| --- | ---: |
| Job start through checkout, JDK, Gradle setup and build-output restore | 29 s |
| Gradle invocation through test executor start | 59 s |
| Test executor | 29 m 42 s |
| Reports and Gradle invocation completion | 3 s |
| Action cleanup through job completion | 30 s |
| Whole job | **31 m 43 s** |

Despite restoring build output, Gradle recompiled core Java (~17 s) and test Java
(~10 s). Configuration, task preparation and other resources occupy the remaining
pre-test time. Setup and compilation cannot explain the 30-minute run.

`HOTStructuralPropertyTest` reports these workload times directly:

| Kind | 2 seeds × 4 versionings × 1600 ops | 3 seeds × 4 versionings × 5000 ops |
| --- | ---: | ---: |
| CAS | 21.574 s | 80.595 s |
| PATH | 69.418 s | 114.117 s |
| NAME | 36.661 s | 121.651 s |
| VALIDTIME | 18.776 s | 105.048 s |
| PROJECTION | 46.924 s | 381.116 s |
| Total | 193.353 s | 802.527 s |

These reported workloads total **16 m 36 s**; the class occupies approximately
16 m 40 s including startup. Remaining test execution is approximately 13 m 02 s.
The next longest visible class windows are `ConcurrentAxisTest` (~139 s),
`DatabaseOwnershipTest` (~64 s), `HOTFormalVerificationTest` (~63 s),
`HOTBulkBuilderTest` (~50 s), `StraddleCanonicityProbe` (~28 s), and
`HOTVersionedLeafStressTest` (~27 s). These are intervals between successive
class stdout headers, **not exact class durations**: silent tests and buffering can
contribute to a window. The baseline did not upload successful XML reports.

The full core log contains 17,994 lines / 3.42 MB. Its timestamps do not establish
an independent logging cost: stdout logging overlaps test execution. Retain
`--info --stacktrace` and test diagnostics; do not claim a speedup from suppressing
logs without an A/B measurement.

Linux CI puts the HOT structural property class and its nested classes in one
job and all remaining classes in another. Expected baseline-equivalent job times
are approximately 18 and 15 minutes including setup/compilation/cleanup, giving
both jobs substantial margin under the unchanged 35-minute timeout. Keep one
serial test fork per job to avoid multiplying the existing heap budget or
introducing shared test-database races. Preserve the HOT property workload
parameters, seeds, versioning types and assertions; oracle checks and cadence are
specified in [Verification](VERIFICATION.md#running-the-layers).

## Coverage and gating

Both lanes invoke the existing `:sirix-core:test` task, with all of its existing
JUnit engines, JVM flags, diagnostic properties and test resources:

```bash
./gradlew :sirix-core:test -PcoreTestLane=hot-property --info --stacktrace
./gradlew :sirix-core:test -PcoreTestLane=remaining --info --stacktrace
```

The class-file pattern is `io/sirix/index/hot/HOTStructuralPropertyTest*.class`.
For the test class set T under the same test configuration and matching set H,
the lanes select T ∩ H and T ∖ H. Their intersection is empty and their union is T.
Future tests automatically enter exactly one lane; nested property classes stay
with their enclosing class.
An unrecognized lane fails configuration instead of silently skipping coverage.
Without the property, `:sirix-core:test` retains its existing test selection:
the default runs the complete suite, while `-PexcludeHeavyTests` still excludes
heavy-tagged tests on macOS and Windows.

The `TestCore` matrix has no conditions, `continue-on-error`, tag exclusions or
reduced workloads. Workflow triggers and the `Build` prerequisite are unchanged,
so both jobs run on every previously covered pull request, main push, release
branch push and scheduled run. `fail-fast: false` lets both finish after a failure.
`Deploy` still depends on `TestCore`, which now requires both matrix jobs to
succeed. Query and cross-platform jobs are unchanged.

## Verification

Use Gradle's `--test-dry-run --info` on the unpartitioned task and on both lanes,
saving each invocation's log. Count the test cases Gradle reports as `SKIPPED`
in each discovery log. Both lane counts must be nonzero and their sum must equal
the unpartitioned count at the same commit with the same test configuration.
Together with the complementary class filters above, this establishes an empty
intersection and a lane union equal to the unpartitioned discovery.
This exercises Gradle/JUnit discovery and the real include/exclude behavior.
A discovery pass skips execution and is coverage evidence, not a correctness run.

The PR's CI must execute both lanes successfully. Read each job's start/end times
from GitHub and compare them with the unchanged 35-minute cap.
