# Small-overflow LZ77: independent progress, not rank one

The native decoder now handles nonempty compressed overflow frames below 1 KiB as well as larger
frames. It uses the existing thread-local landing buffer, validates the decoded length, and returns
independently owned heap bytes. The wire format, zero-length handling and Java fallback are unchanged.

The two matched native rounds cover all five canonical JSONBench queries on the existing cleaned
100M-tier database (99,999,968 accepted rows). Lower after/before score is better:

| Cache class | Round 1 | Round 2 |
| --- | ---: | ---: |
| Cold | 0.952021 | 0.957861 |
| Hot | 0.968987 | 0.995004 |

These are modest improvements with mixed individual hot-query deltas. The score is the geometric
mean of `(candidate seconds + .010)/(control seconds + .010)` across all five queries. Hot is the
minimum of attempts 2 and 3. Every attempt starts a fresh process; own-database eviction with a
mincore-zero check precedes attempt 1. Order is AB then BA per query. Catalog work stays inside the
query timer; process wall time is separate. Q1/Q4 were measured first and Q2/Q3/Q5 in the retained
completion block. This is a paired implementation comparison, **not a leader comparison or fresh-PGO
acceptance result**.

Both builds use the same GraalVM 25i4 compiler, native options, third-party dependencies and retained
accepted-baseline profile. Sirix classes were freshly compiled from the accepted source fixture at
`d685cae276ae7055311a64e4e4b62b92d443f9a8`; the matched control replaces all PageKind classes with
fresh accepted-source classes. The native decoder resource matches the accepted library byte for
byte. Pooled worktree outputs and the separate experimental reader/column-major overlays are absent.
The retained PGO feedback predates this branch change; a final fresh-PGO comparison is outstanding.

Focused validation passed 9/9 tests with the native decoder and 9/9 with forced Java fallback.
Tests cover small boundary sizes, literal and repeated data, scratch-buffer reuse/growth, malformed
lengths, truncation and the native-route counter. Both native variants match all five ClickHouse
reference answers exactly at 1M and 100M, retain the same projection routes, and explicitly report an
active native decoder. Timing was accepted only after those gates.

All build, correctness and timing stages used the continuous fail-closed machine guard: temperature
below 90 C, valid telemetry with gaps at most two seconds, unchanged balanced/balance_power policy,
unchanged throttle counters, 66 GiB disk floor, and the owned 24 GiB/no-swap process scope. No guard
failure occurred in the accepted stages. Only the benchmark process tree is subject to termination.

Upstream remains pinned to ClickHouse/JSONBench
`e6c7c98dc766394d51f7d506a3dd2b5d51165d70`. Its 100M tier exists; the dashboard defaults to 1B.
The 100M hot leader is ClickHouse 25.11, while the cold leader is StarRocks 4.0.1. The local references
use captain-authorized ClickHouse 26.7.3.19. The canonical workload is
[the five Bluesky queries](../../queries.sql), not web-hits ClickBench.

The committed [Q1/Q4 attempts](q1-q4-attempts.jsonl) and [Q2/Q3/Q5 attempts](q2-q3-q5-attempts.jsonl)
preserve the measured evidence. Full raw stdout/stderr, exact dumps, guard samples, source/runtime
manifests and control-equivalence proof remain at hashed laptop paths. The campaign is still in progress, and the
canonical paired rank-one goal has not been achieved. No PR or merge is claimed by this checkpoint.
