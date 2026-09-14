# Dictionary-count experiment: rejected

The complete fresh-PGO 100M workload did not improve over the accepted
`d685cae276ae7055311a64e4e4b62b92d443f9a8` implementation. All five canonical
queries matched exactly, and every timing route and machine guard passed.

| Candidate | Hot after/before, rounds 1 / 2 | Hot Sirix/ClickHouse, rounds 1 / 2 |
| --- | ---: | ---: |
| v1: null-only transform eligibility | 1.05366 / 1.05887 | 9.69584 / 9.97747 |
| v3: actual neutral compiler annotations accepted | 1.00202 / 1.01069 | 9.73446 / 9.33363 |

Lower is better. The score is the geometric mean of the five per-query ratios
after adding 0.010 seconds to each latency, as upstream defines. Hot latency is
the minimum of attempts 2 and 3; every attempt starts a fresh process. Each
round reverses engine order. Own-database eviction and mincore checks classify
attempt 1 as cold. Catalog discovery remains inside query time. Setup, training,
correctness dumps and debugger observations are excluded from ranked latency.

The upstream revision remains
`e6c7c98dc766394d51f7d506a3dd2b5d51165d70`; see [recheck](group-repeat-upstream-recheck.json).
The 100M tier exists, although the dashboard defaults to 1B. At 100M the hot
leader is ClickHouse 25.11; the cold leader is StarRocks 4.0.1. This local
comparison uses the captain-authorized ClickHouse 26.7.3.19 against the same
cleaned 99,999,968-row Bluesky corpus. The five queries are collection counts,
create counts with distinct users, hourly counts for three collections,
earliest three post users, and longest three posting spans. This is JSONBench,
not the separate web-hits ClickBench workload. **Rank one is not established.**

The [assessment](dict-count-v3-assessment.md) explains rejection and the next
bounded experiment. [Compiler counterfactuals](dict-count-compiler-counterfactual.md)
prove that actual Q1 bypassed v1's histogram and entered v3's. The failed v2 test
is retained: arithmetic around a substring cast was already unsupported on the
accepted baseline. The corrected test covers the supported bare cast and the
unchanged interpreter fallback, with exact results and a negative shortcut
observation. V3 passed 83 focused tests and the full 1M gate before native PGO.

The full before/after and leader [v3 audits](dict-count-v3-before-after-100m-validation.json)
and [leader audit](paired-dict-count-v3-100m-attempt1-validation.json) retain
source, binary and profile identities. [Raw files](raw-files.json) inventories
2,668 exact members in `raw-attempts.tar.gz` (7,182,260 bytes, SHA-256
`ebdbf73b2aafed7ed86871ed480ee407675d6458df0fa87ea3fce3fbb82b0e44`).
[Large files](retained-large-files.json) records 24 retained local artifacts by
path, size and checksum, including source archives, profiles, binaries and
profiling data.

The ordinary guard remains 66 GiB, balanced/balance_power, no swap in a 24 GiB
scope, continuous temperature/EPP/throttle sampling, and termination only of the
owned process tree on any violation. Both failed disk-floor attempts and the
explicit once-only cleanup recovery are retained in the [recovery report](dict-count-v3-recovery.md).
That cleanup exception is exhausted; it never applied to a build or query.
The cumulative 4 GiB allowance and permanent 20 GiB floor are unchanged.

Completed source trees and inactive images were losslessly archived, preserving
all files, original hashes, native shared libraries and raw evidence. The
restoration maps include [source v1/v2](dict-count-build-archives.json),
[source v3](dict-count-v3-build-archives.json), [older images](dict-count-native-preservation.json)
and [v3 images](dict-count-v3-native-preservation.json). Use
`restore-campaign-artifact.py --original <recorded-path>` under the ordinary
guard before replaying an archived path; `--verify-only` checks decompressed
bytes without restoration. Never overwrite a hardlinked frozen artifact in
place. The accepted timed binary and both verified 100M databases remain intact.
The independent column-major capacity decision remains open.
