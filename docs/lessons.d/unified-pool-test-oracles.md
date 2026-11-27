---
kind: row
---
| Planned memory-pool tests reused stale expected bytes, confused mapped capacity with ownership, sampled retention after release, and measured their allowance with the same constructor; stress and teardown checks lacked required witnesses. | Capture independent expectations before transitions, assert lent ownership separately from residency, require real releases in stress tests, check each target region's lifetime, and bind each seam to one exact contract in the proper JVM lane. | [Acceptance-oracle write-up](postmortems/unified-pool-test-oracles.md), `MemTestOracleRegressionTest`, `CompactImageAccountTest`, `FrameReuseAllocationBudgetTest`, `OptimisticReaderReleaseStressTest`, `FrameReleaseFailureTest` |
