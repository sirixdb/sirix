---
kind: row
---
| A confined arena released through the shared-arena strategy path, and a staging buffer allocated before the `try` that releases it: both owners looked correct, and both were invisible to every lane that ran. | Prove a resource's owner in a lane that runs the strategy which releases least — the native-image `auto` arena strategy, where the shared close path is a no-op — and allocate nothing between an acquisition and the `try` whose `finally` releases it. | [Two resource-ownership leaks](postmortems/2026-11-resource-ownership-lz4-staging-and-confined-arenas.md), `ConfinedWriterArenaStrategyTest`, `Lz4OwnershipFailureTest` |
