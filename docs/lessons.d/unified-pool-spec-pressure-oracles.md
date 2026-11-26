---
kind: row
---
| A pool spec confused class capacity with byte pressure, reused non-exhausting consumer fixtures, and measured released memory after touching it. | Model slot and byte shortfalls separately across allocator and eviction consumers; exhaust each admission ceiling independently; measure transient release effects before access can restore backing. | [Memory-pool spec write-up](postmortems/unified-pool-spec-pressure-oracles.md), planned `ClassCapacityTest`, `ClassSlotReliefTest`, `CrossClassTransferTest` and `PlatformReleaseSemanticsTest` |
