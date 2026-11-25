---
kind: row
---
| A pool took its permanent memory from the allowance page frames commit from, and gave nothing back before shutdown: closing every cached image could not make a frame allocatable. | When a new consumer joins a shared budget, decide in which direction it is allowed to starve the existing ones, and give it a way to hand bytes back. Hook the giving-back into the pressure path the budget already has, AFTER the caches it asks to evict, take only the refused caller's shortfall, and derive what is idle at that moment instead of counting it on the hot path. | [Compact-image reclamation](postmortems/2026-11-compact-image-decode-arena.md#cireview-the-pools-own-budget-and-why-idle-slabs-are-reclaimed), `CompactImageReclamationTest` |
