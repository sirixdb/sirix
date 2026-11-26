# Bug: Unified-pool specification pressure and residency oracles

- Fix / issue: Review findings R1–R3 on `fm/sirix-unified-pool-spec`; specification-only corrections to [MEM-103](../../spec/MEM-memory-pool.md#mem-103--cross-class-transfer-on-demand), MEM-106, MEM-108/109, MEM-201/202 and step-1 evidence.
- Symptom: Slot exhaustion has no useful byte shortfall; scratch/slab transfer cases pass on the baseline; accessing released pages before measuring residency can reject a correct backend.
- Trigger: R1: at *B* = 64 GiB, *L_f* = 55 GiB, the baseline 64 KiB region fills at 32 GiB while budget remains. R2: at *B* = 256 MiB, 208 MiB of recycled frames leaves scratch and slabs below their respective 224 MiB and 220 MiB ceilings. R3: release one populated Linux page, write it, then measure residency.
- Root cause: R1 is a shared resource-contract gap: byte admission and class capacity were treated as one constraint across allocation, pressure, progress and cache sizing. R2 is an exhaustion-fixture gap across consumers with distinct ceilings. R3 is a local oracle-ordering error: the observation itself restores backing.
- Why tests missed it: All new memory-pool tests remain planned. The draft reused a frame fixture for other consumers and measured a transient OS effect after mutating it.
- Regression test: Manual before/after contract traces below; executable regressions remain explicitly planned in MEM. No runtime, platform or mutation-testing pass is claimed for this documentation change.
- Lesson: [Lessons](../LESSONS.md); distinguish capacity from byte admission, exhaust each consumer independently, and observe release effects before touching released memory.

| Finding | Independent oracle and failing sequence | Corrected contract and planned regression |
|---|---|---|
| R1 | Budget availability cannot create an addressable slot: 32 GiB of 64 KiB slots exhausts the baseline region below the 55 GiB frame limit. Earlier-tier frames in another class cannot recycle a 64 KiB slot. | MEM-109 reserves at least the frame limit in every class and rolls back failed initialization. `ClassCapacityTest` checks 256 MiB, 16 GiB and 64 GiB budgets through a recording VM, including lazy metadata and refusal. `ClassSlotReliefTest`, `FrameStarvationTest` and `PriorityEvictionTest` retain a bounded-capacity witness: evict one requesting-class frame, retain other classes, and retry successfully. Ordinary byte pressure keeps cross-class transfer and tier priority. |
| R2 | Default reserves at 256 MiB leave frame/scratch/slab ceilings of 208/224/220 MiB. The original 4 KiB scratch and slab requests fit without transfer. | `CrossClassTransferTest` independently holds 208 MiB of frames plus 16 MiB live scratch or 12 MiB live slabs. An extra 4 KiB is refused; releasing frames alone cannot make it fit on the baseline, but returning their budget permits it. Live scratch/slabs remain owned throughout. The original frame regression remains. |
| R3 | Linux release provides zero-fill on demand; writing the released page restores backing before the old final residency check. | `PlatformReleaseSemanticsTest` measures immediately after release, before any access, then checks readable contents and writable memory. A separate untouched macOS run checks re-charge and first-touch footprint. The four-lane step-1 gate uses this order. |

Vision check: these corrections preserve the single pool, unchanged same-class/read fast paths,
reader safety and existing performance gates. They specify no persisted format change and claim
no new runtime conformance; the class-capacity policy follows the recorded maintainer decision.
