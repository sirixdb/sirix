# Measurement conditions

A timed result is comparable with another only when both were measured under the same conditions.
This file is the dated record of those conditions: one entry for every change of the measurement
environment — machine, firmware, kernel or operating system, frequency policy, power cap, default
runtime, load control, or the benchmark set. The rule is
[CONV-303](../../spec/CONV-conventions.md#conv-303--results-ledger-and-conditions-record); the
reasons are in [the benchmark protocol](../DEVELOPMENT.md#the-benchmark-protocol).

How it is used:

- Every run that writes to the results ledger computes a fingerprint of the machine it runs on and
  looks it up here. The latest entry that holds the fingerprint gives the run its conditions id. A run
  whose fingerprint is in no entry refuses to start and says which fields changed.
- `python3 bundles/sirix-query/bench/common/bench_ledger.py fingerprint` prints the fingerprint of
  the current machine and the fields it is made of.
- Entries are appended in date order and are not edited afterwards, except by a dated correction.
- A **new baseline** entry starts a condition under a new id, even if its fingerprint is unchanged.
  Nothing measured before it is compared with anything measured after it.
- A **continues** entry keeps the id of the condition it continues. It says what changed, and it
  names the control run in the ledger that shows results stayed comparable.

A **hosted runner** (the nightly workflow's GitHub-hosted `ubuntu-24.04`) has an entry of its own per
class of machine: its fingerprint is made of the runner's provider and label, the architecture, the CPU model,
the logical CPU count and the memory, and not of the kernel and firmware that the provider changes with every
image. `python3 bundles/sirix-query/bench/common/bench_ledger.py fingerprint --runner
kind=hosted,provider=github,label=ubuntu-24.04` prints it on the runner, and the nightly job's summary prints it
when it refuses an unknown one. Each CPU model GitHub's pool contains needs its own entry, because CPU time is
not comparable across processors; the Machine row names the model, Load control says the runner is shared, and
Benchmark set names the nightly's curated set.

Every entry has the same thirteen rows. The fingerprint covers kernel release, operating system,
architecture, firmware version, product name, CPU model, logical CPU count, frequency drivers and
governors, the turbo setting and memory in whole GiB. Readings that vary within one condition are
in each run's capture and not in the fingerprint: temperatures, current frequencies, power limits
the platform moves by itself. Runtimes are recorded per arm of each run.

## 2026-11-23 — xps15-9520 (new baseline)

| Field | Value |
|---|---|
| Machine | Dell XPS 15 9520 laptop, on mains power |
| CPU | Intel Core i7-12700H: 6 performance cores with two threads each and 8 efficiency cores, 20 logical CPUs |
| Memory | 31 GiB |
| Storage | WDC PC SN810 NVMe, 1 TB; each run's capture names the filesystem that held its benchmark data |
| Firmware | Dell 1.31.0 of 2025-03-05 |
| Kernel and OS | Linux 6.8.0-138-generic, Linux Mint 22.3 |
| Frequency policy | `intel_pstate` driver, `powersave` governor on all 20 policies, turbo enabled; long-term package power limit 50 W, with a second platform limit that the firmware moves by itself (76 W when this entry was written) |
| Runtimes | Oracle GraalVM 25.0.3+9.1 for JVM arms and native images; Temurin 25.0.5+8 for the JMH gates. A run records the runtime of each arm |
| Load control | One timed campaign at a time on a machine shared with development work. Timed processes run on the performance cores (CPUs 0-11), other work on the efficiency cores (12-19). **2026-11-24 correction:** Cooling and eviction follow the kit-specific [ClickBench](../../bundles/sirix-query/bench/clickbench/README.md#measuring-the-cold-protocol) and [JSONBench](../../bundles/sirix-query/bench/jsonbench/README.md#4-measurement-discipline) protocols; gate drivers reject a pair on sampled foreign load |
| Benchmark set | ClickBench kit (43 queries), JSONBench kit (5 queries), Supply History 1 bitemporal kit, and the JMH benchmarks of `bundles/sirix-benchmarks` |
| Changed | First entry. The results ledger starts here; no earlier run was recorded in it |
| Comparable with | Nothing earlier |
| Fingerprint | `9f768f25e456de0c5f26a9803525e121d18da7ca099a5f04f65e1f6d29e8aecc` |

## 2026-11-28 — github-ubuntu-24.04-amd-epyc-7763 (new baseline)

| Field | Value |
|---|---|
| Machine | GitHub-hosted runner `ubuntu-24.04`, x86_64, AMD EPYC 7763 64-Core Processor |
| CPU | AMD EPYC 7763 64-Core Processor, 4 logical CPUs |
| Memory | 16 GiB |
| Storage | The runner's local disk; each run's capture names the filesystem that held its benchmark data |
| Firmware | Not identified: a hosted runner is a virtual machine and its firmware is not part of the fingerprint |
| Kernel and OS | Whatever the provider's `ubuntu-24.04` image ships; not part of the fingerprint and recorded in each run's capture |
| Frequency policy | Not controlled: the provider's hypervisor sets it |
| Runtimes | Temurin 25 (`actions/setup-java`, `distribution: temurin`, `java-version: 25`). A run records the runtime of each arm |
| Load control | The runner is shared with other tenants' virtual machines and is not isolated; CPU time is measured per run in fresh JMH processes and compared only within this condition |
| Benchmark set | The nightly's curated set, `bundles/sirix-query/bench/nightly/benchmarks.json` |
| Changed | First entry for a hosted runner. Nightly runs 46068849973 (stopped at the JMH build, before any fingerprint was printed) and 46099221087 (refused with this fingerprint) |
| Comparable with | Nothing earlier |
| Fingerprint | `a238619b683ade3aefd5d05cb45cfbcff8562982a231262d9d804000803f3751` |
