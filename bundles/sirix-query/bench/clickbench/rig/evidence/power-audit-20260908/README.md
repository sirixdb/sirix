# Per-leg power-limit audit, 2026-09-08

The rig verifies the MSR long-term and short-term RAPL limits it pins at 50 W. It never set, and
until now never recorded, the **MMIO** long-term limit — a separate domain the platform moves on its
own. The package is bounded by the **lower** of the two, so a leg could run under a tighter envelope
than its attestation stated, and nobody would see it.

This audit answers whether that happened. It did.

## What the retained telemetry shows

| cohort | legs | legs with an MMIO limit below the MSR limit | samples below | samples |
|---|---:|---:|---:|---:|
| quiet50 (the 20-leg unchanged control) | 20 | **16** | 4335 | 5714 |
| split50 | 40 | **39** | 5413 | 5838 |
| flat50 | 13 | **13** | 2874 | 3998 |
| placement | 4 | 2 | 184 | 645 |
| capped50 (the 65 W cap experiment, see below) | 1 | 1 | 298 | 318 |
| gcswitch | 1 | 1 | 49 | 274 |
| jvm50 | 1 | 1 | 61 | 291 |
| fast33 (the wider-tries pilot) | 20 | **0** | 0 | 664 |
| cool | 2 | — | — | 688 unsampled |

Every cohort in that table except `capped50` recorded MSR 50/50 W throughout, which is exactly why
the MSR-only check passed. **`capped50-01` is the odd row**: it recorded 65/65 W MSR for all 318
samples — it is the 65 W cap experiment (its telemetry runs 04:01–04:04 Z, after the 03:58:48 Z cap
epoch; compare `../thermal-20260908/query-cap65-01.json`), and it predates the MSR check this table
credits. Its 298 count is measured against its own 65 W MSR limit, not against 50 W; only **224** of
those samples were below 50 W. Do not read that row on the same scale as the others.

`baseline` is excluded from the table and from the leg counts: it is not a measurement leg. Its rows
carry `cpu`, `cpu_stat`, `java`, `loadavg`, `meminfo`, `pressure` and `vmstat` at roughly 5 s
intervals across 43 minutes spanning both the cap change and the 65 W window — a background system
monitor, not a rig observer sampling at 0.5 s.

Within the quiet50 control the MMIO limit was not merely low, it was **inconsistent between
legs of the same cohort**: `quiet50-01`, `-15`, `-17` and `-20` read 45 W for every sample, `-05`
read 51.125 W for every sample, and `-10`, `-11` and `-12` read 52.625 W for every sample. The
fast-tries pilot ran at 76 W MMIO throughout and never dipped below the MSR cap at all.

So the cohorts this campaign has been comparing did not share one envelope.

`poweraudit.py` is the committed path from telemetry to readings. It emits per-file samples,
unsampled rows, the below-limit count and the distinct limit combinations observed:

```sh
python3 bundles/sirix-query/bench/clickbench/rig/poweraudit.py <RIG_TRUST_DIR>/<leg>/telemetry.jsonl
```

`summary.json` beside this README is a cohort roll-up derived from those per-file readings, with the
microwatt values relabelled in watts and each file assigned to its cohort by name. That grouping step
is not itself committed; the per-file numbers it rests on reproduce exactly from the command above.

The raw telemetry stays where it was written, in the worker-local diagnostics directory. It is not
committed — that is a deliberate retention choice, see [RETENTION.md](../RETENTION.md) — so this
summary and `poweraudit.py` are the committed record of what it contained.

## What this does and does not establish

- These are **sampled readings of configured limits**, not measured package power. They do not prove
  a limit was enforced continuously, and a limit could have moved and returned between two samples.
- They do not identify **what** changed a limit or when. Nothing here attributes the changes.
- `cool` sampled no powercap path at all — its observer recorded only temperature, frequency and
  throttle counters — so its telemetry does not establish its regime. It is not unknown, though:
  `../thermal-20260908/manifest.json` records MMIO long-term at 45 W in **both** its pre-cap
  (200 W MSR) and post-cap (50 W MSR) readings, and names `cool-02` a transition run. Those are point
  observations bracketing the series, not continuous telemetry, so they establish the bracket and not
  what held during any given sample.
- `baseline` is a background system-monitor log, not a leg, and sampled no powercap path either. It
  is excluded from the counts above rather than reported as agreement.
- Coverage is the direct-child `*/telemetry.jsonl` files. Nested new-launcher diagnostics were not
  scanned, so this is a lower bound on affected legs.
- No timing array, C6A score or archived statistic is changed by this audit. The quiet50 study's
  measured ln spread and its **UNRESOLVED** verdict at 0.5 ln remain exactly what they were; what
  changes is the explanation available for them. That study cannot be described as establishing a
  uniform 50 W regime, because 16 of its 20 legs were sampled under a tighter one.
- Going forward the rig records every RAPL domain in `telemetry.jsonl` and notes a mid-run change in
  the part's `verdict.json` without invalidating the leg. Only the limits the rig actually pins are
  gated, because a firmware-managed limit moving is information about a leg, not grounds to discard
  hours of measurement.
