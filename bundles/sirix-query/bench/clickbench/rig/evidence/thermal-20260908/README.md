# Thermal investigation, 2026-09-08

> **Paused checkpoint:** the captain stopped work via firstmate inbox 007. Later inboxes 005–006 supersede the binary throttle-counter acceptance described below: variance is the criterion, with capped steering separate from uncapped publication. Two full flat-50-W distribution legs completed; the third was interrupted and must not be scored. This intermediate report is retained as historical evidence, not a final verdict. See `PARKED.md` beside the evidence.

This is measurement evidence from unchanged campaign code `aa4d81d547fb0e2353ede959786d6e8ba442edf2`,
not a new campaign standing or an optimization result. The experiment is in progress.

The initially prescribed twenty identical, consecutive `suite100m.sh 3` legs were stopped after
three complete legs when firstmate directed a thermal re-plan. All three reached 100 C and
increased package-throttle counters. Their means, sample standard deviations, minima and maxima
are descriptive: **three observations cannot characterize modality, stationary variance, or the
confidence of a proposed optimization**. None is discarded from the evidence.

`cool-01` tests an unchanged JVM command launched after three package readings below 55 C, five
seconds apart. It also reached 100 C and throttled. It removes the Gradle launcher from the
measured interval so the cool gate is immediately before the query JVM, not before a Gradle
startup that would warm the machine. No query, JVM envelope, JIT policy, cache setting, persisted
format or data changed. This is a protocol experiment, not a causal estimate of cooling alone.

The captain subsequently applied a 50 W sustained package cap, as relayed by firstmate.
`cool-02` overlapped that change and is a **transition run**; it must not be pooled with either
stable power regime. The initial MSR long-term setting was 200 W; the separately exposed MMIO
long-term setting was 45 W. The recorded settings do not establish delivered package power.

## Evidence files

The raw logs, timing exports, CSVs, manifests and replay material were archived off-tree on
2026-09-09 in `build/rig-evidence-archive-20260909.tar.gz`; the findings remain retained here.

The package sensor is `/sys/class/thermal/thermal_zone12` (`x86_pkg_temp`). P-core logical CPUs
are 0–11 and E-core CPUs 12–19 on this i7-12700H. `scaling_cur_freq` snapshots are not effective
APERF/MPERF frequencies and cannot be converted into a query speedup by dividing by 4.6 GHz.
Throttle counts on different logical CPUs overlap; **do not sum them as independent events**.
Per-query thermal boundaries were not recorded in the original three logs, so they cannot be
reconstructed precisely from retrospective samples.

The unchanged C6A score uses `min(try 2, try 3)` per query, then
`sum(log((0.01 + hot) / (0.01 + board_best)))` over 43 queries, and `exp(sum_ln / 43)`.
Use the committed 2026-09-02 board snapshot; do not substitute today's board.

These exports are no longer rankable: `rank.py` refuses any leg that does not state the publication
regime, and these state `diagnostic`; the retained findings preserve the reported figures.

A repeated-pair confidence interval must be based on whole-leg differences, preserving covariance
among the 43 queries. It must not treat queries or the two hot tries as independent replications.
The usual paired Student-t interval is described by [NIST](https://www.itl.nist.gov/div898/handbook/prc/section3/prc312.htm).
A failure to reject unimodality is not proof of unimodality; an appropriately sized follow-up may
use [Hartigan's dip test](https://search.r-project.org/CRAN/refmans/diptest/html/dip.test.html).
