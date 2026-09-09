# Cache-drop mechanism required

Firstmate inbox 010 explicitly requests OS page-cache clearing before fresh split halves, and
requires escalation if root is unavailable. `sudo -n true` returned `sudo: a password is required`
on 2026-09-08 around 10:21 UTC. No privileged cache command has been attempted.

**Historical: this arm was never authorized and never ran.** The `rig/drop-page-cache.py` hook named
below was deleted in 396d145ff along with the harness's cache-drop mode, without ever executing;
this request is retained as the record of what was asked for, not as a live procedure.

Proposed narrow command: `sudo -n /usr/bin/tee /proc/sys/vm/drop_caches`, receiving exactly `1\n`
on standard input. The `rig/drop-page-cache.py` hook implemented this command without
prompting, editing sudo policy, or providing another eviction mechanism. Firstmate/captain can
supply a different narrowly authorized root hook instead; record its exact argv in the plan.

The pending fixed 20-composite plan is `cache-study-plan.json`. The controller must hold the
shared process-owned rig lease, confirm no query JVM is alive, execute the hook before cooldown
for each half, then retain all three tries in one JVM. Record command result, time and meminfo
before/after. Keep PL1/PL2 at 50 W. Database/corpora content is never modified or deleted.

No authorization is inferred from elapsed time. Source implementation remains incomplete and
uncompiled so every protocol experiment can keep the original aa4d81d54 runtime intact.
