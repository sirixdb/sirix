---
kind: note
---
## Pin negative-control oracles with the toolchain

A compiler/runtime upgrade can change the diagnostics and signal handling of a
deliberately broken executable. Re-run both normal and omission callers on the
pinned runtime; require completed data work and specific teardown-worker evidence,
and use a watchdog signal the stuck runtime cannot catch. Do not treat an arbitrary
crash or hang as a successful control. See the
[25i5 shutdown-control postmortem](postmortems/native-25i5-shutdown-control.md).
