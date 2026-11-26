---
section: Added
---
- A nightly benchmark tier on GitHub-hosted runners. Every evening a workflow builds `main` and records
  CPU time and allocation per operation (judged) and wall-clock time (informational) of a curated set of
  JMH benchmarks in 20 fresh processes, appends them to a results ledger kept on the `nightly-data`
  branch, publishes a static dashboard with per-benchmark charts and release markers, and opens one
  labelled issue per confirmed upward change. The nightly trend rule of CONV-304 is the judge; Apache
  Otava runs beside it as an advisory second opinion. Nothing public is committed, pushed or opened on
  a weekday between 04:00 and 17:30 Europe/Berlin, and the guard is in the code that publishes, not
  only in the cron. The ledger gains a `runner` field and a CPU-time metric, a hosted runner is
  fingerprinted by its class instead of its kernel, and a zero allocation level no longer breaks the
  trend rule.
