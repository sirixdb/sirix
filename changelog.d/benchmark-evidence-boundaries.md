---
section: Fixed
---
- Benchmark version probes exclude JVM option variables and discard multiline option announcements
  from captured runtime text. Nightly reports use the latest nightly run's date and report missing
  evidence instead of falling back to an earlier night. ClickBench cold rounds reject missing
  option values before consuming arguments.
