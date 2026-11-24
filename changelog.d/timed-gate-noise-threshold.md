---
section: Changed
---
- **A timed change is judged by measured noise, not by a fixed 1% bar.** The acceptance rule of the
  benchmark protocol now calls a slowdown a regression only when the paired difference is above a
  4% floor and above the benchmark's own noise threshold, measured from the same alternating pairs;
  a benchmark too noisy to judge is reported as unstable, and a run stops on the precision it
  reached (10 to 20 pairs), never on its result. Every protocol run is appended to a results ledger
  with a capture of the machine's state, [`docs/bench/CONDITIONS.md`](docs/bench/CONDITIONS.md)
  records the measurement environment, and a nightly trend analysis reports cumulative drift above
  2%. The fixed bar failed identical code in about four of five runs of a twelve-benchmark gate.
  Nothing a user of the library observes changes; see
  [the benchmark protocol](docs/DEVELOPMENT.md#the-benchmark-protocol) and the
  [tools](bundles/sirix-query/bench/common/README.md).
