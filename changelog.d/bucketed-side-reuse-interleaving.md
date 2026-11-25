---
section: Fixed
---
- **A packed join side could be bucketed twice** — repeated dense-extrema joins at an immutable
  revision now reuse a side whose background packing completes during the cache lookup, avoiding
  duplicate bucketing and retained storage. Query answers are unchanged
  ([write-up](docs/postmortems/2026-11-bucketed-side-reuse-interleaving.md)).
