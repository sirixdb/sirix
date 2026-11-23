---
section: Fixed
---
- **A packed join side could be bucketed twice** — the dense-extrema join's side cache decided that
  neither a resident side nor a build in flight existed from two lookups made in sequence, while a
  background packing publishes its side and only then leaves the in-flight map. A repeated query at
  an immutable revision whose lookups straddled that publication re-ran the side's count-and-pack
  pass and left a duplicate packed side in the derived allowance until it was evicted; answers were
  unaffected. The lookup now re-reads the published sides once the in-flight map reports nothing
  pending, so the work the cache saves no longer depends on the packing's completion order
  ([write-up](docs/postmortems/2026-11-bucketed-side-reuse-interleaving.md)).
