---
section: Fixed
---
- **A NAME index over names whose discriminative bytes straddle byte 255 no longer misroutes.** A
  routing node now splits over levels when no layout of its own can name every discriminative key
  byte, instead of reducing a byte position to the byte it is stored in; names are bounded to the
  routable key length of 4096 bytes, and a longer name is stored under the longest prefix that fits
  with its candidates re-checked exactly against each node's own name. A name inside the bound is
  stored, read and answered exactly as before and pays nothing for the re-check
  ([#1391](https://github.com/sirixdb/sirix/issues/1391),
  [write-up](docs/postmortems/2026-11-hot-name-key-routing-range.md)).
