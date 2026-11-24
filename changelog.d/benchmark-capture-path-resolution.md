---
section: Fixed
---
- Benchmark environment capture records unresolved data paths as unavailable on Python 3.13 and
  later, including symlink loops, instead of guessing their filesystem.
