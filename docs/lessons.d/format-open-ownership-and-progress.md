---
kind: note
---
- **Check failed opens through retry and removal.** Fallible completion must release only
  ownership acquired by the failed attempt, and cached sessions must repeat outstanding
  compatibility checks. Test both the first refusal and subsequent operations on the same
  handle. A zero-filled read buffer proves nothing until the read made progress; include
  stalled file-header reads alongside stalled envelope reads.
  Evidence: [format open ownership and progress](postmortems/format-failed-open-and-header-progress.md).
