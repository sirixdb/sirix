---
kind: note
---
- **Pair recovery and refusal across discovery states.** A missing beacon and a valid revision-zero
  beacon must preserve the same distinction between an incomplete first-commit tail and a readable
  foreign version. Put the version in the incomplete frame itself, not just an earlier complete
  frame, and cover absent or undecodable headers in each byte-pipeline representation. Remove both
  directory and tail-log records, then assert a durable new commit for recovery and unchanged
  whole-resource bytes for refusal, on both persistent backends. Evidence:
  [bootstrap-tail regression](postmortems/store-feature-recovery.md#incomplete-bootstrap-tail-with-revision-zero-beacons).
