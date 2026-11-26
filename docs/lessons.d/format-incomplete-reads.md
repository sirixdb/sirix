---
kind: note
---
- **Exercise incomplete reads as well as incomplete files.** A compatibility check must either
  consume bytes or refuse; test EOF and zero progress at frame-length and payload boundaries,
  alongside a successful real-file control. Keep the oracle on the public refusal and persisted
  bytes, not on private loop iterations. Evidence:
  [format CI follow-up](postmortems/format-baseline-ci.md#ci-follow-up-rebased-mutation-attribution-and-native-builder-failure).
