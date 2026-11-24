---
section: Fixed
---
- **A declared temporal aggregate that cannot be created now fails instead of being dropped.**
  Declaring a temporal aggregate installs a hidden derived resource; a failure to install it was
  absorbed together with the optional open-time activation, so `createIndexes` reported success
  while the index did not exist and the resource silently served the exact fallback for the rest of
  its session. The declaration path now propagates the failure, which also marks the transaction
  rollback-only, and the open path keeps its fallback for the named failures that mean the stored
  derived resource is unusable
  ([write-up](docs/postmortems/declared-temporal-aggregate-silently-dropped.md)).
