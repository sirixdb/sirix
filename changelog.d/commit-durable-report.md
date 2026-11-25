---
section: Fixed
---
- **A commit whose revision is already durable is no longer reported as failed when cleanup fails.**
  Synchronous and pipelined commits preserve known success and make the affected writer terminal.
  See the [commit outcome contract](bundles/sirix-core/src/main/java/io/sirix/api/NodeTrx.java)
  for lifecycle restrictions and uncertain beacon-write outcomes, and the
  [write-up](docs/postmortems/commit-durable-reported-as-nothing-committed.md) for root cause and evidence.
