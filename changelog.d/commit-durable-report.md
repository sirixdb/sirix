---
section: Fixed
---
- **A commit whose revision is already durable is no longer reported as "nothing is durable yet".**
  The commit point is the return of the uber-page beacon write; the steps after it — forgetting the
  spill file, closing durable read pages, releasing the page guard, clearing the transaction-intent
  log, deleting the recovery marker — only tidy up a revision that already exists. A failure in one
  of them used to leave `hardenCommit` the same way a pre-durability failure does, so `commit()`
  threw it and the transaction went back to running with its modification count intact: a caller
  that retried wrote the same change into a second revision, and a caller that reported the failure
  upstream reported a revision that exists. Those steps now run independently of each other, the
  first failure is logged and flags the resource for repair, and the commit returns normally with
  the transaction terminal — further use is rejected, it cannot be retried or rolled back into a
  usable state, and closing it is clean. A failure *before* the commit point is unchanged: it
  throws, consumes no revision number and keeps the transaction's uncommitted work
  ([write-up](docs/postmortems/commit-durable-reported-as-nothing-committed.md)).
