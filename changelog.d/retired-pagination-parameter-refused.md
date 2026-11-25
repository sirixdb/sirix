---
section: Fixed
---
- **A paginated JSON read that still sends `lastTopLevelNodeKey` now fails instead of returning the
  first page forever.** The parameter was renamed to `startNodeKey` without an alias and the server
  ignored the old name, so a stale client never advanced past page one and got no error. The server
  now answers `400` naming `startNodeKey`, also when both names are sent; the Python, TypeScript and
  Kotlin clients send `startNodeKey`, and the removal is recorded in `docs/API_COMPATIBILITY.md`
  ([write-up](docs/postmortems/2026-11-retired-pagination-parameter.md)).
