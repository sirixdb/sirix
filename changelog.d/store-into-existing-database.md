---
section: Fixed
---
- **Storing into an existing database no longer drops it** (breaking) — `jn:store`, `jn:load`,
  `xn:store` and `xn:load` used to delete and recreate the whole database whenever `$create-new`
  was true (the default), destroying every other resource, its history and its indexes. A free
  resource name is now added beside the others, and every other resource keeps its content,
  revisions and indexes. A name that is already taken is refused with `SIRIXDBF0006` and nothing is
  written, whatever `$create-new` says: these functions never replace a resource in place.
  Loading several URIs acquires every source before writing any, so a failing later source leaves
  the database unchanged. **Migration:** to replace a whole database, call
  `jn:drop-database`/`xn:drop-database` first; to write into an existing resource, use
  `jn:import`/`xn:import`; to put different content under a name that is taken, drop that resource
  first ([write-up](docs/postmortems/store-into-existing-database.md), #1384).
