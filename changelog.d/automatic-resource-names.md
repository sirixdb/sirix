---
section: Fixed
---
- **Automatic resource names are always new** — adding unnamed items to a collection (`jn:store`,
  `jn:load`, `xn:store`, `xn:load` and the collection API) could pick a `resourceN` name that
  already existed and write into that resource. Each item now claims the next free name by creating
  it, so it never reaches an existing resource, also against a concurrent caller; CREATE suffices
  ([write-up](docs/postmortems/automatic-resource-names.md), #1478).
