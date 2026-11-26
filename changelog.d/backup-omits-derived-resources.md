---
section: Fixed
---
- **Backups no longer omit the hidden derived resources, so a restore keeps temporal-aggregate
  data.** The backup enumerated resources with the *public* resource listing, which deliberately
  hides each resource's `<name>$derived` sibling — the resource that holds the temporal-aggregate
  data — so a restored backup silently had none of it and the summary said nothing. A backup now
  copies each backed-up resource's derived sibling with it, holding the derived maintainer's history
  lock so both halves come from one state, and refuses with a clear message naming the resource when
  that maintenance does not become quiescent. A restore verifies the derived resources before any
  visible one (opening a visible resource would let the maintainer rebuild a derived store it cannot
  read) and reports them in `BackupSummary.derivedResources()`, which the CLI prints
  ([write-up](docs/postmortems/backup-omits-derived-resources.md)).
