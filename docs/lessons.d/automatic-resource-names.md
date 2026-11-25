---
kind: row
---
| Automatic resource names came from the resource count and were decided before creation, so a gap in the names, or a concurrent creator, made a "new" item write into an existing resource. | Claim a generated name by the atomic operation that fails when it is taken (here `createResource`), and act only on what you claimed. Never treat a count as "free". Test non-contiguous names and a deterministic concurrent claimant. | [Automatic names write-up](postmortems/automatic-resource-names.md), `JsonDBCollectionAutomaticNamesTest`, `XmlDBCollectionAutomaticNamesTest` |
