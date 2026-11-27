---
kind: row
---
| Incarnation publication adopted the installed id only after a directory barrier returned, so a barrier failure left live and persisted identities different; Windows journaling was mistaken for durability. | Adopt installed state at the rename boundary, propagate later durability failures before destructive work, and document platform guarantees from actual barriers. | [Incarnation publication write-up](postmortems/incarnation-publication-failure.md), `ResourceConfigurationTest.fmt209FailureAfterRenameKeepsTheInstalledId` |
