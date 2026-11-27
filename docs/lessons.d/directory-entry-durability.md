---
kind: row
---
| Every crash argument covered file contents: a forced catalogue, settings file or backup copy was called durable while its name in the directory was not, so a power loss could lose a committed catalogue or bring back a removed resource. | State what a durability claim covers. A content force makes bytes durable, not the name that reaches them: route every create, rename or delete a later open relies on through `DurableFiles`, and give crash oracles a separate durability unit per directory. Existence is not proof of a completed barrier: verify successful retries after failed publication or removal, including directory ancestors. | [Directory-entry durability](postmortems/2026-11-directory-entry-durability.md), `DirectoryEntryDurabilityTest`, `DurableFilesTest` |
