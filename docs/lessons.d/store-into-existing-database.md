---
kind: row
---
| A store function given a resource name used its `$create-new` flag to drop and recreate the whole database, destroying every other resource; tests stored into fresh locations or asserted the replacement itself. | Make a flag act at the scope the call names, and check existing state before choosing a destructive path. Prefer refusing a taken name over replacing it in place: a replacement that is not atomic can lose history when the write that follows fails. Test writes into populated containers and assert the untouched neighbours' content, history and indexes. | [Store-into-existing write-up](postmortems/store-into-existing-database.md), `function.jn.io.StoreIntoExistingDatabaseTest`, `function.xml.io.StoreIntoExistingDatabaseTest` |
