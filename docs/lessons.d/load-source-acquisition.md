---
kind: row
---
| A load opened each input URI as it wrote it, so a later unreachable source left the earlier inputs committed, and a single-source replace removed its target before opening the source at all. | Acquire and retain every input a call needs before changing any target, so a failure to acquire leaves nothing written. Test a reachable source followed by an unreachable one, and assert both that nothing was written and that the sources already acquired were released. | [Store-into-existing write-up](postmortems/store-into-existing-database.md), `LoadSourceAcquisitionTest` |
