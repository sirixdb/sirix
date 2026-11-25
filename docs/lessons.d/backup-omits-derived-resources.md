---
kind: row
---
| A copier enumerated the files to copy with the API that hides internal resources from callers, so a whole hidden resource stayed out of every backup. | Never derive "what is on disk" from a listing whose contract is "what callers may see"; copy from the directory, and let a visibility filter only decide what to *report*. | [Backups omit derived resources](postmortems/backup-omits-derived-resources.md), `BackupDerivedResourceTest`, `BackupRestoreTemporalAggregateTest` |
