---
kind: row
---
| An issue named its fix in a pull request that never merged, while a different merged one had already removed the cause; the eviction run that exposed it also exited 0 with the failure only in its log. | Re-prove a reported defect on the current default branch before changing code: instrument the suspect read, count how many of its pages the cache still owns, and score the run by grepping its log for the reported message instead of by its exit code. | [Structural validator half](postmortems/2026-11-hot-writer-unheld-leaf-reads.md#follow-up-the-structural-validator-half-1458), `HOTStructuralValidatorLeafRetentionTest` |
