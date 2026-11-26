---
kind: row
---
| Format discovery treated one tombstone marker as sufficient while the canonical decoder required both. | Decode persisted records at the shared boundary before deciding whether they can skip envelope validation; test single-marker live identities alongside complete tombstones in both directory and salvage representations. | FMT-105 / FMT-401; [tombstone discovery](postmortems/format-tombstone-discovery.md); `FormatRepairBoundaryTest`. |
