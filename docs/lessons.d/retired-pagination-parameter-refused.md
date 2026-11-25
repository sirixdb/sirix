---
kind: row
---
| A request parameter was renamed in a commit about something else, and a parameter the handler does not know is ignored by design, so the old name silently became "no cursor". | Treat a parameter rename as a contract change: keep the old name with a deprecation, or refuse it with a 400 naming the replacement, and record it in `docs/API_COMPATIBILITY.md` in the same change. | [Retired pagination parameter](postmortems/2026-11-retired-pagination-parameter.md), `RetiredPaginationParameterTest` |
