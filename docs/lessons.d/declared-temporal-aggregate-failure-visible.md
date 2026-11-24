---
kind: row
---
| One method hosted both an optional activation and a caller's explicit request, so a `catch (RuntimeException)` meant for the optional path swallowed the request's failure. | Let the caller's intent decide whether a failure is absorbed, pass it in explicitly, and catch named exceptions rather than every `RuntimeException`. | [Declared temporal aggregate](postmortems/declared-temporal-aggregate-silently-dropped.md), `DeclaredTemporalAggregateFailureTest` |
