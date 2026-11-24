---
kind: row
---
| Independent query fixtures relied on store/load replacing old data; REST PIT also selected tests needing a missing Keycloak service. | Own fixture lifecycles explicitly, reuse fixtures for additional reads, and remove a database explicitly when testing recreation. Mutation baselines must carry their lane's service prerequisites; REST uses the existing `offline` tag. | [CI fixture write-up](postmortems/store-refusal-ci-fixtures.md) |
