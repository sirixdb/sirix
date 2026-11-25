---
kind: row
---
| A tooling CLI test used the repository checkout as its root and passed only while a reviewed data file in that checkout stayed empty. | Point every CLI test at a temporary root holding only its own inputs; check that adding the first real entry to a repository data file leaves the tooling suite green. | [Tooling follow-up](postmortems/rest-read-node-id-validation.md#ci-follow-up-the-mutation-tooling-test-read-the-live-dispositions-file), `test_run_cli_skips_empty_selection_and_module_entrypoint_exits_zero` |
