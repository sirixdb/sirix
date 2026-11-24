---
kind: row
---
| Fragment CLI inputs selected write/delete destinations and reached Git option parsing. | Remove unneeded destination overrides; accept only full commit hashes at revision boundaries and test rejected arguments for unchanged files. | [Fragment CLI write-up](postmortems/changelog-fragment-cli.md), `Collect.test_collect_rejects_repository_root_override_without_changing_files`, `GuardOverGit.test_check_rejects_non_hash_bases_without_creating_git_output` |
