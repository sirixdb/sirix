---
section: Security
---
- **Constrain fragment tooling CLI inputs.** Collection and checks use the repository containing
  the script; the unused `--repo-root` override is removed. Checks require a full lowercase
  commit hash so the base cannot be interpreted as a Git option.
