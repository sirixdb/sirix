# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in SirixDB, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, please report vulnerabilities through one of these channels:

1. **GitHub Security Advisories**: Use [GitHub's private vulnerability reporting](https://github.com/sirixdb/sirix/security/advisories/new) to submit a report directly.
2. **Discord**: Contact a maintainer privately on [Discord](https://discord.gg/yC33wVpv7t).

### What to Include

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### Response Timeline

- **Acknowledgment**: Within 72 hours
- **Initial assessment**: Within 1 week
- **Fix or mitigation**: Depends on severity, but we aim for prompt resolution

## Supported Versions

| Version             | Supported |
|---------------------|-----------|
| 1.0.0-alpha (latest)| Yes       |
| < 1.0.0-alpha       | No        |

SirixDB is in its 1.0 alpha series; security fixes land on the latest alpha. Once 1.0.0
is released, this table will track the supported stable line.

## Security Considerations

SirixDB's append-only architecture gives it strong *structural* integrity properties. Note
carefully what they do and do not defend against:

- **Immutable revisions**: Once committed, revision data is never overwritten in place. This
  is a property of the storage design, not an access control — anyone who can write to the
  database files can still rewrite history.
- **Page checksums**: Every page fragment is hashed and the hash is stored in the referencing
  parent page, verified on read (`verifyChecksumsOnRead`, default `true`).
- **Optional per-node subtree hashes** (`HashType.ROLLING` / `POSTORDER`, exposed to queries
  as `sdb:hash`): fast structural change detection over subtrees, used by the diff engine.
- **Keycloak integration**: The REST API supports OAuth2/OpenID Connect authentication via Keycloak

### Hashing is not a security control

Every hash listed above is **XXH3-64**, a fast non-cryptographic hash. It is trivially
recomputable and collision-findable, so it detects accidental corruption and structural
change — **not deliberate tampering**. An adversary who modifies stored data can recompute
every affected hash. SirixDB further does not chain commits to one another, does not sign
them (commit authorship is unauthenticated metadata), and never publishes any state off the
machine, so there is no trusted reference against which history could be checked.

Making revision history cryptographically tamper-evident is a designed but **unimplemented**
feature; see [`docs/TAMPER_EVIDENCE_PLAN.md`](docs/TAMPER_EVIDENCE_PLAN.md) for the threat
model and phased plan. Until it ships, do not rely on SirixDB to detect modifications made
by an adversary with write access to the database files.

When deploying SirixDB in production:

- Use TLS for all REST API connections
- Configure Keycloak with strong authentication policies
- Restrict filesystem access to the database directory
- Review JVM flags required for operation (see README)
