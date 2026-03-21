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

| Version | Supported |
|---------|-----------|
| 0.11.x  | Yes       |
| < 0.11  | No        |

## Security Considerations

SirixDB's append-only architecture provides inherent data integrity guarantees:

- **Immutable revisions**: Once committed, revision data is never overwritten
- **Merkle hash trees**: Optional per-node hashing for tamper detection
- **Keycloak integration**: The REST API supports OAuth2/OpenID Connect authentication via Keycloak

When deploying SirixDB in production:

- Use TLS for all REST API connections
- Configure Keycloak with strong authentication policies
- Restrict filesystem access to the database directory
- Review JVM flags required for operation (see README)
