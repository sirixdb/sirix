# SirixDB specification

Specification target: **SirixDB 1.0.0**. These chapters define intended behavior;
individual requirement status and evidence distinguish design from implementation.
A requirement marked `proposed` does not claim passing runtime conformance.

| Chapter | Contract | Status |
|---|---|---|
| [NATIVE](NATIVE-embedded-c-api.md) | JVM-free C/C++/Rust/Python embedding, stable ABI 1, JSON/XML parity | Design adopted for 1.0; runtime implementation pending |

Requirement IDs are stable and never reused. Uppercase MUST/MUST NOT are
normative; every requirement names verification suites. Tests are derived from
the contract before implementation. Follow [CONTRIBUTING](../CONTRIBUTING.md)
for mutation, adversarial and regression evidence. See [changes](CHANGELOG.md).
