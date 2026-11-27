---
section: Added
---
- **Resources carry an incarnation id and a last-revert revision (FMT-209).** Every resource's
  settings now hold `incarnationId`, 16 lowercase hexadecimal digits of a cryptographically secure
  64-bit draw. A new id is drawn when a resource is created or recreated, replaced through `PUT`,
  restored from a backup, and on `truncateTo`, which publishes it durably, on every platform,
  before removing any data.
  Settings without a valid `incarnationId` are refused, never given a default. A commit that
  publishes a revert records its revision number in the revision root, every later revision
  carries it forward, and the store then sets the conditional-write feature bit, so an engine that
  does not maintain the field can read the store but not write it. These are the identity fields the
  planned conditional-write tokens (REST-204, TXN-311) build on.
