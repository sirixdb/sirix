# REST — REST baseline

Spec target: **1.0.0**. Chapter status: **Draft for adoption before the approximately
9 December 2026 freeze**. Code baseline: `13f65148b117b9c8e1858a7f1510d2f65501bad9` (17 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance.
Existing test links are evidence inventories, not assertions that tests passed in this task.

## Scope

P0 REST semantics beyond the OpenAPI schema: compatibility, auth, resource/revision selection, preconditions, revert, history, diff and error mapping. OpenAPI remains the machine schema; this chapter lists known conflicts rather than duplicating its schemas.

## Requirements

### REST-101 — One REST schema baseline

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [SirixVerticle.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/SirixVerticle.kt):`start`
- **Verified by:** No existing test mapped. **Planned:** RestOpenApiConformance: parse bundled schema, serve /openapi.yaml, compare bytes, validate positive/negative live responses for every operation.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** The baseline schema still labels itself 1.0.0-alpha22 and contains the XML-diff contradiction. This spec-only change does not edit it or claim the release artifact is already frozen.

The release MUST publish its bundled openapi.yaml as the normative route, parameter and response-schema baseline. The final 1.0 schema MUST be identified by an immutable release revision and checked against live responses. Known contradictions listed here MUST be reconciled before claiming freeze conformance; prose MUST NOT silently declare a conflicting schema correct.

### REST-102 — Compatible REST evolution

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [SirixVerticle.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/SirixVerticle.kt)
- **Verified by:** No existing test mapped. **Planned:** RestSchemaCompatibilityConformance: compare final 1.0 schema with candidate and replay retained request/response fixtures.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** Compatibility check and frozen schema corpus are planned. Existing API_COMPATIBILITY guide is a lead; current handlers/schema are the baseline evidence.

After freeze, endpoints, required inputs, response types and documented status semantics MUST remain compatible within 1.x. Additive optional fields MAY be introduced; clients MUST tolerate unknown response fields. Removal/renaming or tightening accepted input MUST be reviewed as a breaking change under CONV-105. Experimental capabilities MUST be explicitly labelled in the schema.

### REST-201 — Authentication and mutation authorization

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [Auth.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/Auth.kt):`checkIfAuthorized`; [PermissionCheckingQuery.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/PermissionCheckingQuery.kt); [SessionMutationAuthorizer.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/SessionMutationAuthorizer.kt); [RevertHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/RevertHandler.kt):`handle`
- **Verified by:** [AuthNegativeTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/AuthNegativeTest.kt); [QueryMutationAuthorizationTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/QueryMutationAuthorizationTest.kt); [StoreMutationBoundaryTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/StoreMutationBoundaryTest.kt); [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt):`viewRoleCannotRevert`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Deployment authentication mode remains configurable; NoneAuth is not a production identity provider. This requirement does not claim auth is enabled in every configuration.

Protected requests MUST require authenticated authorization for the target database. VIEW MUST NOT permit node updates, create/drop, commit, index mutation or revert through either REST CRUD or an updating query. Authorization MUST be checked at mutation boundaries, including indirect collection/store/session operations, before effects. Explicitly public endpoints follow the schema security declarations.

### REST-202 — Path confinement

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [PathValidation.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/PathValidation.kt); [RevertHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/RevertHandler.kt):`handle`
- **Verified by:** [PathValidationTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/PathValidationTest.kt) **Planned:** RestPathConformance: encoded traversal, separators, symlink aliases and malformed names over actual HTTP routes, assert no out-of-root side effects.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** PathValidation rejects blank/long names, dot-dot, separators and NUL. It does not itself resolve symlinks; confinement through real filesystem aliases needs implementation evidence and a live-route regression.

Database/resource path parameters MUST be validated before filesystem access and MUST NOT permit traversal, absolute paths or aliases that escape the configured storage root. Rejection MUST leave database/resource state unchanged.

### REST-203 — Serialization revision selection

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractGetHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractGetHandler.kt); [HandlerPreconditions.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/HandlerPreconditions.kt):`moveToOrNotFound`
- **Verified by:** [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestRevisionSelectionConformance: latest, exact, timestamp, microsecond-separated timestamps under VER-204, range, conflicting/incomplete groups, absent node and pruned target.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Current parameter precedence and missing-node IllegalStateException mapping need route-level coverage; the intended failure requirement is not claimed fully implemented.

GET without a query MUST serialize the requested committed revision/subtree, defaulting to the most recent revision. Numeric/timestamp single selection and start/end ranges MUST follow the schema; multiple revisions MUST use the documented JSON/XML revision envelope. Missing node/revision requests MUST fail rather than serialize another cursor position. Conflicting selection groups MUST NOT be silently interpreted as an arbitrary revision.

### REST-204 — Conditional-write token preconditions

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [HandlerPreconditions.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/HandlerPreconditions.kt):`checkNodeHashPrecondition`; [AbstractUpdateHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractUpdateHandler.kt):`checkHashCode`; [AbstractDeleteHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractDeleteHandler.kt); [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java)
- **Verified by:** [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestConditionalTokenPreconditionConformance: absent, malformed, separator-free, quoted, unquoted, weak, list-valued and `*` If-Match; a mismatched epoch after truncate and after replacement; a token older and newer than the head; an interleaved commit that does and does not touch the target subtree; a concurrent reorder of the target's children; each insert mode against a changed and an unchanged insertion point; a deleted target; a resource reverted after the token; `requirePreconditions` on and off; every case repeated with hashing enabled and disabled and asserted identical; assert status, headers and unchanged head.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** The implemented mechanism is still the node hash and is unavailable under the 1.0 default `HashType.NONE` (CONF-104), so this requirement describes a decided replacement, not current behavior. The hash is retired as a precondition in every hash mode, not only where hashing is off: the TXN-311 rationale records that the rolling hash is order-insensitive, so it cannot detect a concurrent reorder or same-depth move. Today a missing header throws IllegalStateException mapped to 500, a stale or malformed header throws IllegalArgumentException mapped to 400, document-root updates skip the check entirely, and `checkHashCode` is the only call site. The superseded hash wording is recorded in [RETIRED](RETIRED.md).

The node hash is retired as a precondition for 1.0: update and delete MUST evaluate `If-Match` as the opaque token of REST-205 through the single conditional check of JAPI-111/TXN-311, with identical semantics on resources with and without hashing. The token MUST have the form `<epoch>.<revision>` with decimal components, as a strong entity tag, and MUST NOT carry a node hash. A token with no `.` separator is the retired hash form and MUST return 400. Servers MUST accept the token quoted or bare, MUST accept a comma-separated list and succeed when any member succeeds, MUST treat `*` as satisfied exactly when the addressed target exists, and MUST fail a weak (`W/`) tag rather than match it. A token whose epoch differs from the resource's current epoch MUST return 412, which is how a truncate or a whole-resource replacement invalidates tokens from the history it ended. The legacy request `ETag` header MUST remain accepted with identical meaning. The target kind MUST follow the request: a node-targeted update with an insertion mode uses the insertion-point check anchored at that node on the requested side, an `insert=replace` update and a node-targeted delete use the subtree check for that node, and a request with no single node target follows REST-212. The check MUST run after authorization (REST-201) and writer admission, and before any mutation. `If-Match` MUST remain optional unless the resource persists `requirePreconditions` (CONF-109), in which case a mutating request carrying no precondition MUST be answered 428 Precondition Required (RFC 6585). A supplied token that does not match the grammar — no separator, a non-decimal or negative epoch or revision, or trailing content — MUST return 400. A failed precondition MUST return 412 (RFC 9110), including when the addressed node no longer exists at the head revision; an unconditional request for an absent node keeps its 404. Nothing MUST be committed on any of those failures. This MUST NOT be represented as database-wide compare-and-swap: the token's meaning is exactly the TXN-311 predicate for the addressed target.

### REST-205 — Conditional-write token output

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractHeadHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractHeadHandler.kt); [AbstractGetHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractGetHandler.kt); [AbstractUpdateHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractUpdateHandler.kt):`handleResponse`
- **Verified by:** [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestConditionalTokenETagConformance: JSON and XML HEAD and GET, subtree and whole resource, latest, numeric and timestamp selection, a multi-revision envelope, both hash modes, a resource whose epoch was advanced and a mutating response; assert the emitted token names the epoch and the revision actually served, is byte-identical in both hash modes, and round-trips through If-Match.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** The emitted value is still the node hash with no epoch or revision, and HEAD emits an empty ETag under `HashType.NONE`, which this requirement retires; the superseded wording is recorded in [RETIRED](RETIRED.md). The hash-mode branch in `AbstractHeadHandler` and the `handleResponse` branch in `AbstractUpdateHandler` are the two implementation seams.

HEAD and GET MUST return, in `ETag`, the token of the revision actually served — `<epoch>.<revision>` as a strong entity tag, in the grammar of REST-204 — and HEAD MUST return no serialized body. The token MUST NOT depend on the resource's hash configuration: the same request against two resources differing only in `hashType` MUST produce the same form of entity tag. The token MUST reflect the epoch and revision a timestamp or default selection resolved to rather than the request's literal parameters. A response serializing more than one revision MUST NOT carry an `ETag`, because no single revision identifies it. A successful mutating response MUST carry the new head's token, so a client can chain conditional writes without an extra read. A node hash MUST NOT be emitted as an entity tag; `sdb:hash` remains a query-level feature of hash-enabled resources and is not a concurrency token.

### REST-206 — Whole-resource revert

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [RevertHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/RevertHandler.kt):`handle`; [RevertHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/RevertHandler.kt):`positiveRevision`
- **Verified by:** [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Current handler implements the append semantics and has concurrency, pruned-target and authorization cases. No exactly-once retry token is promised after an ambiguous transport failure.

POST /{database}/{resource}/revert MUST accept only targetRevision, optional expectedHead and optional message in its JSON object body. targetRevision/expectedHead MUST be positive 32-bit integers; query parameters and subtree selectors MUST be rejected. Writer admission MUST precede expectedHead comparison. Success MUST return 201 with revision and previousHead and append even when target equals head. A stale expectedHead MUST return 409, pruned target 410, absent database/resource 404 and invalid body/target 400, all without appending.

### REST-207 — History response

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [HistoryHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/HistoryHandler.kt):`handle`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`getHistory`
- **Verified by:** [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestHistoryConformance: newest-first, escaped metadata, limit/range boundary, missing paired bound, conflicting groups and pruned history.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Current handler prefers revisions when both forms are supplied; mutual-exclusion enforcement is an implementation gap. The schema is the intended input contract.

History MUST return newest-first revision metadata in a history array with revision, revisionTimestamp, author and commitMessage. Limit and explicit range parameters MUST obey the documented input contract; malformed values MUST be client errors. Supplying conflicting limit/range selectors MUST be rejected before reading an arbitrary subset.

### REST-208 — JSON-only diff

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [DiffHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/DiffHandler.kt):`handle`; [JsonDiffSerializer.java](../bundles/sirix-core/src/main/java/io/sirix/diff/JsonDiffSerializer.java)
- **Verified by:** [BasicJsonDiffTest.java](../bundles/sirix-core/src/test/java/io/sirix/service/json/BasicJsonDiffTest.java); [JsonDiffSerializerArrayPositionTest.java](../bundles/sirix-core/src/test/java/io/sirix/diff/JsonDiffSerializerArrayPositionTest.java); [JsonDiffSerializerStaleTupleRegressionTest.java](../bundles/sirix-core/src/test/java/io/sirix/diff/JsonDiffSerializerStaleTupleRegressionTest.java) **Planned:** RestDiffConformance: XML rejection, invalid revision order, sidecar present/absent, type-change replacement and include-data parity.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Handler rejects XML; current OpenAPI description still says literal null. Schema correction plus a live XML regression is required before freezing REST-101.

The REST diff route MUST operate on JSON revisions with 1 <= first-revision < second-revision, honoring subtree/depth/include-data selectors. XML diff requests MUST return 400, not the literal JSON null/string null. Type-changing JSON replacement semantics MUST agree with JAPI-301; sidecar and computed diff MUST represent the same change.

### REST-209 — Error status and safe response

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [RequestFailureHandling.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/RequestFailureHandling.kt):`coroutineHandler`; [RequestFailureHandling.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/RequestFailureHandling.kt):`routerFailureHandler`; [RequestFailureHandling.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/RequestFailureHandling.kt):`endWithErrorResponse`
- **Verified by:** [RouteErrorMappingTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RouteErrorMappingTest.kt) **Planned:** RestErrorSchemaConformance: embedded quotes, backslashes, control characters and Unicode in messages; validate emitted JSON against Error schema.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Current escaping only handles backslash/quote; full JSON error serialization conformance for control characters remains unverified. This chapter does not silently bless every IllegalStateException as a client error.

Unhandled request failures MUST terminate with a JSON error response containing statusCode and message rather than hang. IllegalArgumentException and query evaluation errors MUST map to 400; explicit HttpException status (including nested wrapped causes) MUST be preserved. Unknown server failures MUST map to 500 with a generic client message, without stack traces. Fatal VM errors MAY be rethrown after best-effort request failure.

### REST-210 — Resource creation and query import boundaries

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractCreateHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractCreateHandler.kt); [PermissionCheckingQuery.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/PermissionCheckingQuery.kt); [SessionMutationAuthorizer.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/SessionMutationAuthorizer.kt)
- **Verified by:** [ContentImportAuthorizationTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/ContentImportAuthorizationTest.kt); [StoreMutationBoundaryTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/StoreMutationBoundaryTest.kt); [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestCreateImportConformance: existing resource history, unauthorized replace, malformed body, content resembling a path/URL and a token from the replaced resource failing afterwards; assert no unintended reads/writes.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Existing implementation/tests mapped; full OpenAPI response conformance remains planned. The epoch allocation on replacement does not exist at this baseline; it is part of the FMT-209 implementation.

PUT resource creation MUST follow the schema’s replace behavior: replacing an existing resource creates a new resource history, rather than appending an update to old history. Because the new history restarts revision numbering, replacement MUST allocate a new resource history epoch (FMT-209), so every conditional-write token issued against the replaced resource fails under REST-204 instead of being satisfied by an unrelated revision of the same number. REST import/load content MUST be treated as supplied content and MUST NOT read arbitrary server paths/URLs from query input. Creation and import MUST check target authorization before side effects.

### REST-211 — Conditional-write failure response

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [RequestFailureHandling.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/RequestFailureHandling.kt):`coroutineHandler`; [RequestFailureHandling.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/RequestFailureHandling.kt):`endWithErrorResponse`; [AbstractUpdateHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractUpdateHandler.kt):`checkHashCode`
- **Verified by:** No existing test mapped. **Planned:** RestPreconditionFailureResponseConformance: a 412 and a 428 for every mutating route and target kind, including an epoch mismatch; assert the Error body, the current token in ETag, the unchanged head and history after cold reopen, and that a retry with the returned token succeeds.
- **Evidence class:** doc-only (unverified); decided target has unverified implementation gaps
- **Gap/note:** Neither status is produced today: a stale hash maps to 400 and a missing header to 500 through the generic failure handler, and no current-revision header accompanies either.

A 412 or 428 response to a mutating request MUST carry the resource's current token — its epoch and head revision — as an `ETag` response header, together with the REST-209 error object naming the failed precondition, so a client can re-read exactly that revision and retry. A 412 caused by a mismatched epoch MUST say so in the message, because the client's next read starts a new token history rather than continuing its own. It MUST NOT append a revision, change resource metadata or leave a partially applied mutation, and MUST be produced before any content is written. A 412 MUST NOT be reported as 400: a client distinguishes a malformed request it must fix from a stale token it must refresh.

### REST-212 — Precondition scope for root and query mutations

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [AbstractUpdateHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractUpdateHandler.kt):`checkHashCode`; [PermissionCheckingQuery.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/PermissionCheckingQuery.kt); [SessionMutationAuthorizer.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/SessionMutationAuthorizer.kt)
- **Verified by:** No existing test mapped. **Planned:** RestPreconditionScopeConformance: a document-root update with a current and a stale token, in both hash modes with identical verdicts; a resource-scoped updating query with both; an updating query through `POST /` with and without `requirePreconditions`; assert the resource-level verdict, the refusal of an unconditional write to a resource that requires preconditions, and unchanged heads.
- **Evidence class:** doc-only (unverified); decided target has unverified implementation gaps
- **Gap/note:** This replaces the previously open document-root and query scope item. Today the document-root branch of `checkHashCode` skips the check and the query routes carry no precondition field at all, so both are last-writer-wins.

A mutating request whose target is the whole document root, or that has no single node target, MUST use the resource-level check of TXN-311: the precondition fails exactly when the head revision is greater than the token's revision, on resources with and without hashing. A resource-scoped updating query, whose resource the request path names, MUST apply that same check when `If-Match` is supplied. The database-wide query route has no path-addressed resource, so a precondition MUST NOT be silently bound to one there: supplying `If-Match` on it MUST return 400, and an updating query reaching a resource that persists `requirePreconditions` through that route MUST be refused with 428 at the mutation boundary rather than committed unconditionally.

## Machine contract and route coverage (REST-101)

Schema: [openapi.yaml](../bundles/sirix-rest-api/src/main/resources/openapi.yaml).
The route inventory below is a baseline index, not an additional independent schema.
Every operation needs a successful response and each documented failure status in the planned
RestOpenApiConformance suite, including response content type and security declaration.

| Method | Path | operationId |
|---|---|---|
| GET | `/` | `listDatabasesOrQuery` |
| POST | `/` | `executeQuery` |
| DELETE | `/` | `deleteAllDatabases` |
| POST | `/token` | `token` |
| POST | `/logout` | `logout` |
| GET | `/user/authorize` | `authorize` |
| GET | `/health` | `health` |
| GET | `/metrics` | `metrics` |
| GET | `/openapi.yaml` | `openapi` |
| GET | `/{database}` | `listResources` |
| PUT | `/{database}` | `createDatabase` |
| POST | `/{database}` | `createMultipleResources` |
| DELETE | `/{database}` | `deleteDatabase` |
| HEAD | `/{database}/{resource}` | `headResource` |
| GET | `/{database}/{resource}` | `getResource` |
| PUT | `/{database}/{resource}` | `createResource` |
| POST | `/{database}/{resource}` | `updateOrQueryResource` |
| DELETE | `/{database}/{resource}` | `deleteResource` |
| POST | `/{database}/{resource}/revert` | `revertResource` |
| GET | `/{database}/{resource}/history` | `getHistory` |
| GET | `/{database}/{resource}/diff` | `getDiff` |
| GET | `/{database}/{resource}/pathSummary` | `getPathSummary` |

The table MUST match the route inventory in `openapi.yaml`.

## Conditional-write token rationale (informative)

[RFC 9110 §13.1.1](https://www.rfc-editor.org/rfc/rfc9110#section-13.1.1) defines `If-Match` as a
strong-comparison precondition answered with 412 when it fails, and
[RFC 6585 §3](https://www.rfc-editor.org/rfc/rfc6585#section-3) defines 428 for an origin server
that requires a request to be conditional. The entity tag here is a version token for the resource
rather than a digest of the returned bytes, which the RFC permits: it changes whenever the resource
changes. Sirix deliberately evaluates it more permissively than tag equality — a write is accepted
when the token's revision differs from the head but nothing the write depends on changed since.
That is the same relaxation as etcd's `mod_revision` comparison, where a transaction guards only the
keys it names against a revision, and it is why Kubernetes' stricter `resourceVersion` equality, which
rejects any concurrent change to the whole object, is not copied. Elasticsearch's `if_seq_no` is the
per-document form of the same idea. The epoch is the Kubernetes `uid` half of the `uid` plus
`resourceVersion` pair: a version number only means something together with the identity of the
incarnation that issued it, since a deleted and recreated object restarts versioning, which is
exactly what truncate and whole-resource replacement do to a Sirix resource. The insert rule
follows `patch` context lines: an insertion is still valid while the lines around the insertion
point are unchanged, regardless of edits elsewhere in the file. The token deliberately carries no
content digest: TXN-311's rationale records that Sirix's rolling hash is an order-insensitive sum,
so it cannot stand in for one.

## Required schema corrections for the conditional-write token (REST-101, REST-204, REST-205)

No schema file is edited by this specification change. Before REST-101 freeze conformance can be
claimed, `openapi.yaml` MUST be corrected so that the machine contract states the decided
mechanism rather than the retired hash form:

| Schema location | Required correction |
|---|---|
| `components.headers.ETag` | the opaque token `<epoch>.<revision>` as a strong entity tag; present for every single-revision response and identical in form regardless of `hashType`; absent for multi-revision envelopes |
| `If-Match` parameter on update and delete | the token from a previous `ETag`; optional unless the resource sets `requirePreconditions`; quoted, bare, list-valued and `*` forms accepted; a separator-free legacy hash is rejected |
| update and delete responses | add `412` for a failed precondition, including a mismatched epoch, and `428` for a missing required one, both carrying the current token in `ETag` |
| `components.parameters.hashType` | remove the claim that `ROLLING` enables the `ETag`/`If-Match` mechanism: the mechanism is independent of hashing, which now affects `sdb:hash` only |
| info description, `BadRequest` | move stale preconditions from 400 to 412; 400 keeps malformed tokens |
| HEAD description | remove the empty-`ETag`-for-`NONE` behavior |

## Deferred endpoint conformance work

- `If-Match` handling is the decided revision-token mechanism of REST-204/205 and is unimplemented:
  the handlers still compare node hashes, a missing header returns 500, a stale one 400, and no
  route emits 412, 428 or a current-revision header. Retiring the hash form, the `requirePreconditions`
  setting and the schema corrections below belong to that implementation.
- XML diff schema says null; implementation/intended behavior is 400.
- Mixed revision selectors and history selectors need explicit rejection coverage.
- Pruned-read HTTP mapping outside revert remains a deferred endpoint conformance detail; this
  draft does not expand its guarantees. Document-root and query precondition scope is no longer
  open: REST-212 decides it. Timestamp precision must conform to VER-201/204; strict generation is decided.
- A response-validation filter, final schema version and immutable 1.0 baseline are planned.
  No handler, schema or CI implementation is changed in this documentation task.

## Traceability

| Requirement | Existing or planned verification | Status |
|---|---|---|
| REST-101 | No existing test mapped. **Planned:** RestOpenApiConformance: parse bundled schema, serve /openapi.yaml, compare bytes, validate positive/negative live responses for every operation. | proposed |
| REST-102 | No existing test mapped. **Planned:** RestSchemaCompatibilityConformance: compare final 1.0 schema with candidate and replay retained request/response fixtures. | proposed |
| REST-201 | [AuthNegativeTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/AuthNegativeTest.kt); [QueryMutationAuthorizationTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/QueryMutationAuthorizationTest.kt); [StoreMutationBoundaryTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/StoreMutationBoundaryTest.kt); [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt):`viewRoleCannotRevert` | partial |
| REST-202 | [PathValidationTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/PathValidationTest.kt) **Planned:** RestPathConformance: encoded traversal, separators, symlink aliases and malformed names over actual HTTP routes, assert no out-of-root side effects. | designed |
| REST-203 | [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestRevisionSelectionConformance: latest, exact, timestamp, microsecond-separated timestamps under VER-204, range, conflicting/incomplete groups, absent node and pruned target. | partial |
| REST-204 | [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestConditionalTokenPreconditionConformance: header forms, mismatched epochs, stale and future tokens, touching and non-touching interleaved commits, concurrent reorder, every insert mode, deleted target, reverted resource and `requirePreconditions`, each with hashing enabled and disabled; assert status, headers and unchanged head. | designed |
| REST-205 | [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestConditionalTokenETagConformance: HEAD and GET for JSON and XML, subtree and whole resource, every revision selection, multi-revision envelopes, both hash modes, an advanced epoch and mutating responses; assert the emitted epoch and served revision, hash-mode independence and the If-Match round trip. | designed |
| REST-211 | No existing test mapped. **Planned:** RestPreconditionFailureResponseConformance: 412 and 428 on every mutating route and target kind including an epoch mismatch; Error body, current token in ETag, unchanged head after cold reopen and a successful retry. | proposed |
| REST-212 | No existing test mapped. **Planned:** RestPreconditionScopeConformance: document-root and resource-scoped query mutations with current and stale tokens, the database-wide query route with and without `requirePreconditions`, and unchanged heads. | proposed |
| REST-206 | [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt) | partial |
| REST-207 | [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestHistoryConformance: newest-first, escaped metadata, limit/range boundary, missing paired bound, conflicting groups and pruned history. | partial |
| REST-208 | [BasicJsonDiffTest.java](../bundles/sirix-core/src/test/java/io/sirix/service/json/BasicJsonDiffTest.java); [JsonDiffSerializerArrayPositionTest.java](../bundles/sirix-core/src/test/java/io/sirix/diff/JsonDiffSerializerArrayPositionTest.java); [JsonDiffSerializerStaleTupleRegressionTest.java](../bundles/sirix-core/src/test/java/io/sirix/diff/JsonDiffSerializerStaleTupleRegressionTest.java) **Planned:** RestDiffConformance: XML rejection, invalid revision order, sidecar present/absent, type-change replacement and include-data parity. | partial |
| REST-209 | [RouteErrorMappingTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RouteErrorMappingTest.kt) **Planned:** RestErrorSchemaConformance: embedded quotes, backslashes, control characters and Unicode in messages; validate emitted JSON against Error schema. | partial |
| REST-210 | [ContentImportAuthorizationTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/ContentImportAuthorizationTest.kt); [StoreMutationBoundaryTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/StoreMutationBoundaryTest.kt); [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestCreateImportConformance: existing resource history, unauthorized replace, malformed body and content resembling a path/URL; assert no unintended reads/writes. | partial |
