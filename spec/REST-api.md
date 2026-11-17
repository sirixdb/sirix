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

### REST-204 — Node-hash optimistic preconditions

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [HandlerPreconditions.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/HandlerPreconditions.kt):`checkNodeHashPrecondition`; [AbstractUpdateHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractUpdateHandler.kt):`checkHashCode`; [AbstractDeleteHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractDeleteHandler.kt)
- **Verified by:** [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestHashPreconditionConformance: absent/malformed/stale/quoted/unquoted/legacy headers, conflicting headers, HashType.NONE and concurrent writer; assert status and unchanged head.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Missing header currently throws IllegalStateException, which maps to 500; stale/malformed throws IllegalArgumentException -> 400. Document-root updates bypass the hash check; scope for document-root/query mutations remains an explicit open contract detail.

Where hash preconditions apply, update/delete MUST accept If-Match and the legacy request ETag form, stripping surrounding quotes and comparing the numeric node hash under writer ownership. Malformed, missing or stale required values MUST return 400 without committing. For HashType.NONE no hash precondition is available. This MUST NOT be represented as full HTTP entity-tag grammar or database-wide compare-and-swap.

### REST-205 — ETag output

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractHeadHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractHeadHandler.kt); [AbstractGetHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/AbstractGetHandler.kt)
- **Verified by:** [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestETagConformance: JSON/XML HEAD, subtree, historical single revision, multiple revisions and NONE hash mode; compare with Java node hash.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Existing implementation/tests mapped; full OpenAPI response conformance remains planned.

HEAD MUST return the selected node’s hash in ETag when hashing is available, with no serialized body. GET ETag behavior MUST match the schema’s selected single-revision cases; disabled hashing MUST NOT invent a concurrency token.

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
- **Verified by:** [ContentImportAuthorizationTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/ContentImportAuthorizationTest.kt); [StoreMutationBoundaryTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/StoreMutationBoundaryTest.kt); [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestCreateImportConformance: existing resource history, unauthorized replace, malformed body and content resembling a path/URL; assert no unintended reads/writes.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Existing implementation/tests mapped; full OpenAPI response conformance remains planned.

PUT resource creation MUST follow the schema’s replace behavior: replacing an existing resource creates a new resource history, rather than appending an update to old history. REST import/load content MUST be treated as supplied content and MUST NOT read arbitrary server paths/URLs from query input. Creation and import MUST check target authorization before side effects.

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


## Deferred endpoint conformance work

- Missing required If-Match currently returns 500, contrary to the intended 400.
- XML diff schema says null; implementation/intended behavior is 400.
- Mixed revision selectors and history selectors need explicit rejection coverage.
- Document-root/query optimistic-concurrency scope, pruned-read HTTP mapping outside revert,
  remain deferred endpoint conformance details; this draft does not expand their guarantees. Timestamp precision must conform to VER-201/204; strict generation is decided.
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
| REST-204 | [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestHashPreconditionConformance: absent/malformed/stale/quoted/unquoted/legacy headers, conflicting headers, HashType.NONE and concurrent writer; assert status and unchanged head. | designed |
| REST-205 | [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestETagConformance: JSON/XML HEAD, subtree, historical single revision, multiple revisions and NONE hash mode; compare with Java node hash. | partial |
| REST-206 | [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt) | partial |
| REST-207 | [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestHistoryConformance: newest-first, escaped metadata, limit/range boundary, missing paired bound, conflicting groups and pruned history. | partial |
| REST-208 | [BasicJsonDiffTest.java](../bundles/sirix-core/src/test/java/io/sirix/service/json/BasicJsonDiffTest.java); [JsonDiffSerializerArrayPositionTest.java](../bundles/sirix-core/src/test/java/io/sirix/diff/JsonDiffSerializerArrayPositionTest.java); [JsonDiffSerializerStaleTupleRegressionTest.java](../bundles/sirix-core/src/test/java/io/sirix/diff/JsonDiffSerializerStaleTupleRegressionTest.java) **Planned:** RestDiffConformance: XML rejection, invalid revision order, sidecar present/absent, type-change replacement and include-data parity. | partial |
| REST-209 | [RouteErrorMappingTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RouteErrorMappingTest.kt) **Planned:** RestErrorSchemaConformance: embedded quotes, backslashes, control characters and Unicode in messages; validate emitted JSON against Error schema. | partial |
| REST-210 | [ContentImportAuthorizationTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/ContentImportAuthorizationTest.kt); [StoreMutationBoundaryTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/crud/StoreMutationBoundaryTest.kt); [SirixVerticleJsonTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleJsonTest.kt); [SirixVerticleXmlTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/SirixVerticleXmlTest.kt) **Planned:** RestCreateImportConformance: existing resource history, unauthorized replace, malformed body and content resembling a path/URL; assert no unintended reads/writes. | partial |
