# REST API Compatibility

This guide describes the current SirixDB REST surface and its caveats. The
[REST specification](../spec/REST-api.md#rest-101--one-rest-schema-baseline)
owns the 1.0 release baseline and records unresolved schema contradictions;
the current schema is not a frozen 1.0 conformance claim. The machine-readable contract is
[`bundles/sirix-rest-api/src/main/resources/openapi.yaml`](../bundles/sirix-rest-api/src/main/resources/openapi.yaml),
served by every server at `GET /openapi.yaml` (unauthenticated, like
`/health` and `/metrics`).

## Schema surface

For the compatibility boundary, see
[REST-102](../spec/REST-api.md#rest-102--compatible-rest-evolution).
The schema covers:

* Database/resource CRUD (`PUT`/`GET`/`DELETE`/`HEAD` on `/{database}` and
  `/{database}/{resource}`, multipart bulk upload on `POST /{database}`),
  including `hashType`, `useDeweyIDs`, `commitMessage`, `commitTimestamp`.
* Query execution (`GET`/`POST /`, and resource-scoped queries) with
  `startResultSeqIndex` / `endResultSeqIndex` pagination; the JSON result
  wrapper `{"rest": […]}`.
* Revision selection (`revision`, `revision-timestamp`,
  `start-/end-revision`, `start-/end-revision-timestamp`) and the JSON
  serialization parameters (`nodeId`, `maxLevel`, `maxChildren`,
  `nextTopLevelNodes`, `startNodeKey`, `numberOfNodes`,
  `withMetaData=true|nodeKey|nodeKeyAndChildCount`, `prettyPrint`).
* The wire formats: the metadata-wrapped node
  (`{"key", "metadata": {nodeKey, hash?, type, descendantCount?, childCount?}, "value"}`),
  the multi-revision envelope
  (`{"sirix": [{"revisionNumber", "revisionTimestamp", "revision"}, …]}`,
  XML: `<sdb:sirix>`), the history entries
  (`{"history": [{revision, revisionTimestamp, author, commitMessage}]}`),
  the diff document
  (`{database, resource, "old-revision", "new-revision", diffs: [{insert|delete|update|replace: …}]}`),
  and the error envelope (`{"statusCode", "message"}`).
* `ETag`/`If-Match` optimistic concurrency on node-targeted updates/deletes,
  and content negotiation (`application/json` default, `application/xml` via
  `Accept`).
* HTTP metric names on `/metrics` (`http_requests_total`,
  `http_request_duration_seconds*`, `http_active_requests`).

## Current caveats and experimental features

The schema identifies experimental capabilities; REST-102 owns their compatibility
treatment. Other caveats below record implementation/schema conflicts rather than
additional compatibility exemptions.

* `GET /{database}/{resource}/pathSummary` — the whole endpoint.
* `plan` / `planStage` on queries, and especially the **plan JSON shape**,
  which serializes an internal compiler structure.
* `maxNodes`, `validFromPath`, `validToPath`, `useConventionalValidTime` on
  resource creation (bitemporal/import tuning).
* The `POST /{database}/{resource}` content-type dispatch: `application/json`
  /`application/xml` mean *update*, anything else means *query* — so a
  resource-scoped query cannot be sent as `application/json`. Odd, documented
  as implemented, likely to be revised.
* `POST /` (and `GET /`) with a missing/empty `query` falling back to the
  database listing — don't rely on it.
* Empty and updating query response shapes: see the `executeQuery` responses
  in the [OpenAPI contract](../bundles/sirix-rest-api/src/main/resources/openapi.yaml).
* Node-hash precondition behavior and its implementation gaps are recorded in
  [REST-204](../spec/REST-api.md#rest-204--node-hash-optimistic-preconditions).
* XML diff rejection and the conflicting schema description are recorded in
  [REST-208](../spec/REST-api.md#rest-208--json-only-diff).
* The `sirix_*` and `jvm_*` gauge set on `/metrics` — names follow Prometheus
  conventions but track internals and may evolve.
* `auth.mode=none` (local development) — an operational switch, not part of
  the wire contract.

## Deprecation policy

* Deprecations are announced in the release notes of the release that
  introduces them.
* A deprecated path/parameter/field keeps working for **at least one further
  minor release** after the announcement. Removal also follows
  [CONV-105](../spec/CONV-conventions.md#conv-105--compatibility-and-versioning),
  as required by REST-102, and is noted in release notes.
* Experimental items are exempt; they only get a release-notes mention.

## Recent wire-format changes (already in effect)

* **Concrete fused node-type names.** `withMetaData` now reports the stored
  node kind verbatim — object members fused with their value are
  `OBJECT_NAMED_OBJECT|ARRAY|STRING|NUMBER|BOOLEAN|NULL` (instead of the
  legacy `OBJECT_KEY` + separate value node), **consistently across the
  unbounded, limited (`maxLevel`/`maxChildren`), and paginated
  (`nextTopLevelNodes`) serializers** (the latter two historically collapsed
  the name to `OBJECT_KEY`).
* **Fused-member values are bare arrays.** The metadata-wrapped `value` of a
  named object/array member is a bare array of child entries — there is no
  inner `{metadata, value}` wrapper node anymore; clients re-wrap using the
  member's own `metadata.childCount`.
* **Multi-revision envelope.** Range reads (`start-revision`/`end-revision`
  and timestamp forms) wrap results in `{"sirix": […]}` (JSON) /
  `<sdb:sirix>` (XML); single-revision reads stay unwrapped.
* **XML responses are `application/xml`.** Selecting the XML pipeline via
  `Accept: application/xml` yields `<rest:item>`-wrapped output with
  `rest:id` node keys.
* **Client errors are 400s.** Invalid query expressions (`err:` codes) and
  malformed parameters now map to 400 with the engine/validation message
  (previously masked as generic 500s).
