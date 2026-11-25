# Bug: REST node-read fixes failed the Sonar maintainability gate

- Fix / issue: PR #1474; Actions run `45525854124`, Sonar analysis for PR 1474.
- Symptom: both Sonar checks failed new-code maintainability (B, required A), while
  coverage passed at 97.8%, duplication at 0%, and reliability/security passed.
- Trigger: analyze the node-read validation, record-subtree and empty-document fixes.
- Root cause: request orchestration nested context binding, cursor validation and
  pagination within resource-lifetime blocks. Sonar also reported existing large
  per-node emission methods touched by the fixes, ambiguous exception assertions,
  intentional empty fixture branches, and an unused close-only resource variable.
- Why tests missed it: runtime tests establish REST-203 results and ownership, but
  cannot establish the static analyzer's maintainability rules.
- Regression checks: the actual Sonar analyzer is the before/after oracle for these
  static findings; existing serializer and HTTP tests independently establish bytes,
  statuses, ETags and cleanup. Do not substitute source-string assertions.
- Lesson: isolate cold request stages while keeping resource lifetimes visible;
  preserve per-node loop shape with a method-scoped, explained complexity suppression
  as required by the repository's hot-path convention. See [Lessons](../LESSONS.md)
  and [the lesson fragment](../lessons.d/rest-node-read-ci-maintainability.md).

## Invariants and complete scope

No public behavior changes. REST-203's node selection, revision, hash and pagination
rules remain the oracle. Every JSON/XML serializer must preserve its output and cursor
ownership in plain, query, limited and record forms, for empty/populated revisions,
initial/continuation pages, metadata modes and byte/character output. Exception tests
must isolate the operation that throws, for negative/absent record roots and standalone
XML attributes/namespaces. Mode-zero test fixtures intentionally retain builder defaults.

Keep the JSON and XML node switches, both record loops and XML query-item loop intact
under individual S3776 suppressions. Merge only the equivalent document-root/empty-child
condition in record serialization. Remove the forwarding `queryResource` method rather
than invent a parameter carrier. Context binding and continuation-cursor validation
become small private stages; option application uses the same nullable form in both
JSON builders. Query pagination separates window traversal from evaluation/ownership,
using the existing `use` scopes for both iterator and serializer.

## Caller and failure-path trace

- `handle` still parses a malformed node ID before opening storage. `get` opens the
  collection in the same resource/session scope and resolves revisions and windows in
  the same order before `xquery`. The removed forwarding method has no remaining caller.
- Both JSON/XML resource queries select the node, compute its hash, bind context, evaluate
  and serialize, then set ETag. Unknown keys throw before context binding/evaluation;
  readers close on both paths. Root-level queries still take the unbound branch.
- Both JSON builder branches validate a continuation cursor in every selected revision
  before applying metadata, depth, children, pretty-print and node-count options, in the
  original order. The helper closes each revision reader on success and rejection.
- Paginated JSON/XML query callers still validate windows before acquiring ownership.
  Null results close the serializer, single items are included only at index zero,
  sequence windows stop at exhaustion, bounded windows include their end, and exceptions
  close the iterator before the serializer with the same suppressed-exception order.
- Record document-root unwrapping still runs only for root key zero; an empty root emits
  `null` and returns. Named/unnamed container and primitive paths are unchanged.
- Exception-test setup now precedes the assertion; serializer execution still supplies
  the observed refusal. Closing an unused query serializer still checks its final envelope.
  No alias, parameter or branch becomes unreachable beyond the removed forwarding method.

## External checks

The native smoke artifact's `normal/core/gradle.log` shows Maven Central HTTP 429 for
Gradle plugins and Kotlin BOMs during configuration, before any project compilation.
This is external dependency throttling; no native source or retry mechanism is changed.

The separate `sirixdb/sirix-python-client` checkout indexes `resp.headers["etag"]` in
its synchronous/asynchronous request paths for resources created with hashing disabled.
Nine integration cases fail there. REST-203 and the accepted HEAD decision explicitly
require no ETag for `HashType.NONE`; restoring a synthetic header would reverse that
decision. The external client must handle that absence or explicitly enable hashes for
hash-dependent scenarios. This worktree does not own that repository's source.

## Vision check

Preserves exact historical results, node identities, hash defaults and resource ownership.
No format, work-budget, public default, fallback, retry or quality-gate threshold changes.
Verification evidence is retained under the ignored `build/ci-round2/` directory.

## Verification progress

The same Sonar Java 8.46.0.59609 analyzer reproduced the reported Java findings on
archived pre-fix sources. Its rules plus the real suppression filter no longer report
those production/test sites. Legacy issues outside this change and main-only complexity
rules applied indiscriminately to test files by the local harness are not CI findings.
The Kotlin 3.14.0.11245 complexity consumer reports `xquery` 26 -> 13,
`getSerializedBody` 39 -> 14 and `serializePaginated` 48 -> 10; extracted helpers are
5, 5 and 9. The eleven-parameter forwarding method was removed.

The core serializer/budget run passed 373 main-task cases, and MCP budgets passed
3 cases, with no failures or skips; isolated allocation/startup/epoch tasks also passed.
A local overlapping Gradle invocation rewrote the shared test JAR while query test
compilation read it (ZIP end header missing). Stop that overlapping run and repeat
query/REST verification sequentially; this is a local verification error, not a source fix.

An isolated record-serializer mutation selection originally omitted the existing subtree
and empty-document suites that the full-core selection included. Associate both suites
with `JsonRecordSerializer` so those REST-203 contracts follow isolated changes as well.

Sequential query/REST verification passed: 350 query cases (5 pre-existing opt-in
large-data skips) and 100 REST cases, with no failures. Query allocation, lifetime and
translator forks also passed. Java formatting checks and `git diff --check` passed.

The first REST mutation pass exposed incidental changes to existing pretty-print branches
and a redundant collection-close scope. Restore the original pretty-print statements in
both builder branches (final response-builder complexity 14), so this repair does not
expand their diff. Remove the collection `use`: the JSON collection only deregisters and
closes the database, the XML collection only closes it, `xquery` closes its query stores,
and the outer database `use` owns the same handle on every success/failure path. Retain
session and reader scopes; their cleanup is independently observed before parent close.
Re-trace: failures before context construction unwind session/database scopes; failures
after construction unwind query context/readers before those same outer scopes.

## Final verification

The final REST rerun passed all 100 selected cases after removing the duplicate close
scope. The completed main-task results total 826 cases, zero failures/errors and five
pre-existing skips; isolated allocation/startup/lifetime/translator forks also passed.
The final Kotlin analyzer still reports all repaired methods and helpers at most 14.

Complete changed-line PIT: core 38 killed / 38; REST 79 killed, 2 timeout-detected and
14 individually reviewed compiler-assertion equivalents / 95. No mutant is uncovered
or left unaccepted. The final blocking REST helper exited zero, and
`pr_mutation.py dispositions --check` passed against the complete core/REST reports and
censuses. Five newly exposed compiler assertions have separate producer-contract
arguments; the moved query-reader assertion has its exact instruction identity updated.
No cleanup or formatting behavior is waived. Query's annotation-only change adds no
executable mutation; its earlier complete 11/11 report remains evidence for unchanged logic.

Remote Sonar analysis remains for the outer executor; this CI repair did not upload an
analysis, rerun workflows or push. Native smoke remains an external Maven Central 429,
and Python integration remains the external client's hash-disabled ETag incompatibility.

## CI follow-up: malformed lesson fragment

- Symptom / trigger: Actions run `45534678302` rejected the lesson note because its
  first line was a heading instead of the fragment delimiter.
- Root cause / invariant: every changelog and lesson entry must carry its directory's
  front matter before its body. The analyzer repair added a prose lesson without the
  required `kind: note` block, so the shared loader rejected it before validation or
  release collection could finish.
- Why tests missed it: the repair ran runtime, analyzer and mutation checks but omitted
  the fragment consumer after adding the lesson. Existing fragment tests already reject
  missing front matter and verify note collection; no new parser behavior is needed.
- Scope / trace: add the documented front matter at the data boundary. Both `check`
  and `collect` use the same loader for all changelog and lesson fragments. The note
  body still reaches the lesson collector verbatim; no loader, renderer or caller changes.
- Regression / lesson: the real `check --base-sha` command reproduced exit 2 before
  the fix. After repair, `python3 .github/scripts/changelog_fragments.py check
  --base-sha 7c9603baa5131f7aa51dd637aa9e537fa23c226e` passed, as did all 63 tests
  from `python3 -m unittest discover -s .github/scripts/tests -p
  test_changelog_fragments.py`. Run the fragment consumer after documentation
  additions; see [Lessons](../LESSONS.md).
- The same run's nine Python failures still originate in the separate client's
  mandatory HEAD-header lookup for hash-disabled resources, as described above.
  Preserving REST-203 requires fixing that external client rather than adding an ETag.
  Local `:sirix-rest-api:offlineTest --tests
  'io.sirix.rest.crud.ReadNodeIdValidationTest.headBehavesLikeGet'` passed all four
  JSON/XML and NONE/ROLLING cases. The initial Docker-backed test invocation was
  interrupted; its Keycloak container was stopped before the offline run.
- Vision check: documentation becomes collectable without changing REST behavior,
  persisted data, public defaults or quality-gate thresholds.
