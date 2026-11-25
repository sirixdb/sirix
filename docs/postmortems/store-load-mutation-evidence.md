# Bug: store/load mutation evidence omitted ownership and successful PUT behavior

- Fix / issue: PR #1476; PIT run 45500005454 and native smoke run 45500005540.
- Symptom: PIT finished both modules but reported seven undetected query mutants and
  43 undetected REST mutants. Native smoke failed before compiling any source.
- Trigger: the mutation lane selected acquisition/state-preservation tests and a refused
  XML PUT, without asserting source release, direct collection import authorization,
  successful streaming PUT, explicit resource options, suspended write results, or gate release after completion.
- Root cause: XML PUT resumed a paused request before attaching its upload pipe. A buffered
  body could drain before its consumer existed, leaving an empty temporary file and a
  premature-EOF parse failure. JSON already installs its parser before resuming. Removing
  XML's explicit resume delegates flow ownership to Vert.x pipeTo, which installs handlers
  before resuming. Separately, the evidence did not cover the complete REST-210 ownership contract. Acquired
  sources must be released on acquisition, parse and write exits, including when cleanup
  itself fails. The resource hold must span the write and cleanup and then be released.
  This applies to both JSON and XML; named and automatic query loads; REST creation,
  replacement and refusal; and both collection decorators' direct import entry points.
  The bulk flag in AbstractCreateHandler selected a branch that no route could reach:
  database-only routes returned before that branch, while resource routes passed false.
- Why tests missed it: HTTP refusal cannot establish successful response/content behavior;
  target state after an acquisition failure cannot establish ownership of already-open inputs.
  The decorators' CREATE checks were redundant for a free target but still required for an
  existing target when the caller holds MODIFY without CREATE.
- Regression tests: LoadSourceOwnershipTest checks release and primary/suppressed failure
  identity at the source-provider boundary, as well as parsed persisted content.
  PutResourceLifecycleTest exercises the real router, small and large JSON/XML PUTs,
  replacement, persisted Dewey-ID options, malformed input, and preservation of existing history on database routes.
  CollectionImportAuthorizationTest checks both free and existing targets without CREATE.
  XmlCreateUploadCleanupTest models immediate buffered delivery and cancellation at the IO
  boundary; it checks persisted content/history and upload release, not mock call order.
  CreateOrReplaceResourceAuthorizationTest checks suspended return/failure propagation,
  cleanup failure suppression and acquisition of the released hold by another thread.
  The oracle is REST-210 and the submitted document, not source text or private state.
- Correction: associate these tests with their production sources; remove the unreachable
  bulk branch and its constructor flag at every caller, the XML stream's unused clear,
  and the JSON input-cleanup debug call (cleanup remains best-effort and visits every input).
  Pass the supplied write block directly to runBlocking instead of allocating another coroutine
  adapter that only forwards its result; all callers use the same scoped block contract.
  Remaining compiler-generated survivors require individual reviewed dispositions, with
  a contract argument for each identity; uncovered mutants cannot receive one.
  Namespace error-code associations explicitly select their refusal contract; the assertions
  also check the published literal SIRIXDBF0006 independently of the production constant.
- External failure: native smoke's core/gradle.log records Maven Central HTTP 429 for
  plugin dependencies, before classes or a native image were built. No code change or
  retry mechanism is justified by that failure.
- Lesson: [ownership and executable mutation evidence](../lessons.d/store-load-mutation-evidence.md).

Vision check: this strengthens evidence for preserving committed history and releasing owned
resources. It adds no default or replacement API. The gate and bounded import policies remain
on the active creation paths. The deleted bulk-only helpers and parameters have no remaining
callers; XML's subtree writer remains used by its streaming PUT handler.

CI artifacts and local commands/results are retained under `build/ci-fix-evidence/`.

Buffered-input counterfactual: `buffered-upload-before-2.log` records two cases on the old
resume order: refusal passed and the authorized upload failed with premature EOF. After
removing the early resume, all 31 focused REST cases passed, including all three buffered
upload outcomes (success, refusal and cancellation). The public HTTP tests also verify
VER-201's out-of-range commit timestamp remains a 400 after streaming, with no partial target.

Local verification after the fixes: 31 focused offline REST cases passed, as did five source
ownership cases and 266 Python mutation-tooling tests. The required budget commands passed
327 query cases (five skipped), 363 core cases and three MCP cases. Bounds were unchanged.
The first broad local query PIT baseline failed in the unchanged DiffShellLifecycleTest
subprocess check; that baseline passed in the downloaded CI artifact. Its log and thread
snapshots are preserved. The final local mutation selection therefore focuses query analysis
on XML Load and JsonDBCollectionImpl and retains every changed REST source, including the
constructor callers. This is not a claim that the broad local query baseline passed.

Final mutation evidence: XML Load and JsonDBCollectionImpl produced 54 changed-line mutants,
all detected (53 killed, one timed out), with no uncovered mutants. Every changed REST source
completed: 60 mutants, 36 detected (35 killed, one timed out), no uncovered mutants, and 24
individually reviewed compiler-bridge equivalents. The repository's disposition checker
accepted all 24 exact identities with zero errors. Re-judging the complete preserved reports
through the same mutation reporter produced zero failing mutants in both modules. Raw results
before dispositions, final results, source hashes and commands are preserved in the evidence
folder; no remote CI run was triggered by this phase.
