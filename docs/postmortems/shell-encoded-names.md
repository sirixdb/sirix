# Bug: encoded literal names selected another resource's metadata

- Fix / issue: review finding R9 for the shell presentation change.
- Symptom: a prompt could report another database/resource spelling and revision even
  though the query read the intended decoded name.
- Trigger: store `a&b` at revision 1 and `a&amp;b` at revision 3. Executing
  `jn:doc('shop','a&amp;b')` reads the former but the old prompt displayed the latter at 3.
- Root cause: `ShellPrompt.unquote` removed quote delimiters and collapsed doubled
  delimiters, while the query parser also decoded entity and character references.
  The retained encoded spelling fed both name fields, both metadata branches and the label.
- Why tests missed it: plain, Unicode, doubled-quote and backslash cases were covered.
  No fixture held both spellings with distinct contents and revision histories.
- Regression tests: `executedEntityAndCharacterReferencesCannotSelectOtherRevisionMetadata`
  creates raw/decoded JSON and XML names through store APIs, with contents and revisions
  1 and 3. Real queries demonstrate named, decimal and hexadecimal reference decoding
  for database and resource fields, including historical reads. Encoded-name prompts
  stay neutral. JSON updates/stores and a plain-name store exercise the same boundary.
  `encodedNamesStayNeutralAcrossLiteralFieldsAndForms` covers doc/store names for both
  prefixes and quote forms through the public reference interface. The old helper
  reports the encoded counterpart at 3 for the decoded reads, failing the neutral oracle.
- Fix: decline any database/resource literal containing `&` at the existing shared
  unquoting boundary, before constructing a reference or looking up metadata. The
  existing conservative escape handling now covers entity/character references.
- Lesson: query-literal spelling is not necessarily a resource identity. See the
  shared context-inference entry in [Lessons](../LESSONS.md).

## Focused verification

The final JDK 25 Gradle run through `heavy.sh` completed successfully. Spotless formatting
was scoped to the two touched Java files. `MainTest` and `io.sirix.query.shell.*` passed
all 41 tests, excluding the unrelated temporal-reader dependency task. The real query
regression confirmed decoded and encoded database/resource selections with distinct
contents, revision-3 counterpart metadata, revision-2 updates and a revision-1 store.
Plain-name prompt progression and the existing output/allocation checks passed.

PIT targeted `ShellPrompt.unquote`, selecting `ShellPromptTest` and excluding unchanged
methods. The XML report records 11 mutants with nine `KILLED`, two `SURVIVED`, zero
`NO_COVERAGE` and zero timeouts. The local init script bounded heaps and removed
comma-containing JVM properties for PIT compatibility. Evidence remains in
`build/review-r9/pitest/mutations.xml` and `build/review-r9/verification.log`.

Individual surviving-mutant justifications follow.

- `ShellPrompt.unquote`, line 149, `ConditionalsBoundaryMutator`, changes the backslash
  check from `indexOf('\\') >= 0` to `> 0`. The helper's callers first require a STRING
  token, whose initial character is a single or double quote. A backslash therefore
  occurs at index 1 or later, or is absent with index -1. The differing index-0 case
  cannot reach this private helper. Both checks return the same value for every
  reachable input.
- `ShellPrompt.unquote`, line 150, `ConditionalsBoundaryMutator`, changes the ampersand
  check from `indexOf('&') >= 0` to `> 0`. The same STRING-token precondition guarantees
  that any ampersand follows the opening quote. Its index is at least 1 or is -1 when
  absent. The differing index-0 case is unreachable, making the mutant equivalent
  throughout the supported reference boundary.
