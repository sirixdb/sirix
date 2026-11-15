# Shell presentation contract

The shell's Ember Night presentation is a terminal adapter. Queries, updates, transaction
lifetimes and result values are unchanged. No new commands are introduced.

1. JLine highlights keywords, function calls (including jn/sdb/xn/sirix), variables,
   quoted strings, numbers, nested XQuery comments and delimiters while editing.
   Incomplete strings/comments and Unicode remain editable. History, blank-line execution,
   Ctrl-D and multiline input retain their existing behaviour.
   Tests: `ShellHighlighterTest`, `MainTest`.
2. Terminal output is pretty JSON/XML, with blue keys/tags, green strings, orange numbers,
   and distinct green/orange/red insert/update/delete keys in diff results. Formatting is
   streamed through bounded reusable buffers after serialization; it never re-evaluates
   a query or materializes its result. Tests: `ShellResultWriterTest`, `MainTest`.
3. ANSI true colour uses orange #F47B20 and blue #42B6F0; 256-colour and 16-colour
   terminals get nearest palette alternatives. NO_COLOR (even empty), --no-color,
   redirected stdout and dumb terminals disable all ANSI. Redirected/no-colour results
   preserve the previous byte contract, including headers, whitespace and -p.
   Tests: `ShellPaletteTest`, `MainTest` subprocess golden outputs and explicit stream-encoding byte checks.
4. The prompt shows database/resource@revision for successful literal jn/xn doc/store
   calls, reading revision metadata only after result consumption. Explicit historical
   revisions are displayed; writes display the committed revision. Dynamic arguments or
   ambiguous multi-resource queries leave the prompt neutral, never invent context.
   Tests: `ShellPromptTest`, `MainTest` real store/update/time-travel sequence.
5. Terminal results include subdued elapsed time; errors include their query code in red,
   with stack traces only under -d. Pipes keep existing errors and no timing footer.
   Tests: `MainTest` and palette golden tests.

Verification includes adversarial chunk boundaries, unterminated input, supplementary
Unicode, huge strings/results, differential plain-byte comparisons and scoped PIT.
A standalone measurement reports presentation overhead and allocations; no wall-clock
threshold is added to CI. JLine owns cursor rendering/blinking and terminal portability;
the shell does not change a user's terminal background or cursor preferences.
