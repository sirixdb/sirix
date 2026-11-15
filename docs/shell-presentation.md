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

Interactive input still runs on a blank line. Subsequent physical lines use an empty
prompt rather than implying that a complete query is syntactically incomplete. Terminal
results do not add the plain CLI's leading blank line. `MainTest` checks the input/prompt
contract and multiline/EOF/interrupt behavior; `ShellOutputTest` checks exact terminal
spacing while the legacy plain-byte oracle remains unchanged.

Revision inference is limited to complete standalone literal doc/store calls and simple
single-binding JSON updates of the demo resource at its latest revision. Nested calls (including
`sdb:commit`), conditional/iterated/deferred expressions, historical update sources and
compound expressions stay neutral. This applies equally to JSON and XML; the shell neither
re-executes queries nor treats the updating-expression flag as proof of executed access.
Regression oracle: actual query execution followed by the neutral prompt for uncertain
cases, and the existing 1-2-3-4-1 demo sequence for supported access.

Expression keywords retain orange styling even immediately before an opening parenthesis.
The callable built-ins `true`, `false`, `not` and `empty` retain blue function styling when
called, while their bare keyword forms remain orange. Regression oracle: the palette's
specified orange/blue styles on conditional, FLWOR, built-in and qualified calls, including
incomplete input and multiline prefixes.
