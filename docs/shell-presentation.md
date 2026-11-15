# Shell presentation contract

The shell's Ember Night presentation is a terminal adapter. Queries, updates, transaction
lifetimes and result values are unchanged. No new commands are introduced.

1. JLine highlights keywords, function calls (including jn/sdb/xn/sirix), variables,
   quoted strings, numbers, nested XQuery comments and delimiters while editing.
   Incomplete strings/comments and Unicode remain editable. History, blank-line execution,
   Ctrl-D and multiline input retain their existing behaviour.
   Expression keywords retain orange styling even immediately before an opening parenthesis.
   The callable built-ins `true`, `false`, `not`, `empty`, `count`, `last` and `replace`
   retain blue function styling when called; their bare keyword forms remain orange.
   Tests: `ShellHighlighterTest`, `MainTest`.
2. Terminal output is pretty JSON/XML, with blue keys/tags, green strings, orange numbers,
   and operation colours in supported diff results (detailed below). Formatting is
   streamed through bounded reusable buffers after serialization; it never re-evaluates
   a query or materializes its result. Tests: `ShellResultWriterTest`, `ShellOutputTest`.
3. ANSI true colour uses orange #F47B20 and blue #42B6F0; 256-colour and 16-colour
   terminals get nearest palette alternatives. NO_COLOR (even empty), --no-color,
   redirected stdout, dumb terminals and terminals reporting fewer than eight colours
   disable the shell's highlighting, automatic pretty printing and timing footer.
   Without a detected terminal console, presentation is also disabled. Plain results
   preserve the previous byte contract, including headers, whitespace and -p;
   interactive line editing remains under JLine's control.
   Tests: `ShellPaletteTest`, `MainTest` subprocess golden outputs and explicit stream-encoding byte checks.
4. The prompt starts as `sirix> `. After successful queries it shows
   `sirix database/resource@revision> ` for supported forms (detailed below), reading
   revision metadata only after result consumption. Unsupported forms reset it to neutral.
   Tests: `ShellPromptTest`, including the real store/update/time-travel sequence.
5. Terminal results include subdued elapsed time; query errors include their code in red,
   with stack traces only under -d. Pipes keep existing errors and no timing footer.
   Tests: `ShellOutputTest` and `MainTest`.

Verification includes adversarial chunk boundaries, unterminated input, supplementary
Unicode, huge strings/results, differential plain-byte comparisons and scoped PIT.
`ShellResultWriterTest` bounds renderer allocations; no wall-clock threshold is added to CI.
JLine owns cursor rendering/blinking and terminal portability; the shell does not change
a user's terminal background or cursor preferences.

Interactive input still runs on a blank line. Subsequent physical lines use an empty
prompt rather than implying that a complete query is syntactically incomplete. Terminal
results do not add the plain CLI's leading blank line. `MainTest` checks the input/prompt
contract and multiline/EOF/interrupt behavior; `ShellOutputTest` checks exact terminal
spacing while the legacy plain-byte oracle remains unchanged.

Revision inference is limited to complete standalone literal `jn:doc`/`xn:doc` calls with
two arguments or a positive integer revision, three-string-argument `jn:store`/`xn:store`
calls, and simple single-binding JSON replace-value, append and delete updates sourced
from a latest-revision `jn:doc` call. Explicit historical reads display their requested
revision; stores and supported updates display the latest committed revision. Nested calls
(including `sdb:commit`), conditional/iterated/deferred expressions, historical update sources and
compound expressions stay neutral. This applies equally to JSON and XML; the shell neither
re-executes queries nor treats the updating-expression flag as proof of executed access.
Regression oracle: actual query execution followed by the neutral prompt for uncertain
cases, and the existing 1-2-3-4-1 demo sequence for supported access.

XML attribute values use green quote styling within element tags. Quotes, braces, numbers
and operation words in XML text remain text. Comments, processing instructions and CDATA
retain their markup context through their complete terminator, including embedded `>`
characters. Subsequent tags and attributes recover their specified styles. Streaming writes,
flushes and finish/reuse preserve these rules and every payload character with fixed scratch
storage. `ShellResultWriterTest` checks styled output for each split position, every writer
overload, mixed JSON/XML/diff output and XML allocation behavior.

JSON diff keys use green for `insert`, orange for `update` and `replace`, and red for
`delete`. These keys stay blue in ordinary JSON output. XML diff patch words use green
for `insert` and `append`, orange for `replace` and `rename`, and red for `delete`.
`ShellResultWriterTest` checks the exact styles and `ShellOutputTest` executes a structural
replacement followed by `jn:diff` through the terminal output entry point.

Diff operation colours apply when the complete significant source is a direct literal
`jn:diff` or `xn:diff` call. Both support four arguments. JSON's registered five- and
six-argument forms also qualify. Database/resource arguments must be string literals,
and revision/subtree arguments must be numeric literals. Whitespace and comments may
surround or separate these tokens. Skipped, deferred, nested, dynamic and mixed-result
expressions retain ordinary result styling. `ShellOutputTest` executes the reported
skipped branch, both stores' direct diffs, mixed results and deferred calls, checking
actual output styles and serialized payloads.

Prompt inference declines database/resource literals containing `&`, including named
entities and decimal/hexadecimal character references, in either quote form. These source
spellings can differ from the names selected by the query parser. The shared literal-name
boundary rejects them before metadata lookup for JSON/XML doc/store and supported JSON
updates. Literals containing backslashes are also declined. Ordinary names, Unicode and
doubled quote delimiters keep their existing behavior.
`ShellPromptTest` executes entity/character-reference reads against distinct raw/decoded
names at revisions 1 and 3, checks neutral prompts for both stores and fields, and covers
JSON stores/updates plus plain-name controls.
