package io.sirix.rest.crud.json

/**
 * Fail-closed gate deciding whether a resource-scoped JSON query may be compiled with a
 * session-bound vectorized executor (analytical projection/scan serving).
 *
 * Brackit's analytical detection captures only the source PATH of a FLWOR pipeline — not
 * which database/resource it dereferences — so an executor bound to the request's resource
 * would happily answer a same-shaped query over a DIFFERENT resource with the wrong data.
 * The REST layer therefore wires the executor only when the query text provably targets the
 * request's own resource and revision. The proof is an ALLOWLIST — a denylist of "opening"
 * functions cannot be future-proof (a newly added `jn:*` opener would silently bypass it):
 *
 *  - `jn:doc(...)` is permitted only when it names exactly (databaseName, resourceName)
 *    via two string literals and nothing more (a third revision argument, dynamic
 *    arguments, or a different name refuses the wiring);
 *  - every other function CALL must be an unprefixed name on the safe list (aggregates,
 *    string/number/sequence functions, node tests, keywords) or an `xs:*` constructor —
 *    any prefixed function (`jn:open`, `jn:all-times`, `sdb:*`, `local:*`, …) and any
 *    unknown unprefixed name refuses the wiring;
 *  - function REFERENCES (`name#arity`) always refuse — they can smuggle an opener past
 *    call-site checks (`let $f := jn:doc#2 return $f('other','r.jn')`);
 *  - module imports refuse outright (imported functions can open resources unseen).
 *
 * When the request pins a NON-latest revision, `jn:doc` calls are refused entirely
 * ([requireContextItemOnly]): `jn:doc` opens the most recent revision while the executor
 * would be bound to the pinned one.
 *
 * Anything unprovable — malformed literals, unterminated comments, dynamic arguments,
 * unknown functions — refuses the wiring. The generic pipeline is always correct, so a
 * refusal can only cost performance, never correctness.
 */
internal object VectorizedServingGate {

    /** Word-boundary match for `import` (module imports) in code segments. */
    private val IMPORT_WORD = Regex("""\bimport\b""")

    /**
     * Unprefixed names that may appear in call position without threatening the
     * single-resource proof: XQuery keywords/node tests plus side-effect-free `fn:`
     * builtins. Anything absent simply refuses the wiring (performance, not correctness),
     * so the list errs toward the names analytical queries actually use.
     */
    private val SAFE_UNPREFIXED_CALLS = setOf(
        // Keywords and node tests that can precede '('.
        "for", "let", "where", "order", "group", "by", "return", "in", "as", "at", "if",
        "then", "else", "and", "or", "to", "is", "eq", "ne", "lt", "le", "gt", "ge",
        "div", "idiv", "mod", "union", "intersect", "except", "instance", "of", "treat",
        "castable", "cast", "some", "every", "satisfies", "stable", "ascending",
        "descending", "greatest", "least", "collation", "switch", "case", "default",
        "typeswitch", "try", "catch", "node", "item", "text", "comment", "element",
        "attribute", "empty-sequence", "map", "array", "allowing",
        // Side-effect-free fn: builtins common in analytical queries.
        "count", "sum", "min", "max", "avg", "abs", "ceiling", "floor", "round",
        "round-half-to-even", "number", "string", "boolean", "data", "empty", "exists",
        "not", "true", "false", "concat", "string-join", "substring", "string-length",
        "normalize-space", "upper-case", "lower-case", "translate", "contains",
        "starts-with", "ends-with", "matches", "replace", "tokenize", "distinct-values",
        "head", "tail", "reverse", "subsequence", "insert-before", "remove", "index-of",
        "position", "last", "zero-or-one", "one-or-more", "exactly-one", "deep-equal"
    )

    /**
     * Returns `true` when [query] provably targets only ([databaseName], [resourceName]).
     * With [requireContextItemOnly] any `jn:doc` call refuses the wiring too — only pure
     * context-item queries pass (used when the request pins a non-latest revision).
     */
    fun targetsOnlyResource(
        query: String,
        databaseName: String,
        resourceName: String,
        requireContextItemOnly: Boolean = false
    ): Boolean {
        if (query.isEmpty() || databaseName.isEmpty() || resourceName.isEmpty()) {
            return false
        }
        val segments = lex(query) ?: return false
        // A module import can smuggle in functions that open other resources without any
        // in-text call — refuse the wiring outright.
        if (segments.any { it is Segment.Code && IMPORT_WORD.containsMatchIn(it.text) }) {
            return false
        }
        var i = 0
        while (i < segments.size) {
            val seg = segments[i]
            if (seg is Segment.Code) {
                when (scanCode(seg.text)) {
                    Scan.REFUSED -> return false
                    Scan.JN_DOC_CALL -> {
                        // The '(' closed the segment, so the first argument must be the
                        // string literal in the NEXT segment. Expect: Str(db) , Str(res) )
                        // with the literals separated only by a comma and the call closed
                        // immediately after the second literal (a third argument would be
                        // an explicit revision — a different snapshot).
                        if (requireContextItemOnly) return false
                        if (i + 4 >= segments.size) return false
                        val db = segments[i + 1] as? Segment.Str ?: return false
                        val sep = segments[i + 2] as? Segment.Code ?: return false
                        val res = segments[i + 3] as? Segment.Str ?: return false
                        if (db.value != databaseName || res.value != resourceName) return false
                        if (sep.text.trim() != ",") return false
                        val close = segments[i + 4] as? Segment.Code ?: return false
                        if (!close.text.trimStart().startsWith(")")) return false
                        // Re-enter the closing segment: it may contain further calls.
                        i += 4
                        continue
                    }
                    Scan.CLEAN -> {}
                }
            }
            i++
        }
        return true
    }

    private enum class Scan {
        /** No resource-threatening construct in this code segment. */
        CLEAN,

        /** A `jn:doc(` call whose arguments continue in the following segments. */
        JN_DOC_CALL,

        /** An unprovable construct — refuse the wiring. */
        REFUSED
    }

    /**
     * Scans one code segment. Every identifier in call position must be on the safe list
     * (or `xs:*`, or the specially-handled `jn:doc` at segment end); every function
     * reference (`name#arity`) refuses.
     */
    private fun scanCode(code: String): Scan {
        var i = 0
        val n = code.length
        while (i < n) {
            val c = code[i]
            if (c.isLetter() || c == '_') {
                var j = i
                while (j < n && (code[j].isLetterOrDigit() || code[j] == '_' || code[j] == '-' || code[j] == ':' || code[j] == '.')) {
                    j++
                }
                val name = code.substring(i, j)
                var k = j
                while (k < n && code[k].isWhitespace()) k++
                when {
                    k < n && code[k] == '#' ->
                        // Function reference — can smuggle an opener past call checks.
                        return Scan.REFUSED

                    k < n && code[k] == '(' -> {
                        if (name == "jn:doc") {
                            // Only the literal-argument shape is provable: the '(' must end
                            // this segment (string literals start a new segment). Inline
                            // content after '(' means a dynamic first argument — refuse.
                            return if (code.substring(k + 1).isBlank()) Scan.JN_DOC_CALL else Scan.REFUSED
                        }
                        if (!isSafeCallName(name)) {
                            return Scan.REFUSED
                        }
                        i = k + 1
                        continue
                    }
                }
                i = j.coerceAtLeast(i + 1)
            } else {
                i++
            }
        }
        return Scan.CLEAN
    }

    /** Safe in call position: unprefixed allowlisted names and `xs:*` constructors. */
    private fun isSafeCallName(name: String): Boolean {
        if (name.startsWith("xs:")) {
            return name.indexOf(':') == 2 && name.length > 3
        }
        return !name.contains(':') && !name.contains('.') && name in SAFE_UNPREFIXED_CALLS
    }

    private sealed interface Segment {
        class Code(val text: String) : Segment
        class Str(val value: String) : Segment
    }

    /**
     * Splits the query into code and string-literal segments, dropping (possibly nested)
     * XQuery comments. Returns `null` — refusing the wiring — on any malformed input
     * (unterminated literal or comment).
     */
    private fun lex(query: String): List<Segment>? {
        val segments = ArrayList<Segment>(16)
        val code = StringBuilder(query.length)
        var i = 0
        val n = query.length
        while (i < n) {
            val c = query[i]
            when {
                c == '(' && i + 1 < n && query[i + 1] == ':' -> {
                    var depth = 1
                    i += 2
                    while (i < n && depth > 0) {
                        if (query[i] == '(' && i + 1 < n && query[i + 1] == ':') {
                            depth++
                            i += 2
                        } else if (query[i] == ':' && i + 1 < n && query[i + 1] == ')') {
                            depth--
                            i += 2
                        } else {
                            i++
                        }
                    }
                    if (depth != 0) return null
                    code.append(' ')
                }

                c == '\'' || c == '"' -> {
                    if (code.isNotEmpty()) {
                        segments.add(Segment.Code(code.toString()))
                        code.setLength(0)
                    }
                    val quote = c
                    val value = StringBuilder()
                    i++
                    var terminated = false
                    while (i < n) {
                        val d = query[i]
                        if (d == quote) {
                            if (i + 1 < n && query[i + 1] == quote) {
                                value.append(quote)
                                i += 2
                            } else {
                                i++
                                terminated = true
                                break
                            }
                        } else {
                            value.append(d)
                            i++
                        }
                    }
                    if (!terminated) return null
                    segments.add(Segment.Str(value.toString()))
                }

                else -> {
                    code.append(c)
                    i++
                }
            }
        }
        if (code.isNotEmpty()) {
            segments.add(Segment.Code(code.toString()))
        }
        return segments
    }
}
