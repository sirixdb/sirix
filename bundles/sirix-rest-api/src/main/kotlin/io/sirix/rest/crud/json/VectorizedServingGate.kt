package io.sirix.rest.crud.json

/**
 * Fail-closed gate deciding whether a resource-scoped JSON query may be compiled with a
 * session-bound vectorized executor (analytical projection/scan serving).
 *
 * Brackit's analytical detection captures only the source PATH of a FLWOR pipeline — not
 * which database/resource it dereferences — so an executor bound to the request's resource
 * would happily answer a same-shaped query over a DIFFERENT resource with the wrong data.
 * The REST layer therefore wires the executor only when every resource reference in the
 * query text provably targets the request's own resource:
 *
 *  - every `jn:doc(...)` call names exactly (databaseName, resourceName) via two string
 *    literals and nothing more (a third revision argument, dynamic arguments, or a
 *    different name fails the gate), and
 *  - no other document/collection-opening function appears at all (any identifier ending
 *    in `doc`, `collection`, `store` or `load` applied as a function fails the gate).
 *
 * When the request pins a NON-latest revision, `jn:doc` calls are refused entirely
 * ([requireContextItemOnly]): `jn:doc` opens the most recent revision while the executor
 * would be bound to the pinned one.
 *
 * Anything unprovable — malformed literals, unterminated comments, dynamic arguments —
 * refuses the wiring. The generic pipeline is always correct, so a refusal can only cost
 * performance, never correctness.
 */
internal object VectorizedServingGate {

    /** Word-boundary match for `import` (module imports) in code segments. */
    private val IMPORT_WORD = Regex("""\bimport\b""")

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
                val call = findNextOpeningCall(seg.text)
                if (call != null) {
                    if (requireContextItemOnly || call.name != "jn:doc"
                        || !seg.text.substring(call.callEnd).isBlank()
                    ) {
                        // Not the allowed function, or the arguments start with something
                        // other than a string literal directly after '(' — refuse.
                        return false
                    }
                    // Expect: Str(db) , Str(res) ) — literals separated only by a comma.
                    if (i + 3 >= segments.size) return false
                    val db = segments[i + 1] as? Segment.Str ?: return false
                    val sep = segments[i + 2] as? Segment.Code ?: return false
                    val res = segments[i + 3] as? Segment.Str ?: return false
                    if (db.value != databaseName || res.value != resourceName) return false
                    if (sep.text.trim() != ",") return false
                    // The next code segment must CLOSE the call immediately — a third
                    // argument (an explicit revision) targets a different snapshot.
                    if (i + 4 >= segments.size) return false
                    val close = segments[i + 4] as? Segment.Code ?: return false
                    if (!close.text.trimStart().startsWith(")")) return false
                    // Re-enter the closing segment: it may contain further calls.
                    i += 4
                    continue
                }
            }
            i++
        }
        return true
    }

    /** A resource-opening call found in a code segment. */
    private class OpeningCall(val name: String, val callEnd: Int)

    /**
     * Finds the first function application whose local name ends in one of the
     * resource-opening suffixes. Returns the (possibly prefixed) name and the offset just
     * past the '(' — [OpeningCall.callEnd] equals the segment length exactly when the
     * arguments continue in the following segments (i.e. the first argument is a literal).
     */
    private fun findNextOpeningCall(code: String): OpeningCall? {
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
                if (k < n && code[k] == '(' && isOpeningName(name)) {
                    return OpeningCall(name, k + 1)
                }
                // A name at the very end of the segment followed by a literal cannot be a
                // call over a literal (the '(' would be in this segment) — safe to skip.
                i = j.coerceAtLeast(i + 1)
            } else {
                i++
            }
        }
        return null
    }

    /** True when the identifier's local name ends in a resource-opening suffix. */
    private fun isOpeningName(name: String): Boolean {
        val local = name.substringAfterLast(':').lowercase()
        return local == "doc" || local.endsWith("-doc") ||
                local == "collection" || local == "store" || local == "load" ||
                local.startsWith("doc-") || local.startsWith("load-") || local.startsWith("store-")
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
