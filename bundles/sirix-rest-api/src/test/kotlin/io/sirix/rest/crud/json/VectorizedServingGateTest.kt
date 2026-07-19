package io.sirix.rest.crud.json

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

/**
 * The serving gate decides whether a resource-scoped query may be compiled with a
 * session-bound vectorized executor. Its contract is FAIL-CLOSED: a `true` must prove the
 * query targets only the request's resource — every uncertain shape must return `false`
 * (the generic pipeline is always correct, so refusal only costs performance).
 */
@Tag("offline")
@DisplayName("VectorizedServingGate")
class VectorizedServingGateTest {

    private val db = "mydb"
    private val res = "sales.jn"

    private fun gate(query: String, contextItemOnly: Boolean = false) =
        VectorizedServingGate.targetsOnlyResource(query, db, res, contextItemOnly)

    // ---- Provably single-resource: wired ----

    @Test
    fun `pure context-item query passes`() {
        assertTrue(gate("sum(for \$r in .[] return \$r.age)"))
    }

    @Test
    fun `matching jn-doc passes with single quotes`() {
        assertTrue(gate("let \$doc := jn:doc('mydb','sales.jn') return sum(for \$r in \$doc[] return \$r.age)"))
    }

    @Test
    fun `matching jn-doc passes with double quotes and whitespace`() {
        assertTrue(gate("""let ${'$'}d := jn:doc ( "mydb" , "sales.jn" ) return count(${'$'}d[])"""))
    }

    @Test
    fun `several matching jn-doc calls pass`() {
        assertTrue(
            gate(
                """
                let ${'$'}a := jn:doc('mydb','sales.jn')
                let ${'$'}b := jn:doc('mydb','sales.jn')
                return {"s": sum(for ${'$'}r in ${'$'}a[] return ${'$'}r.age),
                        "c": count(for ${'$'}r in ${'$'}b[] return ${'$'}r.age)}
                """.trimIndent()
            )
        )
    }

    @Test
    fun `jn-doc inside a comment is ignored`() {
        assertTrue(gate("(: jn:doc('other','x.jn') :) sum(for \$r in .[] return \$r.age)"))
    }

    @Test
    fun `resource-opening name inside a string literal is data not code`() {
        assertTrue(gate("count(for \$r in .[] where \$r.note = 'jn:doc(' return \$r)"))
    }

    // ---- Anything else: refused ----

    @Test
    fun `different resource is refused`() {
        assertFalse(gate("sum(for \$r in jn:doc('mydb','other.jn')[] return \$r.age)"))
    }

    @Test
    fun `different database is refused`() {
        assertFalse(gate("sum(for \$r in jn:doc('otherdb','sales.jn')[] return \$r.age)"))
    }

    @Test
    fun `one matching and one foreign jn-doc is refused`() {
        assertFalse(
            gate("(jn:doc('mydb','sales.jn'), jn:doc('mydb','other.jn'))")
        )
    }

    @Test
    fun `third revision argument is refused`() {
        // An explicit revision targets a different snapshot than the executor's binding.
        assertFalse(gate("sum(for \$r in jn:doc('mydb','sales.jn', 3)[] return \$r.age)"))
    }

    @Test
    fun `dynamic jn-doc arguments are refused`() {
        assertFalse(gate("let \$n := 'sales.jn' return count(jn:doc('mydb', \$n)[])"))
        assertFalse(gate("count(jn:doc(\$db, \$res)[])"))
    }

    @Test
    fun `other opening functions are refused`() {
        assertFalse(gate("count(jn:collection('mydb')[])"))
        assertFalse(gate("jn:store('mydb','x.jn','[]')"))
        assertFalse(gate("jn:load('mydb','x.jn','/tmp/x.json')"))
        assertFalse(gate("count(fn:doc('x')[])"))
        assertFalse(gate("count(doc('x')[])"))
        assertFalse(gate("count(sdb:doc('mydb','x.jn')[])"))
        assertFalse(gate("count(xml:doc('mydb','x')[])"))
    }

    @Test
    fun `module imports are refused`() {
        assertFalse(
            gate("import module namespace m = 'http://example.com/m'; m:agg(.)")
        )
    }

    @Test
    fun `malformed input is refused`() {
        assertFalse(gate("(: unterminated comment"))
        assertFalse(gate("count(for \$r in .[] where \$r.a = 'unterminated return \$r)"))
        assertFalse(gate(""))
    }

    @Test
    fun `context-item-only mode refuses even a matching jn-doc`() {
        assertFalse(
            gate("sum(for \$r in jn:doc('mydb','sales.jn')[] return \$r.age)", contextItemOnly = true)
        )
        assertTrue(gate("sum(for \$r in .[] return \$r.age)", contextItemOnly = true))
    }
}
