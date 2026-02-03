package io.sirix.rest

import io.brackit.query.Query
import io.brackit.query.util.io.IOUtils
import io.brackit.query.util.serialize.StringSerializer
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

/**
 * Smoke tests for GraalVM native image compilation of sirix-rest-api.
 * These tests verify that the query engine works correctly when compiled to native image.
 *
 * <p>Tests use store-free query evaluation (no database I/O) to avoid requiring the
 * FFM-based LinuxMemorySegmentAllocator which is not yet supported in native images.
 *
 * <p>Note: These tests do NOT require Keycloak or external services.
 *
 * Run with: `./gradlew :sirix-rest-api:nativeSmokeTest`
 */
@Tag("native-image")
@DisplayName("Native Image Smoke Tests")
class NativeImageSmokeTest {

    private fun evaluate(queryStr: String): String {
        SirixQueryContext.create().use { ctx ->
            SirixCompileChain.create().use { chain ->
                val seq = Query(chain, queryStr).evaluate(ctx)
                assertNotNull(seq)
                val buf = IOUtils.createBuffer()
                StringSerializer(buf).use { serializer ->
                    serializer.serialize(seq)
                }
                return buf.toString()
            }
        }
    }

    @Test
    @DisplayName("Basic arithmetic query")
    fun testBasicArithmetic() {
        assertEquals("2", evaluate("1 + 1"))
    }

    @Test
    @DisplayName("String manipulation query")
    fun testStringManipulation() {
        assertEquals("Hello World", evaluate("concat('Hello', ' ', 'World')"))
    }

    @Test
    @DisplayName("FLWOR expression")
    fun testFlworExpression() {
        assertEquals("2 4 6", evaluate("for ${'$'}i in (1, 2, 3) return ${'$'}i * 2"))
    }

    @Test
    @DisplayName("Conditional expression")
    fun testConditionalExpression() {
        assertEquals("yes", evaluate("if (1 < 2) then 'yes' else 'no'"))
    }

    @Test
    @DisplayName("String length function")
    fun testStringLength() {
        assertEquals("5", evaluate("string-length('hello')"))
    }

    @Test
    @DisplayName("Nested arithmetic")
    fun testNestedArithmetic() {
        assertEquals("42", evaluate("(6 * 7)"))
    }

    @Test
    @DisplayName("Sequence operations")
    fun testSequenceOperations() {
        assertEquals("6", evaluate("count((1, 2, 3, 4, 5, 6))"))
    }

    @Test
    @DisplayName("Let expression with computation")
    fun testLetExpression() {
        assertEquals("100", evaluate("let ${'$'}x := 10 return ${'$'}x * ${'$'}x"))
    }
}
