package io.sirix.rest.crud

import io.vertx.ext.web.handler.HttpException
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@DisplayName("PathValidation")
class PathValidationTest {

    @Test
    @DisplayName("accepts a valid simple name")
    fun validSimpleName() {
        assertDoesNotThrow {
            PathValidation.validatePathParam("my-database", "database")
        }
    }

    @Test
    @DisplayName("accepts a name with dots that is not a traversal")
    fun validNameWithDot() {
        assertDoesNotThrow {
            PathValidation.validatePathParam("my.database", "database")
        }
    }

    @Test
    @DisplayName("accepts a name with underscores and digits")
    fun validNameWithUnderscoresAndDigits() {
        assertDoesNotThrow {
            PathValidation.validatePathParam("db_123", "database")
        }
    }

    @Test
    @DisplayName("rejects blank name")
    fun rejectsBlank() {
        val ex = assertThrows<HttpException> {
            PathValidation.validatePathParam("   ", "database")
        }
        assertEquals(400, ex.statusCode)
    }

    @Test
    @DisplayName("rejects empty name")
    fun rejectsEmpty() {
        val ex = assertThrows<HttpException> {
            PathValidation.validatePathParam("", "database")
        }
        assertEquals(400, ex.statusCode)
    }

    @Test
    @DisplayName("rejects double-dot traversal")
    fun rejectsDoubleDot() {
        val ex = assertThrows<HttpException> {
            PathValidation.validatePathParam("../etc/passwd", "database")
        }
        assertEquals(400, ex.statusCode)
    }

    @Test
    @DisplayName("rejects double-dot in middle of name")
    fun rejectsDoubleDotInMiddle() {
        val ex = assertThrows<HttpException> {
            PathValidation.validatePathParam("foo..bar", "database")
        }
        assertEquals(400, ex.statusCode)
    }

    @Test
    @DisplayName("rejects forward slash")
    fun rejectsForwardSlash() {
        val ex = assertThrows<HttpException> {
            PathValidation.validatePathParam("foo/bar", "resource")
        }
        assertEquals(400, ex.statusCode)
    }

    @Test
    @DisplayName("rejects backslash")
    fun rejectsBackslash() {
        val ex = assertThrows<HttpException> {
            PathValidation.validatePathParam("foo\\bar", "resource")
        }
        assertEquals(400, ex.statusCode)
    }

    @Test
    @DisplayName("rejects null byte")
    fun rejectsNullByte() {
        val ex = assertThrows<HttpException> {
            PathValidation.validatePathParam("foo\u0000bar", "database")
        }
        assertEquals(400, ex.statusCode)
    }

    @Test
    @DisplayName("rejects name exceeding max length")
    fun rejectsExcessiveLength() {
        val longName = "a".repeat(256)
        val ex = assertThrows<HttpException> {
            PathValidation.validatePathParam(longName, "database")
        }
        assertEquals(400, ex.statusCode)
    }

    @Test
    @DisplayName("accepts name at exactly max length")
    fun acceptsMaxLength() {
        val maxName = "a".repeat(255)
        assertDoesNotThrow {
            PathValidation.validatePathParam(maxName, "database")
        }
    }

    @Test
    @DisplayName("rejects Windows-style traversal")
    fun rejectsWindowsTraversal() {
        val ex = assertThrows<HttpException> {
            PathValidation.validatePathParam("..\\windows\\system32", "database")
        }
        assertEquals(400, ex.statusCode)
    }

    @Test
    @DisplayName("rejects plain double-dot")
    fun rejectsPlainDoubleDot() {
        val ex = assertThrows<HttpException> {
            PathValidation.validatePathParam("..", "database")
        }
        assertEquals(400, ex.statusCode)
    }
}
