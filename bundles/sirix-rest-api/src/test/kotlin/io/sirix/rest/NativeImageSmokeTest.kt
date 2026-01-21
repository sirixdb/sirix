package io.sirix.rest

import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.service.json.serialize.JsonSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.skyscreamer.jsonassert.JSONAssert
import java.io.StringWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue

private val testDirectory: Path = Paths.get(System.getProperty("java.io.tmpdir"), "sirix-native-test")

/**
 * Smoke tests for GraalVM native image compilation of sirix-rest-api.
 * These tests verify core Sirix functionality works correctly when compiled to native image.
 *
 * Note: These tests do NOT require Keycloak or external services.
 *
 * Run with: ./gradlew :sirix-rest-api:nativeTest
 */
@Tag("native-image")
@DisplayName("Native Image Smoke Tests")
class NativeImageSmokeTest {

    @BeforeEach
    fun setUp() {
        if (Files.exists(testDirectory)) {
            Databases.removeDatabase(testDirectory)
        }
        Files.createDirectories(testDirectory)
    }

    @AfterEach
    fun tearDown() {
        Databases.removeDatabase(testDirectory)
    }

    @Test
    @DisplayName("Create and open JSON database")
    fun testCreateAndOpenDatabase() {
        val dbPath = testDirectory.resolve("test-db")
        val dbConfig = DatabaseConfiguration(dbPath)

        assertDoesNotThrow {
            Databases.createJsonDatabase(dbConfig)
        }

        assertTrue(Databases.existsDatabase(dbPath))

        Databases.openJsonDatabase(dbPath).use { database ->
            assertNotNull(database)
            assertEquals("test-db", database.name)
        }
    }

    @Test
    @DisplayName("Store and retrieve JSON document")
    fun testStoreAndRetrieveJson() {
        val dbPath = testDirectory.resolve("json-db")
        val dbConfig = DatabaseConfiguration(dbPath)

        Databases.createJsonDatabase(dbConfig)

        val inputJson = """{"name":"sirix","version":1,"features":["versioning","native"]}"""

        Databases.openJsonDatabase(dbPath).use { database ->
            database.createResource(ResourceConfiguration.Builder("resource1").build())

            val manager = database.beginResourceSession("resource1")
            manager.use {
                val wtx = it.beginNodeTrx()
                wtx.use { trx ->
                    trx.insertSubtreeAsFirstChild(io.sirix.service.json.shredder.JsonShredder.createStringReader(inputJson))
                    trx.commit()
                }
            }
        }

        // Read back and verify
        Databases.openJsonDatabase(dbPath).use { database ->
            val manager = database.beginResourceSession("resource1")
            manager.use {
                val rtx = it.beginNodeReadOnlyTrx()
                rtx.use { trx ->
                    val writer = StringWriter()
                    val serializer = JsonSerializer.newBuilder(it, writer).build()
                    serializer.call()

                    JSONAssert.assertEquals(inputJson, writer.toString(), false)
                }
            }
        }
    }

    @Test
    @DisplayName("Multiple revisions")
    fun testMultipleRevisions() {
        val dbPath = testDirectory.resolve("revision-db")
        val dbConfig = DatabaseConfiguration(dbPath)

        Databases.createJsonDatabase(dbConfig)

        Databases.openJsonDatabase(dbPath).use { database ->
            database.createResource(ResourceConfiguration.Builder("resource1").build())

            val manager = database.beginResourceSession("resource1")
            manager.use {
                // Revision 1 - initial document
                var wtx = it.beginNodeTrx()
                wtx.use { trx ->
                    trx.insertSubtreeAsFirstChild(
                        io.sirix.service.json.shredder.JsonShredder.createStringReader("""{"count":1}""")
                    )
                    trx.commit()
                }

                // Revision 2 - modify the document
                wtx = it.beginNodeTrx()
                wtx.use { trx ->
                    // Navigate to root and modify
                    trx.moveToDocumentRoot()
                    trx.moveToFirstChild() // Move to the object
                    trx.moveToFirstChild() // Move to "count" key
                    trx.moveToFirstChild() // Move to value
                    trx.setNumberValue(2)
                    trx.commit()
                }

                // Verify we have multiple revisions (at least 2)
                assertTrue(it.mostRecentRevisionNumber >= 2)
            }
        }
    }

    @Test
    @DisplayName("JSON with nested structures")
    fun testNestedJson() {
        val dbPath = testDirectory.resolve("nested-db")
        val dbConfig = DatabaseConfiguration(dbPath)

        Databases.createJsonDatabase(dbConfig)

        val nestedJson = """
            {
                "users": [
                    {"id": 1, "name": "Alice", "roles": ["admin", "user"]},
                    {"id": 2, "name": "Bob", "roles": ["user"]}
                ],
                "metadata": {
                    "version": "1.0",
                    "generated": true
                }
            }
        """.trimIndent().replace("\n", "").replace(" ", "")

        Databases.openJsonDatabase(dbPath).use { database ->
            database.createResource(ResourceConfiguration.Builder("resource1").build())

            val manager = database.beginResourceSession("resource1")
            manager.use {
                val wtx = it.beginNodeTrx()
                wtx.use { trx ->
                    trx.insertSubtreeAsFirstChild(
                        io.sirix.service.json.shredder.JsonShredder.createStringReader(nestedJson)
                    )
                    trx.commit()
                }
            }
        }

        // Verify by reading back
        Databases.openJsonDatabase(dbPath).use { database ->
            val manager = database.beginResourceSession("resource1")
            manager.use {
                val rtx = it.beginNodeReadOnlyTrx()
                rtx.use { trx ->
                    val writer = StringWriter()
                    val serializer = JsonSerializer.newBuilder(it, writer).build()
                    serializer.call()

                    // Just verify it's valid JSON and has expected structure
                    val result = writer.toString()
                    assertTrue(result.contains("\"users\""))
                    assertTrue(result.contains("\"Alice\""))
                    assertTrue(result.contains("\"metadata\""))
                }
            }
        }
    }

    @Test
    @DisplayName("Primitive JSON types")
    fun testPrimitiveTypes() {
        val dbPath = testDirectory.resolve("primitives-db")
        val dbConfig = DatabaseConfiguration(dbPath)

        Databases.createJsonDatabase(dbConfig)

        val testCases = listOf(
            """{"string":"hello"}""",
            """{"number":42}""",
            """{"float":3.14159}""",
            """{"boolean":true}""",
            """{"null":null}""",
            """[1,2,3]""",
            """["a","b","c"]"""
        )

        for ((index, json) in testCases.withIndex()) {
            val resourceName = "resource$index"

            Databases.openJsonDatabase(dbPath).use { database ->
                database.createResource(ResourceConfiguration.Builder(resourceName).build())

                val manager = database.beginResourceSession(resourceName)
                manager.use {
                    val wtx = it.beginNodeTrx()
                    wtx.use { trx ->
                        trx.insertSubtreeAsFirstChild(
                            io.sirix.service.json.shredder.JsonShredder.createStringReader(json)
                        )
                        trx.commit()
                    }
                }
            }

            // Verify
            Databases.openJsonDatabase(dbPath).use { database ->
                val manager = database.beginResourceSession(resourceName)
                manager.use {
                    val writer = StringWriter()
                    val serializer = JsonSerializer.newBuilder(it, writer).build()
                    serializer.call()

                    JSONAssert.assertEquals(json, writer.toString(), false)
                }
            }
        }
    }
}
