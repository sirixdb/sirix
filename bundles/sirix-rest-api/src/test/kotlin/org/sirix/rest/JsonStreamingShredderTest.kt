package org.sirix.rest

import io.vertx.core.buffer.Buffer
import io.vertx.core.parsetools.JsonParser
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.service.json.serialize.JsonSerializer
import java.io.StringWriter
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Test the JSON streamin shredder (indirectly also the JSON serializer).
 */
class JsonStreamingShredderTest {

    private val databaseDirectory: Path = Paths.get(System.getProperty("java.io.tmpdir"), "sirix", "json-path1")

    @BeforeEach
    fun setup() {
        Databases.removeDatabase(databaseDirectory)
    }

    @AfterEach
    fun tearDown() {
        Databases.removeDatabase(databaseDirectory)
    }

    @Test
    fun testSimpleObject() {
        test(
            """
             {"foo":"bar"}
             """.trimIndent()
        )
    }

    @Test
    fun testComplex1() {
        test(
            """
            {"generic":1,"location":{"state":"NY","ddd":{"sssss":[]},"city":"New York","foobar":[[],{"bar":true},[],{}]},"foo":null}
            """.trimIndent()
        )
    }

    @Test
    fun testComplex2() {
        test(
            """
            {"generic":1,"location":{"state":"NY","ddd":{"sssss":[[],[],{"tada":true}]},"city":"New York","foobar":[[],{"bar":true},[],{}]},"foo":null}
            """.trimIndent()
        )
    }

    private fun test(json: String) {
        Databases.createJsonDatabase(DatabaseConfiguration(databaseDirectory))
        val database = Databases.openJsonDatabase(databaseDirectory)
        database.use {
            database.createResource(ResourceConfiguration.Builder("shredded").build())
            val manager = database.openResourceManager("shredded")
            val wtx = manager.beginNodeTrx()

            val parser = JsonParser.newParser()
            val latch = CountDownLatch(1)
            val shredder =
                KotlinJsonStreamingShredder(wtx, parser, latch = latch)
            shredder.call()
            parser.handle(
                Buffer.buffer(
                    json
                )
            )
            parser.end()
            latch.await(5, TimeUnit.SECONDS)
            wtx.commit()

            val writer = StringWriter()
            writer.use {
                val serializer = JsonSerializer.Builder(manager, writer).build()
                serializer.call()
            }
            assertEquals(json, writer.toString())
        }
    }
}
