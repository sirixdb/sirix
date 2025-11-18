package io.sirix.rest

import io.vertx.core.buffer.Buffer
import io.vertx.core.parsetools.JsonParser
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.service.json.serialize.JsonSerializer
import org.skyscreamer.jsonassert.JSONAssert
import java.io.StringWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private val databaseDirectory: Path = Paths.get(System.getProperty("java.io.tmpdir"), "sirix", "json-path1")

private val json = Paths.get("src", "test", "resources", "json")

/**
 * Test the JSON streaming shredder (indirectly also the JSON serializer).
 */
class JsonStreamingShredderTest {

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
        testString(
            """
             {"foo":"bar"}
             """.trimIndent()
        )
    }

    @Test
    fun testComplex1() {
        testString(
            """
            {"generic":1,"location":{"state":"NY","ddd":{"sssss":[]},"city":"New York","foobar":[[],{"bar":true},[],{}]},"foo":null}
            """.trimIndent()
        )
    }

    @Test
    fun testComplex2() {
        testString(
            """
            {"generic":1,"location":{"state":"NY","ddd":{"sssss":[[],[],{"tada":true}]},"city":"New York","foobar":[[],{"bar":true},[],{}]},"foo":null}
            """.trimIndent()
        )
    }

    @Test
    fun testComplex3() {
        testString(
            """
                {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
            """.trimIndent()
        )
    }

    @Test
    fun testCopperfieldBook() {
        testString(Files.readString(json.resolve("copperfield-book.json")))
    }

    private fun testString(json: String) {
        Databases.createJsonDatabase(
            DatabaseConfiguration(
                databaseDirectory
            )
        )
        val database = Databases.openJsonDatabase(databaseDirectory)
        database.use {
            database.createResource(ResourceConfiguration.Builder("shredded").build())
            val manager = database.beginResourceSession("shredded")

            manager.use {
                val wtx = manager.beginNodeTrx()

                wtx.use {
                    val parser = JsonParser.newParser()
                    val shredder =
                        KotlinJsonStreamingShredder(wtx, parser)
                    shredder.call()
                    parser.handle(
                        Buffer.buffer(
                            json
                        )
                    )
                    parser.end()
                    wtx.commit()
                }

                val writer = StringWriter()
                writer.use {
                    val serializer = JsonSerializer.Builder(manager, writer).build()
                    serializer.call()
                }
                val actual = writer.toString()
                JSONAssert.assertEquals(json, actual, true)
            }
        }
    }
}
