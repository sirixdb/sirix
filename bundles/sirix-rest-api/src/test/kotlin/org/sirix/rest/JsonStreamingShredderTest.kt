package org.sirix.rest

import io.vertx.core.buffer.Buffer
import io.vertx.core.parsetools.JsonParser
import junit.framework.Assert.assertEquals
import org.junit.Test
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.sirix.JsonTestHelper
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.access.WriteLocksRegistry
import org.sirix.service.ShredderCommit
import org.sirix.service.json.serialize.JsonSerializer
import java.io.StringWriter
import java.nio.file.Paths

class JsonStreamingShredderTest {

    val databaseDirectory = Paths.get(System.getProperty("java.io.tmpdir"), "sirix", "json-path1")

    @Test
    fun testSimpleObject() {
        Databases.removeDatabase(databaseDirectory)
        Databases.createJsonDatabase(DatabaseConfiguration(databaseDirectory))
        val database = Databases.openJsonDatabase(databaseDirectory)
        database.use {
            database.createResource(ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build())
            var manager = database.openResourceManager(JsonTestHelper.RESOURCE)
            var wtx = manager.beginNodeTrx()

            val parser = JsonParser.newParser()
            val shredder =
                KotlinJsonStreamingShredder(wtx, parser)
            shredder.call()
            parser.handle(
                Buffer.buffer("""
                {"foo":"bar"}
                """.trimIndent()))
            parser.end()
            wtx.commit()

            var writer = StringWriter()
            writer.use {
                val serializer = JsonSerializer.Builder(manager, writer).build()
                serializer.call()
            }
            assertEquals("{\"foo\":\"bar\"}", writer.toString())
        }
        Databases.removeDatabase(databaseDirectory)
    }

    @Test
    fun testComplex1() {
        Databases.removeDatabase(databaseDirectory)
        Databases.createJsonDatabase(DatabaseConfiguration(databaseDirectory))
        val database = Databases.openJsonDatabase(databaseDirectory)
        database.use {
            database.createResource(ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build())
            var manager = database.openResourceManager(JsonTestHelper.RESOURCE)
            var wtx = manager.beginNodeTrx()

            val parser = JsonParser.newParser()
            val shredder =
                KotlinJsonStreamingShredder(wtx, parser)
            shredder.call()
            parser.handle(
                Buffer.buffer("""
                {"generic": 1, "location": {"state": "NY", "ddd": {"sssss": []}, "city": "New York", "foobar": [[],{"bar": true},[],{}]},"foo":null}
                """.trimIndent()))
            parser.end()
            wtx.commit()

            var writer = StringWriter()
            writer.use {
                val serializer = JsonSerializer.Builder(manager, writer).build()
                serializer.call()
            }
            assertEquals("""
                {"generic":1,"location":{"state":"NY","ddd":{"sssss":[]},"city":"New York","foobar":[[],{"bar":true},[],{}]},"foo":null}
                """.trimIndent(), writer.toString())
        }
        Databases.removeDatabase(databaseDirectory)
    }

    @Test
    fun testComplex2() {
        Databases.removeDatabase(databaseDirectory)
        Databases.createJsonDatabase(DatabaseConfiguration(databaseDirectory))
        val database = Databases.openJsonDatabase(databaseDirectory)
        database.use {
            database.createResource(ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build())
            var manager = database.openResourceManager(JsonTestHelper.RESOURCE)
            var wtx = manager.beginNodeTrx()

            val parser = JsonParser.newParser()
            val shredder =
                KotlinJsonStreamingShredder(wtx, parser)
            shredder.call()
            parser.handle(
                Buffer.buffer("""
                {"generic":1,"location":{"state":"NY","ddd":{"sssss":[[],[],{"tada":true}]},"city":"New York","foobar":[[],{"bar":true},[],{}]},"foo":null}
                """.trimIndent()))
            parser.end()
            wtx.commit()

            var writer = StringWriter()
            writer.use {
                val serializer = JsonSerializer.Builder(manager, writer).build()
                serializer.call()
            }
            assertEquals("""
                {"generic":1,"location":{"state":"NY","ddd":{"sssss":[[],[],{"tada":true}]},"city":"New York","foobar":[[],{"bar":true},[],{}]},"foo":null}
                """.trimIndent(), writer.toString())
        }
        Databases.removeDatabase(databaseDirectory)
    }
}