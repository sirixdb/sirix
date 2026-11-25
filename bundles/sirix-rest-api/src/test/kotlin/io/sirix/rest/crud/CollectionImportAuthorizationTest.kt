package io.sirix.rest.crud

import io.brackit.query.atomic.QNm
import io.brackit.query.jdm.Sequence
import io.brackit.query.jsonitem.`object`.ArrayObject
import io.brackit.query.node.parser.DocumentParser
import io.sirix.query.json.BasicJsonDBStore
import io.sirix.query.node.BasicXmlDBStore
import io.sirix.rest.crud.json.AuthCheckingJsonDBCollection
import io.sirix.rest.crud.xml.AuthCheckingXmlDBCollection
import io.sirix.service.json.serialize.JsonSerializer
import io.sirix.service.json.shredder.JsonShredder
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.web.handler.HttpException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mockito
import java.nio.file.Path

/** REST-210: a looked-up collection cannot bypass CREATE at the direct resource import API. */
@Tag("offline")
class CollectionImportAuthorizationTest {
    @TempDir
    lateinit var directory: Path

    @ParameterizedTest
    @CsvSource("true,false,false", "true,false,true", "true,true,false", "false,false,false", "false,false,true", "false,true,false")
    fun resourceImportsRequireCreateAndPreserveExistingContent(xml: Boolean, allowed: Boolean, existing: Boolean) {
        val user = User.fromName("collection-user")
        user.authorizations().put("test", setOf(RoleBasedAuthorization.create(if (allowed) "auth-create" else "auth-modify")))
        val target = if (existing) "keep" else "fresh"
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        if (xml) BasicXmlDBStore.newBuilder().location(directory).build().use { store ->
            val collection = store.create("auth", "keep", DocumentParser("<original/>"))
            val decorated = AuthCheckingXmlDBCollection(collection, user, authz)
            val write = { decorated.importResource(target, DocumentParser("<loaded/>"), null, null) }
            if (allowed) {
                write()
                collection.database.beginResourceSession("fresh").use { session ->
                    session.beginNodeReadOnlyTrx().use { reader ->
                        assertTrue(reader.moveToFirstChild())
                        assertEquals("loaded", reader.name.localName)
                    }
                }
            } else assertEquals(403, assertThrows(HttpException::class.java) { write() }.statusCode)
            assertEquals(if (allowed) setOf("keep", "fresh") else setOf("keep"), collection.database.listResources().map { it.fileName.toString() }.toSet())
            collection.database.beginResourceSession("keep").use { session ->
                assertEquals(1, session.mostRecentRevisionNumber)
                session.beginNodeReadOnlyTrx().use { reader -> reader.moveToFirstChild(); assertEquals("original", reader.name.localName) }
            }
        } else BasicJsonDBStore.newBuilder().location(directory).build().use { store ->
            val collection = store.create("auth", "keep", "{\"original\":true}")
            val decorated = AuthCheckingJsonDBCollection(collection, user, authz)
            JsonShredder.createStringReader("{\"loaded\":true}").use { reader ->
                val write = { decorated.importResource(target, reader, ArrayObject(emptyArray<QNm>(), emptyArray<Sequence>())) }
                if (allowed) {
                    write()
                    collection.database.beginResourceSession("fresh").use { session ->
                        val output = StringBuilder()
                        JsonSerializer.Builder(session, output).build().call()
                        assertEquals("{\"loaded\":true}", output.toString())
                    }
                } else assertEquals(403, assertThrows(HttpException::class.java) { write() }.statusCode)
            }
            assertEquals(if (allowed) setOf("keep", "fresh") else setOf("keep"), collection.database.listResources().map { it.fileName.toString() }.toSet())
            collection.database.beginResourceSession("keep").use { session ->
                assertEquals(1, session.mostRecentRevisionNumber)
                val output = StringBuilder()
                JsonSerializer.Builder(session, output).build().call()
                assertEquals("{\"original\":true}", output.toString())
            }
        }
    }
}
