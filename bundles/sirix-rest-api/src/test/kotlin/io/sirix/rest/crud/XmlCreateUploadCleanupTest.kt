package io.sirix.rest.crud

import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.rest.crud.xml.XmlCreate
import io.sirix.service.xml.shredder.XmlShredder
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.file.AsyncFile
import io.vertx.core.file.FileSystem
import io.vertx.core.file.OpenOptions
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

@Tag("offline")
class XmlCreateUploadCleanupTest {
    @TempDir
    lateinit var directory: Path

    @Test
    fun refusedReplacementDeletesItsUploadedFileAndKeepsHistory() {
        val path = directory.resolve("auth")
        Databases.createXmlDatabase(DatabaseConfiguration(path))
        Databases.openXmlDatabase(path).use { database ->
            database.createResource(ResourceConfiguration.newBuilder("doc").build())
            database.beginResourceSession("doc").use { session ->
                session.beginNodeTrx().use { writer ->
                    val reader = XmlShredder.createStringReader("<original/>")
                    try { writer.insertSubtreeAsFirstChild(reader) } finally { reader.close() }
                }
            }
        }
        val actualVertx = Vertx.vertx()
        try {
            val uploaded = AtomicReference<Path>()
            val fileSystem = mock(FileSystem::class.java)
            `when`(fileSystem.open(anyString(), any(OpenOptions::class.java))).thenAnswer { call ->
                val name = call.getArgument<String>(0)
                uploaded.set(Path.of(name))
                actualVertx.fileSystem().open(name, call.getArgument<OpenOptions>(1))
            }
            val vertx = mock(Vertx::class.java)
            `when`(vertx.fileSystem()).thenReturn(fileSystem)
            val request = mock(HttpServerRequest::class.java)
            `when`(request.pause()).thenReturn(request)
            `when`(request.resume()).thenReturn(request)
            `when`(request.pipeTo(any(AsyncFile::class.java))).thenAnswer { call ->
                val file = call.getArgument<AsyncFile>(0)
                file.write(Buffer.buffer("<replacement/>")).compose { file.end() }
            }
            val user = User.fromName("create-only")
            user.authorizations().put("test", setOf(RoleBasedAuthorization.create("create")))
            user.attributes().put("accessToken", JsonObject()
                .put("sub", UUID.randomUUID().toString()).put("preferred_username", "create-only"))
            val context = mock(RoutingContext::class.java)
            `when`(context.vertx()).thenReturn(vertx)
            `when`(context.request()).thenReturn(request)
            `when`(context.get<User>("user")).thenReturn(user)
            `when`(context.pathParam("database")).thenReturn("auth")
            `when`(context.queryParam(anyString())).thenReturn(emptyList())
            val refusal = assertThrows(HttpException::class.java) {
                runBlocking { XmlCreate(directory).insertResource(path, "doc", context) }
            }
            assertEquals(403, refusal.statusCode)
            assertNotNull(uploaded.get())
            assertFalse(Files.exists(uploaded.get()), "a refused PUT must release its upload")
            Databases.openXmlDatabase(path).use { database ->
                database.beginResourceSession("doc").use { session ->
                    assertEquals(1, session.mostRecentRevisionNumber)
                    session.beginNodeReadOnlyTrx().use { reader ->
                        reader.moveToFirstChild()
                        assertEquals("original", reader.name.localName)
                    }
                }
            }
        } finally {
            runBlocking { actualVertx.close().coAwait() }
        }
    }
}
