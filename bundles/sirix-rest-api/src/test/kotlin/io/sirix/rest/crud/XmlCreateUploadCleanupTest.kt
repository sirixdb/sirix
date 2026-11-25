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
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.Future
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.ThreadContextElement
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.io.TempDir
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyInt
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

    @ParameterizedTest
    @CsvSource("false,false", "true,false", "true,true")
    fun bufferedUploadIsReleasedOnSuccessRefusalAndCancellation(allowed: Boolean, cancelled: Boolean) {
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
        val job = Job()
        val uploadedCompletely = AtomicBoolean()
        val cancellation = if (cancelled) CancelBeforeIo(job, uploadedCompletely) else EmptyCoroutineContext
        val execution = actualVertx.dispatcher() + job + cancellation
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
            val buffered = AtomicReference(Buffer.buffer("<replacement/>"))
            // A paused transport may deliver all buffered bytes immediately when resumed.
            // Without a registered consumer that delivery is lost; a pipe owns subsequent flow.
            `when`(request.resume()).thenAnswer { buffered.set(null); request }
            `when`(request.pipeTo(any(AsyncFile::class.java))).thenAnswer { call ->
                val file = call.getArgument<AsyncFile>(0)
                file.write(buffered.getAndSet(null) ?: Buffer.buffer()).compose { file.end() }
                    .compose { uploadedCompletely.set(true); Future.succeededFuture<Void>() }
            }
            val user = User.fromName("create-only")
            val roles = if (allowed) setOf("create", "modify", "delete") else setOf("create")
            user.authorizations().put("test", roles.map { RoleBasedAuthorization.create(it) }.toSet())
            user.attributes().put("accessToken", JsonObject()
                .put("sub", UUID.randomUUID().toString()).put("preferred_username", "create-only"))
            val context = mock(RoutingContext::class.java)
            `when`(context.vertx()).thenReturn(vertx)
            `when`(context.request()).thenReturn(request)
            `when`(context.get<User>("user")).thenReturn(user)
            `when`(context.pathParam("database")).thenReturn("auth")
            `when`(context.queryParam(anyString())).thenReturn(emptyList())
            val response = mock(HttpServerResponse::class.java)
            `when`(context.response()).thenReturn(response)
            `when`(response.setStatusCode(anyInt())).thenReturn(response)
            `when`(response.putHeader(anyString(), anyString())).thenReturn(response)
            `when`(response.end(any(Buffer::class.java))).thenReturn(Future.succeededFuture())
            if (cancelled) {
                assertThrows(CancellationException::class.java) {
                    runBlocking(execution) { XmlCreate(directory).insertResource(path, "doc", context) }
                }
            } else if (allowed) {
                runBlocking(execution) { XmlCreate(directory).insertResource(path, "doc", context) }
            } else {
                val refusal = assertThrows(HttpException::class.java) {
                    runBlocking(execution) { XmlCreate(directory).insertResource(path, "doc", context) }
                }
                assertEquals(403, refusal.statusCode)
            }
            assertNotNull(uploaded.get())
            assertFalse(Files.exists(uploaded.get()), "a refused PUT must release its upload")
            Databases.openXmlDatabase(path).use { database ->
                database.beginResourceSession("doc").use { session ->
                    assertEquals(1, session.mostRecentRevisionNumber)
                    session.beginNodeReadOnlyTrx().use { reader ->
                        reader.moveToFirstChild()
                        assertEquals(if (allowed && !cancelled) "replacement" else "original", reader.name.localName)
                    }
                }
            }
        } finally {
            job.cancel()
            runBlocking { actualVertx.close().coAwait() }
        }
    }

    /** Cancel at IO dispatch after upload, before the database write can start. */
    private class CancelBeforeIo(private val job: Job, private val uploaded: AtomicBoolean) :
        ThreadContextElement<Unit>, AbstractCoroutineContextElement(Key) {
        companion object Key : CoroutineContext.Key<CancelBeforeIo>

        override fun updateThreadContext(context: CoroutineContext) {
            if (uploaded.get() && context[ContinuationInterceptor] === Dispatchers.IO) {
                job.cancel(CancellationException("cancel before database write"))
            }
        }

        override fun restoreThreadContext(context: CoroutineContext, oldState: Unit) = Unit
    }
}
