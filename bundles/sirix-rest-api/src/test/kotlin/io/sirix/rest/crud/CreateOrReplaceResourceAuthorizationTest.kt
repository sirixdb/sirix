package io.sirix.rest.crud

import io.sirix.access.MutationGate
import kotlinx.coroutines.yield
import org.mockito.Mockito
import org.junit.jupiter.api.Assertions.assertSame
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.api.Database
import io.sirix.api.json.JsonResourceSession
import io.sirix.io.StorageType
import io.sirix.service.json.shredder.JsonShredder
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.web.handler.HttpException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.Assertions.assertTrue
import io.sirix.api.xml.XmlNodeReadOnlyTrx
import io.sirix.api.json.JsonNodeReadOnlyTrx
import io.sirix.api.xml.XmlResourceSession
import io.sirix.service.xml.shredder.XmlShredder

/**
 * REST-210 (#1477): PUT creates a missing resource with the CREATE role the route checks, but
 * replacing an existing one discards its history and additionally needs MODIFY and DELETE on the
 * database, checked before anything is removed.
 */
@Tag("offline")
class CreateOrReplaceResourceAuthorizationTest {
    @TempDir
    lateinit var directory: Path

    @ParameterizedTest
    @ValueSource(strings = ["create", "create|modify", "create|delete", "auth-create|auth-modify"])
    fun replacingWithoutModifyAndDeleteIsRefusedAndKeepsTheResource(roles: String) {
        withDatabase { database ->
            val refused = assertThrows(HttpException::class.java) {
                AbstractCreateHandler.createAndWriteResource(database, config("doc"), "doc", userWith(roles), "auth") {}
            }
            assertEquals(403, refused.statusCode)
            assertEquals(setOf("doc"), resources(database))
            assertEquals(2, revisions(database, "doc"), "a refused replacement must keep the resource's history")
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["create|modify|delete", "auth-create|auth-modify|auth-delete"])
    fun replacingWithModifyAndDeleteStartsANewHistory(roles: String) {
        withDatabase { database ->
            AbstractCreateHandler.createAndWriteResource(database, config("doc"), "doc", userWith(roles), "auth") {}
            assertEquals(setOf("doc"), resources(database))
            assertEquals(0, revisions(database, "doc"), "the replacement is a new, still empty resource")
        }
    }

    @Test
    fun creatingAMissingResourceNeedsNothingBeyondTheRoutesCreateRole() {
        withDatabase { database ->
            AbstractCreateHandler.createAndWriteResource(database, config("fresh"), "fresh", userWith("create"), "auth") {}
            assertEquals(setOf("doc", "fresh"), resources(database))
            assertEquals(2, revisions(database, "doc"))
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @Timeout(30)
    fun creationAndCleanupHoldAuthorizationThroughSuspension(xml: Boolean) {
        val path = directory.resolve("race")
        if (xml) Databases.createXmlDatabase(DatabaseConfiguration(path))
        else Databases.createJsonDatabase(DatabaseConfiguration(path))
        val database: Database<*> = if (xml) Databases.openXmlDatabase(path) else Databases.openJsonDatabase(path)
        database.use {
            val entered = CountDownLatch(1)
            val release = CompletableDeferred<Unit>()
            val firstResult = CompletableFuture<Unit>()
            val secondResult = CompletableFuture<Unit>()
            val first = Thread {
                try {
                    AbstractCreateHandler.createAndWriteResource(database, config("fresh"), "fresh", userWith("create"), "auth") {
                        entered.countDown()
                        release.await()
                        withContext(Dispatchers.Default) { Unit }
                        writeValue(database, "fresh", "first")
                    }
                    firstResult.complete(Unit)
                } catch (failure: Throwable) {
                    firstResult.completeExceptionally(failure)
                }
            }
            val second = Thread {
                try {
                    AbstractCreateHandler.createAndWriteResource(database, config("fresh"), "fresh", userWith("create|modify|delete"), "auth") {
                        writeValue(database, "fresh", "second")
                    }
                    secondResult.complete(Unit)
                } catch (failure: Throwable) {
                    secondResult.completeExceptionally(failure)
                }
            }
            first.start()
            try {
                assertTrue(entered.await(10, TimeUnit.SECONDS))
                second.start()
                while (second.state != Thread.State.WAITING && second.isAlive) Thread.onSpinWait()
                assertEquals(false, secondResult.isDone, "replacement must wait for the complete first write")
            } finally {
                release.complete(Unit)
            }
            firstResult.get(10, TimeUnit.SECONDS)
            secondResult.get(10, TimeUnit.SECONDS)
            first.join()
            second.join()
            database.beginResourceSession("fresh").use { session ->
                assertEquals(1, session.mostRecentRevisionNumber)
                session.beginNodeReadOnlyTrx().use { reader ->
                    reader.moveToFirstChild()
                    reader.moveToFirstChild()
                    if (session is JsonResourceSession) reader.moveToFirstChild()
                    assertEquals("second", if (session is XmlResourceSession) (reader as XmlNodeReadOnlyTrx).value
                        else (reader as JsonNodeReadOnlyTrx).value)
                }
            }
        }
    }

    @Test
    fun aFailedWriteRemovesOnlyItsOwnNewResource() {
        withDatabase { database ->
            assertThrows(IllegalArgumentException::class.java) {
                AbstractCreateHandler.createAndWriteResource(database, config("fresh"), "fresh", userWith("create"), "auth") {
                    writeValue(database, "fresh", "partial")
                    throw IllegalArgumentException("invalid input")
                }
            }
            assertEquals(setOf("doc"), resources(database))
            assertEquals(2, revisions(database, "doc"))
        }
    }

    /** REST-210: the gated write returns its result, propagates failure and releases the hold. */
    @Test
    fun gatedWriteReturnsItsValueAndReleasesTheGateOnBothOutcomes() {
        withDatabase { database ->
            val result = AbstractCreateHandler.createAndWriteResource(database, config("fresh"), "fresh", userWith("create"), "auth") {
                withContext(Dispatchers.Default) { yield() }
                "committed"
            }
            assertEquals("committed", result)
            assertGateReleased(database, "fresh")
            val failure = IllegalArgumentException("invalid body")
            val thrown = assertThrows(IllegalArgumentException::class.java) {
                AbstractCreateHandler.createAndWriteResource(database, config("failed"), "failed", userWith("create"), "auth") {
                    withContext(Dispatchers.Default) { yield(); throw failure }
                }
            }
            assertEquals("invalid body", thrown.message)
            // Coroutine stack recovery may copy the exception while retaining its original cause.
            assertTrue(generateSequence<Throwable>(thrown) { it.cause }.any { it === failure })
            assertGateReleased(database, "failed")
            assertEquals(setOf("doc", "fresh"), resources(database))
        }
    }

    @Test
    fun writeFailureRetainsItsCauseWhenCleanupAlsoFails() {
        withDatabase { database ->
            val unavailable = IllegalStateException("storage unavailable during cleanup")
            val boundary = Mockito.spy(database)
            Mockito.doThrow(unavailable).`when`(boundary).removeResource("fresh")
            val invalid = IllegalArgumentException("invalid body")
            val thrown = assertThrows(IllegalArgumentException::class.java) {
                AbstractCreateHandler.createAndWriteResource(boundary, config("fresh"), "fresh", userWith("create"), "auth") {
                    throw invalid
                }
            }
            assertSame(invalid, thrown)
            assertEquals(listOf(unavailable), thrown.suppressed.toList())
            assertGateReleased(database, "fresh")
            database.removeResource("fresh")
        }
    }

    private fun assertGateReleased(database: Database<*>, name: String) {
        val completed = CompletableFuture<Boolean>()
        Thread.ofVirtual().start {
            try {
                completed.complete(MutationGate.acquireResource(database.databaseConfig.databaseFile, name).use { true })
            } catch (failure: Throwable) {
                completed.completeExceptionally(failure)
            }
        }
        assertTrue(completed.get(5, TimeUnit.SECONDS))
    }

    private fun writeValue(database: Database<*>, name: String, value: String) {
        database.beginResourceSession(name).use { session ->
            when (session) {
                is JsonResourceSession -> session.beginNodeTrx().use { writer ->
                    JsonShredder.createStringReader("{\"v\":\"$value\"}").use { reader ->
                        writer.insertSubtreeAsFirstChild(reader)
                    }
                }
                is XmlResourceSession -> session.beginNodeTrx().use { writer ->
                    val reader = XmlShredder.createStringReader("<v>$value</v>")
                    try { writer.insertSubtreeAsFirstChild(reader) } finally { reader.close() }
                }
            }
        }
    }

    /** Database `auth` holding resource `doc` with two revisions. */
    private fun withDatabase(body: (Database<JsonResourceSession>) -> Unit) {
        val path = directory.resolve("auth")
        Databases.createJsonDatabase(DatabaseConfiguration(path))
        Databases.openJsonDatabase(path).use { database ->
            database.createResource(config("doc"))
            database.beginResourceSession("doc").use { session ->
                session.beginNodeTrx().use { writer ->
                    writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"v\":1}"))
                    writer.moveToDocumentRoot()
                    writer.moveToFirstChild()
                    writer.moveToFirstChild()
                    writer.moveToFirstChild()
                    writer.setNumberValue(2)
                    writer.commit()
                }
            }
            body(database)
        }
    }

    private fun config(name: String): ResourceConfiguration =
        ResourceConfiguration.newBuilder(name).storageType(StorageType.FILE_CHANNEL).build()

    private fun resources(database: Database<JsonResourceSession>): Set<String> =
        database.listResources().map { it.fileName.toString() }.toSet()

    private fun revisions(database: Database<JsonResourceSession>, name: String): Int =
        database.beginResourceSession(name).use { it.mostRecentRevisionNumber }

    private fun userWith(roles: String): User {
        val user = User.fromName("replace-test")
        user.authorizations().put("test", roles.split('|').map { RoleBasedAuthorization.create(it) }.toSet())
        return user
    }
}
