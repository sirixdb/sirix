package io.sirix.rest.crud.json

import com.google.gson.stream.JsonReader
import io.brackit.query.atomic.Str
import io.brackit.query.jdm.Stream
import io.brackit.query.jdm.json.Object
import io.sirix.query.json.JsonDBCollection
import io.sirix.query.json.JsonDBStore
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.Authorization
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.nio.file.Path

/**
 * Offline regression tests for #1070: the REST query endpoint runs under the VIEW role and injects
 * a [JsonSessionDBStore] into the brackit query context. A VIEW token must be rejected on every
 * write path — both the store-level create/drop/makeDir methods and the mutating methods
 * (`add`/`remove`/`delete`) of any collection it hands out (see [AuthCheckingJsonDBCollection],
 * which guards by delegation).
 *
 * These tests use plain Vert.x [User] authorizations and Mockito stubs, so they need neither a
 * running Keycloak nor Docker (run via the `offlineTest` Gradle task).
 */
@Tag("offline")
@DisplayName("JSON REST session store rejects a VIEW token on every write path (#1070)")
class JsonSessionDBStoreAuthTest {

    private companion object {
        const val DB = "mydb"
    }

    private fun userWith(vararg roles: String): User {
        val user = User.fromName("test-user")
        val authorizations: Set<Authorization> = roles.map { RoleBasedAuthorization.create(it) }.toSet()
        user.authorizations().put("test-provider", authorizations)
        return user
    }

    private fun stubCollection(): JsonDBCollection {
        val collection = Mockito.mock(JsonDBCollection::class.java)
        Mockito.`when`(collection.getName()).thenReturn(DB)
        return collection
    }

    private fun assertForbidden(executable: () -> Unit) {
        val exception = assertThrows(HttpException::class.java) { executable() }
        assertEquals(403, exception.statusCode, "a VIEW token must be forbidden (403) on write paths")
    }

    @Test
    @DisplayName("Every store-level write method throws 403 and never reaches the delegate")
    fun viewTokenRejectedOnAllStoreWriteMethods() {
        val delegate = Mockito.mock(JsonDBStore::class.java)
        val ctx = Mockito.mock(RoutingContext::class.java)
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val reader = Mockito.mock(JsonReader::class.java)
        val options = Mockito.mock(Object::class.java)
        val path = Mockito.mock(Path::class.java)
        @Suppress("UNCHECKED_CAST")
        val pathStream = Mockito.mock(Stream::class.java) as Stream<Path>
        @Suppress("UNCHECKED_CAST")
        val strStream = Mockito.mock(Stream::class.java) as Stream<Str>

        val store = JsonSessionDBStore(ctx, delegate, userWith("$DB-view"), authz)

        assertForbidden { store.create(DB) }
        assertForbidden { store.create(DB, "path") }
        assertForbidden { store.create(DB, "path", options) }
        assertForbidden { store.create(DB, path) }
        assertForbidden { store.create(DB, path, options) }
        assertForbidden { store.createFromPaths(DB, pathStream) }
        assertForbidden { store.create(DB, "res", path) }
        assertForbidden { store.create(DB, "res", path, options) }
        assertForbidden { store.create(DB, "res", "json") }
        assertForbidden { store.create(DB, "res", "json", options) }
        assertForbidden { store.create(DB, "res", reader) }
        assertForbidden { store.create(DB, "res", reader, options) }
        assertForbidden { store.create(DB, setOf(reader)) }
        assertForbidden { store.create(DB, setOf(reader), options) }
        assertForbidden { store.createFromJsonStrings(DB, strStream) }
        assertForbidden { store.makeDir(DB) }
        assertForbidden { store.drop(DB) }

        // Not a single write reached the underlying store.
        Mockito.verifyNoInteractions(delegate)
    }

    @Test
    @DisplayName("lookup is allowed for a VIEW token and returns an authorization-checking collection")
    fun viewTokenMayLookUpButGetsGuardedCollection() {
        val delegate = Mockito.mock(JsonDBStore::class.java)
        val ctx = Mockito.mock(RoutingContext::class.java)
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val looked = stubCollection()
        Mockito.`when`(delegate.lookup(DB)).thenReturn(looked)

        val store = JsonSessionDBStore(ctx, delegate, userWith("$DB-view"), authz)

        val collection = assertDoesNotThrow<JsonDBCollection> { store.lookup(DB) }
        assertInstanceOf(AuthCheckingJsonDBCollection::class.java, collection)
    }

    @Test
    @DisplayName("A collection handed to a VIEW token rejects add/remove/delete with 403")
    fun guardedCollectionRejectsViewTokenMutations() {
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val reader = Mockito.mock(JsonReader::class.java)
        val options = Mockito.mock(Object::class.java)
        val path = Mockito.mock(Path::class.java)
        val wrapped = stubCollection()

        val guarded = AuthCheckingJsonDBCollection(wrapped, userWith("$DB-view"), authz)

        assertForbidden { guarded.delete() }
        assertForbidden { guarded.remove(1L) }
        assertForbidden { guarded.add("res", reader, options) }
        assertForbidden { guarded.add("res", reader) }
        assertForbidden { guarded.add("{}") }
        assertForbidden { guarded.add(path) }

        // The writes were rejected before being delegated to the real collection.
        Mockito.verify(wrapped, Mockito.never()).delete()
        Mockito.verify(wrapped, Mockito.never()).remove(Mockito.anyLong())
        Mockito.verify(wrapped, Mockito.never()).add(Mockito.anyString())
        Mockito.verify(wrapped, Mockito.never()).add(Mockito.any(Path::class.java))
    }

    @Test
    @DisplayName("The store-level check still authorizes a CREATE token and reaches the delegate")
    fun createTokenIsAuthorizedAtStoreLevel() {
        val delegate = Mockito.mock(JsonDBStore::class.java)
        val ctx = Mockito.mock(RoutingContext::class.java)
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val created = stubCollection()
        Mockito.`when`(delegate.create(DB)).thenReturn(created)

        val store = JsonSessionDBStore(ctx, delegate, userWith("$DB-create"), authz)

        val collection = assertDoesNotThrow<JsonDBCollection> { store.create(DB) }
        assertInstanceOf(AuthCheckingJsonDBCollection::class.java, collection)
        Mockito.verify(delegate).create(DB)
    }

    @Test
    @DisplayName("The collection-level check still authorizes a DELETE token (remove is delegated)")
    fun deleteTokenIsAuthorizedAtCollectionLevel() {
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val wrapped = stubCollection()

        val guarded = AuthCheckingJsonDBCollection(wrapped, userWith("$DB-delete"), authz)

        assertDoesNotThrow { guarded.remove(1L) }
        Mockito.verify(wrapped).remove(1L)
    }
}
