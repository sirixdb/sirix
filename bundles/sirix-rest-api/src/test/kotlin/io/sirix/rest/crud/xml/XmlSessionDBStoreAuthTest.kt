package io.sirix.rest.crud.xml

import io.brackit.query.jdm.Stream
import io.brackit.query.node.parser.NodeSubtreeParser
import io.sirix.query.node.XmlDBCollection
import io.sirix.query.node.XmlDBStore
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

/**
 * Offline regression tests for #1070, XML variant: a VIEW token injected into the brackit query
 * context via [XmlSessionDBStore] must be rejected on every write path — the store-level
 * create/drop/makeDir methods and the mutating methods of any collection it hands out (see
 * [AuthCheckingXmlDBCollection], which guards by delegation).
 *
 * Uses plain Vert.x [User] authorizations and Mockito stubs, so it needs neither Keycloak nor
 * Docker (run via the `offlineTest` Gradle task).
 */
@Tag("offline")
@DisplayName("XML REST session store rejects a VIEW token on every write path (#1070)")
class XmlSessionDBStoreAuthTest {

    private companion object {
        const val DB = "mydb"
    }

    private fun userWith(vararg roles: String): User {
        val user = User.fromName("test-user")
        val authorizations: Set<Authorization> = roles.map { RoleBasedAuthorization.create(it) }.toSet()
        user.authorizations().put("test-provider", authorizations)
        return user
    }

    private fun stubCollection(): XmlDBCollection {
        val collection = Mockito.mock(XmlDBCollection::class.java)
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
        val delegate = Mockito.mock(XmlDBStore::class.java)
        val ctx = Mockito.mock(RoutingContext::class.java)
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val parser = Mockito.mock(NodeSubtreeParser::class.java)
        @Suppress("UNCHECKED_CAST")
        val parserStream = Mockito.mock(Stream::class.java) as Stream<NodeSubtreeParser>

        val store = XmlSessionDBStore(ctx, delegate, userWith("$DB-view"), authz)

        assertForbidden { store.create(DB) }
        assertForbidden { store.create(DB, parser) }
        assertForbidden { store.create(DB, parser, "message", null) }
        assertForbidden { store.create(DB, parserStream) }
        assertForbidden { store.create(DB, "res", parser) }
        assertForbidden { store.create(DB, "res", parser, "message", null) }
        assertForbidden { store.makeDir(DB) }
        assertForbidden { store.drop(DB) }

        Mockito.verifyNoInteractions(delegate)
    }

    @Test
    @DisplayName("lookup is allowed for a VIEW token and returns an authorization-checking collection")
    fun viewTokenMayLookUpButGetsGuardedCollection() {
        val delegate = Mockito.mock(XmlDBStore::class.java)
        val ctx = Mockito.mock(RoutingContext::class.java)
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val looked = stubCollection()
        Mockito.`when`(delegate.lookup(DB)).thenReturn(looked)

        val store = XmlSessionDBStore(ctx, delegate, userWith("$DB-view"), authz)

        val collection = assertDoesNotThrow<XmlDBCollection> { store.lookup(DB) }
        assertInstanceOf(AuthCheckingXmlDBCollection::class.java, collection)
    }

    @Test
    @DisplayName("A collection handed to a VIEW token rejects add/remove/delete with 403")
    fun guardedCollectionRejectsViewTokenMutations() {
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val parser = Mockito.mock(NodeSubtreeParser::class.java)
        val wrapped = stubCollection()

        val guarded = AuthCheckingXmlDBCollection(wrapped, userWith("$DB-view"), authz)

        assertForbidden { guarded.delete() }
        assertForbidden { guarded.remove(1L) }
        assertForbidden { guarded.add("res", parser) }
        assertForbidden { guarded.add("res", parser, "message", null) }
        assertForbidden { guarded.add(parser) }

        Mockito.verify(wrapped, Mockito.never()).delete()
        Mockito.verify(wrapped, Mockito.never()).remove(Mockito.anyLong())
        Mockito.verify(wrapped, Mockito.never()).add(Mockito.any(NodeSubtreeParser::class.java))
    }

    @Test
    @DisplayName("The store-level check still authorizes a CREATE token and reaches the delegate")
    fun createTokenIsAuthorizedAtStoreLevel() {
        val delegate = Mockito.mock(XmlDBStore::class.java)
        val ctx = Mockito.mock(RoutingContext::class.java)
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val created = stubCollection()
        Mockito.`when`(delegate.create(DB)).thenReturn(created)

        val store = XmlSessionDBStore(ctx, delegate, userWith("$DB-create"), authz)

        val collection = assertDoesNotThrow<XmlDBCollection> { store.create(DB) }
        assertInstanceOf(AuthCheckingXmlDBCollection::class.java, collection)
        Mockito.verify(delegate).create(DB)
    }

    @Test
    @DisplayName("The collection-level check still authorizes a DELETE token (remove is delegated)")
    fun deleteTokenIsAuthorizedAtCollectionLevel() {
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val wrapped = stubCollection()

        val guarded = AuthCheckingXmlDBCollection(wrapped, userWith("$DB-delete"), authz)

        assertDoesNotThrow { guarded.remove(1L) }
        Mockito.verify(wrapped).remove(1L)
    }
}
