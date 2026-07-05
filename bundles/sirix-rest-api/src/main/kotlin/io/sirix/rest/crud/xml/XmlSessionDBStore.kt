package io.sirix.rest.crud.xml

import io.sirix.query.node.XmlDBCollection
import io.sirix.query.node.XmlDBStore
import io.sirix.rest.Auth
import io.sirix.rest.AuthRole
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.RoutingContext
import io.brackit.query.jdm.Stream
import io.brackit.query.node.parser.NodeSubtreeParser
import java.time.Instant

class XmlSessionDBStore(
    private val ctx: RoutingContext,
    private val dbStore: XmlDBStore,
    private val user: User,
    private val authz: AuthorizationProvider
) : XmlDBStore by dbStore {

    /**
     * Wrap a collection so that every mutating operation performed through it re-checks the
     * caller's authorization. Without this the store-level checks are bypassable: a VIEW token can
     * [lookup] a collection (a read) and then call `add`/`remove`/`delete` directly on it.
     */
    private fun wrap(collection: XmlDBCollection): XmlDBCollection =
        AuthCheckingXmlDBCollection(collection, user, authz)

    override fun lookup(name: String): XmlDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.VIEW, authz)

        return wrap(dbStore.lookup(name))
    }

    override fun create(name: String): XmlDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name))
    }

    override fun create(name: String, parser: NodeSubtreeParser): XmlDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, parser))
    }

    override fun create(
        name: String,
        parser: NodeSubtreeParser,
        commitMessage: String?,
        commitTimestamp: Instant?
    ): XmlDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, parser, commitMessage, commitTimestamp))
    }

    override fun create(name: String, parsers: Stream<NodeSubtreeParser>): XmlDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, parsers))
    }

    override fun create(dbName: String, resourceName: String, parsers: NodeSubtreeParser): XmlDBCollection {
        Auth.checkIfAuthorized(user, dbName, AuthRole.CREATE, authz)

        return wrap(dbStore.create(dbName, resourceName, parsers))
    }

    override fun create(
        dbName: String, resourceName: String, parser: NodeSubtreeParser, commitMessage: String?,
        commitTimestamp: Instant?
    ): XmlDBCollection {
        Auth.checkIfAuthorized(user, dbName, AuthRole.CREATE, authz)

        return wrap(dbStore.create(dbName, resourceName, parser, commitMessage, commitTimestamp))
    }

    override fun drop(name: String) {
        Auth.checkIfAuthorized(user, name, AuthRole.DELETE, authz)

        return dbStore.drop(name)
    }

    override fun makeDir(path: String) {
        Auth.checkIfAuthorized(user, path, AuthRole.CREATE, authz)

        return dbStore.makeDir(path)
    }
}
