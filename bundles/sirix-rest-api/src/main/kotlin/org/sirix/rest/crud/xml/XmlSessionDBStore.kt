package org.sirix.rest.crud.xml

import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import org.brackit.xquery.jdm.Stream
import org.brackit.xquery.node.parser.NodeSubtreeParser
import org.sirix.rest.Auth
import org.sirix.rest.AuthRole
import org.sirix.xquery.node.XmlDBCollection
import org.sirix.xquery.node.XmlDBStore
import java.time.Instant

class XmlSessionDBStore(
    private val ctx: RoutingContext,
    private val dbStore: XmlDBStore,
    private val user: User,
    private val authz: AuthorizationProvider
) : XmlDBStore by dbStore {
    override fun lookup(name: String): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.VIEW, authz)

        return dbStore.lookup(name)
    }

    override fun create(name: String): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name)
    }

    override fun create(name: String, parser: NodeSubtreeParser): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, parser)
    }

    override fun create(
        name: String,
        parser: NodeSubtreeParser,
        commitMessage: String?,
        commitTimestamp: Instant?
    ): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, parser, commitMessage, commitTimestamp)
    }

    override fun create(name: String, parsers: Stream<NodeSubtreeParser>): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, parsers)
    }

    override fun create(dbName: String, resourceName: String, parsers: NodeSubtreeParser): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), dbName, AuthRole.CREATE, authz)

        return dbStore.create(dbName, resourceName, parsers)
    }

    override fun create(
        dbName: String, resourceName: String, parser: NodeSubtreeParser, commitMessage: String?,
        commitTimestamp: Instant?
    ): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), dbName, AuthRole.CREATE, authz)

        return dbStore.create(dbName, resourceName, parser, commitMessage, commitTimestamp)
    }

    override fun drop(name: String) {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.DELETE, authz)

        return dbStore.drop(name)
    }
}