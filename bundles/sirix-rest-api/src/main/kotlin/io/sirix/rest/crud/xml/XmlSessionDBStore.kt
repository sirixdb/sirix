package io.sirix.rest.crud.xml

import io.sirix.query.node.XmlDBCollection
import io.sirix.query.node.XmlDBStore
import io.sirix.rest.AuthRole
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import org.brackit.xquery.jdm.Stream
import org.brackit.xquery.node.parser.NodeSubtreeParser
import java.time.Instant

class XmlSessionDBStore(
    private val ctx: RoutingContext,
    private val dbStore: XmlDBStore,
    private val user: User,
    private val authz: AuthorizationProvider
) : XmlDBStore by dbStore {
    override fun lookup(name: String): XmlDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.VIEW, authz)

        return dbStore.lookup(name)
    }

    override fun create(name: String): XmlDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name)
    }

    override fun create(name: String, parser: NodeSubtreeParser): XmlDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, parser)
    }

    override fun create(
        name: String,
        parser: NodeSubtreeParser,
        commitMessage: String?,
        commitTimestamp: Instant?
    ): XmlDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, parser, commitMessage, commitTimestamp)
    }

    override fun create(name: String, parsers: Stream<NodeSubtreeParser>): XmlDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, parsers)
    }

    override fun create(dbName: String, resourceName: String, parsers: NodeSubtreeParser): XmlDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), dbName, AuthRole.CREATE, authz)

        return dbStore.create(dbName, resourceName, parsers)
    }

    override fun create(
        dbName: String, resourceName: String, parser: NodeSubtreeParser, commitMessage: String?,
        commitTimestamp: Instant?
    ): XmlDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), dbName, AuthRole.CREATE, authz)

        return dbStore.create(dbName, resourceName, parser, commitMessage, commitTimestamp)
    }

    override fun drop(name: String) {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.DELETE, authz)

        return dbStore.drop(name)
    }
}