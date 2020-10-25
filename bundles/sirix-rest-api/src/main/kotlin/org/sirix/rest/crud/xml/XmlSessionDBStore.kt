package org.sirix.rest.crud.xml

import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import org.brackit.xquery.node.parser.SubtreeParser
import org.brackit.xquery.xdm.Stream
import org.sirix.rest.Auth
import org.sirix.rest.AuthRole
import org.sirix.xquery.node.XmlDBCollection
import org.sirix.xquery.node.XmlDBStore

class XmlSessionDBStore(private val ctx: RoutingContext, private val dbStore: XmlDBStore, private val user: User) : XmlDBStore by dbStore {
    override fun lookup(name: String): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.VIEW)

        return dbStore.lookup(name)
    }

    override fun create(name: String): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE)

        return dbStore.create(name)
    }

    override fun create(name: String, parser: SubtreeParser): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE)

        return dbStore.create(name, parser)
    }

    override fun create(name: String, parsers: Stream<SubtreeParser>): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE)

        return dbStore.create(name, parsers)
    }

    override fun create(dbName: String, resourceName: String, parsers: SubtreeParser): XmlDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), dbName, AuthRole.CREATE)

        return dbStore.create(dbName, resourceName, parsers)
    }

    override fun drop(name: String) {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.DELETE)

        return dbStore.drop(name)
    }
}