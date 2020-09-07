package org.sirix.rest.crud.xml

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.ext.auth.isAuthorizedAwait
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.brackit.xquery.node.parser.SubtreeParser
import org.brackit.xquery.xdm.Stream
import org.sirix.rest.AuthRole
import org.sirix.xquery.node.XmlDBCollection
import org.sirix.xquery.node.XmlDBStore
import java.lang.IllegalStateException

class XmlSessionDBStore(private val ctx: RoutingContext, private val dbStore: XmlDBStore, private val user: User) :
    XmlDBStore by dbStore {
    override fun lookup(name: String): XmlDBCollection {
        checkIfAuthorized(name, AuthRole.VIEW)

        return dbStore.lookup(name)
    }

    override fun create(name: String): XmlDBCollection {
        checkIfAuthorized(name, AuthRole.CREATE)

        return dbStore.create(name)
    }

    override fun create(name: String, parser: SubtreeParser): XmlDBCollection {
        checkIfAuthorized(name, AuthRole.CREATE)

        return dbStore.create(name, parser)
    }

    override fun create(name: String, parsers: Stream<SubtreeParser>): XmlDBCollection {
        checkIfAuthorized(name, AuthRole.CREATE)

        return dbStore.create(name, parsers)
    }

    override fun create(dbName: String, resourceName: String, parsers: SubtreeParser): XmlDBCollection {
        checkIfAuthorized(dbName, AuthRole.CREATE)

        return dbStore.create(dbName, resourceName, parsers)
    }

    override fun drop(name: String) {
        checkIfAuthorized(name, AuthRole.DELETE)

        return dbStore.drop(name)
    }

    private fun checkIfAuthorized(name: String, role: AuthRole) {
        GlobalScope.launch(ctx.vertx().dispatcher()) {
            val isAuthorized = user.isAuthorizedAwait(role.databaseRole(name))

            require(isAuthorized || user.isAuthorizedAwait(role.keycloakRole())) {
                throw IllegalStateException("${HttpResponseStatus.UNAUTHORIZED.code()}: User is not allowed to $role the database $name")
            }
        }
    }
}