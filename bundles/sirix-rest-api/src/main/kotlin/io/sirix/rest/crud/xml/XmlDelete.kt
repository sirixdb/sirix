package io.sirix.rest.crud.xml

import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.brackit.query.jdm.StructuredItemStore
import io.sirix.access.DatabaseType
import io.sirix.access.Databases
import io.sirix.access.trx.node.HashType
import io.sirix.api.Database
import io.sirix.api.ResourceSession
import io.sirix.api.xml.XmlResourceSession
import io.sirix.rest.crud.AbstractDeleteHandler
import io.sirix.query.node.BasicXmlDBStore
import java.nio.file.Path

class XmlDelete(location: Path, private val authz: AuthorizationProvider) : AbstractDeleteHandler(location) {
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName: String? = ctx.pathParam("database") ?: ctx.get("databaseName")
        val resource: String? = ctx.pathParam("resource")
        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        if (databaseName == null) {
            dropDatabasesOfType(ctx, DatabaseType.XML)
        } else {
            delete(databaseName, resource, nodeId?.toLongOrNull(), ctx)
        }

        return ctx.currentRoute()
    }

    override fun createStore(ctx: RoutingContext): StructuredItemStore {
        return XmlSessionDBStore(ctx, BasicXmlDBStore.newBuilder().build(), ctx.get("user") as User, authz)
    }

    override fun database(dbFile: Path, sirixDBUser: io.sirix.access.User): Database<*> {
        return Databases.openXmlDatabase(dbFile,  sirixDBUser)
    }

    override fun hashType(manager: ResourceSession<*, *>): HashType {
        if (manager is XmlResourceSession)
            return manager.resourceConfig.hashType
        else
            throw IllegalArgumentException("Resource manager is not of XML type.")
    }
}