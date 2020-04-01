package org.sirix.rest.crud.xml

import io.vertx.ext.auth.User
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import org.brackit.xquery.xdm.StructuredItemStore
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.ResourceManager
import org.sirix.api.xml.XmlResourceManager
import org.sirix.rest.crud.AbstractDeleteHandler
import org.sirix.xquery.node.BasicXmlDBStore
import java.nio.file.Path

class XmlDelete(location: Path) : AbstractDeleteHandler(location) {
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName: String? = ctx.pathParam("database")
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
        return XmlSessionDBStore(ctx, BasicXmlDBStore.newBuilder().build(), ctx.get("user") as User)
    }

    override fun database(dbFile: Path, sirixDBUser: org.sirix.access.User): Database<*> {
        return Databases.openXmlDatabase(dbFile,  sirixDBUser)
    }

    override fun hashType(manager: ResourceManager<*, *>): HashType {
        if (manager is XmlResourceManager)
            return manager.resourceConfig.hashType
        else
            throw IllegalArgumentException("Resource manager is not of XML type.")
    }
}