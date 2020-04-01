package org.sirix.rest.crud.json

import io.vertx.ext.auth.User
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import org.brackit.xquery.xdm.StructuredItemStore
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.ResourceManager
import org.sirix.api.json.JsonResourceManager
import org.sirix.rest.crud.AbstractDeleteHandler
import org.sirix.xquery.json.BasicJsonDBStore
import java.nio.file.Path

class JsonDelete(location: Path) : AbstractDeleteHandler(location) {
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName: String? = ctx.pathParam("database")
        val resource: String? = ctx.pathParam("resource")
        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        if (databaseName == null) {
            dropDatabasesOfType(ctx, DatabaseType.JSON)
        } else {
            delete(databaseName, resource, nodeId?.toLongOrNull(), ctx)
        }

        return ctx.currentRoute()
    }

    override fun createStore(ctx: RoutingContext): StructuredItemStore {
        return JsonSessionDBStore(ctx, BasicJsonDBStore.newBuilder().build(), ctx.get("user") as User)
    }

    override fun database(dbFile: Path, sirixDBUser: org.sirix.access.User): Database<*> {
        return Databases.openJsonDatabase(dbFile,  sirixDBUser)
    }

    override fun hashType(manager: ResourceManager<*, *>): HashType {
        if (manager is JsonResourceManager)
            return manager.resourceConfig.hashType
        else
            throw IllegalArgumentException("Resource manager is not of JSON type.")
    }
}