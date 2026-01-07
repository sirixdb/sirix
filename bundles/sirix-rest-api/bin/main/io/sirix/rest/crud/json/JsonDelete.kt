package io.sirix.rest.crud.json

import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.RoutingContext
import io.brackit.query.jdm.StructuredItemStore
import io.sirix.access.DatabaseType
import io.sirix.access.Databases
import io.sirix.access.trx.node.HashType
import io.sirix.api.Database
import io.sirix.api.ResourceSession
import io.sirix.api.json.JsonResourceSession
import io.sirix.rest.crud.AbstractDeleteHandler
import io.sirix.query.json.BasicJsonDBStore
import java.nio.file.Path

class JsonDelete(location: Path, private val authz: AuthorizationProvider) : AbstractDeleteHandler(location) {

    override fun createStore(ctx: RoutingContext): StructuredItemStore {
        return JsonSessionDBStore(ctx, BasicJsonDBStore.newBuilder().build(), ctx.get("user") as User, authz)
    }

    override fun database(dbFile: Path, sirixDBUser: io.sirix.access.User): Database<*> {
        return Databases.openJsonDatabase(dbFile, sirixDBUser)
    }

    override fun hashType(manager: ResourceSession<*, *>): HashType {
        if (manager is JsonResourceSession)
            return manager.resourceConfig.hashType
        else
            throw IllegalArgumentException("Resource manager is not of JSON type.")
    }

    override fun getDatabaseType(): DatabaseType = DatabaseType.JSON
}