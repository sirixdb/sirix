package org.sirix.rest.crud.json

import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.json.JsonNodeTrx
import org.sirix.api.json.JsonResourceManager
import org.sirix.rest.crud.SirixDBUser
import org.sirix.xquery.json.BasicJsonDBStore
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path

class JsonDelete(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val dbName: String? = ctx.pathParam("database")
        val resName: String? = ctx.pathParam("resource")
        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        if (dbName == null) {
            // Initialize queryResource context and store.
            val dbStore = JsonSessionDBStore(ctx, BasicJsonDBStore.newBuilder().build(), ctx.get("user") as User)

            ctx.vertx().executeBlockingAwait { promise: Promise<Unit> ->
                val databases = Files.list(location)

                databases.use {
                    databases.filter { Files.isDirectory(it) && Databases.getDatabaseType(it) == DatabaseType.JSON }
                        .forEach {
                            dbStore.drop(it.fileName.toString())
                        }
                }

                ctx.response().setStatusCode(200).end()
                promise.complete(null)
            }
        } else {
            delete(dbName, resName, nodeId?.toLongOrNull(), ctx)
        }

        return ctx.currentRoute()
    }

    private suspend fun delete(dbPathName: String, resPathName: String?, nodeId: Long?, ctx: RoutingContext) {
        val dbFile = location.resolve(dbPathName)
        val context = ctx.vertx().orCreateContext
        val dispatcher = ctx.vertx().dispatcher()

        if (resPathName == null) {
            removeDatabase(dbFile, dispatcher)
            ctx.response().setStatusCode(200).end()
            return
        }

        val sirixDBUser = SirixDBUser.createSirixDBUser(ctx)

        val database = Databases.openJsonDatabase(dbFile, sirixDBUser)

        database.use {
            if (nodeId == null) {
                removeResource(dispatcher, database, resPathName, ctx)
            } else {
                val manager = database.openResourceManager(resPathName)

                removeSubtree(manager, nodeId, context, ctx)
            }
        }

        if (!ctx.failed())
            ctx.response().setStatusCode(200).end()
    }


    private suspend fun removeDatabase(dbFile: Path?, dispatcher: CoroutineDispatcher) {
        withContext(dispatcher) {
            Databases.removeDatabase(dbFile)
        }
    }

    private suspend fun removeResource(
        dispatcher: CoroutineDispatcher, database: Database<JsonResourceManager>,
        resPathName: String?,
        ctx: RoutingContext
    ): Any? {
        return try {
            withContext(dispatcher) {
                database.removeResource(resPathName)
            }
        } catch (e: IllegalStateException) {
            ctx.fail(IllegalStateException("Open resource managers found."))
        }
    }

    private suspend fun removeSubtree(
        manager: JsonResourceManager,
        nodeId: Long,
        context: Context,
        routingContext: RoutingContext
    ): JsonNodeTrx? {
        return context.executeBlockingAwait { promise: Promise<JsonNodeTrx> ->
            manager.use { resourceManager ->
                val wtx = resourceManager.beginNodeTrx()

                if (wtx.moveTo(nodeId).hasMoved()) {
                    if (manager.resourceConfig.hashType != HashType.NONE && !wtx.isDocumentRoot) {
                        val hashCode = routingContext.request().getHeader(HttpHeaders.ETAG)

                        if (hashCode == null) {
                            routingContext.fail(IllegalStateException("Hash code is missing in ETag HTTP-Header."))
                        }

                        if (wtx.hash != BigInteger(hashCode)) {
                            routingContext.fail(IllegalArgumentException("Someone might have changed the resource in the meantime."))
                        }
                    }

                    wtx.remove()
                    wtx.commit()
                }

                promise.complete(wtx)
            }
        }
    }
}