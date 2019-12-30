package org.sirix.rest.crud.xml

import io.vertx.core.Context
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.xml.XmlNodeTrx
import org.sirix.api.xml.XmlResourceManager
import org.sirix.rest.crud.SirixDBUtils
import org.sirix.xquery.node.BasicXmlDBStore
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path

class XmlDelete(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName: String? = ctx.pathParam("database")
        val resource: String? = ctx.pathParam("resource")
        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        if (databaseName == null) {
            // Initialize queryResource context and store.
            val dbStore = XmlSessionDBStore(ctx, BasicXmlDBStore.newBuilder().build(), ctx.get("user") as User)

            ctx.vertx().executeBlockingAwait { promise: Promise<Nothing> ->
                val databases = Files.list(location)

                databases.use {
                    databases.filter { Files.isDirectory(it) && Databases.getDatabaseType(it) == DatabaseType.XML }
                        .forEach {
                            dbStore.drop(it.fileName.toString())
                        }
                }

                ctx.response().setStatusCode(200).end()
                promise.complete(null)
            }
        } else {
            delete(databaseName, resource, nodeId?.toLongOrNull(), ctx)
        }

        return ctx.currentRoute()
    }

    private suspend fun delete(databaseName: String, resPathName: String?, nodeId: Long?, ctx: RoutingContext) {
        val dbFile = location.resolve(databaseName)
        val context = ctx.vertx().orCreateContext
        val dispatcher = ctx.vertx().dispatcher()

        if (resPathName == null) {
            removeDatabase(dbFile, dispatcher)
            ctx.response().setStatusCode(200).end()
            return
        }

        val sirixDBUser = SirixDBUtils.createSirixDBUser(ctx)

        val database = Databases.openXmlDatabase(dbFile, sirixDBUser)

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
        dispatcher: CoroutineDispatcher, database: Database<XmlResourceManager>,
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
        manager: XmlResourceManager,
        nodeId: Long,
        context: Context,
        routingContext: RoutingContext
    ): XmlNodeTrx? {
        return context.executeBlockingAwait { promise: Promise<XmlNodeTrx> ->
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