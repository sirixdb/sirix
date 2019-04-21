package org.sirix.rest.crud.xml

import io.vertx.core.Context
import io.vertx.core.Future
import io.vertx.ext.auth.User
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import org.sirix.access.Databases
import org.sirix.api.Database
import org.sirix.api.xml.XmlNodeTrx
import org.sirix.api.xml.XmlResourceManager
import org.sirix.rest.XmlSessionDBStore
import org.sirix.xquery.node.BasicXmlDBStore
import java.nio.file.Files
import java.nio.file.Path

class XmlDelete(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val dbName = ctx.pathParam("database")
        val resName: String? = ctx.pathParam("resource")
        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        if (dbName == null) {
            // Initialize queryResource context and store.
            val dbStore = XmlSessionDBStore(BasicXmlDBStore.newBuilder().build(), ctx.get("user") as User)

            ctx.vertx().executeBlockingAwait { future: Future<Nothing> ->
                val databases = Files.list(location)

                databases.use {
                    databases.filter { Files.isDirectory(it) }.forEach {
                        dbStore.drop(it.fileName.toString())
                    }
                }

                future.complete(null)
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

        val database = Databases.openXmlDatabase(dbFile)

        database.use {
            if (nodeId == null) {
                removeResource(dispatcher, database, resPathName, ctx)
            } else {
                val manager = database.openResourceManager(resPathName)

                removeSubtree(manager, nodeId, context)
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

    private suspend fun removeResource(dispatcher: CoroutineDispatcher, database: Database<XmlResourceManager>,
                                       resPathName: String?,
                                       ctx: RoutingContext): Any? {
        return try {
            withContext(dispatcher) {
                database.removeResource(resPathName)
            }
        } catch (e: IllegalStateException) {
            ctx.fail(IllegalStateException("Open resource managers found."))
        }
    }

    private suspend fun removeSubtree(manager: XmlResourceManager, nodeId: Long, context: Context): XmlNodeTrx? {
        return context.executeBlockingAwait { future: Future<XmlNodeTrx> ->
            manager.use { resourceManager ->
                val wtx = resourceManager.beginNodeTrx()

                wtx.moveTo(nodeId)

                wtx.remove()
                wtx.commit()

                future.complete(wtx)
            }
        }
    }
}