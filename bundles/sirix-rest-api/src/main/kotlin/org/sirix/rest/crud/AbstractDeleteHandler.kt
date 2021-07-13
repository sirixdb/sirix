package org.sirix.rest.crud

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import org.brackit.xquery.xdm.StructuredItemStore
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.access.User
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.ResourceManager
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path

abstract class AbstractDeleteHandler(protected val location: Path) {
    protected suspend fun dropDatabasesOfType(ctx: RoutingContext, dbType: DatabaseType) {
        // Initialize queryResource context and store.
        val dbStore = createStore(ctx)

        ctx.vertx().executeBlocking { promise: Promise<Unit> ->
            val databases = Files.list(location)

            databases.use {
                databases.filter { Files.isDirectory(it) && Databases.getDatabaseType(it) == dbType }
                    .forEach {
                        dbStore.drop(it.fileName.toString())
                    }
            }

            ctx.response().setStatusCode(204).end()
            promise.complete(null)
        }.await()
    }

    abstract fun createStore(ctx: RoutingContext): StructuredItemStore

    protected suspend fun delete(databaseName: String, resPathName: String?, nodeId: Long?, ctx: RoutingContext) {
        val dbFile = location.resolve(databaseName)
        val context = ctx.vertx().orCreateContext
        val dispatcher = ctx.vertx().dispatcher()

        if (resPathName == null) {
            if (!Files.exists(dbFile)) {
                ctx.fail(
                    HttpException(
                        HttpResponseStatus.NOT_FOUND.code(),
                        IllegalStateException("Database not found.")
                    )
                )
                return
            }
            removeDatabase(dbFile, dispatcher)
            ctx.response().setStatusCode(204).end()
            return
        }

        val sirixDBUser = SirixDBUser.create(ctx)

        val database = database(dbFile, sirixDBUser)

        database.use {
            if (!database.existsResource(resPathName)) {
                ctx.fail(
                    HttpException(
                        HttpResponseStatus.NOT_FOUND.code(),
                        IllegalStateException("Resource not found.")
                    )
                )
                return@use
            }

            if (nodeId == null) {
                removeResource(dispatcher, database, resPathName)
            } else {
                removeSubtree(database, resPathName, nodeId, context, ctx)
            }
        }

        if (!ctx.failed()) {
            ctx.response().setStatusCode(204).end()
        }
    }

    abstract fun database(
        dbFile: Path,
        sirixDBUser: User
    ): Database<*>

    private suspend fun removeDatabase(dbFile: Path?, dispatcher: CoroutineDispatcher) {
        withContext(dispatcher) {
            Databases.removeDatabase(dbFile)
        }
    }

    private suspend fun removeResource(
        dispatcher: CoroutineDispatcher, database: Database<*>,
        resPathName: String?
    ) {
        withContext(dispatcher) {
            database.removeResource(resPathName)
        }
    }

    private suspend fun removeSubtree(
        database: Database<*>,
        resPathName: String,
        nodeId: Long,
        context: Context,
        routingContext: RoutingContext
    ) {
        context.executeBlocking { promise: Promise<Unit> ->
            val manager = database.openResourceManager(resPathName)
            manager.use {
                val wtx = manager.beginNodeTrx()

                wtx.use {
                    if (wtx.moveTo(nodeId).hasMoved()) {
                        if (hashType(manager) != HashType.NONE && !wtx.isDocumentRoot) {
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
                }

                promise.complete()
            }
        }.await()
    }

    protected abstract fun hashType(manager: ResourceManager<*, *>): HashType
}