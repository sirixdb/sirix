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
import org.sirix.api.ResourceSession
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

            promise.complete()
        }.await()
    }

    abstract fun createStore(ctx: RoutingContext): StructuredItemStore

    protected suspend fun delete(databaseName: String, resPathName: String?, nodeId: Long?, ctx: RoutingContext) {
        val dbFile = location.resolve(databaseName)
        val context = ctx.vertx().orCreateContext
        val dispatcher = ctx.vertx().dispatcher()

        if (resPathName == null) {
            if (!Files.exists(dbFile)) {
                throw HttpException(
                    HttpResponseStatus.NOT_FOUND.code(),
                    IllegalStateException("Database not found.")
                )
            }
            removeDatabase(dbFile, dispatcher)
            return
        }

        val sirixDBUser = SirixDBUser.create(ctx)

        val database = database(dbFile, sirixDBUser)

        database.use {
            if (!database.existsResource(resPathName)) {
                throw HttpException(
                    HttpResponseStatus.NOT_FOUND.code(),
                    IllegalStateException("Resource not found.")
                )
            }

            if (nodeId == null) {
                removeResource(dispatcher, database, resPathName)
            } else {
                removeSubtree(database, resPathName, nodeId, context, ctx)
            }
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
        ctx: Context,
        routingCtx: RoutingContext
    ) {
        ctx.executeBlocking { promise: Promise<Unit> ->
            val manager = database.beginResourceSession(resPathName)
            manager.use {
                val wtx = manager.beginNodeTrx()

                wtx.use {
                    if (wtx.moveTo(nodeId)) {
                        if (hashType(manager) != HashType.NONE && !wtx.isDocumentRoot) {
                            val hashCode = routingCtx.request().getHeader(HttpHeaders.ETAG)
                                ?: throw IllegalStateException("Hash code is missing in ETag HTTP-Header.")

                            if (wtx.hash != BigInteger(hashCode)) {
                                throw IllegalArgumentException("Someone might have changed the resource in the meantime.")
                            }
                        }

                        wtx.remove()
                        wtx.commit()
                    }
                }
            }

            promise.complete()
        }.await()
    }

    protected abstract fun hashType(manager: ResourceSession<*, *>): HashType
}