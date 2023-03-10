package org.sirix.rest.crud

import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.access.trx.node.HashType
import org.sirix.index.IndexDef.DbType
import org.sirix.rest.crud.json.JsonCreate2
import org.sirix.rest.crud.xml.XmlCreate2
import java.nio.file.Files
import java.nio.file.Path

class CreateHandler(
        private val location: Path,
        private val createMultipleResources: Boolean = false
) {

    suspend fun handle(ctx: RoutingContext): Route? {
        val databaseName = ctx.pathParam("database")
        val resource = ctx.pathParam("resource")

        val contentType = ctx.request().getHeader(HttpHeaders.CONTENT_TYPE)

        val dbFile = location.resolve(databaseName)
        val vertxContext = ctx.vertx().orCreateContext
        createDatabaseIfNotExists(dbFile, vertxContext, contentType)

        if (resource == null) {
            ctx.response().setStatusCode(201).end()
            return ctx.currentRoute()
        }

        if (databaseName == null) {
            throw IllegalArgumentException("Database name and resource data to store not given.")
        }
        if (createMultipleResources) {
            createMultipleResources(databaseName, ctx)
            ctx.response().setStatusCode(201).end()
            return ctx.currentRoute()
        }
        shredder(databaseName, resource, ctx)

        return ctx.currentRoute()
    }

    private suspend fun shredder(
            databaseName: String, resPathName:String = databaseName,
            ctx: RoutingContext
    ) {
        val dbFile = location.resolve(databaseName)
        val contentType = ctx.request().getHeader(HttpHeaders.CONTENT_TYPE)
        val dispatcher = ctx.vertx().dispatcher()
        if(contentType.equals("application/json")) {
            JsonCreate2().insertResource(dbFile, resPathName, ctx)
        }
        else if(contentType.equals("application/xml")) {
            XmlCreate2().insertResource(dbFile, resPathName, dispatcher, ctx)
        }
        else if(contentType.equals("multipart/form-data")) {
            //TODO
        }

    }
    private suspend fun createDatabaseIfNotExists(
            dbFile: Path,
            context: Context,
            dbType: String
    ):DatabaseConfiguration?{
        return context.executeBlocking { promise: Promise<DatabaseConfiguration> ->
            val dbExists = Files.exists(dbFile)
            if(!dbExists) {
                Files.createDirectories(dbFile.parent)
            }

            val dbConfig = DatabaseConfiguration(dbFile)

            if(!Databases.existsDatabase(dbFile)) {
                if(dbType == "application/json") {
                    Databases.createJsonDatabase(dbConfig)
                }
                else if(dbType == "application/xml") {
                    Databases.createXmlDatabase(dbConfig)
                }
            }
            promise.complete(dbConfig)
        }.await()
    }

    private suspend fun createMultipleResources(
            databaseName: String,
            ctx: RoutingContext
    ) {
        val contentType = ctx.request().getHeader(HttpHeaders.CONTENT_TYPE)

        if(contentType.equals("application/json")) {
            JsonCreate2().createMultipleResources(location, databaseName, ctx)
        }
        else if(contentType.equals("application/xml")) {
            XmlCreate2().createMultipleResources(location, databaseName, ctx)
        }
        else if(contentType.equals("multipart/form-data")) {
            // TODO
        }
    }
}