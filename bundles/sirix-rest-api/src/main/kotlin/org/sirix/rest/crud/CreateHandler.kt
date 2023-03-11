package org.sirix.rest.crud

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import org.sirix.access.DatabasesInternals
import org.sirix.rest.crud.json.JsonCreate2
import org.sirix.rest.crud.xml.XMLCreate2
import org.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory
import java.nio.file.Path

private val logger = LogWrapper(LoggerFactory.getLogger(DatabasesInternals::class.java))

class CreateHandler(private val location: Path,
                    private val createMultipleResources: Boolean) {
    suspend fun handle(ctx: RoutingContext): Route {
        var abstractCreateHandler: AbstractCreateHandler? = null
        when(ctx.request().getHeader(HttpHeaders.CONTENT_TYPE)) {
            "application/json" -> {
                abstractCreateHandler = JsonCreate2(location, createMultipleResources)
            }

            "application/xml" -> {
                abstractCreateHandler = XMLCreate2(location, createMultipleResources)
            }
        }

        val databaseName = ctx.pathParam("database")
        val resource = ctx.pathParam("resource")

        val dbFile = location.resolve(databaseName)
        val vertxContext = ctx.vertx().orCreateContext

        abstractCreateHandler?.createDatabaseIfNotExists(dbFile, vertxContext)

        if (resource == null) {
            ctx.response().setStatusCode(201).end()
            return ctx.currentRoute()
        }

        if (databaseName == null) {
            throw IllegalArgumentException("Database name and resource data to store not given.")
        }

        if (createMultipleResources) {
            abstractCreateHandler?.createMultipleResources(databaseName, ctx)
            ctx.response().setStatusCode(201).end()
            return ctx.currentRoute()
        }

        abstractCreateHandler?.shredder(databaseName, resource, ctx)
        return ctx.currentRoute()
    }
}