package org.sirix.rest.crud

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.impl.HttpStatusException
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.Databases
import org.sirix.api.Database
import org.sirix.api.json.JsonResourceManager
import org.sirix.exception.SirixUsageException
import org.sirix.rest.crud.json.JsonGet
import org.sirix.rest.crud.xml.XmlGet
import org.sirix.service.json.serialize.StringValue
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

@Suppress("RedundantLambdaArrow")
class GetHandler(private val location: Path, private val keycloak: OAuth2Auth) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName: String? = ctx.pathParam("database")
        val resourceName: String? = ctx.pathParam("resource")
        val jsonBody = ctx.bodyAsJson
        val query: String? = ctx.queryParam("query").getOrElse(0) {
            jsonBody?.getString("query")
        }

        val acceptHeader = ctx.request().getHeader(HttpHeaders.ACCEPT)

        if (databaseName == null && resourceName == null) {
            if (query == null || query.isEmpty()) {
                listDatabases(ctx, context)
            } else {
                val startResultSeqIndexAsString =
                    ctx.queryParam("startResultSeqIndex").getOrNull(0)

                var startResultSeqIndex = startResultSeqIndexAsString?.toLong()

                if (startResultSeqIndex == null) {
                    startResultSeqIndex = jsonBody?.getLong("startResultSeqIndex")
                }

                val endResultSeqIndexAsSring =
                    ctx.queryParam("endResultSeqIndex").getOrNull(0)

                var endResultSeqIndex = endResultSeqIndexAsSring?.toLong()

                if (endResultSeqIndex == null) {
                    endResultSeqIndex = jsonBody?.getLong("endResultSeqIndex")
                }

                val body: String?

                with(acceptHeader) {
                    when {
                        contains("application/json") -> {
                            body = JsonGet(location, keycloak).xquery(
                                null,
                                null,
                                null,
                                null,
                                query,
                                ctx,
                                context,
                                ctx.get("user") as User,
                                startResultSeqIndex,
                                endResultSeqIndex
                            )
                        }
                        contains("application/xml") -> {
                            body = XmlGet(location, keycloak).xquery(
                                null,
                                null,
                                null,
                                null,
                                query,
                                ctx,
                                context,
                                ctx.get("user") as User,
                                startResultSeqIndex,
                                endResultSeqIndex
                            )
                        }
                        else -> {
                            body = JsonGet(location, keycloak).xquery(
                                null,
                                null,
                                null,
                                null,
                                query,
                                ctx,
                                context,
                                ctx.get("user") as User,
                                startResultSeqIndex,
                                endResultSeqIndex
                            )
                        }
                    }
                }

                if (body != null) {
                    ctx.response().end(body)
                } else {
                    ctx.response().end()
                }
            }
        } else if (databaseName != null && resourceName == null) {
            val buffer = StringBuilder()
            buffer.append("{")
            emitResourcesOfDatabase(buffer, location.resolve(databaseName), ctx)

            if (!ctx.failed()) {
                buffer.append("}")

                val content = buffer.toString()

                ctx.response().setStatusCode(200)
                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .putHeader(HttpHeaders.CONTENT_LENGTH, content.toByteArray(StandardCharsets.UTF_8).size.toString())
                    .write(content)
                    .result()
            }
        } else {
            with(acceptHeader) {
                @Suppress("IMPLICIT_CAST_TO_ANY")
                when {
                    contains("application/json") -> JsonGet(location, keycloak).handle(ctx)
                    contains("application/xml") -> XmlGet(location, keycloak).handle(ctx)
                    else -> JsonGet(location, keycloak).handle(ctx)
                }
            }
        }

        return ctx.currentRoute()
    }

    private suspend fun listDatabases(ctx: RoutingContext, context: Context) {
        context.executeBlockingAwait { _: Promise<Unit> ->
            val databases = Files.list(location)

            val buffer = StringBuilder()

            buffer.append("{\"databases\":[")

            databases.use {
                val databasesList = it.collect(Collectors.toList())
                val databaseDirectories =
                    databasesList.filter { database -> Files.isDirectory(database) }.toList()

                for ((index, database) in databaseDirectories.withIndex()) {
                    val databaseName = database.fileName
                    val databaseType = Databases.getDatabaseType(database.toAbsolutePath()).stringType
                    buffer.append(
                        "{\"name\":\"${StringValue.escape(databaseName.toString())}\",\"type\":\"${StringValue.escape(
                            databaseType
                        )}\""
                    )

                    val withResources = ctx.queryParam("withResources")
                    if (withResources.isNotEmpty() && withResources[0]!!.toBoolean()) {
                        buffer.append(",")
                        emitResourcesOfDatabase(buffer, databaseName, ctx)
                    }
                    buffer.append("}")

                    if (index != databaseDirectories.size - 1)
                        buffer.append(",")
                }
            }

            buffer.append("]}")

            val content = buffer.toString()

            ctx.response().setStatusCode(200)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .end(content)
        }
    }

    private fun emitResourcesOfDatabase(
        buffer: StringBuilder,
        databaseName: Path,
        ctx: RoutingContext
    ) {
        try {
            val database = Databases.openJsonDatabase(location.resolve(databaseName))

            database.use {
                buffer.append("\"resources\":[")
                emitCommaSeparatedResourceString(it, buffer)
                buffer.append("]")
            }
        } catch (e: SirixUsageException) {
            ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
        }
    }

    private fun emitCommaSeparatedResourceString(
        it: Database<JsonResourceManager>,
        buffer: StringBuilder
    ) {
        val resources = it.listResources()

        for ((index, resource) in resources.withIndex()) {
            buffer.append("\"${resource.fileName}\"")
            if (index != resources.size - 1)
                buffer.append(",")
        }
    }
}