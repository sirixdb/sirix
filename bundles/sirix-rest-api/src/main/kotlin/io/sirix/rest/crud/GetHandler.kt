package io.sirix.rest.crud

import io.vertx.core.Context
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.coAwait
import io.sirix.access.Databases
import io.sirix.api.Database
import io.sirix.api.json.JsonResourceSession
import io.sirix.rest.crud.json.JsonGet
import io.sirix.rest.crud.xml.XmlGet
import io.sirix.service.json.serialize.StringValue
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

@Suppress("RedundantLambdaArrow")
class GetHandler(
    private val location: Path,
    private val keycloak: OAuth2Auth,
    private val authz: AuthorizationProvider
) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName: String? = ctx.pathParam("database")
        val resourceName: String? = ctx.pathParam("resource")
        val jsonBody = ctx.body().asJsonObject()
        val query: String? = ctx.queryParam("query").getOrElse(0) {
            jsonBody?.getString("query")
        }

        val acceptHeader = ctx.request().getHeader(HttpHeaders.ACCEPT) ?: "application/json"

        if (databaseName == null && resourceName == null) {
            if (query.isNullOrEmpty()) {
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
                            body = JsonGet(location, keycloak, authz).xquery(
                                null,
                                null,
                                null,
                                null,
                                query,
                                ctx,
                                context,
                                ctx.get("user") as User,
                                startResultSeqIndex,
                                endResultSeqIndex,
                                jsonBody
                            )
                        }

                        contains("application/xml") -> {
                            body = XmlGet(location, keycloak, authz).xquery(
                                null,
                                null,
                                null,
                                null,
                                query,
                                ctx,
                                context,
                                ctx.get("user") as User,
                                startResultSeqIndex,
                                endResultSeqIndex,
                                jsonBody
                            )
                        }

                        else -> {
                            body = JsonGet(location, keycloak, authz).xquery(
                                null,
                                null,
                                null,
                                null,
                                query,
                                ctx,
                                context,
                                ctx.get("user") as User,
                                startResultSeqIndex,
                                endResultSeqIndex,
                                jsonBody
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
            val databasePath = location.resolve(databaseName)
            // Check if database exists first
            if (!Databases.existsDatabase(databasePath)) {
                ctx.response().setStatusCode(404).end()
                return ctx.currentRoute()
            }
            val buffer = StringBuilder()
            buffer.append("{")
            val databaseType = Databases.getDatabaseType(databasePath.toAbsolutePath()).stringType
            emitResourcesOfDatabase(buffer, databasePath, databaseType)

            if (!ctx.failed()) {
                buffer.append("}")

                val content = buffer.toString()

                val res = ctx.response().setStatusCode(200)
                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .putHeader(HttpHeaders.CONTENT_LENGTH, content.toByteArray(StandardCharsets.UTF_8).size.toString())
                res.write(content)
                res.end()
            }
        } else {
            with(acceptHeader) {
                @Suppress("IMPLICIT_CAST_TO_ANY")
                when {
                    contains("application/json") -> JsonGet(location, keycloak, authz).handle(ctx)
                    contains("application/xml") -> XmlGet(location, keycloak, authz).handle(ctx)
                    else -> JsonGet(location, keycloak, authz).handle(ctx)
                }
            }
        }

        return ctx.currentRoute()
    }

    private suspend fun listDatabases(ctx: RoutingContext, context: Context) {
        context.executeBlocking {
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
                        "{\"name\":\"${StringValue.escape(databaseName.toString())}\",\"type\":\"${
                            StringValue.escape(
                                databaseType
                            )
                        }\""
                    )

                    val withResources = ctx.queryParam("withResources")
                    if (withResources.isNotEmpty() && withResources[0]!!.toBoolean()) {
                        buffer.append(",")
                        emitResourcesOfDatabase(buffer, databaseName, databaseType)
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
        }.coAwait()
    }

    private fun emitResourcesOfDatabase(
        buffer: StringBuilder,
        databasePath: Path,
        databaseType: String
    ) {
        val resolvedPath = if (databasePath.isAbsolute) databasePath else location.resolve(databasePath)
        
        val database: Database<*> = when (databaseType) {
            "json" -> Databases.openJsonDatabase(resolvedPath)
            "xml" -> Databases.openXmlDatabase(resolvedPath)
            else -> throw IllegalArgumentException("Unsupported database type: $databaseType")
        }

        database.use {
            buffer.append("\"resources\":[")
            emitCommaSeparatedResourceString(it, buffer)
            buffer.append("]")
        }
    }

    private fun emitCommaSeparatedResourceString(
        database: Database<*>,
        buffer: StringBuilder
    ) {
        val resources = database.listResources()

        for ((index, resource) in resources.withIndex()) {
            buffer.append("\"${resource.fileName}\"")
            if (index != resources.size - 1)
                buffer.append(",")
        }
    }
}