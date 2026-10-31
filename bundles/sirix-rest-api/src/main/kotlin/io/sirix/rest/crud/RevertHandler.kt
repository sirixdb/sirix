package io.sirix.rest.crud

import io.sirix.access.DatabaseType
import io.sirix.access.Databases
import io.sirix.api.Database
import io.sirix.exception.SirixRevisionPrunedException
import io.sirix.rest.Auth
import io.sirix.rest.AuthRole
import io.vertx.core.http.HttpHeaders
import io.vertx.core.json.DecodeException
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.file.Path

/** Appends a whole-resource restoration while holding the resource's exclusive writer lock. */
class RevertHandler(private val location: Path, private val authz: AuthorizationProvider) {
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName = ctx.pathParam("database")!!
        val resourceName = ctx.pathParam("resource")!!
        PathValidation.validatePathParam(databaseName, "database")
        PathValidation.validatePathParam(resourceName, "resource")
        Auth.checkIfAuthorized(ctx.get<User>("user")!!, databaseName, AuthRole.MODIFY, authz)
        require(ctx.queryParams().isEmpty) { "Revert accepts a JSON body and always restores the whole resource" }
        val body = try {
            ctx.body().asJsonObject() ?: throw IllegalArgumentException("A JSON object body is required")
        } catch (e: DecodeException) {
            throw IllegalArgumentException("A JSON object body is required", e)
        } catch (e: ClassCastException) {
            // Vert.x decodes valid non-object JSON before casting it to JsonObject.
            throw IllegalArgumentException("A JSON object body is required", e)
        }
        require(body.fieldNames().all { it == "targetRevision" || it == "expectedHead" || it == "message" }) {
            "Supported fields are targetRevision, expectedHead and message"
        }
        val targetRevision = positiveRevision(body, "targetRevision")
        val expectedHead = if (body.containsKey("expectedHead")) positiveRevision(body, "expectedHead") else null
        val message = body.getValue("message")
        require(message == null || message is String) { "message must be a string" }
        val author = SirixDBUser.create(ctx)
        val dbPath = location.resolve(databaseName)

        val result = withContext(Dispatchers.IO) {
            if (!Databases.existsDatabase(dbPath)) throw HttpException(404, "Database not found")
            @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
            val database: Database<*> = when (Databases.getDatabaseType(dbPath)) {
                DatabaseType.JSON -> Databases.openJsonDatabase(dbPath, author)
                DatabaseType.XML -> Databases.openXmlDatabase(dbPath, author)
            }
            database.use {
                if (!database.existsResource(resourceName)) throw HttpException(404, "Resource not found")
                database.beginResourceSession(resourceName).use { session ->
                    // Acquiring the writer precedes the head check. A concurrent writer cannot
                    // publish between this check and commit, including through another session.
                    session.beginNodeTrx().use { wtx ->
                        val newRevision = wtx.revisionNumber
                        val previousHead = newRevision - 1
                        if (expectedHead != null && expectedHead != previousHead) {
                            throw HttpException(409, "Expected head $expectedHead but current head is $previousHead")
                        }
                        require(targetRevision <= previousHead) { "targetRevision must not exceed the current head" }
                        try {
                            wtx.revertTo(targetRevision)
                        } catch (e: SirixRevisionPrunedException) {
                            throw HttpException(410, "Target revision $targetRevision was pruned")
                        }
                        wtx.commit(message as String?)
                        JsonObject().put("revision", newRevision).put("previousHead", previousHead)
                    }
                }
            }
        }
        ctx.response().setStatusCode(201).putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(result.encode())
        return ctx.currentRoute()!!
    }

    private fun positiveRevision(body: JsonObject, name: String): Int {
        val value = body.getValue(name)
        require(value is Int || value is Long) { "$name must be a positive 32-bit integer" }
        val revision = (value as Number).toLong()
        require(revision in 1..Int.MAX_VALUE.toLong()) { "$name must be a positive 32-bit integer" }
        return revision.toInt()
    }
}
