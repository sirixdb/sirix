package org.sirix.rest.crud.json

import com.google.gson.stream.JsonReader
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import org.joda.time.Instant
import org.sirix.rest.Auth
import org.sirix.rest.AuthRole
import org.sirix.xquery.json.JsonDBCollection
import org.sirix.xquery.json.JsonDBStore
import java.nio.file.Path

class JsonSessionDBStore(
    private val ctx: RoutingContext,
    private val dbStore: JsonDBStore,
    private val user: User,
    private val authz: AuthorizationProvider
) : JsonDBStore by dbStore {
    override fun lookup(name: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.VIEW, authz)

        return dbStore.lookup(name)
    }

    override fun create(name: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name)
    }

    override fun create(name: String, path: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, path)
    }

    override fun create(name: String, path: String, commitMessage: String?,
                        commitTimestamp: Instant?): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, path, commitMessage, commitTimestamp)
    }

    override fun create(name: String, path: Path): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, path)
    }

    override fun create(name: String, path: Path, commitMessage: String?,
                        commitTimestamp: Instant?): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, path, commitMessage, commitTimestamp)
    }

    override fun create(name: String, resourceName: String, path: Path): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, path)
    }

    override fun create(name: String, resourceName: String, path: Path, commitMessage: String?,
                        commitTimestamp: Instant?): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, path, commitMessage, commitTimestamp)
    }

    override fun create(name: String, resourceName: String, json: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, json)
    }

    override fun create(
        name: String, resourceName: String, json: String, commitMessage: String?,
        commitTimestamp: Instant?
    ): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, json, commitMessage, commitTimestamp)
    }

    override fun create(name: String, resourceName: String, json: JsonReader): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, json)
    }

    override fun create(
        name: String,
        resourceName: String,
        json: JsonReader,
        commitMessage: String?,
        commitTimestamp: Instant?
    ): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, json, commitMessage, commitTimestamp)
    }

    override fun create(name: String, jsonReaders: Set<JsonReader>): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, jsonReaders)
    }

    override fun drop(name: String) {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.DELETE, authz)

        return dbStore.drop(name)
    }
}