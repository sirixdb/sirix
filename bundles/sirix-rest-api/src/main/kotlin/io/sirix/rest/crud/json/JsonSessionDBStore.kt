package io.sirix.rest.crud.json

import com.google.gson.stream.JsonReader
import io.brackit.query.jdm.json.Object
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import io.sirix.rest.AuthRole
import io.sirix.query.json.JsonDBCollection
import io.sirix.query.json.JsonDBStore
import java.nio.file.Path
import java.time.Instant

class JsonSessionDBStore(
    private val ctx: RoutingContext,
    private val dbStore: JsonDBStore,
    private val user: User,
    private val authz: AuthorizationProvider
) : JsonDBStore by dbStore {
    override fun lookup(name: String): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.VIEW, authz)

        return dbStore.lookup(name)
    }

    override fun create(name: String): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name)
    }

    override fun create(name: String, path: String): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, path)
    }

    override fun create(name: String, path: String, options: Object): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, path, options)
    }

    override fun create(name: String, path: Path): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, path)
    }

    override fun create(name: String, path: Path, options: Object): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, path, options)
    }

    override fun create(name: String, resourceName: String, path: Path): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, path)
    }

    override fun create(name: String, resourceName: String, path: Path, options: Object): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, path, options)
    }

    override fun create(name: String, resourceName: String, json: String): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, json)
    }

    override fun create(
        name: String, resourceName: String, json: String, options: Object
    ): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, json, options)
    }

    override fun create(name: String, resourceName: String, json: JsonReader): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, json)
    }

    override fun create(
        name: String,
        resourceName: String,
        json: JsonReader,
        options: Object
    ): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, resourceName, json, options)
    }

    override fun create(name: String, jsonReaders: Set<JsonReader>): JsonDBCollection {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.CREATE, authz)

        return dbStore.create(name, jsonReaders)
    }

    override fun drop(name: String) {
        io.sirix.rest.Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), name, AuthRole.DELETE, authz)

        return dbStore.drop(name)
    }
}