package io.sirix.rest.crud.json

import com.google.gson.stream.JsonReader
import io.brackit.query.atomic.Str
import io.brackit.query.jdm.Stream
import io.brackit.query.jdm.json.Object
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.RoutingContext
import io.sirix.rest.Auth
import io.sirix.rest.AuthRole
import io.sirix.query.MutationAuthorizer
import io.sirix.query.json.JsonDBCollection
import io.sirix.query.json.JsonDBStore
import java.nio.file.Path

class JsonSessionDBStore(
    private val ctx: RoutingContext,
    private val dbStore: JsonDBStore,
    private val user: User,
    private val authz: AuthorizationProvider
) : JsonDBStore by dbStore, MutationAuthorizer {

    /**
     * Bind [MutationAuthorizer]'s guard to the request identity using the full collection name.
     */
    override fun authorizeWrite(databaseName: String, resourceName: String) {
        Auth.checkIfAuthorized(user, databaseName, AuthRole.MODIFY, authz)
    }

    /**
     * Authorize creating a resource a query is about to store or load into.
     *
     * `jn:store` and `jn:load` create a resource that does not exist yet, so the permission is
     * CREATE rather than MODIFY - the same role the HTTP PUT routes and the store's own `create`
     * overloads require. Which of the two applies is decided by the target's existence, in
     * [io.sirix.query.MutationAuthorization].
     */
    override fun authorizeCreate(databaseName: String, resourceName: String?) {
        Auth.checkIfAuthorized(user, databaseName, AuthRole.CREATE, authz)
    }

    /**
     * Wrap a collection so that every mutating operation performed through it re-checks the
     * caller's authorization. Without this the store-level checks are bypassable: a VIEW token can
     * [lookup] a collection (a read) and then call `add`/`remove`/`delete` directly on it.
     */
    private fun wrap(collection: JsonDBCollection): JsonDBCollection =
        AuthCheckingJsonDBCollection(collection, user, authz)

    override fun lookup(name: String): JsonDBCollection? {
        Auth.checkIfAuthorized(user, name, AuthRole.VIEW, authz)

        // The undecorated store answers null for a database that does not exist. Decorating null
        // threw NullPointerException from the decorator's own initialiser, so absence must pass
        // through: a caller asking whether a target exists is entitled to the answer "it does not".
        val collection = dbStore.lookup(name) ?: return null
        return wrap(collection)
    }

    override fun create(name: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name))
    }

    override fun create(name: String, path: String): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, path))
    }

    override fun create(name: String, path: String, options: Object): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, path, options))
    }

    override fun create(name: String, path: Path): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, path))
    }

    override fun create(name: String, path: Path, options: Object): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, path, options))
    }

    override fun createFromPaths(name: String, paths: Stream<Path>): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.createFromPaths(name, paths))
    }

    override fun create(name: String, resourceName: String?, path: Path): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, resourceName, path))
    }

    override fun create(name: String, resourceName: String?, path: Path, options: Object): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, resourceName, path, options))
    }

    override fun create(name: String, resourceName: String?, json: String?): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, resourceName, json))
    }

    override fun create(
        name: String, resourceName: String?, json: String?, options: Object
    ): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, resourceName, json, options))
    }

    override fun create(name: String, resourceName: String?, json: JsonReader): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, resourceName, json))
    }

    override fun create(
        name: String,
        resourceName: String?,
        json: JsonReader,
        options: Object
    ): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, resourceName, json, options))
    }

    override fun create(name: String, jsonReaders: Set<JsonReader>): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, jsonReaders))
    }

    override fun create(name: String, jsonReaders: Set<JsonReader>, options: Object): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.create(name, jsonReaders, options))
    }

    override fun createFromJsonStrings(name: String, jsons: Stream<Str>): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.createFromJsonStrings(name, jsons))
    }

    override fun createFromJsonStrings(name: String, jsons: Stream<Str>, options: Object): JsonDBCollection {
        authorizeReplacement(name)

        return wrap(dbStore.createFromJsonStrings(name, jsons, options))
    }

    private fun authorizeReplacement(name: String) {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)
        if (dbStore.lookup(name) != null) {
            Auth.checkIfAuthorized(user, name, AuthRole.DELETE, authz)
        }
    }

    override fun drop(name: String) {
        Auth.checkIfAuthorized(user, name, AuthRole.DELETE, authz)

        return dbStore.drop(name)
    }

    override fun drop(name: String, databasePath: Path) {
        Auth.checkIfAuthorized(user, name, AuthRole.DELETE, authz)

        return dbStore.drop(name, databasePath)
    }

    override fun makeDir(path: String) {
        Auth.checkIfAuthorized(user, path, AuthRole.CREATE, authz)

        return dbStore.makeDir(path)
    }
}
