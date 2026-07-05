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
import io.sirix.query.json.JsonDBCollection
import io.sirix.query.json.JsonDBStore
import java.nio.file.Path

class JsonSessionDBStore(
    private val ctx: RoutingContext,
    private val dbStore: JsonDBStore,
    private val user: User,
    private val authz: AuthorizationProvider
) : JsonDBStore by dbStore {

    /**
     * Wrap a collection so that every mutating operation performed through it re-checks the
     * caller's authorization. Without this the store-level checks are bypassable: a VIEW token can
     * [lookup] a collection (a read) and then call `add`/`remove`/`delete` directly on it.
     */
    private fun wrap(collection: JsonDBCollection): JsonDBCollection =
        AuthCheckingJsonDBCollection(collection, user, authz)

    override fun lookup(name: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.VIEW, authz)

        return wrap(dbStore.lookup(name))
    }

    override fun create(name: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name))
    }

    override fun create(name: String, path: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, path))
    }

    override fun create(name: String, path: String, options: Object): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, path, options))
    }

    override fun create(name: String, path: Path): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, path))
    }

    override fun create(name: String, path: Path, options: Object): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, path, options))
    }

    override fun createFromPaths(name: String, paths: Stream<Path>): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.createFromPaths(name, paths))
    }

    override fun create(name: String, resourceName: String, path: Path): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, resourceName, path))
    }

    override fun create(name: String, resourceName: String, path: Path, options: Object): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, resourceName, path, options))
    }

    override fun create(name: String, resourceName: String, json: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, resourceName, json))
    }

    override fun create(
        name: String, resourceName: String, json: String, options: Object
    ): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, resourceName, json, options))
    }

    override fun create(name: String, resourceName: String, json: JsonReader): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, resourceName, json))
    }

    override fun create(
        name: String,
        resourceName: String,
        json: JsonReader,
        options: Object
    ): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, resourceName, json, options))
    }

    override fun create(name: String, jsonReaders: Set<JsonReader>): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, jsonReaders))
    }

    override fun create(name: String, jsonReaders: Set<JsonReader>, options: Object): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.create(name, jsonReaders, options))
    }

    override fun createFromJsonStrings(name: String, jsons: Stream<Str>): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return wrap(dbStore.createFromJsonStrings(name, jsons))
    }

    override fun drop(name: String) {
        Auth.checkIfAuthorized(user, name, AuthRole.DELETE, authz)

        return dbStore.drop(name)
    }

    override fun makeDir(path: String) {
        Auth.checkIfAuthorized(user, path, AuthRole.CREATE, authz)

        return dbStore.makeDir(path)
    }
}
