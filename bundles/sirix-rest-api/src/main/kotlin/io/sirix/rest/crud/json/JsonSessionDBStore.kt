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
import io.sirix.query.function.ImportInput
import io.sirix.query.ContentOnlyStore
import io.sirix.query.MutationAuthorization
import io.sirix.rest.crud.SessionMutationAuthorizer
import io.sirix.query.MutationAuthorizer
import io.sirix.query.json.JsonDBCollection
import io.sirix.query.json.JsonDBStore
import java.nio.file.Path

class JsonSessionDBStore(
    private val ctx: RoutingContext,
    private val dbStore: JsonDBStore,
    private val user: User,
    private val authz: AuthorizationProvider
) : JsonDBStore by dbStore, MutationAuthorizer by SessionMutationAuthorizer(user, authz), ContentOnlyStore {

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

        return MutationAuthorization.where(this).call<JsonDBCollection, RuntimeException> { wrap(dbStore.create(name)) }
    }

    override fun create(name: String, path: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return MutationAuthorization.where(this).call<JsonDBCollection, RuntimeException> { wrap(dbStore.create(name, path)) }
    }

    override fun create(name: String, path: String, options: Object): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return MutationAuthorization.where(this).call<JsonDBCollection, RuntimeException> { wrap(dbStore.create(name, path, options)) }
    }

    override fun create(name: String, path: Path): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        throw ImportInput.invalidContent("JSON")
    }

    override fun create(name: String, path: Path, options: Object): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        throw ImportInput.invalidContent("JSON")
    }

    override fun createFromPaths(name: String, paths: Stream<Path>): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        throw ImportInput.invalidContent("JSON")
    }

    override fun create(name: String, resourceName: String?, path: Path): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        throw ImportInput.invalidContent("JSON")
    }

    override fun create(name: String, resourceName: String?, path: Path, options: Object): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        throw ImportInput.invalidContent("JSON")
    }

    override fun create(name: String, resourceName: String?, json: String?): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return MutationAuthorization.where(this).call<JsonDBCollection, RuntimeException> { wrap(dbStore.create(name, resourceName, json)) }
    }

    override fun create(
        name: String, resourceName: String?, json: String?, options: Object
    ): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return MutationAuthorization.where(this).call<JsonDBCollection, RuntimeException> { wrap(dbStore.create(name, resourceName, json, options)) }
    }

    override fun create(name: String, resourceName: String?, json: JsonReader): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return MutationAuthorization.where(this).call<JsonDBCollection, RuntimeException> { wrap(dbStore.create(name, resourceName, json)) }
    }

    override fun create(
        name: String,
        resourceName: String?,
        json: JsonReader,
        options: Object
    ): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return MutationAuthorization.where(this).call<JsonDBCollection, RuntimeException> { wrap(dbStore.create(name, resourceName, json, options)) }
    }

    override fun create(name: String, jsonReaders: Set<JsonReader>): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return MutationAuthorization.where(this).call<JsonDBCollection, RuntimeException> { wrap(dbStore.create(name, jsonReaders)) }
    }

    override fun create(name: String, jsonReaders: Set<JsonReader>, options: Object): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return MutationAuthorization.where(this).call<JsonDBCollection, RuntimeException> { wrap(dbStore.create(name, jsonReaders, options)) }
    }

    override fun createFromUris(name: String, uris: List<String>, options: Object): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        throw ImportInput.invalidContent("JSON")
    }

    override fun createFromJsonStrings(name: String, jsons: Stream<Str>): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return MutationAuthorization.where(this).call<JsonDBCollection, RuntimeException> { wrap(dbStore.createFromJsonStrings(name, jsons)) }
    }

    override fun createFromJsonStrings(name: String, jsons: Stream<Str>, options: Object): JsonDBCollection {
        Auth.checkIfAuthorized(user, name, AuthRole.CREATE, authz)

        return MutationAuthorization.where(this).call<JsonDBCollection, RuntimeException> { wrap(dbStore.createFromJsonStrings(name, jsons, options)) }
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
