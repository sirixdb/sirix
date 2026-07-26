package io.sirix.rest.crud.json

import com.google.gson.stream.JsonReader
import io.brackit.query.jdm.json.Object
import io.sirix.query.json.JsonDBCollection
import io.sirix.query.json.JsonDBItem
import io.sirix.rest.Auth
import io.sirix.rest.AuthRole
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import java.io.InputStream
import java.nio.file.Path

/**
 * A [JsonDBCollection] decorator that enforces per-request authorization on every mutating
 * operation, by composition (Kotlin interface delegation) rather than inheritance.
 *
 * The REST query endpoint runs under the VIEW role and injects a [JsonSessionDBStore] into the
 * brackit query context. Looking a collection up is a read (VIEW) and legitimately returns a
 * collection, but the raw collection then exposes `add`/`remove`/`delete`, which a query such as
 * `jn:store(...)` calls directly on the collection object — bypassing the store-level checks. Every
 * read is delegated straight to [wrapped]; the mutating methods re-check the caller's role first,
 * so a VIEW-only token cannot mutate through a looked-up collection.
 */
class AuthCheckingJsonDBCollection(
    private val wrapped: JsonDBCollection,
    private val user: User,
    private val authz: AuthorizationProvider
) : JsonDBCollection by wrapped {

    private val collectionName: String = wrapped.getName()

    override fun delete() {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.DELETE, authz)
        wrapped.delete()
    }

    override fun remove(documentID: Long) {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.DELETE, authz)
        wrapped.remove(documentID)
    }

    override fun add(resourceName: String, reader: JsonReader, options: Object): JsonDBItem {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.CREATE, authz)
        return wrapped.add(resourceName, reader, options)
    }

    override fun add(resourceName: String, reader: JsonReader): JsonDBItem {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.CREATE, authz)
        return wrapped.add(resourceName, reader)
    }

    override fun add(json: String): JsonDBItem {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.CREATE, authz)
        return wrapped.add(json)
    }

    override fun add(file: Path): JsonDBItem {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.CREATE, authz)
        return wrapped.add(file)
    }

    override fun add(inputStream: InputStream): JsonDBItem {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.CREATE, authz)
        return wrapped.add(inputStream)
    }

    // Interface delegation does not forward Object methods; key equality on the underlying
    // database (as the concrete collection does) so the decorator remains a drop-in replacement.
    override fun equals(other: Any?): Boolean = wrapped == other

    override fun hashCode(): Int = wrapped.hashCode()

    override fun toString(): String = wrapped.toString()
}
