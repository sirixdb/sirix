package org.sirix.rest.crud.json

import com.google.gson.stream.JsonReader
import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.ext.auth.isAuthorizedAwait
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.sirix.rest.AuthRole
import org.sirix.xquery.json.JsonDBCollection
import org.sirix.xquery.json.JsonDBStore
import java.lang.IllegalStateException
import java.nio.file.Path

class JsonSessionDBStore(private val ctx: RoutingContext, private val dbStore: JsonDBStore, private val user: User) : JsonDBStore by dbStore {
    override fun lookup(name: String): JsonDBCollection {
        checkIfAuthorized(name, AuthRole.VIEW)

        return dbStore.lookup(name)
    }

    override fun create(name: String): JsonDBCollection {
        checkIfAuthorized(name, AuthRole.CREATE)

        return dbStore.create(name)
    }

    override fun create(name: String, path: String): JsonDBCollection {
        checkIfAuthorized(name, AuthRole.CREATE)

        return dbStore.create(name, path)
    }

    override fun create(name: String, path: Path): JsonDBCollection {
        checkIfAuthorized(name, AuthRole.CREATE)

        return dbStore.create(name, path)
    }

    override fun create(collName: String, resourceName: String, path: Path): JsonDBCollection {
        checkIfAuthorized(collName, AuthRole.CREATE)

        return dbStore.create(collName, path)
    }

    override fun create(collName: String, resourceName: String, json: String): JsonDBCollection {
        checkIfAuthorized(collName, AuthRole.CREATE)

        return dbStore.create(collName, resourceName, json)
    }

    override fun create(collName: String, resourceName: String, json: JsonReader): JsonDBCollection {
        checkIfAuthorized(collName, AuthRole.CREATE)

        return dbStore.create(collName, resourceName, json)
    }

    override fun create(collName: String, jsonReaders: Set<JsonReader>): JsonDBCollection {
        checkIfAuthorized(collName, AuthRole.CREATE)

        return dbStore.create(collName, jsonReaders)
    }

    override fun drop(name: String) {
        checkIfAuthorized(name, AuthRole.DELETE)

        return dbStore.drop(name)
    }

    private fun checkIfAuthorized(name: String, role: AuthRole) {
        GlobalScope.launch(ctx.vertx().dispatcher()) {
            val isAuthorized = user.isAuthorizedAwait(role.databaseRole(name));

            require(isAuthorized || user.isAuthorizedAwait(role.keycloakRole())) {
                IllegalStateException("User is not allowed to $role the database $name")
            }
        }
    }
}