package org.sirix.rest.crud.json

import com.google.gson.stream.JsonReader
import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import org.sirix.rest.Auth
import org.sirix.rest.AuthRole
import org.sirix.xquery.json.JsonDBCollection
import org.sirix.xquery.json.JsonDBStore
import java.nio.file.Path

class JsonSessionDBStore(private val ctx: RoutingContext, private val dbStore: JsonDBStore, private val user: User) :
    JsonDBStore by dbStore {
    override fun lookup(collName: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), collName, AuthRole.CREATE)

        return dbStore.lookup(collName)
    }

    override fun create(collName: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), collName, AuthRole.CREATE)

        return dbStore.create(collName)
    }

    override fun create(collName: String, path: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), collName, AuthRole.CREATE)

        return dbStore.create(collName, path)
    }

    override fun create(collName: String, path: Path): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), collName, AuthRole.CREATE)

        return dbStore.create(collName, path)
    }

    override fun create(collName: String, resourceName: String, path: Path): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), collName, AuthRole.CREATE)

        return dbStore.create(collName, path)
    }

    override fun create(collName: String, resourceName: String, json: String): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), collName, AuthRole.CREATE)

        return dbStore.create(collName, resourceName, json)
    }

    override fun create(collName: String, resourceName: String, json: JsonReader): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), collName, AuthRole.CREATE)

        return dbStore.create(collName, resourceName, json)
    }

    override fun create(collName: String, jsonReaders: Set<JsonReader>): JsonDBCollection {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), collName, AuthRole.CREATE)

        return dbStore.create(collName, jsonReaders)
    }

    override fun drop(collName: String) {
        Auth.checkIfAuthorized(user, ctx.vertx().dispatcher(), collName, AuthRole.DELETE)

        return dbStore.drop(collName)
    }
}