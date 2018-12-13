package org.sirix.rest

import io.vertx.ext.auth.User
import org.brackit.xquery.node.parser.SubtreeParser
import org.brackit.xquery.xdm.Stream
import org.sirix.xquery.node.DBCollection
import org.sirix.xquery.node.DBStore

class SessionDBStore(private val dbStore: DBStore, private val user: User) : DBStore by dbStore {
    override fun lookup(name: String): DBCollection {
        checkIfAuthorized(name, "view")

        return dbStore.lookup(name)
    }

    override fun create(name: String): DBCollection {
        checkIfAuthorized(name, "create")

        return dbStore.create(name)
    }

    override fun create(name: String, parser: SubtreeParser): DBCollection {
        checkIfAuthorized(name, "create")

        return dbStore.create(name, parser)
    }

    override fun create(name: String, parsers: Stream<SubtreeParser>): DBCollection {
        checkIfAuthorized(name, "create")

        return dbStore.create(name, parsers)
    }

    override fun create(dbName: String, resourceName: String, parsers: SubtreeParser): DBCollection {
        checkIfAuthorized(dbName, "create")

        return dbStore.create(dbName, resourceName, parsers)
    }

    override fun drop(name: String) {
        checkIfAuthorized(name, "delete")

        return dbStore.drop(name)
    }

    private fun checkIfAuthorized(name: String, role: String) {
        user.isAuthorized("realm:${name.toLowerCase()}-$role") {
            if (!it.succeeded()) {
                throw IllegalArgumentException("User is not allowed to $role the database $name")
            }
        }
    }
}