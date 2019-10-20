package org.sirix.rest.crud.xml

import io.vertx.ext.auth.User
import org.brackit.xquery.node.parser.SubtreeParser
import org.brackit.xquery.xdm.Stream
import org.sirix.rest.AuthRole
import org.sirix.xquery.node.XmlDBCollection
import org.sirix.xquery.node.XmlDBStore

class XmlSessionDBStore(private val dbStore: XmlDBStore, private val user: User) : XmlDBStore by dbStore {
    override fun lookup(name: String): XmlDBCollection {
        checkIfAuthorized(name, AuthRole.VIEW)

        return dbStore.lookup(name)
    }

    override fun create(name: String): XmlDBCollection {
        checkIfAuthorized(name, AuthRole.CREATE)

        return dbStore.create(name)
    }

    override fun create(name: String, parser: SubtreeParser): XmlDBCollection {
        checkIfAuthorized(name, AuthRole.CREATE)

        return dbStore.create(name, parser)
    }

    override fun create(name: String, parsers: Stream<SubtreeParser>): XmlDBCollection {
        checkIfAuthorized(name, AuthRole.CREATE)

        return dbStore.create(name, parsers)
    }

    override fun create(dbName: String, resourceName: String, parsers: SubtreeParser): XmlDBCollection {
        checkIfAuthorized(dbName, AuthRole.CREATE)

        return dbStore.create(dbName, resourceName, parsers)
    }

    override fun drop(name: String) {
        checkIfAuthorized(name, AuthRole.DELETE)

        return dbStore.drop(name)
    }

    private fun checkIfAuthorized(name: String, role: AuthRole) {
        user.isAuthorized(role.databaseRole(name)) {
            if (!it.succeeded()) {
                throw IllegalArgumentException("User is not allowed to $role the database $name")
            }
        }
    }
}