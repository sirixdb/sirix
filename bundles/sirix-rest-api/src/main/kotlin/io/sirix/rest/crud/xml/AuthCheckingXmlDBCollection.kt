package io.sirix.rest.crud.xml

import io.brackit.query.node.parser.NodeSubtreeParser
import io.sirix.query.node.XmlDBCollection
import io.sirix.query.node.XmlDBNode
import io.sirix.rest.Auth
import io.sirix.rest.AuthRole
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import java.time.Instant

/**
 * An [XmlDBCollection] decorator that enforces per-request authorization on every mutating
 * operation, by composition (Kotlin interface delegation) rather than inheritance.
 *
 * The REST query endpoint runs under the VIEW role and injects an [XmlSessionDBStore] into the
 * brackit query context. Looking a collection up is a read (VIEW) and legitimately returns a
 * collection, but the raw collection then exposes `add`/`remove`/`delete`, which a query calls
 * directly on the collection object — bypassing the store-level checks. Every read is delegated
 * straight to [wrapped]; the mutating methods re-check the caller's role first, so a VIEW-only
 * token cannot mutate through a looked-up collection.
 */
class AuthCheckingXmlDBCollection(
    private val wrapped: XmlDBCollection,
    private val user: User,
    private val authz: AuthorizationProvider
) : XmlDBCollection by wrapped {

    private val collectionName: String = wrapped.getName()

    override fun delete() {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.DELETE, authz)
        wrapped.delete()
    }

    override fun remove(documentID: Long) {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.DELETE, authz)
        wrapped.remove(documentID)
    }

    override fun add(resourceName: String, parser: NodeSubtreeParser): XmlDBNode {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.CREATE, authz)
        return wrapped.add(resourceName, parser)
    }

    override fun add(
        resourceName: String,
        parser: NodeSubtreeParser,
        commitMessage: String?,
        commitTimestamp: Instant?
    ): XmlDBNode {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.CREATE, authz)
        return wrapped.add(resourceName, parser, commitMessage, commitTimestamp)
    }

    override fun add(parser: NodeSubtreeParser): XmlDBNode {
        Auth.checkIfAuthorized(user, collectionName, AuthRole.CREATE, authz)
        return wrapped.add(parser)
    }

    // Interface delegation does not forward Object methods; key equality on the underlying
    // database (as the concrete collection does) so the decorator remains a drop-in replacement.
    override fun equals(other: Any?): Boolean = wrapped == other

    override fun hashCode(): Int = wrapped.hashCode()

    override fun toString(): String = wrapped.toString()
}
