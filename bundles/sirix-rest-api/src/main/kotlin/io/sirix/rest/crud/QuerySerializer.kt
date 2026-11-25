package io.sirix.rest.crud

import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.brackit.query.jdm.Item
import io.brackit.query.jdm.Iter
import io.brackit.query.util.serialize.Serializer
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext

class QuerySerializer {
    companion object {
        fun serializePaginated(
            sirixCompileChain: SirixCompileChain,
            query: String,
            queryCtx: SirixQueryContext,
            startResultSeqIndex: Long,
            endResultSeqIndex: Long?,
            keycloak: OAuth2Auth,
            authz: AuthorizationProvider,
            user: User,
            serializer: Serializer,
            serialize: (Serializer, Item?) -> Unit
        ) {
            // Validate the window up front: these values come straight from query params. A
            // negative start, or an end before the start, is a client error — not something to
            // silently normalize.
            if (startResultSeqIndex < 0) {
                throw IllegalArgumentException("startResultSeqIndex must be >= 0 but is $startResultSeqIndex")
            }
            if (endResultSeqIndex != null && endResultSeqIndex < startResultSeqIndex) {
                throw IllegalArgumentException(
                    "endResultSeqIndex ($endResultSeqIndex) must be >= startResultSeqIndex ($startResultSeqIndex)"
                )
            }

            serializer.use {
                val sequence =
                    PermissionCheckingQuery(sirixCompileChain, query, keycloak, user, authz).execute(queryCtx)

                if (sequence == null) return
                if (sequence is Item) {
                    if (startResultSeqIndex == 0L) {
                        serialize(serializer, sequence)
                    }
                    return
                }
                sequence.iterate().use { itemIterator ->
                    serializeWindow(itemIterator, startResultSeqIndex, endResultSeqIndex, serializer, serialize)
                }
            }
        }

        private fun serializeWindow(
            itemIterator: Iter,
            startResultSeqIndex: Long,
            endResultSeqIndex: Long?,
            serializer: Serializer,
            serialize: (Serializer, Item?) -> Unit
        ) {
            // Stop skipping at exhaustion even for a very large requested start index.
            var skipped = 0L
            while (skipped < startResultSeqIndex && itemIterator.next() != null) {
                skipped++
            }
            if (skipped < startResultSeqIndex) return

            if (endResultSeqIndex == null) {
                while (true) {
                    val item = itemIterator.next() ?: break
                    serialize(serializer, item)
                }
            } else {
                for (i in startResultSeqIndex..endResultSeqIndex) {
                    val item = itemIterator.next() ?: break
                    serialize(serializer, item)
                }
            }
        }
    }
}
