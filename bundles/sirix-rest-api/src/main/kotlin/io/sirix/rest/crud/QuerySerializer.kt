package io.sirix.rest.crud

import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.brackit.query.jdm.Item
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

                if (sequence != null) {
                    val itemIterator = sequence.iterate()

                    // Skip to the window start, STOPPING at end-of-sequence: brackit iterators
                    // return null forever once exhausted, so an unbounded skip loop with a huge
                    // start index (e.g. ?startResultSeqIndex=10^15) spun for that many iterations
                    // on a worker thread and head-of-line-blocked the ordered context — a DoS any
                    // VIEW-role user could trigger.
                    var skipped = 0L
                    while (skipped < startResultSeqIndex && itemIterator.next() != null) {
                        skipped++
                    }
                    if (skipped < startResultSeqIndex) {
                        return // sequence shorter than the window start — empty result
                    }

                    if (endResultSeqIndex == null) {
                        while (true) {
                            val item = itemIterator.next()

                            if (item == null)
                                break
                            else
                                serialize(serializer, item)
                        }
                    } else {
                        for (i in startResultSeqIndex..endResultSeqIndex) {
                            val item = itemIterator.next()

                            if (item == null)
                                break
                            else
                                serialize(serializer, item)
                        }
                    }
                }
            }
        }
    }
}