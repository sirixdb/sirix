package org.sirix.rest.crud

import io.vertx.ext.auth.User
import io.vertx.ext.auth.oauth2.OAuth2Auth
import org.brackit.xquery.util.serialize.Serializer
import org.brackit.xquery.xdm.Item
import org.sirix.rest.AuthRole
import org.sirix.xquery.SirixCompileChain
import org.sirix.xquery.SirixQueryContext

class QuerySerializer {
    companion object {
        suspend fun serializePaginated(
            sirixCompileChain: SirixCompileChain,
            query: String,
            queryCtx: SirixQueryContext,
            startResultSeqIndex: Long,
            endResultSeqIndex: Long?,
            role: AuthRole,
            keycloak: OAuth2Auth,
            user: User,
            serializer: Serializer,
            serialize: (Serializer, Item?) -> Unit
        ) {
            serializer.use {
                val sequence =
                    PermissionCheckingXQuery(sirixCompileChain, query, role, keycloak, user).execute(queryCtx)

                val itemIterator = sequence.iterate()

                for (i in 0 until startResultSeqIndex) {
                    itemIterator.next()
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