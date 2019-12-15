package org.sirix.rest.crud

import org.brackit.xquery.XQuery
import org.brackit.xquery.util.serialize.Serializer
import org.brackit.xquery.xdm.Item
import org.sirix.xquery.SirixCompileChain
import org.sirix.xquery.SirixQueryContext

class QuerySerializer {
    companion object {
        fun serializePaginated(
            sirixCompileChain: SirixCompileChain?,
            query: String,
            queryCtx: SirixQueryContext?,
            startResultSeqIndex: String,
            endResultSeqIndex: String?,
            serializer: Serializer,
            serialize: (Serializer, Item?) -> Unit
        ) {
            serializer.use {
                val sequence = XQuery(sirixCompileChain, query).execute(queryCtx)

                if (sequence != null) {
                    val startIndex = startResultSeqIndex.toLong()
                    val itemIterator = sequence.iterate()

                    for (i in 0 until startIndex) {
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
                        for (i in startIndex..endResultSeqIndex.toLong()) {
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