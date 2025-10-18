package io.sirix.rest.crud.xml

import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import io.sirix.api.xml.XmlNodeReadOnlyTrx
import io.sirix.query.StructuredDBItem

class XmlSessionDBNode<T>(
    private val ctx: RoutingContext,
    private val dbNode: T,
    private val user: User
) : StructuredDBItem<XmlNodeReadOnlyTrx> by dbNode, AutoCloseable by dbNode
        where T : StructuredDBItem<XmlNodeReadOnlyTrx>, T : AutoCloseable {

}