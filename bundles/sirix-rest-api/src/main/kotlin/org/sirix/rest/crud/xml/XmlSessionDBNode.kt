package org.sirix.rest.crud.xml

import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import org.sirix.api.xml.XmlNodeReadOnlyTrx
import org.sirix.xquery.StructuredDBItem

class XmlSessionDBNode<T>(
    private val ctx: RoutingContext,
    private val dbNode: T,
    private val user: User
) : StructuredDBItem<XmlNodeReadOnlyTrx> by dbNode, AutoCloseable by dbNode
        where T : StructuredDBItem<XmlNodeReadOnlyTrx>, T : AutoCloseable {

}