package org.sirix.rest.crud.xml

import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import org.brackit.xquery.xdm.node.AbstractTemporalNode
import org.brackit.xquery.xdm.node.TemporalNodeCollection
import org.sirix.xquery.node.XmlDBNode

class XmlSessionDBCollection<T>(
    private val ctx: RoutingContext,
    private val dbCollection: T,
    private val user: User
) : TemporalNodeCollection<AbstractTemporalNode<XmlDBNode>> by dbCollection, AutoCloseable by dbCollection
        where T : TemporalNodeCollection<AbstractTemporalNode<XmlDBNode>>, T : AutoCloseable
{

}