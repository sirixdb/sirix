package io.sirix.rest.crud.xml

import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import io.brackit.query.jdm.node.AbstractTemporalNode
import io.brackit.query.jdm.node.TemporalNodeCollection
import io.sirix.query.node.XmlDBNode

class XmlSessionDBCollection<T>(
    private val ctx: RoutingContext,
    private val dbCollection: T,
    private val user: User
) : TemporalNodeCollection<AbstractTemporalNode<XmlDBNode>> by dbCollection, AutoCloseable by dbCollection
        where T : TemporalNodeCollection<AbstractTemporalNode<XmlDBNode>>, T : AutoCloseable
{

}