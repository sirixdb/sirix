package org.sirix.rest.crud.json

import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import org.brackit.xquery.xdm.json.TemporalJsonCollection
import org.sirix.xquery.json.JsonDBItem

class JsonSessionDBCollection<T>(
    private val ctx: RoutingContext,
    private val dbCollection: T,
    private val user: User
) : TemporalJsonCollection<JsonDBItem> by dbCollection, AutoCloseable by dbCollection
        where T : TemporalJsonCollection<JsonDBItem>, T : AutoCloseable {

}