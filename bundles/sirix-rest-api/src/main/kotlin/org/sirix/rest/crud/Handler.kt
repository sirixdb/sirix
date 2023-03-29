package org.sirix.rest.crud

import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext

interface Handler {
    suspend fun handle(ctx: RoutingContext): Route
}