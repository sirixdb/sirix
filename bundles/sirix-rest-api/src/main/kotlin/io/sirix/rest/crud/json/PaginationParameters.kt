package io.sirix.rest.crud.json

import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException

/** Query parameter of the paginated JSON read that continues after the given node key. */
internal const val START_NODE_KEY_PARAM = "startNodeKey"

/**
 * Former name of [START_NODE_KEY_PARAM]. The server stopped reading it in the rename and ignored
 * it silently, so a client still sending it got the first page back on every request.
 */
internal const val RETIRED_LAST_TOP_LEVEL_NODE_KEY_PARAM = "lastTopLevelNodeKey"

/**
 * Refuses a read that carries the retired pagination parameter (REST-213).
 *
 * Ignoring it would reset the cursor without any signal, so the request fails with 400 and a message
 * naming the replacement, whatever the value and whether or not [START_NODE_KEY_PARAM] is present.
 *
 * @throws HttpException with status 400 if [RETIRED_LAST_TOP_LEVEL_NODE_KEY_PARAM] is present
 */
internal fun rejectRetiredPaginationParameter(ctx: RoutingContext) {
    if (ctx.queryParam(RETIRED_LAST_TOP_LEVEL_NODE_KEY_PARAM).isNotEmpty()) {
        throw HttpException(
            400,
            "Query parameter '$RETIRED_LAST_TOP_LEVEL_NODE_KEY_PARAM' is no longer supported; " +
                "use '$START_NODE_KEY_PARAM' instead."
        )
    }
}
