package org.sirix.rest.crud

import io.vertx.core.Handler
import io.vertx.ext.web.RoutingContext
import org.sirix.access.Databases
import java.nio.file.Path

class Delete(private val location: Path) : Handler<RoutingContext> {
    override fun handle(ctx: RoutingContext) {
        val dbName = ctx.pathParam("database")
        val resName: String? = ctx.pathParam("resource")
        val nodeId: String? = if (ctx.queryParam("nodeId").isEmpty()) null else ctx.queryParam("nodeId")[0]

        if (dbName == null)
            ctx.fail(IllegalArgumentException("Database name not given."))

        delete(dbName, resName, nodeId?.toLongOrNull(), ctx)
    }

    private fun delete(dbPathName: String, resPathName: String?, nodeId: Long?, ctx: RoutingContext) {
        val dbFile = location.resolve(dbPathName)

        if (resPathName == null) {
            Databases.removeDatabase(dbFile)
            ctx.response().setStatusCode(204).end()
            return
        }

        val database = Databases.openDatabase(dbFile)

        database.use {
            if (nodeId == null) {
                try {
                    database.removeResource(resPathName)
                } catch (e: IllegalStateException) {
                    ctx.fail(IllegalStateException("Open resource managers found."))
                }
            } else {
                val manager = database.getResourceManager(resPathName)

                manager.use {
                    val wtx = it.beginNodeWriteTrx()

                    wtx.moveTo(nodeId)

                    wtx.remove()
                    wtx.commit()
                }
            }
        }

        if (!ctx.failed())
            ctx.response().setStatusCode(200).end()
    }
}