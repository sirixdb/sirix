package org.sirix.rest.crud

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.ext.auth.authenticateAwait
import io.vertx.kotlin.ext.auth.isAuthorizedAwait
import org.sirix.access.Databases
import org.sirix.api.XdmNodeWriteTrx
import org.sirix.rest.Serialize
import org.sirix.service.xml.serialize.XMLSerializer
import org.sirix.service.xml.shredder.XMLShredder
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import javax.xml.stream.XMLEventReader

private enum class InsertionMode {
    ASFIRSTCHILD {
        override fun insert(wtx: XdmNodeWriteTrx, xmlReader: XMLEventReader) {
            wtx.insertSubtreeAsFirstChild(xmlReader)
        }
    }, ASRIGHTSIBLING {
        override fun insert(wtx: XdmNodeWriteTrx, xmlReader: XMLEventReader) {
            wtx.insertSubtreeAsRightSibling(xmlReader)
        }
    }, ASLEFTSIBLING {
        override fun insert(wtx: XdmNodeWriteTrx, xmlReader: XMLEventReader) {
            wtx.insertSubtreeAsLeftSibling(xmlReader)
        }
    }, REPLACE {
        override fun insert(wtx: XdmNodeWriteTrx, xmlReader: XMLEventReader) {
            wtx.replaceNode(xmlReader)
        }
    };

    abstract fun insert(wtx: XdmNodeWriteTrx, xmlReader: XMLEventReader)

    companion object {
        fun getInsertionModeByName(name: String) = valueOf(name.toUpperCase())
    }
}

class Update(private val location: Path, private val keycloak: OAuth2Auth) {
    suspend fun handle(ctx: RoutingContext) {
        val dbName = ctx.pathParam("database")

        val user = authenticateUser(ctx)

        val isAuthorized =
                if (dbName != null)
                    user.isAuthorizedAwait("realm:${dbName.toLowerCase()}-modify")
                else
                    user.isAuthorizedAwait("realm:modify")

        if (!isAuthorized) {
            ctx.fail(HttpResponseStatus.UNAUTHORIZED.code())
            return
        }

        val resName = ctx.pathParam("resource")
        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)
        val insertionMode: String? = ctx.queryParam("insert").getOrNull(0)

        if (dbName == null || resName == null) {
            ctx.fail(IllegalArgumentException("Database name and resource name not given."))
            return
        }

        val body = ctx.bodyAsString

        update(dbName, resName, nodeId?.toLongOrNull(), insertionMode, body, ctx)
    }

    private suspend fun authenticateUser(ctx: RoutingContext): User {
        val token = ctx.request().getHeader(HttpHeaders.AUTHORIZATION.toString())

        val tokenToAuthenticate = json {
            obj("access_token" to token.substring(7),
                    "token_type" to "Bearer")
        }

        return keycloak.authenticateAwait(tokenToAuthenticate)
    }

    private suspend fun update(dbPathName: String, resPathName: String, nodeId: Long?, insertionMode: String?, resFileToStore: String, ctx: RoutingContext) {
        val vertxContext = ctx.vertx().orCreateContext

        vertxContext.executeBlockingAwait(Handler<Future<Nothing>> {
            val dbFile = location.resolve(dbPathName)

            val database = Databases.openDatabase(dbFile)

            database.use {
                val manager = database.getResourceManager(resPathName)

                val wtx = manager.beginNodeWriteTrx()
                wtx.use {
                    if (nodeId != null)
                        wtx.moveTo(nodeId)

                    if (wtx.isDocumentRoot && wtx.hasFirstChild())
                        wtx.moveToFirstChild()

                    val xmlReader = XMLShredder.createStringReader(resFileToStore)

                    if (insertionMode != null)
                        InsertionMode.getInsertionModeByName(insertionMode).insert(wtx, xmlReader)
                    else
                        wtx.replaceNode(xmlReader)
                }

                val out = ByteArrayOutputStream()
                val serializerBuilder = XMLSerializer.XMLSerializerBuilder(manager, out)
                val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

                Serialize().serializeXml(serializer, out, ctx)
            }

            it.complete(null)
        })
    }
}