package org.sirix.rest.crud

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.ext.auth.User
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.handler.OAuth2AuthHandler
import io.vertx.kotlin.coroutines.awaitBlocking
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.ext.auth.isAuthorizedAwait
import io.vertx.kotlin.ext.web.handler.authorizeAwait
import kotlinx.coroutines.*
import org.brackit.xquery.ErrorCode
import org.brackit.xquery.QueryContext
import org.brackit.xquery.QueryException
import org.brackit.xquery.compiler.CompileChain
import org.brackit.xquery.module.Module
import org.brackit.xquery.operator.TupleImpl
import org.brackit.xquery.util.Cfg
import org.brackit.xquery.util.serialize.Serializer
import org.brackit.xquery.util.serialize.StringSerializer
import org.brackit.xquery.xdm.Item
import org.brackit.xquery.xdm.Sequence
import org.sirix.rest.AuthRole
import java.io.PrintStream
import java.io.PrintWriter
import java.lang.IllegalStateException

/**
 * @author Johannes Lichtenberger
 */
class PermissionCheckingXQuery {
    val module: Module
    var isPrettyPrint: Boolean = false
    val role: AuthRole
    val keycloak: OAuth2Auth
    val user: User

    constructor(module: Module, role: AuthRole, keycloak: OAuth2Auth, user: User) {
        this.module = module
        this.role = role
        this.keycloak = keycloak
        this.user = user
    }

    constructor(query: String, role: AuthRole, keycloak: OAuth2Auth, user: User) {
        this.module = CompileChain().compile(query)
        this.role = role
        this.keycloak = keycloak
        this.user = user
    }

    constructor(chain: CompileChain, query: String, role: AuthRole, keycloak: OAuth2Auth, user: User) {
        this.module = chain.compile(query)
        this.role = role
        this.keycloak = keycloak
        this.user = user
    }

    fun execute(ctx: QueryContext): Sequence? {
        return run(ctx, true)
    }

    fun evaluate(ctx: QueryContext): Sequence? {
        return run(ctx, false)
    }

    private fun run(ctx: QueryContext, lazy: Boolean): Sequence? {
        val body = module.body
            ?: throw QueryException(ErrorCode.BIT_DYN_INT_ERROR, "Module does not contain a query body.")
        val result = body.evaluate(ctx, TupleImpl())

        if (body.isUpdating) {
            // val isAuthorized = user.isAuthorizedAwait(role.databaseRole(name))

            // FIXME: Better way?
            runBlocking {
                require(user.isAuthorizedAwait(role.keycloakRole())) {
                    throw IllegalStateException("${HttpResponseStatus.UNAUTHORIZED.code()}: User is not allowed to modify the database")
                }
            }
        }

        if (!lazy || body.isUpdating) {
            // iterate possibly lazy result sequence to "pull-in" all pending updates
            if (result != null && !(result is Item)) {
                result.iterate().use {
                    while (it.next() != null) {
                    }
                }
            }

            ctx.applyUpdates()
        }

        return result
    }

    fun serialize(ctx: QueryContext, out: PrintStream) {
        serialize(ctx, PrintWriter(out))
    }

    fun serialize(ctx: QueryContext, out: PrintWriter) {
        val result = run(ctx, true) ?: return
        StringSerializer(out).use { serializer ->
            serializer.isFormat = isPrettyPrint
            serializer.use {
                serializer.serialize(result)
            }
        }
    }

    fun serialize(ctx: QueryContext, serializer: Serializer) {
        val result = run(ctx, true) ?: return
        serializer.use {
            serializer.serialize(result);
        }
    }

    fun prettyPrint(): PermissionCheckingXQuery {
        this.isPrettyPrint = true
        return this
    }

    companion object {
        val DEBUG_CFG = "org.brackit.xquery.debug"
        val DEBUG_DIR_CFG = "org.brackit.xquery.debugDir"
        var DEBUG = Cfg.asBool(DEBUG_CFG, false)
        var DEBUG_DIR = Cfg.asString(DEBUG_DIR_CFG, "debug/")
    }
}