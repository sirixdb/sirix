package org.sirix.rest.crud

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.runBlocking
import org.brackit.xquery.ErrorCode
import org.brackit.xquery.QueryContext
import org.brackit.xquery.QueryException
import org.brackit.xquery.compiler.CompileChain
import org.brackit.xquery.jdm.Item
import org.brackit.xquery.jdm.Sequence
import org.brackit.xquery.module.Module
import org.brackit.xquery.operator.TupleImpl
import org.brackit.xquery.util.serialize.Serializer
import org.brackit.xquery.util.serialize.StringSerializer
import org.sirix.rest.AuthRole
import java.io.PrintStream
import java.io.PrintWriter

/**
 * @author Johannes Lichtenberger
 */
@Suppress("unused")
class PermissionCheckingXQuery {
    private val module: Module
    private var isPrettyPrint: Boolean = false
    val keycloak: OAuth2Auth
    val user: User
    val authz: AuthorizationProvider

    constructor(module: Module, keycloak: OAuth2Auth, user: User, authz: AuthorizationProvider) {
        this.module = module
        this.keycloak = keycloak
        this.user = user
        this.authz = authz
    }

    constructor(query: String, keycloak: OAuth2Auth, user: User, authz: AuthorizationProvider) {
        this.module = CompileChain().compile(query)
        this.keycloak = keycloak
        this.user = user
        this.authz = authz
    }

    constructor(chain: CompileChain, query: String, keycloak: OAuth2Auth, user: User, authz: AuthorizationProvider) {
        this.module = chain.compile(query)
        this.keycloak = keycloak
        this.user = user
        this.authz = authz
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
                authz.getAuthorizations(user).await()
                if (!RoleBasedAuthorization.create(AuthRole.MODIFY.keycloakRole()).match(user)) {
                    throw IllegalStateException("${HttpResponseStatus.UNAUTHORIZED.code()}: User is not allowed to modify the database")
                }
            }
        }

        if (!lazy || body.isUpdating) {
            // iterate possibly lazy result sequence to "pull-in" all pending updates
            if (result != null && result !is Item) {
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
        val result: Sequence? = run(ctx, true)
        StringSerializer(out).use { serializer ->
            serializer.isFormat = isPrettyPrint
            serializer.use {
                serializer.serialize(result)
            }
        }
    }

    fun serialize(ctx: QueryContext, serializer: Serializer) {
        val result: Sequence? = run(ctx, true)
        serializer.use {
            serializer.serialize(result)
        }
    }

    fun prettyPrint(): PermissionCheckingXQuery {
        this.isPrettyPrint = true
        return this
    }
}