package io.sirix.rest.crud

import io.netty.handler.codec.http.HttpResponseStatus
import io.sirix.rest.AuthRole
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.runBlocking
import io.brackit.query.ErrorCode
import io.brackit.query.QueryContext
import io.brackit.query.QueryException
import io.brackit.query.compiler.CompileChain
import io.brackit.query.jdm.Item
import io.brackit.query.jdm.Sequence
import io.brackit.query.module.Module
import io.brackit.query.operator.TupleImpl
import io.brackit.query.util.serialize.Serializer
import io.brackit.query.util.serialize.StringSerializer
import java.io.PrintStream
import java.io.PrintWriter

/**
 * @author Johannes Lichtenberger
 */
@Suppress("unused")
class PermissionCheckingQuery {
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
        // #region agent log
        try { java.io.FileWriter("/home/johannes/IdeaProjects/.cursor/debug.log", true).use { w -> w.write("{\"hypothesisId\":\"A\",\"location\":\"PermissionCheckingQuery.run\",\"message\":\"query start\",\"data\":{\"isUpdating\":${module.body?.isUpdating ?: false},\"contextId\":\"${System.identityHashCode(ctx)}\"},\"timestamp\":${System.currentTimeMillis()}}\n") } } catch (e: Exception) {}
        // #endregion
        val body = module.body
            ?: throw QueryException(ErrorCode.BIT_DYN_INT_ERROR, "Module does not contain a query body.")
        val result = body.evaluate(ctx, TupleImpl())

        if (body.isUpdating) {
            // val isAuthorized = user.isAuthorizedAwait(role.databaseRole(name))

            // FIXME: Better way?
            runBlocking {
                authz.getAuthorizations(user).coAwait()
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

    fun prettyPrint(): PermissionCheckingQuery {
        this.isPrettyPrint = true
        return this
    }
}