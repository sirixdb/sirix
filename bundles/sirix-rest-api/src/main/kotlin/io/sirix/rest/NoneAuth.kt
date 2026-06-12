package io.sirix.rest

import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authentication.Credentials
import io.vertx.ext.auth.authorization.Authorization
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.auth.oauth2.OAuth2AuthorizationURL

/**
 * Permissive in-process authentication/authorization provider used when the server is started
 * with `auth.mode=none` (or the `SIRIX_AUTH_MODE=none` environment variable).
 *
 * In this mode no Keycloak instance is required: every request — with or without an
 * `Authorization` header — runs as a synthetic `admin` user that holds all global roles
 * (`create`, `modify`, `view`, `delete`). [Auth.handle] short-circuits on this provider, and the
 * `/token`, `/logout` and refresh endpoints keep working against the stub so existing clients
 * (e.g. the web UI) don't need code changes.
 *
 * **This mode is for local development and evaluation only.** [SirixVerticle] logs a loud warning
 * at startup when it is active; the default remains Keycloak.
 */
object NoneAuth : OAuth2Auth, AuthorizationProvider {

    /** Provider id under which the synthetic authorizations are registered on the user. */
    private const val PROVIDER_ID = "none-auth"

    /**
     * Fixed subject for the synthetic admin user. [io.sirix.rest.crud.SirixDBUser.create] parses
     * this with [java.util.UUID.fromString], so it must be a valid UUID; the nil UUID makes the
     * synthetic identity easy to recognize in commit metadata.
     */
    private const val ADMIN_SUBJECT = "00000000-0000-0000-0000-000000000000"

    /** Username recorded as commit author in auth-less development mode. */
    private const val ADMIN_USERNAME = "admin"

    /** Placeholder token returned from `/token` so token-based clients keep functioning. */
    private const val PLACEHOLDER_TOKEN = "auth-mode-none"

    /**
     * Creates the synthetic all-permissions admin user. A fresh instance is created per request
     * because [User] is mutable (principal/attributes can be merged by downstream handlers).
     */
    fun createUser(): User {
        val accessToken = JsonObject()
            .put("sub", ADMIN_SUBJECT)
            .put("preferred_username", ADMIN_USERNAME)
        val principal = JsonObject()
            .put("access_token", PLACEHOLDER_TOKEN)
            .put("token_type", "Bearer")
            .put("sub", ADMIN_SUBJECT)
            .put("preferred_username", ADMIN_USERNAME)
        val user = User.create(principal, JsonObject().put("accessToken", accessToken))
        user.authorizations().put(PROVIDER_ID, *allRoles())
        return user
    }

    /** One [RoleBasedAuthorization] per [AuthRole], i.e. the global create/modify/view/delete roles. */
    private fun allRoles(): Array<Authorization> =
        AuthRole.entries.map { RoleBasedAuthorization.create(it.keycloakRole()) }.toTypedArray()

    // --- AuthenticationProvider -------------------------------------------------------------

    override fun authenticate(credentials: Credentials): Future<User> =
        Future.succeededFuture(createUser())

    // --- AuthorizationProvider --------------------------------------------------------------

    override fun getId(): String = PROVIDER_ID

    override fun getAuthorizations(user: User): Future<Void> {
        user.authorizations().put(PROVIDER_ID, *allRoles())
        return Future.succeededFuture()
    }

    // --- OAuth2Auth -------------------------------------------------------------------------

    override fun jWKSet(): Future<Void> = Future.succeededFuture()

    override fun missingKeyHandler(handler: Handler<String>): OAuth2Auth = this

    override fun authorizeURL(url: OAuth2AuthorizationURL): String =
        throw IllegalStateException("The authorization-code flow is not available with auth.mode=none")

    override fun refresh(user: User): Future<User> = Future.succeededFuture(createUser())

    override fun revoke(user: User, tokenType: String): Future<Void> = Future.succeededFuture()

    override fun userInfo(user: User): Future<JsonObject> = Future.succeededFuture(user.principal())

    override fun endSessionURL(user: User, params: JsonObject): String = ""

    override fun close() {
        // Nothing to release.
    }
}
