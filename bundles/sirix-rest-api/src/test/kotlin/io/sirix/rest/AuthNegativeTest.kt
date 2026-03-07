package io.sirix.rest

import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.concurrent.TimeUnit

/**
 * Negative tests for REST API authentication and authorization.
 *
 * Verifies that:
 * - Requests without tokens are rejected with 401
 * - Requests with invalid tokens are rejected with 401
 * - Requests with malformed auth headers are rejected with 401
 * - Requests with insufficient permissions are rejected with 403
 */
@ExtendWith(VertxExtension::class)
@DisplayName("Auth negative tests")
class AuthNegativeTest {
    private val server = "https://localhost:9443"
    private var adminAccessToken = ""

    private lateinit var client: WebClient

    @BeforeEach
    @DisplayName("Deploy a verticle")
    fun setup(vertx: Vertx, testContext: VertxTestContext) {
        val options = DeploymentOptions().setConfig(
            JsonObject().put("port", 9443)
                .put("client.secret", "78a294c4-0492-4e44-a35f-7eb9cab0d831")
                .put("keycloak.url", "http://localhost:8080/realms/sirixdb")
        )
        vertx.deployVerticle("io.sirix.rest.SirixVerticle", options)
            .onComplete(testContext.succeedingThenComplete())

        client = WebClient.create(vertx, WebClientOptions().setTrustAll(true).setFollowRedirects(false))
    }

    @AfterEach
    @DisplayName("Remove databases")
    fun delete(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                if (adminAccessToken.isNotEmpty()) {
                    val httpResponse = client.deleteAbs(server).putHeader(
                        HttpHeaders.AUTHORIZATION.toString(), "Bearer $adminAccessToken"
                    ).send().coAwait()

                    if (204 == httpResponse.statusCode() || 401 == httpResponse.statusCode()) {
                        testContext.completeNow()
                    } else {
                        testContext.failNow(httpResponse.bodyAsString())
                    }
                } else {
                    testContext.completeNow()
                }
            }
        }
    }

    /**
     * Obtains a valid access token for the given username/password from Keycloak.
     */
    private suspend fun obtainAccessToken(username: String, password: String): String {
        val credentials = json {
            obj(
                "username" to username,
                "password" to password
            )
        }
        val response = client.postAbs("$server/token").sendJson(credentials).coAwait()
        check(response.statusCode() == 200) {
            "Failed to obtain token for $username: ${response.statusCode()} ${response.bodyAsString()}"
        }
        return response.bodyAsJsonObject().getString("access_token")
    }

    // ──────────────────────────────────────────────────────────────────────
    // 401 Tests: No auth header
    // ──────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("No Authorization header")
    inner class NoAuthHeader {

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("GET / without auth header returns 401")
        fun testGetRootNoAuth(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.getAbs(server)
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("GET /:database without auth header returns 401")
        fun testGetDatabaseNoAuth(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.getAbs("$server/database")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("GET /:database/:resource without auth header returns 401")
        fun testGetResourceNoAuth(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.getAbs("$server/database/resource")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("PUT /:database/:resource without auth header returns 401")
        fun testPutResourceNoAuth(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.putAbs("$server/database/resource")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{}")).coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("DELETE /:database without auth header returns 401")
        fun testDeleteDatabaseNoAuth(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.deleteAbs("$server/database")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("DELETE / without auth header returns 401")
        fun testDeleteRootNoAuth(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.deleteAbs(server)
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("POST /:database/:resource without auth header returns 401")
        fun testPostResourceNoAuth(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.postAbs("$server/database/resource")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{\"nodeId\": 1}")).coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // 401 Tests: Invalid token
    // ──────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Invalid Bearer token")
    inner class InvalidToken {

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("GET / with invalid Bearer token returns 401")
        fun testGetRootInvalidToken(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.getAbs(server)
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer invalid-token-value")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("GET /:database with invalid Bearer token returns 401")
        fun testGetDatabaseInvalidToken(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.getAbs("$server/database")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer invalid-token-value")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("PUT /:database/:resource with invalid Bearer token returns 401")
        fun testPutResourceInvalidToken(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.putAbs("$server/database/resource")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer invalid-token-value")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{}")).coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("DELETE /:database with invalid Bearer token returns 401")
        fun testDeleteDatabaseInvalidToken(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.deleteAbs("$server/database")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer invalid-token-value")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("POST /:database/:resource with invalid Bearer token returns 401")
        fun testPostResourceInvalidToken(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.postAbs("$server/database/resource")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer invalid-token-value")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{\"nodeId\": 1}")).coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // 401 Tests: Malformed Authorization header
    // ──────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Malformed Authorization header")
    inner class MalformedAuthHeader {

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("GET / with 'NotBearer xxx' auth header returns 401")
        fun testGetRootMalformedScheme(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.getAbs(server)
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "NotBearer some-token")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("PUT /:database/:resource with 'Basic xxx' auth header returns 401")
        fun testPutResourceBasicAuth(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.putAbs("$server/database/resource")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Basic dXNlcjpwYXNz")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{}")).coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("DELETE / with empty Bearer token returns 401")
        fun testDeleteRootEmptyBearer(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.deleteAbs(server)
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer ")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("GET /:database with 'Bearer' only (no token) returns 401")
        fun testGetDatabaseBearerNoToken(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.getAbs("$server/database")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // 403 Tests: Insufficient permissions (viewer-only user)
    //
    // The Auth class returns 403 (Forbidden) for authenticated users
    // with insufficient roles.
    // ──────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Insufficient permissions (viewer-only user)")
    inner class InsufficientPermissions {

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Viewer user can GET a database (has view role)")
        fun testViewerCanGet(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    // First, use admin to create a database+resource
                    adminAccessToken = obtainAccessToken("admin", "admin")

                    val httpResponse = client.putAbs("$server/database/resource")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $adminAccessToken")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{}")).coAwait()

                    testContext.verify {
                        assertEquals(200, httpResponse.statusCode())
                    }

                    // Now use viewer to read
                    val viewerToken = obtainAccessToken("viewer", "viewer")

                    val getResponse = client.getAbs("$server/database/resource")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $viewerToken")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(200, getResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Viewer user cannot PUT (create) a database — returns 403")
        fun testViewerCannotCreate(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val viewerToken = obtainAccessToken("viewer", "viewer")

                    val httpResponse = client.putAbs("$server/database/resource")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $viewerToken")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{}")).coAwait()

                    testContext.verify {
                        assertEquals(403, httpResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Viewer user cannot DELETE a database — returns 403")
        fun testViewerCannotDelete(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    // Create a database first with admin
                    adminAccessToken = obtainAccessToken("admin", "admin")

                    val putResponse = client.putAbs("$server/database/resource")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $adminAccessToken")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{}")).coAwait()

                    testContext.verify {
                        assertEquals(200, putResponse.statusCode())
                    }

                    // Try to delete with viewer
                    val viewerToken = obtainAccessToken("viewer", "viewer")

                    val deleteResponse = client.deleteAbs("$server/database")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $viewerToken")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(403, deleteResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Viewer user cannot POST (modify) a resource — returns 403")
        fun testViewerCannotModify(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    // Create a database first with admin
                    adminAccessToken = obtainAccessToken("admin", "admin")

                    val putResponse = client.putAbs("$server/database/resource")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $adminAccessToken")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{\"foo\":\"bar\"}")).coAwait()

                    testContext.verify {
                        assertEquals(200, putResponse.statusCode())
                    }

                    // Try to modify with viewer (POST with JSON body to modify)
                    val viewerToken = obtainAccessToken("viewer", "viewer")

                    val postResponse = client.postAbs("$server/database/resource?nodeId=1&insert=asFirstChild")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $viewerToken")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{\"new\":\"data\"}")).coAwait()

                    testContext.verify {
                        assertEquals(403, postResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Viewer user cannot DELETE a specific resource — returns 403")
        fun testViewerCannotDeleteResource(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    // Create a database/resource first with admin
                    adminAccessToken = obtainAccessToken("admin", "admin")

                    val putResponse = client.putAbs("$server/database/resource")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $adminAccessToken")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{}")).coAwait()

                    testContext.verify {
                        assertEquals(200, putResponse.statusCode())
                    }

                    // Try to delete the resource with viewer
                    val viewerToken = obtainAccessToken("viewer", "viewer")

                    val deleteResponse = client.deleteAbs("$server/database/resource")
                        .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $viewerToken")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(403, deleteResponse.statusCode())
                        testContext.completeNow()
                    }
                }
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Verify response body contains expected error structure
    // ──────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Error response body format")
    inner class ErrorResponseFormat {

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("401 response contains JSON body with statusCode and message")
        fun testUnauthorizedResponseBody(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.getAbs(server)
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(401, httpResponse.statusCode())
                        val body = httpResponse.bodyAsJsonObject()
                        assertEquals(401, body.getInteger("statusCode"))
                        assertTrue(body.getString("message").isNotEmpty())
                        testContext.completeNow()
                    }
                }
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // Verify unauthenticated endpoints remain accessible
    // ──────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Unauthenticated endpoints")
    inner class UnauthenticatedEndpoints {

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Health endpoint is accessible without auth")
        fun testHealthNoAuth(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val httpResponse = client.getAbs("$server/health")
                        .send().coAwait()

                    testContext.verify {
                        assertEquals(200, httpResponse.statusCode())
                        val body = httpResponse.bodyAsJsonObject()
                        assertEquals("UP", body.getString("status"))
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Token endpoint is accessible without auth")
        fun testTokenEndpointNoAuth(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val credentials = json {
                        obj(
                            "username" to "admin",
                            "password" to "admin"
                        )
                    }
                    val httpResponse = client.postAbs("$server/token")
                        .sendJson(credentials).coAwait()

                    testContext.verify {
                        assertEquals(200, httpResponse.statusCode())
                        val body = httpResponse.bodyAsJsonObject()
                        assertTrue(body.containsKey("access_token"))
                        testContext.completeNow()
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Token endpoint rejects wrong credentials")
        fun testTokenEndpointWrongCredentials(vertx: Vertx, testContext: VertxTestContext) {
            GlobalScope.launch(vertx.dispatcher()) {
                testContext.verifyCoroutine {
                    val credentials = json {
                        obj(
                            "username" to "admin",
                            "password" to "wrong-password"
                        )
                    }
                    val httpResponse = client.postAbs("$server/token")
                        .sendJson(credentials).coAwait()

                    testContext.verify {
                        assertTrue(
                            httpResponse.statusCode() >= 400,
                            "Expected 4xx for wrong credentials but got ${httpResponse.statusCode()}"
                        )
                        testContext.completeNow()
                    }
                }
            }
        }
    }

    private suspend fun VertxTestContext.verifyCoroutine(block: suspend () -> Unit) = coroutineScope {
        launch(coroutineContext) {
            try {
                block()
            } catch (t: Throwable) {
                failNow(t)
            }
        }
        this
    }
}
