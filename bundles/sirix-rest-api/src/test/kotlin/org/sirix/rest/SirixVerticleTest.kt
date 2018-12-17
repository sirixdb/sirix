package org.sirix.rest

import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.http.HttpHeaders
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.ext.web.client.sendAwait
import io.vertx.kotlin.ext.web.client.sendJsonAwait
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.concurrent.TimeUnit

@ExtendWith(VertxExtension::class)
@DisplayName("Integration test")
class SirixVerticleTest {
    private val server = "https://localhost:9443"
    private val serverPath = "/database/resource1"

    private lateinit var client: WebClient

    @BeforeEach
    @DisplayName("Deploy a verticle")
    fun setup(vertx: Vertx, testContext: VertxTestContext) {
        val options = DeploymentOptions().setConfig(JsonObject().put("https.port", 9443))
        vertx.deployVerticle("org.sirix.rest.SirixVerticle", options, testContext.completing())

        client = WebClient.create(vertx, WebClientOptions().setTrustAll(true).setFollowRedirects(false))
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Test Login via POST-Request and send a subsequent GET-Request")
    fun login(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val expectString = """
                    <rest:sequence xmlns:rest="https://sirix.io/rest">
                      <rest:item>
                        <xml rest:id="1">
                          foo
                          <bar rest:id="3"/>
                        </xml>
                      </rest:item>
                    </rest:sequence>
                    """.trimIndent()

                val credentials = json {
                    obj("username" to "admin",
                            "password" to "admin")
                }

                val response = client.postAbs("$server/login").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    val accessToken = user.getString("access_token")

                    val httpResponse = client.getAbs("$server/$serverPath").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendAwait()

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectString.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                            testContext.completeNow()
                        }
                    }
                }

            }
        }
    }

    suspend fun VertxTestContext.verifyCoroutine(block: suspend () -> Unit) = coroutineScope {
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
