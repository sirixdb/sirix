package org.sirix.rest

import io.vertx.core.DeploymentOptions
import io.vertx.core.MultiMap
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.ext.web.client.WebClientSession
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.ext.web.client.sendAwait
import io.vertx.kotlin.ext.web.client.sendBufferAwait
import io.vertx.kotlin.ext.web.client.sendFormAwait
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.jsoup.Jsoup
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
    private lateinit var sessionClient: WebClientSession

    @BeforeEach
    @DisplayName("Deploy a verticle")
    fun setup(vertx: Vertx, testContext: VertxTestContext) {
        val options = DeploymentOptions().setConfig(JsonObject().put("https.port", 9443))
        vertx.deployVerticle("org.sirix.rest.SirixVerticle", options, testContext.completing())

        client = WebClient.create(vertx, WebClientOptions().setTrustAll(true).setFollowRedirects(false))
        sessionClient = WebClientSession.create(client)
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Test HTTP-PUT method")
    fun testPut(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine { executePut(testContext) }
        }
    }

    private suspend fun executePut(testContext: VertxTestContext) {
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

        val resource = Buffer.buffer("<xml>foo<bar/></xml>")

        var response = sessionClient.putAbs("$server$serverPath")
                .ssl(true)
                .followRedirects(false)
                .sendBufferAwait(resource)

        if (302 == response.statusCode()) {
            val location = response.getHeader(HttpHeaders.LOCATION.toString())

            response = sessionClient.getAbs(location).sendAwait()

            if (200 == response.statusCode()) {
                response = sendFormForLoginPage(response, sessionClient)

                if (302 == response.statusCode()) {
                    val location = response.getHeader(HttpHeaders.LOCATION.toString())

                    response = sessionClient.getAbs(location).ssl(true).sendAwait()

                    if (302 == response.statusCode()) {
                        val location = response.getHeader(HttpHeaders.LOCATION.toString())

                        response = sessionClient.putAbs("$server$location").sendBufferAwait(resource)

                        if (200 == response.statusCode()) {
                            testContext.verify {
                                assertEquals(expectString.replace("\n", System.getProperty("line.separator")),
                                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                                testContext.completeNow()
                            }
                        }
                    }
                }
            }
        }
    }

    private suspend fun sendFormForLoginPage(response: HttpResponse<Buffer>, sessionClient: WebClientSession): HttpResponse<Buffer> {
        var response1 = response
        val document = Jsoup.parse(response1.bodyAsString())

        val formElement = document.getElementById("kc-form-login")
        val url = formElement.attr("action")

        val form = MultiMap.caseInsensitiveMultiMap()
        form.set("username", "admin")
        form.set("password", "admin")

        response1 = sessionClient.postAbs(url)
                .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/x-www-form-urlencoded")
                .putHeader(HttpHeaders.CONTENT_LENGTH.toString(), form.toString().length.toString())
                .sendFormAwait(form)
        return response1
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Test HTTP-PUT and then HTTP-GET")
    fun testGet(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                executePut(testContext)

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

                var response = sessionClient.getAbs("$server$serverPath")
                        .ssl(true)
                        .followRedirects(false)
                        .sendAwait()

                if (302 == response.statusCode()) {
                    val location = response.getHeader(HttpHeaders.LOCATION.toString())

                    response = sessionClient.getAbs(location).sendAwait()

                    if (200 == response.statusCode()) {
                        response = sendFormForLoginPage(response, sessionClient)

                        if (302 == response.statusCode()) {
                            val location = response.getHeader(HttpHeaders.LOCATION.toString())

                            response = sessionClient.getAbs(location).ssl(true).sendAwait()

                            if (302 == response.statusCode()) {
                                val location = response.getHeader(HttpHeaders.LOCATION.toString())

                                response = sessionClient.getAbs("$server$location").sendAwait()

                                if (200 == response.statusCode()) {
                                    testContext.verify {
                                        assertEquals(expectString.replace("\n", System.getProperty
                                        ("line.separator")),
                                                response.bodyAsString().replace("\r\n", System.getProperty("line.separator")))

                                        testContext.completeNow()
                                    }
                                }
                            }
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
