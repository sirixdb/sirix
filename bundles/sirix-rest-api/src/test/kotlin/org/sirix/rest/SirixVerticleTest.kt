package org.sirix.rest

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
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.ext.web.client.sendAwait
import io.vertx.kotlin.ext.web.client.sendBufferAwait
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
    @DisplayName("POST-Request followed by sending a PUT-Request and then send a subsequent GET-Request")
    fun testGet(vertx: Vertx, testContext: VertxTestContext) {
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

                val xml = """
                    <xml>
                      foo
                      <bar/>
                    </xml>
                """.trimIndent()

                val credentials = json {
                    obj("username" to "admin",
                            "password" to "admin")
                }

                val response = client.postAbs("$server/login").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    val accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectString.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    httpResponse = client.getAbs("$server$serverPath").putHeader(HttpHeaders.AUTHORIZATION
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

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("POST-Request followed by sending a PUT-Request, then a POST-Request and then send a subsequent " +
            "GET-Request")
    fun testPUTthenPOSTthenGet(vertx: Vertx, testContext: VertxTestContext) {
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

                val xml = """
                    <xml>
                      foo
                      <bar/>
                    </xml>
                """.trimIndent()

                val credentials = json {
                    obj("username" to "admin",
                            "password" to "admin")
                }

                val response = client.postAbs("$server/login").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    val accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectString.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    val expectUpdatedString = """
                    <rest:sequence xmlns:rest="https://sirix.io/rest">
                      <rest:item>
                        <xml rest:id="1">
                          foo
                          <bar rest:id="3">
                            <xml rest:id="4">
                              foo
                              <bar rest:id="6"/>
                            </xml>
                          </bar>
                        </xml>
                      </rest:item>
                    </rest:sequence>
                    """.trimIndent()

                    val url = "$server$serverPath?nodeId=3&insert=asFirstChild"

                    httpResponse = client.postAbs(url).putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    httpResponse = client.getAbs("$server$serverPath?query=/xml/all-time::*").putHeader(HttpHeaders
                            .AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendAwait()

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            println(httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                            testContext.completeNow()
                        }
                    }
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("POST-Request followed by sending a PUT-Request and then send a subsequent PUT-Request")
    fun testPut(vertx: Vertx, testContext: VertxTestContext) {
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

                val xml = """
                    <xml>
                      foo
                      <bar/>
                    </xml>
                """.trimIndent()

                val credentials = json {
                    obj("username" to "admin",
                            "password" to "admin")
                }

                val response = client.postAbs("$server/login").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    val accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectString.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    httpResponse = client.putAbs("$server$serverPath").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendBufferAwait(Buffer.buffer(xml))

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

    @Test
    @Timeout(value = 10000, timeUnit = TimeUnit.SECONDS)
    @DisplayName("POST-Request followed by sending a PUT-Request and then send a subsequent POST-Request in order to " +
            "send an XQuery-expression")
    fun testXQueryPost(vertx: Vertx, testContext: VertxTestContext) {
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

                val xml = """
                    <xml>
                      foo
                      <bar/>
                    </xml>
                """.trimIndent()

                val credentials = json {
                    obj("username" to "admin",
                            "password" to "admin")
                }

                val response = client.postAbs("$server/login").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    val accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectString.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    val expectedResult = """
                        <rest:sequence xmlns:rest="https://sirix.io/rest">
                          <rest:item rest:revision="1">
                            <bar rest:id="3"/>
                          </rest:item>
                        </rest:sequence>
                    """.trimIndent()

                    val url = "$server"

                    httpResponse = client.postAbs(url).putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendBufferAwait(Buffer.buffer("sdb:doc('database', " +
                            "'resource1')//bar"))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            val result =
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                                            .replace(" rest:revisionTimestamp=\"(?!\").*\"".toRegex(), "")
                            assertEquals(expectedResult.replace("\n", System.getProperty("line.separator")), result)
                            testContext.completeNow()
                        }
                    }
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("POST-Request followed by sending a PUT-Request and then send a subsequent POST-Request")
    fun testPost(vertx: Vertx, testContext: VertxTestContext) {
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

                val xml = """
                    <xml>
                      foo
                      <bar/>
                    </xml>
                """.trimIndent()

                val credentials = json {
                    obj("username" to "admin",
                            "password" to "admin")
                }

                val response = client.postAbs("$server/login").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    val accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectString.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    val expectUpdatedString = """
                    <rest:sequence xmlns:rest="https://sirix.io/rest">
                      <rest:item>
                        <xml rest:id="1">
                          foo
                          <bar rest:id="3">
                            <xml rest:id="4">
                              foo
                              <bar rest:id="6"/>
                            </xml>
                          </bar>
                        </xml>
                      </rest:item>
                    </rest:sequence>
                    """.trimIndent()

                    val url = "$server$serverPath?nodeId=3&insert=asFirstChild"

                    httpResponse = client.postAbs(url).putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                            testContext.completeNow()
                        }
                    }
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("POST-Request followed by sending a PUT-Request and then send a subsequent DELETE-Request")
    fun testDelete(vertx: Vertx, testContext: VertxTestContext) {
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

                val xml = """
                    <xml>
                      foo
                      <bar/>
                    </xml>
                """.trimIndent()

                val credentials = json {
                    obj("username" to "admin",
                            "password" to "admin")
                }

                val response = client.postAbs("$server/login").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    val accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectString.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    val expectUpdatedString = """
                    <rest:sequence xmlns:rest="https://sirix.io/rest">
                      <rest:item>
                        <xml rest:id="1">
                          foo
                        </xml>
                      </rest:item>
                    </rest:sequence>
                    """.trimIndent()

                    val url = "$server$serverPath?nodeId=3"

                    httpResponse = client.deleteAbs(url).putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").sendAwait()

                    if (200 == httpResponse.statusCode()) {
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
