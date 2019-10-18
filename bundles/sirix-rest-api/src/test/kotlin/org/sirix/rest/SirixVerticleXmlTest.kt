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
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.concurrent.TimeUnit

@ExtendWith(VertxExtension::class)
@DisplayName("Integration test")
class SirixVerticleXmlTest {
    private val server = "https://localhost:9443"
    private val serverPath = "/database/resource1"
    private var accessToken = ""

    private lateinit var client: WebClient

    @BeforeEach
    @DisplayName("Deploy a verticle")
    fun setup(vertx: Vertx, testContext: VertxTestContext) {
        val options = DeploymentOptions().setConfig(
            JsonObject().put("https.port", 9443)
                .put("client.secret", "2fcc8dda-0362-4923-bdab-fd1b78eae2d1")
                .put("keycloak.url", "http://localhost:8080/auth/realms/sirixdb")
        )
        vertx.deployVerticle("org.sirix.rest.SirixVerticle", options, testContext.completing())

        client = WebClient.create(vertx, WebClientOptions().setTrustAll(true).setFollowRedirects(false))

        delete(vertx, testContext)
    }

    @AfterEach
    @DisplayName("Remove databases")
    fun delete(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val httpResponse = client.deleteAbs(server).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).sendAwait()

                if (200 == httpResponse.statusCode()) {
                    testContext.completeNow()
                }
            }
        }
    }

    @Test
    @Timeout(value = 1000, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing diffing of a database/resource with two revisions.")
    fun testDiff(vertx: Vertx, testContext: VertxTestContext) {
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
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

                    httpResponse = client.headAbs("$server$serverPath?nodeId=3").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendAwait()

                    val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                    var url = "$server$serverPath?nodeId=3&insert=asFirstChild"

                    httpResponse =
                        client.postAbs(url).putHeader(
                            HttpHeaders.AUTHORIZATION.toString(),
                            "Bearer $accessToken"
                        ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                            .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml")
                            .putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
                        }
                    }

                    url = "$server/"

                    httpResponse =
                        client.getAbs(url).addQueryParam("query", "sdb:diff('database','resource1',1,2)").putHeader(
                            HttpHeaders.AUTHORIZATION.toString(),
                            "Bearer $accessToken"
                        ).putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendAwait()

                    if (200 == httpResponse.statusCode()) {
                        val expectDiffString = """
                            <rest:sequence xmlns:rest="https://sirix.io/rest">
                            let ${"$"}doc := sdb:doc('database','resource1', 1)
                            return (
                              insert nodes <xml>foo<bar/></xml> as first into sdb:select-node(${"$"}doc, 3)
                            )
                            </rest:sequence>
                        """.trimIndent()

                        val responseBody = httpResponse.bodyAsString()

                        testContext.verify {
                            assertEquals(
                                expectDiffString.replace("\n", System.getProperty("line.separator")),
                                responseBody.replace("\r\n", System.getProperty("line.separator"))
                            )
                            testContext.completeNow()
                        }
                    }
                }
            }
        }
    }

    @Test
    @Timeout(value = 1000, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing viewing of a database/resource content")
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
                        }
                    }

                    httpResponse = client.getAbs("$server$serverPath").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendAwait()

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
                            testContext.completeNow()
                        }
                    }
                }

            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the creation and storage of a database/resource as well as a subsequent modification thereof")
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
                        }
                    }

                    httpResponse = client.headAbs("$server$serverPath?nodeId=3").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendAwait()

                    val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

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

                    httpResponse = client.postAbs(url).putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml")
                        .putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
                        }
                    }

                    httpResponse = client.getAbs("$server$serverPath?query=/xml/all-times::*").putHeader(
                        HttpHeaders
                            .AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendAwait()

                    if (200 == httpResponse.statusCode()) {
                        val expectedResult = """
                            <rest:sequence xmlns:rest="https://sirix.io/rest">
                              <rest:item rest:revision="1">
                                <xml rest:id="1">
                                  foo
                                  <bar rest:id="3"/>
                                </xml>
                              </rest:item>
                              <rest:item rest:revision="2">
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
    @DisplayName("Testing the creation and storage of a database/resource")
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
                        }
                    }

                    val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                    httpResponse = client.putAbs("$server$serverPath").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml")
                        .putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
                            testContext.completeNow()
                        }
                    }
                }

            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing an XQuery-expression sent via the body of a POST-request")
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
                        }
                    }

                    val expectedResult = """
                        <rest:sequence xmlns:rest="https://sirix.io/rest">
                          <rest:item rest:revision="1">
                            <bar rest:id="3"/>
                          </rest:item>
                        </rest:sequence>
                    """.trimIndent()

                    httpResponse = client.postAbs(server).putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBufferAwait(
                            Buffer.buffer("sdb:doc('database','resource1')//bar")
                        )

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
    @DisplayName("Testing the update of a resource")
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
                        }
                    }

                    httpResponse = client.headAbs("$server$serverPath?nodeId=3").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendAwait()

                    val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

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

                    httpResponse = client.postAbs(url).putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml")
                        .putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
                            testContext.completeNow()
                        }
                    }
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a subtree of a resource")
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJsonAwait(credentials)

                if (200 == response.statusCode()) {
                    val user = response.bodyAsJsonObject()
                    accessToken = user.getString("access_token")

                    var httpResponse = client.putAbs("$server$serverPath").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBufferAwait(Buffer.buffer(xml))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(
                                expectString.replace("\n", System.getProperty("line.separator")),
                                httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            )
                        }
                    }

                    httpResponse = client.headAbs("$server$serverPath?nodeId=3").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendAwait()

                    val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                    val url = "$server$serverPath?nodeId=3"

                    httpResponse = client.deleteAbs(url).putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ETAG.toString(), hashCode).sendAwait()

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
