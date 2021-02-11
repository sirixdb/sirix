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
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
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
            JsonObject().put("port", 9443)
                .put("client.secret", "78a294c4-0492-4e44-a35f-7eb9cab0d831")
                .put("keycloak.url", "http://localhost:8080/auth/realms/sirixdb")
        )
        vertx.deployVerticle("org.sirix.rest.SirixVerticle", options, testContext.succeedingThenComplete())

        client = WebClient.create(vertx, WebClientOptions().setTrustAll(true).setFollowRedirects(false))
    }

    @AfterEach
    @DisplayName("Remove databases")
    fun delete(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val httpResponse = client.deleteAbs(server).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).send().await()

                if (204 == httpResponse.statusCode()) {
                    testContext.completeNow()
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
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

                val response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                var httpResponse = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
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
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").send().await()

                val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                var url = "$server$serverPath?nodeId=3&insert=asFirstChild"

                httpResponse =
                    client.postAbs(url).putHeader(
                        HttpHeaders.AUTHORIZATION.toString(),
                        "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml")
                        .putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBuffer(Buffer.buffer(xml)).await()
                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                }

                url = "$server/"

                httpResponse =
                    client.getAbs(url).addQueryParam("query", "xml:diff('database','resource1',1,2)").putHeader(
                        HttpHeaders.AUTHORIZATION.toString(),
                        "Bearer $accessToken"
                    ).putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").send().await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val expectDiffString = """
                            <rest:sequence xmlns:rest="https://sirix.io/rest">
                            let ${"$"}doc := xml:doc('database','resource1', 1)
                            return (
                              insert nodes <xml>foo<bar/></xml> as first into sdb:select-item(${"$"}doc, 3)
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

                if (testContext.failed()) {
                    throw testContext.causeOfFailure();
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
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

                val response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                var httpResponse = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                }

                httpResponse = client.getAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").send().await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                    testContext.completeNow()
                }

                if (testContext.failed()) {
                    throw testContext.causeOfFailure();
                }
            }
        }
    }

    @Test
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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

                val response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                var httpResponse = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                }

                httpResponse = client.headAbs("$server$serverPath?nodeId=3").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").send().await()

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
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                }

                httpResponse = client.getAbs("$server$serverPath?query=/xml/all-times::*").putHeader(
                    HttpHeaders
                        .AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").send().await()

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
                    assertEquals(200, response.statusCode())
                    val result =
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            .replace(" rest:revisionTimestamp=\"(?!\").*\"".toRegex(), "")
                    assertEquals(expectedResult.replace("\n", System.getProperty("line.separator")), result)
                    testContext.completeNow()
                }

                if (testContext.failed()) {
                    throw testContext.causeOfFailure();
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

                val response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                var httpResponse = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                }

                val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                httpResponse = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                    testContext.completeNow()
                }

                if (testContext.failed()) {
                    throw testContext.causeOfFailure();
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

                val response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                var httpResponse = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
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
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBuffer(
                        Buffer.buffer("{\"query\":\"xml:doc('database','resource1')//bar\"}")
                    ).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    val result =
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            .replace(" rest:revisionTimestamp=\"(?!\").*\"".toRegex(), "")
                    assertEquals(expectedResult.replace("\n", System.getProperty("line.separator")), result)
                    testContext.completeNow()
                }

                if (testContext.failed()) {
                    throw testContext.causeOfFailure();
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing an XQuery-expression paged in a GET-request")
    fun testXQueryPagedWithBeginAndEndSequenceIndex(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val expectString = """
                    <rest:sequence xmlns:rest="https://sirix.io/rest">
                      <rest:item>
                        <xml rest:id="1">
                          <foo rest:id="2"/>
                          <bar rest:id="3"/>
                          <baz rest:id="4"/>
                          <foobar rest:id="5"/>
                        </xml>
                      </rest:item>
                    </rest:sequence>
                    """.trimIndent()

                val xml = """
                    <xml>
                      <foo/>
                      <bar/>
                      <baz/>
                      <foobar/>
                    </xml>
                """.trimIndent()

                val credentials = json {
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                var httpResponse = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                }

                val expectedResult = """
                        <rest:sequence xmlns:rest="https://sirix.io/rest">
                          <rest:item rest:revision="1">
                            <foo rest:id="2"/>
                          </rest:item>
                          <rest:item rest:revision="1">
                            <bar rest:id="3"/>
                          </rest:item>
                          <rest:item rest:revision="1">
                            <baz rest:id="4"/>
                          </rest:item>
                        </rest:sequence>
                    """.trimIndent()

                httpResponse =
                    client.getAbs("$server$serverPath?query=%2Fdescendant::*&startResultSeqIndex=1&endResultSeqIndex=3")
                        .putHeader(
                            HttpHeaders.AUTHORIZATION
                                .toString(), "Bearer $accessToken"
                        ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").send().await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    val result =
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            .replace(" rest:revisionTimestamp=\"(?!\").*\"".toRegex(), "")
                    assertEquals(expectedResult.replace("\n", System.getProperty("line.separator")), result)
                    testContext.completeNow()
                }

                if (testContext.failed()) {
                    throw testContext.causeOfFailure();
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing an XQuery-expression paged in a GET-request")
    fun testXQueryPagedWithBeginIndex(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val expectString = """
                    <rest:sequence xmlns:rest="https://sirix.io/rest">
                      <rest:item>
                        <xml rest:id="1">
                          <foo rest:id="2"/>
                          <bar rest:id="3"/>
                        </xml>
                      </rest:item>
                    </rest:sequence>
                    """.trimIndent()

                val xml = """
                    <xml>
                      <foo/>
                      <bar/>
                    </xml>
                """.trimIndent()

                val credentials = json {
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                var httpResponse = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                }

                val expectedResult = """
                        <rest:sequence xmlns:rest="https://sirix.io/rest">
                          <rest:item rest:revision="1">
                            <foo rest:id="2"/>
                          </rest:item>
                          <rest:item rest:revision="1">
                            <bar rest:id="3"/>
                          </rest:item>
                        </rest:sequence>
                    """.trimIndent()


                httpResponse =
                    client.getAbs("$server$serverPath?query=%2Fdescendant::*&startResultSeqIndex=1").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").send().await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    val result =
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            .replace(" rest:revisionTimestamp=\"(?!\").*\"".toRegex(), "")
                    assertEquals(expectedResult.replace("\n", System.getProperty("line.separator")), result)
                    testContext.completeNow()
                }

                if (testContext.failed()) {
                    throw testContext.causeOfFailure();
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

                val response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                var httpResponse = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                }

                httpResponse = client.headAbs("$server$serverPath?nodeId=3").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").send().await()

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
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                    testContext.completeNow()
                }

                if (testContext.failed()) {
                    throw testContext.causeOfFailure();
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

                val response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                var httpResponse = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                }

                httpResponse = client.headAbs("$server$serverPath?nodeId=3").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").send().await()

                val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                val url = "$server$serverPath?nodeId=3"

                httpResponse = client.deleteAbs(url).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode).send().await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    testContext.completeNow()
                }

                if (testContext.failed()) {
                    throw testContext.causeOfFailure();
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing serialization up to a specific level")
    fun testSerialize(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val expectString = """
                    <rest:sequence xmlns:rest="https://sirix.io/rest">
                      <rest:item>
                        <p:a xmlns:p="ns" rest:id="1" i="j">
                          oops1
                          <b rest:id="5">
                            foo
                            <c rest:id="7"/>
                          </b>
                          oops2
                          <b rest:id="9" p:x="y">
                            <c rest:id="11"/>
                            bar
                          </b>
                          oops3
                        </p:a>
                      </rest:item>
                    </rest:sequence>
                    """.trimIndent()

                val xml = """
                    <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                    <p:a xmlns:p="ns" i="j">
                      oops1
                      <b>
                        foo
                        <c/>
                      </b>
                      oops2
                      <b p:x="y">
                        <c/>
                        bar
                      </b>
                      oops3
                    </p:a>
                """.trimIndent()

                val credentials = json {
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                var httpResponse = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").sendBuffer(Buffer.buffer(xml)).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                }

                val expectQueryResult = """
                    <rest:sequence xmlns:rest="https://sirix.io/rest">
                      <rest:item>
                        <p:a xmlns:p="ns" rest:id="1" i="j">
                          oops1
                          <b rest:id="5"/>
                          oops2
                          <b rest:id="9" p:x="y"/>
                          oops3
                        </p:a>
                      </rest:item>
                    </rest:sequence>
                    """.trimIndent()

                httpResponse = client.getAbs("$server$serverPath?maxLevel=2").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/xml").send().await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                    assertEquals(
                        expectQueryResult.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                    )
                    testContext.completeNow()
                }

                if (testContext.failed()) {
                    throw testContext.causeOfFailure();
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
