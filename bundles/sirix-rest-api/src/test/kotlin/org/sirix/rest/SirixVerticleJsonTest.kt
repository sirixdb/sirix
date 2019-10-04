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
class SirixVerticleJsonTest {
    private val server = "https://localhost:9443"
    private val serverPath = "/database/json-resource"

    private lateinit var client: WebClient

    @BeforeEach
    @DisplayName("Deploy a verticle")
    fun setup(vertx: Vertx, testContext: VertxTestContext) {
        val options = DeploymentOptions().setConfig(JsonObject().put("https.port", 9443)
                .put("client.secret", "c8b9b4ed-67bb-47d9-bd73-a3babc470b2c")
                .put("keycloak.url", "http://localhost:8080/auth/realms/master"))
        vertx.deployVerticle("org.sirix.rest.SirixVerticle", options, testContext.completing())

        client = WebClient.create(vertx, WebClientOptions().setTrustAll(true).setFollowRedirects(false))
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing viewing of a database/resource content")
    fun testGet(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val expectedJson = """
                    {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                """.trimIndent()

                val json = """
                 {
                   "foo": ["bar", null, 2.33],
                   "bar": { "hello": "world", "helloo": true },
                   "baz": "hello",
                   "tada": [{"foo":"bar"},{"baz":false},"boo",{},[]]
                 }
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
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendBufferAwait(Buffer.buffer(json))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectedJson.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    httpResponse = client.getAbs("$server$serverPath").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendAwait()

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectedJson.replace("\n", System.getProperty("line.separator")),
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
    @DisplayName("Testing a simple query to get the node-key of a node")
    fun testGetQuery(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val expectedJson = """
                    {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                """.trimIndent()

                val json = """
                 {
                   "foo": ["bar", null, 2.33],
                   "bar": { "hello": "world", "helloo": true },
                   "baz": "hello",
                   "tada": [{"foo":"bar"},{"baz":false},"boo",{},[]]
                 }
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
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendBufferAwait(Buffer.buffer(json))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectedJson.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    httpResponse = client.getAbs("$server$serverPath?query=let%20%24nodeKey%20%3A%3D%20sdb%3Anodekey(.%3D%3Efoo%5B%5B2%5D%5D)%0Areturn%20%7B%22nodeKey%22%3A%20%24nodeKey%7D").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendAwait()

                    if (200 == httpResponse.statusCode()) {
                        val expectedQueryResponse = """
                                {"rest":[{"nodeKey":6}]}
                        """.trimIndent()

                        testContext.verify {
                            assertEquals(expectedQueryResponse.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                            testContext.completeNow()
                        }
                    }
                }

            }
        }
    }

    @Test
    @Timeout(value = 1000, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the creation and storage of a database/resource")
    fun testPut(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val expectedJson = """
                    {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                """.trimIndent()

                val json = """
                 {
                   "foo": ["bar", null, 2.33],
                   "bar": { "hello": "world", "helloo": true },
                   "baz": "hello",
                   "tada": [{"foo":"bar"},{"baz":false},"boo",{},[]]
                 }
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
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendBufferAwait(Buffer.buffer(json))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectedJson.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                    httpResponse = client.putAbs("$server$serverPath").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBufferAwait(Buffer.buffer(json))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectedJson.replace("\n", System.getProperty("line.separator")),
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
    @DisplayName("Testing the update of a resource")
    fun testPost(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val expectedJson = """
                    {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                """.trimIndent()

                val json = """
                 {
                   "foo": ["bar", null, 2.33],
                   "bar": { "hello": "world", "helloo": true },
                   "baz": "hello",
                   "tada": [{"foo":"bar"},{"baz":false},"boo",{},[]]
                 }
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
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendBufferAwait(Buffer.buffer(json))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectedJson.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    httpResponse = client.headAbs("$server$serverPath?nodeId=6").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendAwait()

                    val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                    val expectUpdatedString = """
                        {"foo":["bar",null,2.33,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()

                    val url = "$server$serverPath?nodeId=6&insert=asRightSibling"

                    httpResponse = client.postAbs(url).putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBufferAwait(Buffer.buffer("{\"tadaaa\":true}"))

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
    @Timeout(value = 10000, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a subtree of a resource")
    fun testDelete(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val expectedJson = """
                    {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                """.trimIndent()

                val json = """
                 {
                   "foo": ["bar", null, 2.33],
                   "bar": { "hello": "world", "helloo": true },
                   "baz": "hello",
                   "tada": [{"foo":"bar"},{"baz":false},"boo",{},[]]
                 }
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
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendBufferAwait(Buffer.buffer(json))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectedJson.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    httpResponse = client.headAbs("$server$serverPath?nodeId=4").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendAwait()

                    val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                    val url = "$server$serverPath?nodeId=4"

                    httpResponse = client.deleteAbs(url).putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").putHeader(HttpHeaders.ETAG.toString(), hashCode).sendAwait()

                    if (200 == httpResponse.statusCode()) {
                        testContext.completeNow()
                    }
                }
            }
        }
    }

    @Test
    @Timeout(value = 10000, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the creation and storage of a database/resource as well as a subsequent modification thereof")
    fun testPUTthenPOSTthenGet(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val expectedJson = """
                    {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                """.trimIndent()

                val json = """
                 {
                   "foo": ["bar", null, 2.33],
                   "bar": { "hello": "world", "helloo": true },
                   "baz": "hello",
                   "tada": [{"foo":"bar"},{"baz":false},"boo",{},[]]
                 }
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
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendBufferAwait(Buffer.buffer(json))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectedJson.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    httpResponse = client.headAbs("$server$serverPath?nodeId=6").putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendAwait()

                    val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                    val expectUpdatedString = """
                        {"foo":["bar",null,2.33,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()

                    val url = "$server$serverPath?nodeId=6&insert=asRightSibling"

                    httpResponse = client.postAbs(url).putHeader(HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBufferAwait(Buffer.buffer("{\"tadaaa\":true}"))

                    if (200 == httpResponse.statusCode()) {
                        testContext.verify {
                            assertEquals(expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                        }
                    }

                    httpResponse = client.getAbs("$server$serverPath?query=jn:all-times(.)").putHeader(HttpHeaders
                            .AUTHORIZATION
                            .toString(), "Bearer $accessToken").putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendAwait()

                    if (200 == httpResponse.statusCode()) {
                        val expectedResult = """
                           {"rest":[{"revisionNumber":1,"revision":{"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}},{"revisionNumber":2,"revision":{"foo":["bar",null,2.33,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}}]}
                        """.trimIndent()

                        testContext.verify {
                            println(httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                            val result =
                                    httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                                            .replace("\"revisionTimestamp\":\"(?!\").+?\",\"revision".toRegex(), "\"revision")
                            println(result)
                            assertEquals(expectedResult.replace("\n", System.getProperty("line.separator")), result)
                            testContext.completeNow()
                        }
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
