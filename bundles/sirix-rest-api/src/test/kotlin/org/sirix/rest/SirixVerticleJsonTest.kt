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
import io.vertx.kotlin.coroutines.awaitBlocking
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.extension.ExtendWith
import org.skyscreamer.jsonassert.JSONAssert
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit


@ExtendWith(VertxExtension::class)
@DisplayName("Integration test")
class SirixVerticleJsonTest {
    private val server = "https://localhost:9443"
    private val serverPath = "/database/json-resource"
    private var accessToken = ""

    private lateinit var client: WebClient

    @BeforeEach
    @DisplayName("Deploy a verticle")
    fun setup(vertx: Vertx, testContext: VertxTestContext) {
        val options = DeploymentOptions().setConfig(
            JsonObject().put("port", 9443)
                .put("client.secret", "78a294c4-0492-4e44-a35f-7eb9cab0d831") // "64aaf9b2-9ea1-43cd-bcb6-87d2f430aaa2"
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
                } else {
                    testContext.failNow(httpResponse.bodyAsString())
                }
            }
        }
    }

    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource1(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDeleteResource(testContext)
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the retrieval of the history of a resource")
    fun testResourceHistory(vertx: Vertx, testContext: VertxTestContext) {
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
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJson.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, httpResponse.statusCode())
                }

                httpResponse = client.headAbs("$server$serverPath?nodeId=6").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                val expectUpdatedString = """
                        {"foo":["bar",null,2.33,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()

                val url = "$server$serverPath?nodeId=6&insert=asRightSibling"

                httpResponse = client.postAbs(url).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("{\"tadaaa\":true}")).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, httpResponse.statusCode())
                }

                httpResponse = client.getAbs("$server$serverPath/history").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                val expectedHistoryJsonString = """
                    {"history":[{"revision":2,"revisionTimestamp":"2020-02-12T17:59:59.457Z","author":"admin","commitMessage":""},{"revision":1,"revisionTimestamp":"2020-02-12T17:59:59.457Z","author":"admin","commitMessage":""}]}
                """.trimIndent()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedHistoryJsonString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace(
                            "\"revisionTimestamp\":\"(?!\").+?\"".toRegex(),
                            "\"revisionTimestamp\":\"2020-02-12T17:59:59.457Z\""
                        ),
                        false
                    )
                    assertEquals(200, httpResponse.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource2(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDeleteResource(testContext)
            }
        }
    }

    private suspend fun testDeleteResource(testContext: VertxTestContext) {
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

        val httpPutResponseJson =
            client.putAbs("$server/database/resource").putHeader(
                HttpHeaders.AUTHORIZATION
                    .toString(), "Bearer $accessToken"
            ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                .sendBuffer(Buffer.buffer("{}")).await()

        testContext.verify {
            assertEquals(200, httpPutResponseJson.statusCode())
        }

        val httpDeleteResponseJson = client.deleteAbs("$server/database/resource").putHeader(
            HttpHeaders.AUTHORIZATION
                .toString(), "Bearer $accessToken"
        ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
            .send().await()

        testContext.verify {
            assertNull(httpDeleteResponseJson.bodyAsString())
            assertEquals(204, httpDeleteResponseJson.statusCode())
            testContext.completeNow()
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the retrieval of the diff of a resource")
    fun testResourceDiff(vertx: Vertx, testContext: VertxTestContext) {
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
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJson.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, httpResponse.statusCode())
                }

                httpResponse = client.headAbs("$server$serverPath?nodeId=6").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                val hashCode = httpResponse.getHeader(HttpHeaders.ETAG.toString())

                val expectUpdatedString = """
                        {"foo":["bar",null,2.33,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()

                val url = "$server$serverPath?nodeId=6&insert=asRightSibling"

                httpResponse = client.postAbs(url).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("{\"tadaaa\":true}")).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, httpResponse.statusCode())
                }

                httpResponse = client.getAbs("$server$serverPath/diff?first-revision=1&second-revision=2").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                val expectedDiffJsonString = """
                    {"database":"database","resource":"json-resource","old-revision":1,"new-revision":2,"diffs":[{"insert":{"nodeKey":26,"insertPositionNodeKey":6,"insertPosition":"asRightSibling","type":"jsonFragment","data":"{\"tadaaa\":true}"}}]}
                """.trimIndent()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedDiffJsonString,
                        httpResponse.bodyAsString(),
                        false
                    )
                    assertEquals(200, httpResponse.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource3(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDeleteResource(testContext)
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a database")
    fun testDeleteDatabase(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
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

                val httpPutResponseJson =
                    client.putAbs("$server/database/resource").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{}")).await()

                testContext.verify {
                    assertEquals(200, httpPutResponseJson.statusCode())
                }

                val httpDeleteResponseJson = client.deleteAbs("$server/database").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .send().await()

                testContext.verify {
                    assertNull(httpDeleteResponseJson.bodyAsString())
                    assertEquals(204, httpDeleteResponseJson.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Test
    @Timeout(value = 100000, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the serialization of the first N-records")
    fun testRecordSerializer(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val json = """
                 {
                   "foo": ["bar", null, 2.33],
                   "bar": { "hello": "world", "helloo": true },
                   "baz": "hello",
                   "tada": [{"foo":"bar"},{"baz":false},"boo",{},[]]
                 }
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

                val httpPutResponseJson =
                    client.putAbs("$server/database/resource").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    assertEquals(200, httpPutResponseJson.statusCode())
                }

                var httpGetResponseJson = client.getAbs("$server/database/resource?nextTopLevelNodes=3").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .send().await()

                testContext.verify {
                    val expectedJson = """
                         {"foo":["bar",null,2.33]},{"bar":{"hello":"world","helloo":true}},{"baz":"hello"}}
                     """.trimIndent()

                    JSONAssert.assertEquals(
                        expectedJson,
                        httpGetResponseJson.bodyAsString(),
                        false
                    )
                    assertEquals(200, httpGetResponseJson.statusCode())
                }

                httpGetResponseJson = client.getAbs("$server/database/resource?nextTopLevelNodes=3&lastTopLevelNodeKey=13").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .send().await()

                testContext.verify {
                    val expectedJson = """
                        {"tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                     """.trimIndent()

                    JSONAssert.assertEquals(
                        expectedJson,
                        httpGetResponseJson.bodyAsString(),
                        false
                    )
                    assertEquals(200, httpGetResponseJson.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource4(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDeleteResource(testContext)
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the listing of databases")
    fun testListDatabases(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
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

                var httpResponseJson =
                    client.deleteAbs(server).putHeader(
                        HttpHeaders.AUTHORIZATION.toString(),
                        "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").send().await()

                var httpResponseXml =
                    client.deleteAbs(server).putHeader(
                        HttpHeaders.AUTHORIZATION.toString(),
                        "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml").send().await()

                testContext.verify {
                    assertEquals(204, httpResponseJson.statusCode())
                    assertEquals(204, httpResponseXml.statusCode())
                }

                httpResponseJson =
                    client.putAbs("$server/database1").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").send().await()

                testContext.verify {
                    assertEquals(201, httpResponseJson.statusCode())
                }

                httpResponseXml = client.putAbs("$server/database2").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .send().await()

                testContext.verify {
                    assertEquals(201, httpResponseXml.statusCode())
                }

                val expectedResult = """
                            {"databases":[{"name":"database1","type":"json"},{"name":"database2","type":"xml"}]}
                        """.trimIndent()

                httpResponseJson = client.getAbs(server).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                testContext.verify {
                    val result =
                        httpResponseJson.bodyAsString().replace(
                            "\r\n",
                            System.getProperty("line.separator")
                        )
                    JSONAssert.assertEquals(
                        expectedResult.replace("\n", System.getProperty("line.separator")),
                        result,
                        false
                    )
                    assertEquals(200, httpResponseJson.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource5(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDeleteResource(testContext)
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the listing of databases with resources")
    fun testListDatabasesWithResource(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
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

                var httpResponseJson =
                    client.deleteAbs(server).putHeader(
                        HttpHeaders.AUTHORIZATION.toString(),
                        "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").send().await()

                var httpResponseXml =
                    client.deleteAbs(server).putHeader(
                        HttpHeaders.AUTHORIZATION.toString(),
                        "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml").send().await()

                testContext.verify {
                    assertEquals(204, httpResponseJson.statusCode())
                    assertEquals(204, httpResponseXml.statusCode())
                }

                httpResponseJson =
                    client.putAbs("$server/database1/resource1").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{}")).await()

                testContext.verify {
                    assertEquals(200, httpResponseJson.statusCode())
                }

                httpResponseJson =
                    client.putAbs("$server/database1/resource2").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .sendBuffer(Buffer.buffer("{}")).await()

                testContext.verify {
                    assertEquals(200, httpResponseJson.statusCode())
                }

                httpResponseXml = client.putAbs("$server/database2/resource1").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .sendBuffer(Buffer.buffer("<root/>")).await()

                testContext.verify {
                    assertEquals(200, httpResponseXml.statusCode())
                }

                httpResponseXml = client.putAbs("$server/database3").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/xml")
                    .send().await()

                testContext.verify {
                    assertEquals(201, httpResponseXml.statusCode())
                }

                val expectedResult = """
                            {"databases":[{"name":"database1","type":"json","resources":["resource1","resource2"]},{"name":"database2","type":"xml","resources":["resource1"]},{"name":"database3","type":"xml","resources":[]}]}
                        """.trimIndent()

                httpResponseJson = client.getAbs("$server/?withResources=true").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                testContext.verify {
                    val result =
                        httpResponseJson.bodyAsString().replace(
                            "\r\n",
                            System.getProperty("line.separator")
                        )
                    JSONAssert.assertEquals(
                        expectedResult.replace("\n", System.getProperty("line.separator")),
                        result,
                        false
                    )
                    assertEquals(200, httpResponseJson.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource6(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDeleteResource(testContext)
            }
        }
    }


    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing insert, then update, then query, then delete")
    fun testInsertUpdateQueryAndDelete(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val expectedJson = """
                    []
                """.trimIndent()

                val json = """
                    []
                """.trimIndent()

                val credentials = json {
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                var response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                response = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJson.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                val hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                val updateURLInsertAsRightSibling = "$server$serverPath?nodeId=1&insert=asFirstChild"

                response = client.postAbs(updateURLInsertAsRightSibling).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("{\"city\": \"New York\", \"state\": \"NY\"}")).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val currentDateTime = LocalDateTime.now(ZoneId.of("UTC")).plus(500, ChronoUnit.MILLIS)

                val currentDateTimeAsString = currentDateTime.toString()

                val query =
                    "for \$i in bit:array-values(jn:open('database','json-resource',xs:dateTime('${currentDateTimeAsString}'))) where \$i=>city eq \"New York\" return { \$i, 'nodeKey': sdb:nodekey(\$i) }"

                val jsonData = json {
                    obj(
                        "query" to query
                    )
                }

                response = client.postAbs(server).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendJson(jsonData).await()

                val expectedJsonAnswer = """
                    {"rest":[{"city":"New York","state":"NY","nodeKey":2}]}
                """.trimIndent()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJsonAnswer.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                response = client.deleteAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendJson(jsonData).await()

                testContext.verify {
                    assertNull(response.bodyAsString())
                    assertEquals(204, response.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource7(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDelete(vertx, testContext)
            }
        }
    }


    @Test
    @Timeout(value = 100000, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing POST query with start index")
    fun testPostQueryWithStartIndex(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val json = json {
                    obj(
                        "query" to "jn:store('mycol.jn','mydoc.jn','[{\"generic\": 1, \"location\": {\"state\": \"CA\", \"city\": \"Los Angeles\"}}, {\"generic\": 1, \"location\": {\"state\": \"NY\", \"city\": \"New York\"}}]')"
                    )
                }

                val credentials = json {
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                var response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                response = client.postAbs(server).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendJson(json).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val query =
                    "for \$i in jn:doc('mycol.jn','mydoc.jn') where deep-equal(\$i=>generic, 1) return { \$i,'nodeKey': sdb:nodekey(\$i)}"

                val jsonData = json {
                    obj(
                        "startResultSeqIndex" to 1,
                        "query" to query
                    )
                }

                response = client.postAbs(server).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendJson(jsonData).await()

                val expectedJsonAnswer = """
                    {"rest":[{"generic":1,"location":{"state":"NY","city":"New York"},"nodeKey":11}]}
                """.trimIndent()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJsonAnswer.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource8(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDelete(vertx, testContext)
            }
        }
    }


    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing POST query with start and end index")
    fun testPostQueryWithStartAndEndZeroIndex(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val json = json {
                    obj(
                        "query" to "jn:store('mycol.jn','mydoc.jn','[{\"generic\": 1, \"location\": {\"state\": \"CA\", \"city\": \"Los Angeles\"}}, {\"generic\": 1, \"location\": {\"state\": \"NY\", \"city\": \"New York\"}}]')"
                    )
                }

                val credentials = json {
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                var response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                response = client.postAbs(server).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendJson(json).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val query =
                    "for \$i in jn:doc('mycol.jn','mydoc.jn') where deep-equal(\$i=>generic, 1) return { \$i,'nodeKey': sdb:nodekey(\$i)}"

                val jsonData = json {
                    obj(
                        "startResultSeqIndex" to 1,
                        "endResultSeqIndex" to 1,
                        "query" to query
                    )
                }

                response = client.postAbs(server).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendJson(jsonData).await()

                val expectedJsonAnswer = """
                    {"rest":[{"generic":1,"location":{"state":"NY","city":"New York"},"nodeKey":11}]}
                """.trimIndent()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJsonAnswer.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource9(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDelete(vertx, testContext)
            }
        }
    }


    @Test
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing POST query with start-end index")
    fun testPostQueryWithStartAndEndIndex(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                val json = json {
                    obj(
                        "query" to "jn:store('mycol.jn','mydoc.jn','[{\"generic\": 1, \"location\": {\"state\": \"CA\", \"city\": \"Los Angeles\"}}, {\"generic\": 1, \"location\": {\"state\": \"NY\", \"city\": \"New York\"}}]')"
                    )
                }

                val credentials = json {
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                var response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                response = client.postAbs(server).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendJson(json).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val query =
                    "for \$i in jn:doc('mycol.jn','mydoc.jn') where deep-equal(\$i=>generic, 1) return { \$i,'nodeKey': sdb:nodekey(\$i)}"

                val jsonData = json {
                    obj(
                        "startResultSeqIndex" to 0,
                        "endResultSeqIndex" to 1,
                        "query" to query
                    )
                }

                response = client.postAbs(server).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendJson(jsonData).await()

                val expectedJsonAnswer = """
                    {"rest":[{"generic":1,"location":{"state":"CA","city":"Los Angeles"},"nodeKey":2},{"generic":1,"location":{"state":"NY","city":"New York"},"nodeKey":11}]}
                """.trimIndent()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJsonAnswer.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource10(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDelete(vertx, testContext)
            }
        }
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                var response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                response = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJson.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                response = client.getAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJson.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
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
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJson.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, httpResponse.statusCode())
                }

                httpResponse =
                    client.getAbs("$server$serverPath?query=let%20%24nodeKey%20%3A%3D%20sdb%3Anodekey(.%3D%3Efoo%5B%5B2%5D%5D)%0Areturn%20%7B%22nodeKey%22%3A%20%24nodeKey%7D")
                        .putHeader(
                            HttpHeaders.AUTHORIZATION
                                .toString(), "Bearer $accessToken"
                        ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                val expectedQueryResponse = """
                    {"rest":[{"nodeKey":6}]}
                """.trimIndent()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedQueryResponse.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, httpResponse.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource12(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDelete(vertx, testContext)
            }
        }
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                var response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                response = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJson.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                val hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                response = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode).sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJson.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }

    @Test
    @Timeout(value = 1000, timeUnit = TimeUnit.SECONDS)
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                var response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                response = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJson.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                response = client.headAbs("$server$serverPath?nodeId=6").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                var hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                val updateURLInsertAsRightSibling = "$server$serverPath?nodeId=6&insert=asRightSibling"

                response = client.postAbs(updateURLInsertAsRightSibling).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("{\"tadaaa\":true}")).await()

                testContext.verify {
                    val expectUpdatedString = """
                        {"foo":["bar",null,2.33,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                response = client.postAbs(updateURLInsertAsRightSibling).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("null")).await()

                testContext.verify {
                    val expectUpdatedString = """
                        {"foo":["bar",null,2.33,null,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                response = client.postAbs(updateURLInsertAsRightSibling).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("44")).await()

                testContext.verify {
                    val expectUpdatedString = """
                        {"foo":["bar",null,2.33,44,null,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                response = client.postAbs(updateURLInsertAsRightSibling).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("foobar")).await()

                testContext.verify {
                    val expectUpdatedString = """
                        {"foo":["bar",null,2.33,"foobar",44,null,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                response = client.postAbs(updateURLInsertAsRightSibling).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("false")).await()

                testContext.verify {
                    val expectUpdatedString = """
                        {"foo":["bar",null,2.33,false,"foobar",44,null,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                response = client.headAbs("$server$serverPath?nodeId=3").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendAwait()

                hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                val updateURLInsertAsFirstChild = "$server$serverPath?nodeId=3&insert=asFirstChild"

                response = client.postAbs(updateURLInsertAsFirstChild).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("0")).await()

                hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                testContext.verify {
                    val expectUpdatedString = """
                        {"foo":[0,"bar",null,2.33,false,"foobar",44,null,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                response = client.postAbs(updateURLInsertAsFirstChild).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("test")).await()

                testContext.verify {
                    val expectUpdatedString = """
                        {"foo":["test",0,"bar",null,2.33,false,"foobar",44,null,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                response = client.postAbs(updateURLInsertAsFirstChild).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("null")).await()

                testContext.verify {
                    val expectUpdatedString = """
                        {"foo":[null,"test",0,"bar",null,2.33,false,"foobar",44,null,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                response = client.postAbs(updateURLInsertAsFirstChild).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("false")).await()

                testContext.verify {
                    val expectUpdatedString = """
                        {"foo":[false,null,"test",0,"bar",null,2.33,false,"foobar",44,null,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                response = client.postAbs(updateURLInsertAsFirstChild).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("{\"tadaa:\":3}")).await()

                testContext.verify {
                    val expectUpdatedString = """
                        {"foo":[{"tadaa:":3},false,null,"test",0,"bar",null,2.33,false,"foobar",44,null,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                    testContext.completeNow()
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                var response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                response = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJson.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                response = client.headAbs("$server$serverPath?nodeId=4").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                val hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                val url = "$server$serverPath?nodeId=4"

                response = client.deleteAbs(url).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode).send().await()

                testContext.verify {
                    assertNull(response.bodyAsString())
                    assertEquals(204, response.statusCode())
                    testContext.completeNow()
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
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                var response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                response = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectedJson.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                response = client.headAbs("$server$serverPath?nodeId=6").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                val hashCode = response.getHeader(HttpHeaders.ETAG.toString())

                val expectUpdatedString = """
                        {"foo":["bar",null,2.33,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()

                val url = "$server$serverPath?nodeId=6&insert=asRightSibling"

                response = client.postAbs(url).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .putHeader(HttpHeaders.ETAG.toString(), hashCode)
                    .sendBuffer(Buffer.buffer("{\"tadaaa\":true}")).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                response = client.getAbs("$server$serverPath?query=jn:all-times(.)").putHeader(
                    HttpHeaders
                        .AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                val expectedResult = """
                           {"rest":[{"revisionNumber":1,"revision":{"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}},{"revisionNumber":2,"revision":{"foo":["bar",null,2.33,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}}]}
                        """.trimIndent()

                testContext.verify {
                    println(response.bodyAsString().replace("\r\n", System.getProperty("line.separator")))
                    val result =
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator"))
                            .replace("\"revisionTimestamp\":\"(?!\").+?\",\"revision".toRegex(), "\"revision")
                    println(result)
                    JSONAssert.assertEquals(
                        expectedResult.replace("\n", System.getProperty("line.separator")),
                        result,
                        false
                    )
                    assertEquals(200, response.statusCode())
                    testContext.completeNow()
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
                    {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()

                val json = """
                    {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                """.trimIndent()

                val credentials = json {
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                var response = client.postAbs("$server/token").sendJson(credentials).await()

                testContext.verify {
                    assertEquals(200, response.statusCode())
                }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                response = client.putAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendBuffer(Buffer.buffer(json)).await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectString.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                }

                val expectQueryResult = """
                    {"foo":[],"bar":{},"baz":"hello","tada":[]}
                    """.trimIndent()

                response = client.getAbs("$server$serverPath?maxLevel=2").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectQueryResult.replace("\n", System.getProperty("line.separator")),
                        response.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, response.statusCode())
                    testContext.completeNow()
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
