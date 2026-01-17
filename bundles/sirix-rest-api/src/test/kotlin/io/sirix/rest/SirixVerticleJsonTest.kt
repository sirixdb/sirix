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
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.extension.ExtendWith
import org.skyscreamer.jsonassert.JSONAssert
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit


@ExtendWith(VertxExtension::class)
@DisplayName("JSON Integration test")
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
                .put("keycloak.url", "http://localhost:8080/realms/sirixdb")
        )
        vertx.deployVerticle("io.sirix.rest.SirixVerticle", options, testContext.succeedingThenComplete())

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

    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the update of a resource with JSONiq update statements")
    fun testJSONiqUpdates(vertx: Vertx, testContext: VertxTestContext) {
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

                var httpResponse =
                    client.putAbs("$server$serverPath?commitTimestamp=2014-01-01T12:11:12&commitMessage=Initial+Commit")
                        .putHeader(
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

                val url = "$server"

                val updateQuery = """
                        {"query":"let ${"$"}doc := jn:doc('database','json-resource') return ( append json {\"tadaaa\":true()} into sdb:select-item(${"$"}doc, 3) )",
                         "commitMessage":"this is a commit message",
                         "commitTimestamp":"2014-01-01T12:12:12"}
                     """.trimIndent()

                httpResponse = client.postAbs(url).putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(updateQuery)).await()

                testContext.verify {
                    assertEquals(200, httpResponse.statusCode())
                }

                httpResponse = client.getAbs("$server$serverPath").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                val expectUpdatedString = """
                        {"foo":["bar",null,2.33,{"tadaaa":true}],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
                    """.trimIndent()

                testContext.verify {
                    JSONAssert.assertEquals(
                        expectUpdatedString.replace("\n", System.getProperty("line.separator")),
                        httpResponse.bodyAsString().replace("\r\n", System.getProperty("line.separator")),
                        false
                    )
                    assertEquals(200, httpResponse.statusCode())
                    testContext.completeNow()
                }
            }
        }
    }


    @Test
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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

    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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

                // Enable DeweyIDs for this test since the diff functionality with update operations requires them
                var httpResponse = client.putAbs("$server$serverPath?useDeweyIDs=true").putHeader(
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

                httpResponse = client.getAbs("$server$serverPath/diff?first-revision=1&second-revision=2&include-data=true").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

                val expectedDiffJsonString = """
                    {"database":"database","resource":"json-resource","old-revision":1,"new-revision":2,"diffs":[{"insert":{"nodeKey":26,"insertPositionNodeKey":6,"insertPosition":"asRightSibling","path":"/foo/[3]","type":"jsonFragment","data":"{\"tadaaa\":true}"}}]}
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

    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 100, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the serialization of the first N-records with pagination")
    fun testRecordSerializer(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                // Create a simple array with multiple elements for easier pagination testing
                val json = """[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]""".trimIndent()

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

                // First request: get first 5 elements with metadata
                var httpGetResponseJson = client.getAbs("$server/database/resource?nodeId=1&maxChildren=5&maxLevel=2&withMetaData=nodeKeyAndChildCount").putHeader(
                    HttpHeaders.AUTHORIZATION
                        .toString(), "Bearer $accessToken"
                ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .send().await()

                testContext.verify {
                    assertEquals(200, httpGetResponseJson.statusCode())
                    val responseJson = httpGetResponseJson.bodyAsJsonObject()

                    // Should have metadata with nodeKey=1 (root array) and childCount=10
                    val metadata = responseJson.getJsonObject("metadata")
                    assertEquals(1, metadata.getInteger("nodeKey"))
                    assertEquals(10, metadata.getInteger("childCount"))

                    // Should have 5 children (limited by maxChildren)
                    val value = responseJson.getJsonArray("value")
                    assertEquals(5, value.size())
                }

                // Get the 5th element's nodeKey for pagination
                val firstResponse = httpGetResponseJson.bodyAsJsonObject()
                val firstBatchChildren = firstResponse.getJsonArray("value")
                val fifthChild = firstBatchChildren.getJsonObject(4)
                val fifthChildNodeKey = fifthChild.getJsonObject("metadata").getInteger("nodeKey")

                println("5th child nodeKey: $fifthChildNodeKey")

                // Second request: paginate from the 5th child to get remaining elements
                httpGetResponseJson =
                    client.getAbs("$server/database/resource?startNodeKey=$fifthChildNodeKey&nextTopLevelNodes=5&maxLevel=2&withMetaData=nodeKeyAndChildCount").putHeader(
                        HttpHeaders.AUTHORIZATION
                            .toString(), "Bearer $accessToken"
                    ).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                        .send().await()

                testContext.verify {
                    assertEquals(200, httpGetResponseJson.statusCode())
                    val responseJson = httpGetResponseJson.bodyAsJsonObject()

                    println("Pagination response: ${responseJson.encode()}")

                    // Pagination response should have parent metadata (the array, nodeKey=1)
                    val metadata = responseJson.getJsonObject("metadata")
                    assertEquals(1, metadata.getInteger("nodeKey"), "Parent should be root array (nodeKey=1)")
                    assertEquals(10, metadata.getInteger("childCount"))

                    // Should have remaining 5 children (6, 7, 8, 9, 10)
                    val value = responseJson.getJsonArray("value")
                    assertEquals(5, value.size(), "Should have 5 remaining children")

                    // Verify the values are correct (6, 7, 8, 9, 10)
                    for (i in 0 until 5) {
                        val child = value.getJsonObject(i)
                        val childValue = child.getInteger("value")
                        assertEquals(i + 6, childValue, "Child $i should be ${i + 6}")
                    }

                    testContext.completeNow()
                }
            }
        }
    }

    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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

    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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

    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
                    "for \$i in bit:array-values(jn:open('database','json-resource',xs:dateTime('${currentDateTimeAsString}'))) where \$i.city eq \"New York\" return { \$i, 'nodeKey': sdb:nodekey(\$i) }"

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

    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
                    "for \$i in jn:doc('mycol.jn','mydoc.jn') where deep-equal(\$i.generic, 1) return { \$i,'nodeKey': sdb:nodekey(\$i)}"

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

    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
                    "for \$i in jn:doc('mycol.jn','mydoc.jn') where deep-equal(\$i.generic, 1) return { \$i,'nodeKey': sdb:nodekey(\$i)}"

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

    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
                    "for \$i in jn:doc('mycol.jn','mydoc.jn') where deep-equal(\$i.generic, 1) return { \$i,'nodeKey': sdb:nodekey(\$i)}"

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

    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing the deletion of a resource")
    @RepeatedTest(3)
    fun testDeleteResource1000000(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                testDelete(vertx, testContext)
            }
        }
    }


    @Test
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
                    client.getAbs("$server$serverPath?query=let+%24nodeKey+%3A%3D+sdb%3Anodekey%28%24%24.foo%5B2%5D%29%0D%0Areturn+%7B%22nodeKey%22%3A+%24nodeKey%7D")
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

    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 100000000, timeUnit = TimeUnit.SECONDS)
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
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json").send().await()

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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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

                response = client.getAbs("$server$serverPath?query=jn:all-times($$)").putHeader(
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
    @Timeout(value = 1000000, timeUnit = TimeUnit.SECONDS)
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
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json").sendBuffer(Buffer.buffer(json))
                    .await()

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

    @Test
    @Timeout(value = 100, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing pagination with nextTopLevelNodes returns correct parent metadata")
    fun testPaginationParentMetadata(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                // Create JSON with array of 20 elements
                val elements = (1..20).joinToString(",") { """{"idx":$it}""" }
                val json = """{"data":[$elements]}"""

                val credentials = json {
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJson(credentials).await()
                testContext.verify { assertEquals(200, response.statusCode()) }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                // Create the resource
                var httpResponse = client.putAbs("$server/pagination-test/array-resource")
                    .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $accessToken")
                    .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify { assertEquals(200, httpResponse.statusCode()) }

                // First, get the data array directly with nodeId to find structure
                // Get root first to find data array nodeKey
                httpResponse = client.getAbs("$server/pagination-test/array-resource?maxLevel=3&withMetaData=nodeKeyAndChildCount")
                    .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $accessToken")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .send().await()

                testContext.verify { assertEquals(200, httpResponse.statusCode()) }

                val initialResult = httpResponse.bodyAsString()
                println("Initial result (raw): ${initialResult.take(500)}...")

                val initialJson = httpResponse.bodyAsJsonObject()
                val rootMetadata = initialJson.getJsonObject("metadata")
                val rootNodeKey = rootMetadata.getLong("nodeKey")
                println("Root nodeKey: $rootNodeKey")

                // Parse the structure to find data array nodeKey
                // For withMetaData, structure is different - let's navigate carefully
                val rootValue = initialJson.getValue("value")
                println("Root value type: ${rootValue?.javaClass?.simpleName}")

                // The root value should contain data key pointing to array
                // Let's use a simpler approach: query nodeId=3 which should be the data array
                // based on the structure: 1=root obj, 2=data key, 3=data array

                // Get the data array directly (maxLevel=2 to include children)
                httpResponse = client.getAbs("$server/pagination-test/array-resource?nodeId=3&maxLevel=2&maxChildren=10&withMetaData=nodeKeyAndChildCount")
                    .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $accessToken")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .send().await()

                testContext.verify { assertEquals(200, httpResponse.statusCode()) }

                val arrayResult = httpResponse.bodyAsJsonObject()
                println("Array result: $arrayResult")

                val arrayMetadata = arrayResult.getJsonObject("metadata")
                val dataArrayNodeKey = arrayMetadata.getLong("nodeKey")
                val dataArrayChildCount = arrayMetadata.getLong("childCount")
                println("Data array nodeKey: $dataArrayNodeKey, childCount: $dataArrayChildCount")

                // Get children from the array
                val arrayValue = arrayResult.getJsonArray("value")
                println("Array has ${arrayValue.size()} children in response")

                // Get the 5th child's nodeKey
                val fifthChild = arrayValue.getJsonObject(4)
                val fifthChildNodeKey = fifthChild.getJsonObject("metadata").getLong("nodeKey")
                println("5th child nodeKey: $fifthChildNodeKey")

                // Now paginate using nextTopLevelNodes and startNodeKey
                httpResponse = client.getAbs("$server/pagination-test/array-resource?nodeId=$dataArrayNodeKey&nextTopLevelNodes=5&startNodeKey=$fifthChildNodeKey&withMetaData=nodeKeyAndChildCount")
                    .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $accessToken")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .send().await()

                testContext.verify { assertEquals(200, httpResponse.statusCode()) }

                val paginationResult = httpResponse.bodyAsString()
                println("Pagination result: $paginationResult")

                // Parse the result
                val paginationJson = httpResponse.bodyAsJsonObject()
                val paginationMetadata = paginationJson.getJsonObject("metadata")
                val paginationParentNodeKey = paginationMetadata.getLong("nodeKey")
                val paginationParentChildCount = paginationMetadata.getLong("childCount")

                println("Pagination parent nodeKey: $paginationParentNodeKey, childCount: $paginationParentChildCount")

                // CRITICAL: The parent nodeKey should be the DATA ARRAY, not the root object!
                testContext.verify {
                    assertEquals(dataArrayNodeKey, paginationParentNodeKey,
                        "Pagination parent should be data array ($dataArrayNodeKey), not root ($rootNodeKey)")
                    assertEquals(dataArrayChildCount, paginationParentChildCount,
                        "Pagination parent childCount should match data array childCount")
                    testContext.completeNow()
                }
            }
        }
    }

    @Test
    @Timeout(value = 100, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Testing pagination on historical revision")
    fun testPaginationHistoricalRevision(vertx: Vertx, testContext: VertxTestContext) {
        GlobalScope.launch(vertx.dispatcher()) {
            testContext.verifyCoroutine {
                // Create JSON with array of 10 elements
                val elements = (1..10).joinToString(",") { """{"idx":$it}""" }
                val json = """{"data":[$elements]}"""

                val credentials = json {
                    obj(
                        "username" to "admin",
                        "password" to "admin"
                    )
                }

                val response = client.postAbs("$server/token").sendJson(credentials).await()
                testContext.verify { assertEquals(200, response.statusCode()) }

                val user = response.bodyAsJsonObject()
                accessToken = user.getString("access_token")

                // Create the resource (revision 1)
                var httpResponse = client.putAbs("$server/historical-test/array-resource")
                    .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $accessToken")
                    .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(json)).await()

                testContext.verify { assertEquals(200, httpResponse.statusCode()) }

                // Get history to find revision number
                httpResponse = client.getAbs("$server/historical-test/array-resource/history")
                    .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $accessToken")
                    .send().await()

                val history = httpResponse.bodyAsJsonObject().getJsonArray("history")
                val revision1 = history.getJsonObject(history.size() - 1).getInteger("revision")
                println("Initial revision: $revision1")

                // Create revision 2 by updating a value within the array
                // Using SirixDB JSONiq syntax: $array[index] for array access
                val updateJson = """{"query":"let ${"$"}doc := jn:doc('historical-test','array-resource') return replace json value of ${"$"}doc.data[1].idx with 100"}"""
                httpResponse = client.postAbs(server)
                    .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $accessToken")
                    .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .sendBuffer(Buffer.buffer(updateJson)).await()

                testContext.verify { assertEquals(200, httpResponse.statusCode()) }
                println("Update response: ${httpResponse.statusCode()}")

                // Get data array directly using nodeId=3 (1=root, 2=data key, 3=data array)
                // maxLevel=2 to include children
                httpResponse = client.getAbs("$server/historical-test/array-resource?revision=$revision1&nodeId=3&maxLevel=2&maxChildren=5&withMetaData=nodeKeyAndChildCount")
                    .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $accessToken")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .send().await()

                testContext.verify { assertEquals(200, httpResponse.statusCode()) }

                val arrayResult = httpResponse.bodyAsJsonObject()
                println("Historical revision $revision1 array result: $arrayResult")

                val arrayMetadata = arrayResult.getJsonObject("metadata")
                val dataArrayNodeKey = arrayMetadata.getLong("nodeKey")
                val dataArrayChildCount = arrayMetadata.getLong("childCount")
                println("Data array nodeKey: $dataArrayNodeKey, childCount: $dataArrayChildCount")

                // Get 3rd child's nodeKey
                val arrayValue = arrayResult.getJsonArray("value")
                val thirdChildNodeKey = arrayValue.getJsonObject(2).getJsonObject("metadata").getLong("nodeKey")
                println("3rd child nodeKey: $thirdChildNodeKey")

                // Paginate on historical revision
                httpResponse = client.getAbs("$server/historical-test/array-resource?revision=$revision1&nodeId=$dataArrayNodeKey&nextTopLevelNodes=3&startNodeKey=$thirdChildNodeKey&withMetaData=nodeKeyAndChildCount")
                    .putHeader(HttpHeaders.AUTHORIZATION.toString(), "Bearer $accessToken")
                    .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
                    .send().await()

                testContext.verify { assertEquals(200, httpResponse.statusCode()) }

                val paginationResult = httpResponse.bodyAsJsonObject()
                println("Historical pagination result: $paginationResult")

                val paginationMetadata = paginationResult.getJsonObject("metadata")
                val paginationParentNodeKey = paginationMetadata.getLong("nodeKey")

                testContext.verify {
                    assertEquals(dataArrayNodeKey, paginationParentNodeKey,
                        "Historical pagination parent should be data array ($dataArrayNodeKey)")
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
