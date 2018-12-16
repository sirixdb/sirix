package org.sirix.rest

import io.vertx.core.DeploymentOptions
import io.vertx.core.MultiMap
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.Async
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.ext.web.client.WebClientSession
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.ext.web.client.sendAwait
import io.vertx.kotlin.ext.web.client.sendBufferAwait
import io.vertx.kotlin.ext.web.client.sendFormAwait
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.jsoup.Jsoup
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(VertxUnitRunner::class)
class SirixVerticleTest {
    private lateinit var vertx: Vertx
    private lateinit var client: WebClient
    private lateinit var sessionClient: WebClientSession

    private val server = "https://localhost:9443"
    private val serverPath = "/database/resource1"

    @Before
    fun setup(context: TestContext) {
        vertx = Vertx.vertx()
        val options = DeploymentOptions().setConfig(JsonObject().put("https.port", 9443))
        vertx.deployVerticle("org.sirix.rest.SirixVerticle", options, context.asyncAssertSuccess())

        client = WebClient.create(vertx, WebClientOptions().setTrustAll(true).setFollowRedirects(false))
        sessionClient = WebClientSession.create(client)
    }

    @After
    fun tearDown(context: TestContext) {
        vertx.close(context.asyncAssertSuccess())
    }

    @Test
    fun testPut(context: TestContext) {
        val async = context.async()
        context.verify {
            GlobalScope.launch(vertx.dispatcher()) {
                executePut(context, async)
            }
        }

        async.awaitSuccess(5000)
    }

    private suspend fun SirixVerticleTest.executePut(context: TestContext, async: Async) {
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
                            context.assertEquals(expectString.replace("\n", System.getProperty("line.separator")),
                                    response.bodyAsString().replace("\r\n", System.getProperty("line.separator")))

                            async.complete()
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

    @Test()
    fun testGet(context: TestContext) {
        val async = context.async()
        context.verify {
            GlobalScope.launch(vertx.dispatcher()) {
                val asyncPut = context.async()
                executePut(context, asyncPut)
                asyncPut.awaitSuccess(5000)

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
                                    context.assertEquals(expectString.replace("\n", System.getProperty("line.separator")),
                                            response.bodyAsString().replace("\r\n", System.getProperty("line.separator")))

                                    async.complete()
                                }
                            }
                        }
                    }
                }
            }
        }

        async.awaitSuccess(20000)
    }
}
