package org.sirix.rest

import io.vertx.core.DeploymentOptions
import io.vertx.core.MultiMap
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.ext.web.client.WebClientSession
import org.jsoup.Jsoup
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals


@RunWith(VertxUnitRunner::class)
class SirixVerticleTest {
    lateinit var vertx: Vertx

    @Before
    fun setup(context: TestContext) {
        vertx = Vertx.vertx()
        val options = DeploymentOptions().setConfig(JsonObject().put("https.port", 9443))
        vertx.deployVerticle("org.sirix.rest.SirixVerticle", options, context.asyncAssertSuccess())
    }

    @After
    fun tearDown(context: TestContext) {
        vertx.close(context.asyncAssertSuccess())
    }

    @Test
    fun testPut(context: TestContext) {
        val client = WebClient.create(vertx, WebClientOptions().setTrustAll(true).setFollowRedirects(false))
        val sessionClient = WebClientSession.create(client)

        val async = context.async()

        val resource = Buffer.buffer("<xml>foo<bar/></xml>")
        val server = "https://localhost:9443"
        val serverPath = "/database/resource1"

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

        sessionClient.putAbs("$server$serverPath")
                .ssl(true)
                .followRedirects(false)
                .sendBuffer(resource) { response ->
                    if (response.succeeded() && 302 == response.result().statusCode()) {
                        val location = response.result().getHeader(HttpHeaders.LOCATION.toString())

                        sessionClient.getAbs(location).send() { response ->
                            if (response.succeeded() && 200 == response.result().statusCode()) {
                                val document = Jsoup.parse(response.result().bodyAsString())

                                val formElement = document.getElementById("kc-form-login")
                                val url = formElement.attr("action")

                                val form = MultiMap.caseInsensitiveMultiMap()
                                form.set("username", "admin")
                                form.set("password", "admin")

                                sessionClient.postAbs(url)
                                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/x-www-form-urlencoded")
                                        .putHeader(HttpHeaders.CONTENT_LENGTH.toString(), form.toString().length.toString())
                                        .sendForm(form) { response ->
                                            if (response.succeeded() && 302 == response.result().statusCode()) {
                                                val location = response.result().getHeader(HttpHeaders.LOCATION.toString())

                                                sessionClient.getAbs(location).ssl(true).send() { response ->
                                                    if (response.succeeded() && 302 == response.result().statusCode()) {
                                                        val location = response.result().getHeader(HttpHeaders.LOCATION.toString())

                                                        sessionClient.putAbs("$server$location").sendBuffer(resource) { response ->
                                                            if (response.succeeded() && 200 == response.result().statusCode()) {
                                                                assertEquals(expectString.replace("\n", System.getProperty("line.separator")), response.result().bodyAsString().replace("\r\n", System.getProperty("line.separator")))

                                                                async.complete()
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                            }
                        }
                    }
                }

        async.awaitSuccess(500000)
    }

    @Test
    fun testGet(context: TestContext) {
        val client = WebClient.create(vertx, WebClientOptions().setTrustAll(true).setFollowRedirects(false))
        val sessionClient = WebClientSession.create(client)

        val server = "https://localhost:9443"
        val serverPath = "/database/resource1"

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

        val async = context.async()

        sessionClient.getAbs("$server$serverPath")
                .ssl(true)
                .followRedirects(false)
                .send() { response ->
                    if (response.succeeded() && 302 == response.result().statusCode()) {
                        val location = response.result().getHeader(HttpHeaders.LOCATION.toString())

                        sessionClient.getAbs(location).send() { response ->
                            if (response.succeeded() && 200 == response.result().statusCode()) {
                                val document = Jsoup.parse(response.result().bodyAsString())

                                val formElement = document.getElementById("kc-form-login")
                                val url = formElement.attr("action")

                                val form = MultiMap.caseInsensitiveMultiMap()
                                form.set("username", "admin")
                                form.set("password", "admin")

                                sessionClient.postAbs(url)
                                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/x-www-form-urlencoded")
                                        .putHeader(HttpHeaders.CONTENT_LENGTH.toString(), form.toString().length.toString())
                                        .sendForm(form) { response ->
                                            if (response.succeeded() && 302 == response.result().statusCode()) {
                                                val location = response.result().getHeader(HttpHeaders.LOCATION.toString())

                                                sessionClient.getAbs(location).ssl(true).send() { response ->
                                                    if (response.succeeded() && 302 == response.result().statusCode()) {
                                                        val location = response.result().getHeader(HttpHeaders.LOCATION.toString())

                                                        sessionClient.getAbs("$server$location").send() { response ->
                                                            if (response.succeeded() && 200 == response.result().statusCode()) {
                                                                assertEquals(expectString.replace("\n", System.getProperty("line.separator")),
                                                                        response.result().bodyAsString().replace("\r\n", System.getProperty("line.separator")))

                                                                async.complete()
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                            }
                        }
                    }
                }

        async.awaitSuccess(5000)
    }
}
