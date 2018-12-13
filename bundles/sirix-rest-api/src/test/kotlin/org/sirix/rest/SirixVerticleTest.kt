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
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith


@RunWith(VertxUnitRunner::class)
class SirixVerticleTest {
    lateinit var vertx: Vertx

    @Before
    fun setup(context: TestContext) {
        vertx = Vertx.vertx()
        val options = DeploymentOptions().setConfig(JsonObject().put("https.port", 10443))
        vertx.deployVerticle("org.sirix.rest.SirixVerticle", options, context.asyncAssertSuccess())
    }

    @Test
    fun testPut(context: TestContext) {
        val resource: Buffer = Buffer.buffer("<xml>foo<bar/></xml>")
        val client = WebClient.create(vertx, WebClientOptions().setTrustAll(true))

        val async = context.async()

        client.putAbs("https://localhost:10443/database/resource1").ssl(true).sendBuffer(resource) { ar ->
            if (ar.succeeded() && 302 == ar.result().statusCode()) {
                val form = MultiMap.caseInsensitiveMultiMap()
                form.set("username", "admin")
                form.set("password", "admin")
                form.set("client_id", "sirix")
                form.set("client_secret", "c8b9b4ed-67bb-47d9-bd73-a3babc470b2c")
                form.set("grant_type", "authorization_code")

                client.postAbs("http://localhost:8080/auth/realms/master/protocol/openid-connect/auth?client_id=sirix&client_secret=c8b9b4ed-67bb-47d9-bd73-a3babc470b2c&grant_type=authorization_code")
                        .putHeader(HttpHeaders.REFERER.toString(), "http://localhost:8080/auth/realms/master/protocol/openid-connect/auth?state=%2Fdatabase%2Fresource1&response_type=code&client_id=sirix")
                        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/x-www-form-urlencoded")
                        .putHeader(HttpHeaders.CONTENT_LENGTH.toString(), form.toString().length.toString())
                        .sendForm(form) { response ->
                            if (response.succeeded() && 200 == response.result().statusCode()) {
                                val authJson = response.result().bodyAsJsonObject()
                                val accessToken = authJson.getString("access_token")
                                val header = "Bearer $accessToken"
                                println("okay")
                            } else {
                                println("not okay")
                            }
                            async.complete()
                        }
            }
        }

        async.awaitSuccess(500000)
    }
}
