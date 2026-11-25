package io.sirix.rest

import io.brackit.query.atomic.QNm
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.api.Database
import io.sirix.api.json.JsonNodeTrx
import io.sirix.api.xml.XmlNodeTrx
import io.sirix.rest.crud.HistoryHandler
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.time.Instant

@Tag("offline")
class CommitTimestampOutputTest {
    @TempDir
    lateinit var directory: Path

    @Test
    fun historyPreservesIsoTimestampPrecision() = runBlocking {
        val times = listOf("2021-03-04T05:06:07Z", "2021-03-04T05:06:07.123Z", "2021-03-04T05:06:07.123456Z")
        for (xml in listOf(false, true)) {
            val name = if (xml) "xml" else "json"
            val path = directory.resolve(name)
            val config = DatabaseConfiguration(path)
            if (xml) Databases.createXmlDatabase(config) else Databases.createJsonDatabase(config)
            val database: Database<*> = if (xml) Databases.openXmlDatabase(path) else Databases.openJsonDatabase(path)
            database.use { db ->
                db.createResource(ResourceConfiguration.newBuilder("res").customCommitTimestamps(true).build())
                db.beginResourceSession("res").use { session ->
                    session.beginNodeTrx().use { writer ->
                        if (xml) (writer as XmlNodeTrx).insertElementAsFirstChild(QNm("value"))
                        else (writer as JsonNodeTrx).insertStringValueAsFirstChild("value")
                        for (text in times) writer.commit(null, Instant.parse(text))
                    }
                }
            }
        }
        val vertx = Vertx.vertx()
        val client = WebClient.create(vertx)
        try {
            val router = Router.router(vertx)
            router.get("/:database/:resource/history")
                .handler { ctx -> ctx.put("user", NoneAuth.createUser()); ctx.next() }
                .coroutineHandler(CoroutineScope(vertx.dispatcher())) { HistoryHandler(directory, NoneAuth).handle(it) }
            router.route().failureHandler(routerFailureHandler())
            val server = vertx.createHttpServer().requestHandler(router).listen(0).coAwait()
            try {
                for (xml in listOf(false, true)) {
                    val name = if (xml) "xml" else "json"
                    val response = client.get(server.actualPort(), "localhost", "/$name/res/history").send().coAwait()
                    assertEquals(200, response.statusCode(), response.bodyAsString())
                    val history = checkNotNull(response.bodyAsJsonObject()).getJsonArray("history")
                    assertEquals(3, history.size())
                    val database: Database<*> = if (xml) Databases.openXmlDatabase(directory.resolve(name))
                                               else Databases.openJsonDatabase(directory.resolve(name))
                    database.use { db ->
                        db.beginResourceSession("res").use { session ->
                            for (index in times.indices) {
                                val emitted = history.getJsonObject(2 - index).getString("revisionTimestamp")
                                assertEquals(times[index], emitted)
                                session.beginNodeReadOnlyTrx(index + 1).use { reader ->
                                    assertEquals(reader.revisionTimestamp, Instant.parse(emitted))
                                }
                            }
                        }
                    }
                }
            } finally {
                server.close().coAwait()
            }
        } finally {
            client.close()
            vertx.close().coAwait()
        }
    }
}
