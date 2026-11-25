package io.sirix.rest.crud.xml

import io.vertx.core.buffer.Buffer
import io.vertx.core.file.OpenOptions
import io.vertx.ext.auth.User as VertxUser
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.access.trx.node.HashType
import io.sirix.api.xml.XmlNodeTrx
import io.sirix.api.xml.XmlResourceSession
import io.sirix.rest.crud.AbstractCreateHandler
import io.sirix.rest.crud.Revisions
import io.sirix.rest.crud.SirixDBUser
import io.sirix.service.xml.serialize.XmlSerializer
import io.sirix.service.xml.shredder.XmlShredder
import java.io.ByteArrayOutputStream
import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID


class XmlCreate(
    location: Path
) : AbstractCreateHandler<XmlResourceSession>(location) {
    override suspend fun insertResource(
        dbFile: Path?, resPathName: String,
        ctx: RoutingContext
    ) {
        ctx.request().pause()
        val tempFilePath = withContext(Dispatchers.IO) {
            Files.createTempFile(UUID.randomUUID().toString(), null)
        }
        try {
            val file = ctx.vertx().fileSystem().open(tempFilePath.toString(), OpenOptions()).coAwait()
            // pipeTo installs the consumer before it resumes buffered request data.
            ctx.request().pipeTo(file).coAwait()
            withContext(Dispatchers.IO) {
                var body: Buffer? = null
                val sirixDBUser = SirixDBUser.create(ctx)
                Databases.openXmlDatabase(dbFile, sirixDBUser).use { database ->
                    val hashType = ctx.queryParam("hashType").getOrNull(0) ?: "NONE"
                    val useDeweyIDs = ctx.queryParam("useDeweyIDs").getOrNull(0)?.toBoolean() ?: false
                    val commitTimestampAsString = ctx.queryParam("commitTimestamp").getOrNull(0)
                    val resConfig = ResourceConfiguration.Builder(resPathName)
                        .hashKind(HashType.valueOf(hashType.uppercase()))
                        .useDeweyIDs(useDeweyIDs)
                        .customCommitTimestamps(commitTimestampAsString != null).build()
                    createAndWriteResource(database, resConfig, resPathName, ctx.get<VertxUser>("user")!!, ctx.pathParam("database")!!) {
                        database.beginResourceSession(resPathName).use { manager ->
                            val maxNodeKey = insertResourceSubtreeAsFirstChild(manager, tempFilePath, ctx)
                            if (maxNodeKey < MAX_NODES_TO_SERIALIZE) {
                                body = serializeResource(manager, ctx)
                            } else {
                                ctx.response().setStatusCode(200)
                            }
                        }
                    }
                }
                if (body != null) {
                    ctx.response().end(body)
                } else {
                    ctx.response().end()
                }
            }
        } finally {
            withContext(NonCancellable + Dispatchers.IO) {
                Files.deleteIfExists(tempFilePath)
            }
        }
    }

    override fun serializeResource(
        manager: XmlResourceSession,
        routingContext: RoutingContext
    ): Buffer {
        val out = ByteArrayOutputStream()
        val serializerBuilder = XmlSerializer.XmlSerializerBuilder(manager, out)
        val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

        // Serializes the just-created revision — the most recent one at this point.
        return XmlSerializeHelper().serializeXml(
            serializer, out, routingContext, manager, manager.mostRecentRevisionNumber, null
        )
    }


    fun insertResourceSubtreeAsFirstChild(
        manager: XmlResourceSession,
        filePath: Path,
        ctx: RoutingContext
    ): Long {
        val commitMessage = ctx.queryParam("commitMessage").getOrNull(0)
        val commitTimestampAsString = ctx.queryParam("commitTimestamp").getOrNull(0)
        val commitTimestamp = if (commitTimestampAsString == null) {
            null
        } else {
            Revisions.parseRevisionTimestamp(commitTimestampAsString).toInstant()
        }

        val wtx = manager.beginNodeTrx()
        return wtx.use {
            val inputStream = FileInputStream(filePath.toFile())
            return@use inputStream.use {
                val eventStream = XmlShredder.createFileReader(inputStream)
                wtx.insertSubtreeAsFirstChild(eventStream, XmlNodeTrx.Commit.No)
                eventStream.close()
                wtx.commit(commitMessage, commitTimestamp)
                wtx.maxNodeKey
            }
        }
    }

    override fun createDatabase(dbConfig: DatabaseConfiguration?) {
        Databases.createXmlDatabase(dbConfig)
    }
}
