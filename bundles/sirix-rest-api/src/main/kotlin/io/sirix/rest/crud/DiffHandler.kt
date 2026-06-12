package io.sirix.rest.crud

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.coAwait
import io.sirix.access.DatabaseType
import io.sirix.access.Databases.getDatabaseType
import io.sirix.access.Databases.openJsonDatabase
import io.sirix.access.Databases.openXmlDatabase
import io.sirix.access.DatabasesInternals
import io.sirix.access.ResourceConfiguration
import io.sirix.api.Database
import io.sirix.api.json.JsonNodeReadOnlyTrx
import io.sirix.api.json.JsonResourceSession
import io.sirix.rest.Auth
import io.sirix.rest.AuthRole
import io.sirix.service.json.BasicJsonDiff
import io.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.ThreadLocalRandom

/**
 * [LogWrapper] reference.
 */
private val logger =
    LogWrapper(LoggerFactory.getLogger(DiffHandler::class.java))

class DiffHandler(private val location: Path, private val authz: AuthorizationProvider) {

    companion object {
        /**
         * In-flight computations of cacheable (consecutive, unfiltered) diffs, keyed by
         * `database/resource/firstRevision/secondRevision/includeData`. Concurrent identical
         * first-hits compute once — the rest await the same result. Entries are removed as soon
         * as a computation completes, so the map only ever holds in-flight work (revisions are
         * immutable; the durable caching is done by the diff files themselves).
         */
        private val inFlightDiffComputations = ConcurrentHashMap<String, CompletableFuture<String>>()
    }

    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName = ctx.pathParam("database")
        val resourceName = ctx.pathParam("resource")

        if (databaseName == null || resourceName == null) {
            throw IllegalArgumentException("Database name and resource name must be in the URL path.")
        }

        PathValidation.validatePathParam(databaseName, "database")
        PathValidation.validatePathParam(resourceName, "resource")

        val user = ctx.get<User>("user")!!
        Auth.checkIfAuthorized(user, databaseName, AuthRole.VIEW, authz)

        logger.debug("Open databases before: ${DatabasesInternals.getOpenDatabases()}")

        val database = openDatabase(databaseName)

        val diff = context.executeBlocking {
            var diffString: String? = null
            database.use {
                val resourceSession = database.beginResourceSession(resourceName)

                resourceSession.use {
                    if (resourceSession is JsonResourceSession) {
                        val firstRevision: String? = ctx.queryParam("first-revision").getOrNull(0)
                        val secondRevision: String? = ctx.queryParam("second-revision").getOrNull(0)

                        if (firstRevision == null || secondRevision == null) {
                            throw IllegalArgumentException("First and second revision must be specified.")
                        }

                        // Validate revision numbers and ordering up front: non-numeric input would
                        // 500 on toInt(), and a reversed pair silently produced a "backwards" diff
                        // (old=second, new=first) instead of an error.
                        val firstRevisionNumber = requireIntParam("first-revision", firstRevision)
                        val secondRevisionNumber = requireIntParam("second-revision", secondRevision)
                        if (firstRevisionNumber < 1 || secondRevisionNumber < 1) {
                            throw IllegalArgumentException("Revisions must be >= 1.")
                        }
                        if (firstRevisionNumber >= secondRevisionNumber) {
                            throw IllegalArgumentException(
                                "first-revision ($firstRevisionNumber) must be less than second-revision ($secondRevisionNumber)."
                            )
                        }

                        val startNodeKey: String? = ctx.queryParam("startNodeKey").getOrNull(0)
                        val maxDepth: String? = ctx.queryParam("maxDepth").getOrNull(0)
                        // include-data controls whether full subtree data is included for inserts
                        // Default is false (compact mode) for scalability with large resources
                        val includeData: Boolean = ctx.queryParam("include-data").getOrNull(0)?.toBoolean() ?: false

                        val startNodeKeyAsLong = startNodeKey?.let { startNodeKey.toLong() } ?: 0
                        val maxDepthAsLong = maxDepth?.let { maxDepth.toLong() } ?: Long.MAX_VALUE

                        val isConsecutive = secondRevisionNumber - 1 == firstRevisionNumber
                        val isFiltered = startNodeKey != null || maxDepth != null

                        // The per-revision diff file written at commit time (if the resource
                        // stores diffs and no crash got in between).
                        val diffPath = resourceSession.getResourceConfig()
                            .resource
                            .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.path)
                            .resolve("diffFromRev${firstRevisionNumber}toRev${secondRevisionNumber}.json")

                        if (isConsecutive && !isFiltered) {
                            // Pre-computed/cached diff files apply here. Concurrent identical
                            // first-hits compute once instead of stampeding the recomputation.
                            val cacheKey =
                                "$databaseName/$resourceName/$firstRevisionNumber/$secondRevisionNumber/$includeData"
                            diffString = computeOncePerKey(cacheKey) {
                                if (includeData) {
                                    val enrichedDiffPath = diffPath.resolveSibling(
                                        "diffFromRev${firstRevisionNumber}toRev${secondRevisionNumber}-enriched.json"
                                    )
                                    readValidDiffFile(enrichedDiffPath) ?: run {
                                        val enrichedDiff = computeEnrichedDiff(
                                            resourceSession,
                                            databaseName,
                                            resourceName,
                                            firstRevisionNumber,
                                            secondRevisionNumber,
                                            diffPath
                                        )
                                        // Revisions are immutable, so the enriched diff can be
                                        // cached indefinitely; the file dies with the resource
                                        // directory on delete/recreate.
                                        writeDiffFileAtomically(enrichedDiffPath, enrichedDiff)
                                        enrichedDiff
                                    }
                                } else {
                                    readValidDiffFile(diffPath) ?: run {
                                        // Missing (resource doesn't store diffs, or a crash hit
                                        // before the file was written) or torn — recompute
                                        // instead of failing the request.
                                        val replaceTornFile = Files.exists(diffPath)
                                        val diff = BasicJsonDiff(databaseName).generateDiff(
                                            resourceSession,
                                            firstRevisionNumber,
                                            secondRevisionNumber,
                                            startNodeKeyAsLong,
                                            maxDepthAsLong,
                                            false
                                        )
                                        if (replaceTornFile) {
                                            writeDiffFileAtomically(diffPath, diff)
                                        }
                                        diff
                                    }
                                }
                            }
                        } else if (isConsecutive && resourceSession.resourceConfig.areDeweyIDsStored
                            && readValidDiffFile(diffPath) != null
                        ) {
                            // Consecutive revisions with filtering — filter the pre-computed
                            // update operations (only usable if the diff file is intact).
                            val rtx = resourceSession.beginNodeReadOnlyTrx(secondRevisionNumber)
                            rtx.use {
                                diffString = useUpdateOperations(
                                    rtx,
                                    startNodeKeyAsLong,
                                    databaseName,
                                    resourceName,
                                    firstRevisionNumber,
                                    secondRevisionNumber,
                                    maxDepthAsLong,
                                    includeData
                                )
                            }
                        } else {
                            // Non-consecutive revisions, or no (intact) pre-computed diff file.
                            diffString = BasicJsonDiff(databaseName).generateDiff(
                                resourceSession,
                                firstRevisionNumber,
                                secondRevisionNumber,
                                startNodeKeyAsLong,
                                maxDepthAsLong,
                                includeData
                            )
                        }
                    }
                }
            }

            diffString
        }.coAwait()

        logger.debug("Open databases after: ${DatabasesInternals.getOpenDatabases()}")

        val res = ctx.response().setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            .putHeader(
                HttpHeaders.CONTENT_LENGTH,
                diff!!.toByteArray(StandardCharsets.UTF_8).size.toString()
            )
        res.write(diff)
        res.end()

        return ctx.currentRoute()!!
    }

    /**
     * Computes the diff for [key] once across concurrent identical requests: the first request
     * computes, the rest await its result. The entry is removed when the computation completes
     * (successfully or not), so the map cannot leak.
     */
    private fun computeOncePerKey(key: String, compute: () -> String): String {
        val future = CompletableFuture<String>()
        val inFlight = inFlightDiffComputations.putIfAbsent(key, future)
        if (inFlight != null) {
            return try {
                inFlight.get()
            } catch (e: ExecutionException) {
                throw e.cause ?: e
            }
        }
        try {
            val result = compute()
            future.complete(result)
            return result
        } catch (e: Throwable) {
            future.completeExceptionally(e)
            throw e
        } finally {
            inFlightDiffComputations.remove(key, future)
        }
    }

    /**
     * Computes the enriched (include-data=true) diff. Prefers lazily enriching the pre-computed
     * update-operations file — only the jsonFragment subtrees have to be serialized — over a full
     * diff recomputation; falls back to [BasicJsonDiff] when the file is missing/torn or the
     * resource doesn't store deweyIDs.
     */
    private fun computeEnrichedDiff(
        resourceSession: JsonResourceSession,
        databaseName: String,
        resourceName: String,
        firstRevisionNumber: Int,
        secondRevisionNumber: Int,
        diffPath: Path
    ): String {
        if (resourceSession.resourceConfig.areDeweyIDsStored && readValidDiffFile(diffPath) != null) {
            resourceSession.beginNodeReadOnlyTrx(secondRevisionNumber).use { rtx ->
                return useUpdateOperations(
                    rtx,
                    0L,
                    databaseName,
                    resourceName,
                    firstRevisionNumber,
                    secondRevisionNumber,
                    Long.MAX_VALUE,
                    true
                )
            }
        }
        return BasicJsonDiff(databaseName).generateDiff(
            resourceSession,
            firstRevisionNumber,
            secondRevisionNumber,
            0L,
            Long.MAX_VALUE,
            true
        )
    }

    /**
     * Reads a pre-computed diff file and validates that it parses as a diff JSON object: a crash
     * while the file was written (after the storage commit was already durable) may have left a
     * torn file behind, which must not be served verbatim forever. The parse is cheap relative to
     * a full diff recomputation.
     *
     * @return the file content, or `null` if the file is missing or doesn't parse
     */
    private fun readValidDiffFile(diffFilePath: Path): String? {
        if (!Files.exists(diffFilePath)) {
            return null
        }
        val content = try {
            Files.readString(diffFilePath)
        } catch (e: IOException) {
            logger.warn("Pre-computed diff file $diffFilePath could not be read — recomputing.", e)
            return null
        }
        val isValid = runCatching {
            JsonParser.parseString(content).asJsonObject.getAsJsonArray("diffs") != null
        }.getOrDefault(false)
        if (!isValid) {
            logger.warn("Pre-computed diff file $diffFilePath is corrupt (torn write?) — recomputing.")
            return null
        }
        return content
    }

    /**
     * Persists a computed diff next to the regular pre-computed diff files: write to a temp file
     * in the same directory, then move it into place atomically, so concurrent readers never see
     * a partially written file. Failures are logged and swallowed — persisting is best-effort and
     * must not fail the request being served.
     */
    private fun writeDiffFileAtomically(target: Path, content: String) {
        val tmpFile = target.resolveSibling(
            "${target.fileName}.tmp${ThreadLocalRandom.current().nextLong().toULong()}"
        )
        try {
            Files.writeString(tmpFile, content, StandardOpenOption.CREATE_NEW)
            try {
                Files.move(tmpFile, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            } catch (e: AtomicMoveNotSupportedException) {
                Files.move(tmpFile, target, StandardCopyOption.REPLACE_EXISTING)
            }
        } catch (e: IOException) {
            runCatching { Files.deleteIfExists(tmpFile) }
            logger.warn("Could not persist diff file $target — serving the computed diff anyway.", e)
        }
    }

    private fun useUpdateOperations(
        rtx: JsonNodeReadOnlyTrx,
        startNodeKeyAsLong: Long,
        databaseName: String,
        resourceName: String,
        firstRevisionNumber: Int,
        secondRevisionNumber: Int,
        maxDepthAsLong: Long,
        includeData: Boolean
    ): String {
        rtx.moveTo(startNodeKeyAsLong)
        val metaInfo = createMetaInfo(
            databaseName,
            resourceName,
            firstRevisionNumber,
            secondRevisionNumber
        )

        val diffs = metaInfo.getAsJsonArray("diffs")
        val updateOperations =
            rtx.getUpdateOperationsInSubtreeOfNode(rtx.deweyID, maxDepthAsLong)
        updateOperations.forEach {
            if (!includeData) {
                stripJsonFragmentData(it)
            }
            diffs.add(it)
        }
        return metaInfo.toString()
    }

    /**
     * The pre-computed update operations always carry serialized subtree data for jsonFragment
     * inserts/replaces; strip it for include-data=false responses so the compact contract holds
     * no matter which code path produced the diff.
     */
    private fun stripJsonFragmentData(updateOperation: JsonObject) {
        for (operationName in arrayOf("insert", "replace")) {
            val operation = updateOperation.getAsJsonObject(operationName) ?: continue
            if (operation.getAsJsonPrimitive("type")?.asString == "jsonFragment") {
                operation.remove("data")
            }
        }
    }

    private fun openDatabase(databaseName: String): Database<*> {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        return when (getDatabaseType(location.resolve(databaseName).toAbsolutePath())) {
            DatabaseType.JSON -> openJsonDatabase(location.resolve(databaseName))
            DatabaseType.XML -> openXmlDatabase(location.resolve(databaseName))
        }
    }

    private fun createMetaInfo(
        databaseName: String, resourceName: String, oldRevision: Int,
        newRevision: Int
    ): JsonObject {
        val json = JsonObject()
        json.addProperty("database", databaseName)
        json.addProperty("resource", resourceName)
        json.addProperty("old-revision", oldRevision)
        json.addProperty("new-revision", newRevision)
        val diffsArray = JsonArray()
        json.add("diffs", diffsArray)
        return json
    }
}
