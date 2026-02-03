package io.sirix.rest

import io.sirix.access.trx.node.json.objectvalue.*
import io.sirix.access.trx.node.json.InternalJsonNodeTrx
import io.sirix.api.json.JsonNodeTrx
import io.sirix.node.NodeKind
import io.sirix.service.InsertPosition
import io.sirix.settings.Fixed
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.parsetools.JsonEventType
import io.vertx.core.parsetools.JsonParser
import it.unimi.dsi.fastutil.longs.LongArrayList
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicLong
import java.io.File

/**
 * Channel-based streaming JSON shredder that keeps the Vert.x event loop responsive.
 *
 * Architecture:
 * - Producer (event loop): JsonParser.handler puts events into bounded channel
 * - Consumer (IO thread): Coroutine processes events and does blocking DB inserts
 * - Back-pressure: Channel capacity + parser.pause()/resume() prevents OOM
 *
 * Thread safety:
 * - Producer runs on Vert.x event loop (single-threaded)
 * - Consumer runs on Dispatchers.IO (worker pool)
 * - Communication via thread-safe Channel and AtomicBoolean
 */
class KotlinJsonStreamingShredder(
    private val wtx: JsonNodeTrx,
    private val parser: JsonParser,
    private val vertx: Vertx? = null,  // Optional - if null, runs synchronously (for testing)
    private val httpRequest: io.vertx.core.http.HttpServerRequest? = null, // HTTP request for back-pressure control
    private var insert: InsertPosition = InsertPosition.AS_FIRST_CHILD,
    private val channelCapacity: Int = 100_000  // Large buffer to reduce pause/resume cycles
) {
    private val parents = LongArrayList()

    companion object {
        /**
         * Enable debug tracing via system property: -Dsirix.shredder.debug=true
         * Or environment variable: SIRIX_SHREDDER_DEBUG=true
         */
        private val DEBUG_ENABLED: Boolean = System.getProperty("sirix.shredder.debug")?.toBoolean()
            ?: System.getenv("SIRIX_SHREDDER_DEBUG")?.toBoolean()
            ?: false
        
        /**
         * Debug log file path. Configurable via:
         * - System property: -Dsirix.shredder.debug.logfile=/path/to/file.log
         * - Environment variable: SIRIX_SHREDDER_DEBUG_LOGFILE=/path/to/file.log
         * Default: sirix-shredder-debug.log in current directory
         */
        private val DEBUG_LOG_PATH: String = System.getProperty("sirix.shredder.debug.logfile")
            ?: System.getenv("SIRIX_SHREDDER_DEBUG_LOGFILE")
            ?: "sirix-shredder-debug.log"
    }

    // Debug tracing - only active when DEBUG_ENABLED is true
    private val logFile by lazy { if (DEBUG_ENABLED) File(DEBUG_LOG_PATH) else null }
    private val producerCount = if (DEBUG_ENABLED) AtomicLong(0) else null
    private val consumerCount = if (DEBUG_ENABLED) AtomicLong(0) else null
    private val pauseCount = if (DEBUG_ENABLED) AtomicLong(0) else null
    private val resumeCount = if (DEBUG_ENABLED) AtomicLong(0) else null

    private inline fun debugLog(hyp: String, msg: String, data: () -> Map<String, Any?> = { emptyMap() }) {
        if (!DEBUG_ENABLED) return
        try {
            val dataMap = data()
            val json = """{"hypothesisId":"$hyp","message":"$msg","data":${dataMap.entries.joinToString(",", "{", "}") { "\"${it.key}\":${when(val v = it.value) { is String -> "\"$v\""; null -> "null"; else -> v.toString() }}"}}, "timestamp":${System.currentTimeMillis()}}"""
            logFile?.appendText(json + "\n")
            System.err.println("DEBUG[$hyp]: $msg $dataMap")
        } catch (e: Exception) { System.err.println("DEBUG LOG FAILED: ${e.message}") }
    }

    /**
     * Sealed interface for type-safe event passing between threads.
     * Using value classes where possible to minimize allocation overhead.
     */
    private sealed interface ShredderEvent {
        data class StartObject(val fieldName: String?) : ShredderEvent
        data class StartArray(val fieldName: String?) : ShredderEvent
        data class Value(val fieldName: String?, val value: Any?) : ShredderEvent
        data object EndObject : ShredderEvent
        data object EndArray : ShredderEvent
        data object StreamEnd : ShredderEvent
    }

    /**
     * Main entry point. Returns a Future that completes when processing is done.
     * 
     * When vertx is provided (REST API usage): Uses Channel-based async approach
     * with handlers set up synchronously, consumer running on worker thread.
     * IMPORTANT: Caller must resume the request AFTER calling this method.
     * 
     * When vertx is null (testing): Uses synchronous approach where parser
     * handlers directly execute DB operations.
     *
     * @return Future that completes with the revision number
     */
    fun call(): io.vertx.core.Future<Int> {
        return if (vertx != null) {
            callAsyncFuture()
        } else {
            // For sync mode, wrap in a succeeded future
            io.vertx.core.Future.succeededFuture(callSync())
        }
    }

    /**
     * Async Future-based implementation for REST API usage.
     * Handlers are set up immediately (synchronously on current thread).
     * Consumer processes events on worker thread via executeBlocking.
     * Returns a Future that completes when all events are processed.
     */
    private fun callAsyncFuture(): io.vertx.core.Future<Int> {
        // #region agent log
        debugLog("B", "callAsync_started") { mapOf("channelCapacity" to channelCapacity, "vertx" to (vertx != null)) }
        // #endregion
        
        val promise = io.vertx.core.Promise.promise<Int>()
        val channel = Channel<ShredderEvent>(capacity = channelCapacity)
        val paused = AtomicBoolean(false)
        val producerError = AtomicReference<Throwable?>(null)

        parents.push(Fixed.NULL_NODE_KEY.standardProperty)
        val revision = wtx.revisionNumber
        
        // Enable bulk insert mode to skip expensive serializeUpdateDiffs during auto-commits
        // This is what JsonShredder does internally and is critical for performance
        val internalTrx = wtx as InternalJsonNodeTrx
        internalTrx.setBulkInsertion(true)
                
        // Setup producer FIRST - synchronously on current thread (must be event loop)
        // This attaches parser handlers BEFORE request is resumed
        setupProducer(channel, paused, producerError)
        
        // #region agent log
        debugLog("F", "producer_setup_complete") { mapOf("handlersAttached" to true) }
        // #endregion

        // Start consumer on worker thread using vertx.executeBlocking
        // #region agent log
        debugLog("G", "executeBlocking_called") { mapOf("setup" to true) }
        // #endregion
        vertx!!.executeBlocking {
            // #region agent log
            debugLog("G", "consumer_blocking_start") { mapOf("thread" to Thread.currentThread().name) }
            // #endregion
            try {
                // Run blocking consumer using runBlocking from kotlinx.coroutines
                kotlinx.coroutines.runBlocking {
                    processEvents(channel, paused)
                }

                // Check for producer errors
                producerError.get()?.let { throw it }

                // Disable bulk insert mode after all events are processed
                // Note: We do NOT call adaptHashesInPostorderTraversal() here because
                // when using auto-commit (maxNodes), hashes are adapted incrementally.
                // Calling it would traverse 300M+ nodes and cause OOM.
                internalTrx.setBulkInsertion(false)
            } catch (e: Throwable) {
                // Ensure bulk insert is disabled even on error
                try { internalTrx.setBulkInsertion(false) } catch (_: Exception) {}
                throw e
            }
        }.onSuccess {
            promise.complete(revision)
        }.onFailure { e ->
            promise.fail(e)
        }

        return promise.future()
    }

    /**
     * Synchronous implementation for testing (when vertx is null).
     * Parser handlers directly execute DB operations on calling thread.
     * This matches the original behavior and works with manual event sending in tests.
     */
    private fun callSync(): Int {
        parents.push(Fixed.NULL_NODE_KEY.standardProperty)
        val revision = wtx.revisionNumber
        
        // Enable bulk insert mode to skip expensive serializeUpdateDiffs during auto-commits
        val internalTrx = wtx as InternalJsonNodeTrx
        internalTrx.setBulkInsertion(true)
        
        setupSyncProducer(internalTrx)
        
        return revision
    }

    /**
     * Sets up synchronous parser handlers for testing.
     * DB operations execute directly in the handler (blocking).
     */
    private fun setupSyncProducer(internalTrx: InternalJsonNodeTrx) {
        var level = 0
                var insertedRootNodeKey = -1L
                var keyValue: KeyValue? = null
        var lastEndEventType: JsonEventType? = null
                val parentKind = ArrayDeque<JsonEventType>()

        fun firstParentKind(): JsonEventType? = parentKind.firstOrNull()

        parser.exceptionHandler { t ->
            throw t
        }

                parser.handler { event ->
            // Process pending end event FIRST before handling current event
                    when (event.type()) {
                        JsonEventType.START_OBJECT -> {
                            level++
                    // Flush any pending key-value before processing
                            if (keyValue != null) {
                                val value = keyValue!!.value
                        // isNextParent is true for atomic values (we need to move cursor for sibling insertion)
                        addObjectRecord(keyValue!!.field, value ?: NullValue(), value !is JsonObject && value !is JsonArray)
                                keyValue = null
                            }
                            val name = event.fieldName()
                    if (lastEndEventType != null) {
                                processEndArrayOrEndObject(name != null && firstParentKind() == JsonEventType.START_OBJECT)
                        lastEndEventType = null
                            }
                            if (name == null || firstParentKind() != JsonEventType.START_OBJECT) {
                        val key = addObject()
                        if (insertedRootNodeKey == -1L) insertedRootNodeKey = key
                            } else {
                                keyValue = KeyValue(name, JsonObject())
                            }
                            parentKind.addFirst(event.type())
                        }

                JsonEventType.START_ARRAY -> {
                    // Flush any pending key-value before processing
                    if (keyValue != null) {
                        val value = keyValue!!.value
                        addObjectRecord(keyValue!!.field, value ?: NullValue(), value !is JsonObject && value !is JsonArray)
                        keyValue = null
                    }
                    val name = event.fieldName()
                    if (lastEndEventType != null) {
                        processEndArrayOrEndObject(name != null && firstParentKind() == JsonEventType.START_OBJECT)
                        lastEndEventType = null
                    }
                    level++
                    if (name == null || firstParentKind() != JsonEventType.START_OBJECT) {
                        val key = insertArray()
                        if (insertedRootNodeKey == -1L) insertedRootNodeKey = key
                    } else {
                        keyValue = KeyValue(name, JsonArray())
                    }
                    parentKind.addFirst(event.type())
                }

                        JsonEventType.VALUE -> {
                            val name = event.fieldName()
                    // Flush any pending key-value before processing
                            if (keyValue != null) {
                        val isNextTokenParentToken = name != null && firstParentKind() == JsonEventType.START_OBJECT
                        addObjectRecord(keyValue!!.field, keyValue!!.value ?: NullValue(), isNextTokenParentToken)
                                keyValue = null
                            }
                    if (lastEndEventType != null) {
                                processEndArrayOrEndObject(name != null)
                        lastEndEventType = null
                            }
                            val value = event.value()
                            if (name != null) {
                        keyValue = KeyValue(name, value ?: NullValue())
                                } else {
                        // Array element or root value
                        val key = when (value) {
                            is Number -> insertNumberValue(value, false)
                            is String -> insertStringValue(value, false)
                            is Boolean -> insertBooleanValue(value, false)
                            null -> insertNullValue(false)
                            else -> throw AssertionError("Unexpected value: ${value.javaClass}")
                        }
                        if (insertedRootNodeKey == -1L) insertedRootNodeKey = key
                    }
                }

                JsonEventType.END_OBJECT, JsonEventType.END_ARRAY -> {
                    if (keyValue != null) {
                        addObjectRecord(keyValue!!.field, keyValue!!.value ?: NullValue(), event.type() == JsonEventType.END_OBJECT)
                        keyValue = null
                    }
                    level--
                    if (lastEndEventType != null) {
                        processEndArrayOrEndObject(event.type() == JsonEventType.END_OBJECT)
                    }
                    lastEndEventType = event.type()
                    if (parentKind.isNotEmpty()) parentKind.removeFirst()
                }

                else -> { /* Ignore */ }
            }
        }

        parser.endHandler {
            keyValue?.let { kv ->
                addObjectRecord(kv.field, kv.value ?: NullValue(), false)
            }
            if (lastEndEventType != null) {
                processEndArrayOrEndObject(lastEndEventType == JsonEventType.END_OBJECT)
            }
            if (insertedRootNodeKey != -1L) {
                wtx.moveTo(insertedRootNodeKey)
            }
            
            // Disable bulk insert mode after all events are processed
            // Note: We do NOT call adaptHashesInPostorderTraversal() because
            // when using auto-commit (maxNodes), hashes are adapted incrementally.
            internalTrx.setBulkInsertion(false)
        }
    }

    /**
     * Sets up the JsonParser event handlers that produce events into the channel.
     * All callbacks run on the Vert.x event loop thread.
     */
    private fun setupProducer(
        channel: Channel<ShredderEvent>,
        paused: AtomicBoolean,
        errorRef: AtomicReference<Throwable?>
    ) {
        // Handle parser errors
        parser.exceptionHandler { t ->
            errorRef.set(t)
            channel.close(t)
        }
        
        // Pending event storage for when channel is full
        val pendingEvent = AtomicReference<ShredderEvent?>(null)
        
        // Helper to send event with back-pressure - returns true if event was accepted
        fun trySendEvent(event: ShredderEvent): Boolean {
            val result = channel.trySend(event)
            if (result.isSuccess) {
                // #region debug trace
                producerCount?.incrementAndGet()?.let { cnt ->
                    if (cnt % 100000 == 0L) debugLog("A", "producer_sent") { mapOf("count" to cnt) }
                }
                // #endregion
                return true
            }
            if (!channel.isClosedForSend) {
                // #region debug trace
                pauseCount?.incrementAndGet()?.let { pCnt ->
                    debugLog("A", "channel_full_pause") { mapOf("pauseCount" to pCnt, "producerCount" to producerCount?.get(), "consumerCount" to consumerCount?.get()) }
                }
                // #endregion
                // Channel full - pause BOTH parser and HTTP request for true back-pressure
                pendingEvent.set(event)
                paused.set(true)
                parser.pause()
                httpRequest?.pause()
            }
            return false
        }

        // Main event handler - runs on event loop, must be non-blocking
        parser.handler { event ->
            try {
                // First try to send any pending event from previous pause
                val pending = pendingEvent.get()
                if (pending != null) {
                    if (trySendEvent(pending)) {
                        pendingEvent.set(null)
                    } else {
                        return@handler  // Still can't send, stay paused
                    }
                }
                
                val shredderEvent = when (event.type()) {
                    JsonEventType.START_OBJECT -> ShredderEvent.StartObject(event.fieldName())
                    JsonEventType.START_ARRAY -> ShredderEvent.StartArray(event.fieldName())
                    JsonEventType.VALUE -> ShredderEvent.Value(event.fieldName(), event.value())
                    JsonEventType.END_OBJECT -> ShredderEvent.EndObject
                    JsonEventType.END_ARRAY -> ShredderEvent.EndArray
                    else -> null
                }
                
                if (shredderEvent != null) {
                    trySendEvent(shredderEvent)
                }
            } catch (t: Throwable) {
                errorRef.set(t)
                channel.close(t)
            }
        }

        // End of stream handler
        parser.endHandler {
            // #region debug trace
            debugLog("H1", "parser_end_handler_called") { mapOf("producerCount" to producerCount?.get(), "consumerCount" to consumerCount?.get()) }
            // #endregion
            try {
                // Send any pending event first
                pendingEvent.get()?.let { pending ->
                    while (!channel.trySend(pending).isSuccess && !channel.isClosedForSend) {
                        Thread.yield()
                    }
                }
                channel.trySend(ShredderEvent.StreamEnd)
                channel.close()
                // #region debug trace
                debugLog("H1", "parser_end_channel_closed") { mapOf("producerCount" to producerCount?.get()) }
                // #endregion
            } catch (t: Throwable) {
                errorRef.set(t)
                channel.close(t)
            }
        }
    }

    /**
     * Consumer coroutine that processes events on a worker thread.
     * This is where all blocking DB operations happen.
     */
    private suspend fun processEvents(
        channel: Channel<ShredderEvent>,
        paused: AtomicBoolean
    ) {
        var processedSinceResume = 0
        // Resume after processing 50K events - much higher threshold to reduce pause/resume cycles
        val resumeThreshold = 50_000

        var insertedRootNodeKey = -1L
        var level = 0
        val parentKind = ArrayDeque<JsonEventType>()
        var pendingKeyValue: KeyValue? = null
        var pendingEndEventType: JsonEventType? = null

        fun firstParentKind(): JsonEventType? = parentKind.firstOrNull()

        try {
            for (event in channel) {
                when (event) {
                    is ShredderEvent.StartObject -> {
                        level++
                        val name = event.fieldName
                        
                        // Handle pending key-value - isNextParent is true for atomic values
                        pendingKeyValue?.let { kv ->
                            val v = kv.value
                            addObjectRecord(kv.field, v ?: NullValue(), v !is JsonObject && v !is JsonArray)
                            pendingKeyValue = null
                        }
                        
                        // Process pending end event
                        if (pendingEndEventType != null) {
                            processEndArrayOrEndObject(name != null && firstParentKind() == JsonEventType.START_OBJECT)
                            pendingEndEventType = null
                        }
                        
                        if (name == null || firstParentKind() != JsonEventType.START_OBJECT) {
                            val key = addObject()
                            if (insertedRootNodeKey == -1L) {
                                insertedRootNodeKey = key
                            }
                        } else {
                            pendingKeyValue = KeyValue(name, JsonObject())
                        }
                        parentKind.addFirst(JsonEventType.START_OBJECT)
                    }

                    is ShredderEvent.StartArray -> {
                        level++
                        val name = event.fieldName
                        
                        // Handle pending key-value - isNextParent is true for atomic values
                        pendingKeyValue?.let { kv ->
                            val v = kv.value
                            addObjectRecord(kv.field, v ?: NullValue(), v !is JsonObject && v !is JsonArray)
                            pendingKeyValue = null
                        }
                        
                        // Process pending end event
                        if (pendingEndEventType != null) {
                            processEndArrayOrEndObject(name != null && firstParentKind() == JsonEventType.START_OBJECT)
                            pendingEndEventType = null
                        }
                        
                        if (name == null || firstParentKind() != JsonEventType.START_OBJECT) {
                            val key = insertArray()
                            if (insertedRootNodeKey == -1L) {
                                insertedRootNodeKey = key
                            }
                        } else {
                            pendingKeyValue = KeyValue(name, JsonArray())
                        }
                        parentKind.addFirst(JsonEventType.START_ARRAY)
                    }

                    is ShredderEvent.Value -> {
                        val name = event.fieldName
                        val value = event.value

                        // Handle any pending key-value first
                        // isNextParent depends on whether this is another property
                        pendingKeyValue?.let { kv ->
                            val isNextTokenParentToken = name != null && firstParentKind() == JsonEventType.START_OBJECT
                            addObjectRecord(kv.field, kv.value ?: NullValue(), isNextTokenParentToken)
                            pendingKeyValue = null
                        }
                        
                        // Process pending end event
                        if (pendingEndEventType != null) {
                            processEndArrayOrEndObject(name != null)
                            pendingEndEventType = null
                        }

                        if (name != null) {
                            // Object property
                            pendingKeyValue = KeyValue(name, value ?: NullValue())
                        } else {
                            // Array element or root value
                            val key = when (value) {
                                is Number -> insertNumberValue(value, false)
                                is String -> insertStringValue(value, false)
                                is Boolean -> insertBooleanValue(value, false)
                                null -> insertNullValue(false)
                                else -> throw AssertionError("Unexpected value type: ${value.javaClass}")
                            }
                                if (insertedRootNodeKey == -1L) {
                                insertedRootNodeKey = key
                            }
                        }
                    }

                    is ShredderEvent.EndObject, is ShredderEvent.EndArray -> {
                        val isEndObject = event is ShredderEvent.EndObject
                        
                        // Flush pending key-value
                        pendingKeyValue?.let { kv ->
                            addObjectRecord(kv.field, kv.value ?: NullValue(), isEndObject)
                            pendingKeyValue = null
                        }
                        level--
                        
                        // Process pending end event
                        if (pendingEndEventType != null) {
                            processEndArrayOrEndObject(isEndObject)
                        }
                        pendingEndEventType = if (isEndObject) JsonEventType.END_OBJECT else JsonEventType.END_ARRAY
                        if (parentKind.isNotEmpty()) parentKind.removeFirst()
                    }

                    is ShredderEvent.StreamEnd -> {
                        // Final cleanup - process any pending end event
                        if (pendingEndEventType != null) {
                            processEndArrayOrEndObject(false)
                        }
                        break
                    }
                }

                // Resume producer if we've drained enough buffer
                processedSinceResume++
                // #region debug trace
                val cCnt = consumerCount?.incrementAndGet() ?: 0L
                if (cCnt % 100000 == 0L) debugLog("C", "consumer_processed") { mapOf("count" to cCnt, "paused" to paused.get()) }
                // #endregion
                if (paused.get() && processedSinceResume >= resumeThreshold) {
                    // #region debug trace
                    val rCnt = resumeCount?.incrementAndGet() ?: 0L
                    debugLog("D", "resume_triggered") { mapOf("resumeCount" to rCnt, "consumerCount" to cCnt) }
                    // #endregion
                    processedSinceResume = 0
                    paused.set(false)
                    if (vertx != null) {
                        vertx.runOnContext { 
                            // Resume BOTH parser and HTTP request
                            parser.resume()
                            httpRequest?.resume()
                            // #region debug trace
                            debugLog("E", "resume_executed") { mapOf("resumeCount" to rCnt) }
                            // #endregion
                        }
                    } else {
                        parser.resume()
                    }
                }
            }

            // Move to root node
            if (insertedRootNodeKey != -1L) {
                wtx.moveTo(insertedRootNodeKey)
            }
        } catch (e: ClosedReceiveChannelException) {
            // Channel closed - either normally or due to error
            // The actual error is captured in consumerError/producerError and thrown in call()
        }
    }

    // ==================== DB Insert Operations (unchanged from original) ====================

    private fun processEndArrayOrEndObject(isNextTokenParentToken: Boolean) {
        if (parents.isEmpty) return
        parents.popLong()
        if (parents.isEmpty) return
        wtx.moveTo(parents.peekLong(0))
        if (isNextTokenParentToken) {
            if (parents.isEmpty) return
            parents.popLong()
            if (parents.isEmpty) return
            wtx.moveTo(parents.peekLong(0))
        }
    }

    private fun insertStringValue(stringValue: String, isNextTokenParent: Boolean): Long {
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> {
                if (isParentNull) wtx.insertStringValueAsFirstChild(stringValue).nodeKey
                else wtx.insertStringValueAsRightSibling(stringValue).nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> {
                if (isParentNull) wtx.insertStringValueAsLastChild(stringValue).nodeKey
                else wtx.insertStringValueAsRightSibling(stringValue).nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertStringValueAsLeftSibling(stringValue).nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertStringValueAsRightSibling(stringValue).nodeKey
        }
        adaptTrxPosAndStack(isNextTokenParent, key)
        return key
    }

    private fun insertBooleanValue(boolValue: Boolean, isNextTokenParent: Boolean): Long {
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> {
                if (isParentNull) wtx.insertBooleanValueAsFirstChild(boolValue).nodeKey
                else wtx.insertBooleanValueAsRightSibling(boolValue).nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> {
                if (isParentNull) wtx.insertBooleanValueAsLastChild(boolValue).nodeKey
                else wtx.insertBooleanValueAsRightSibling(boolValue).nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertBooleanValueAsLeftSibling(boolValue).nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertBooleanValueAsRightSibling(boolValue).nodeKey
        }
        adaptTrxPosAndStack(isNextTokenParent, key)
        return key
    }

    private fun insertNumberValue(numberValue: Number, isNextTokenParent: Boolean): Long {
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> {
                if (isParentNull) wtx.insertNumberValueAsFirstChild(numberValue).nodeKey
                else wtx.insertNumberValueAsRightSibling(numberValue).nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> {
                if (isParentNull) wtx.insertNumberValueAsLastChild(numberValue).nodeKey
                else wtx.insertNumberValueAsRightSibling(numberValue).nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertNumberValueAsLeftSibling(numberValue).nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertNumberValueAsRightSibling(numberValue).nodeKey
            else -> throw AssertionError()
        }
        adaptTrxPosAndStack(isNextTokenParent, key)
        return key
    }

    private fun insertNullValue(isNextTokenParent: Boolean): Long {
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> {
                if (isParentNull) wtx.insertNullValueAsFirstChild().nodeKey
                else wtx.insertNullValueAsRightSibling().nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> {
                if (isParentNull) wtx.insertNullValueAsLastChild().nodeKey
                else wtx.insertNullValueAsRightSibling().nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertNullValueAsLeftSibling().nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertNullValueAsRightSibling().nodeKey
            else -> throw AssertionError()
        }
        adaptTrxPosAndStack(isNextTokenParent, key)
        return key
    }

    private fun adaptTrxPosAndStack(nextTokenIsParent: Boolean, key: Long) {
        if (!parents.isEmpty) {
            parents.popLong()
        }
        if (nextTokenIsParent && !parents.isEmpty) {
            wtx.moveTo(parents.peekLong(0))
        } else {
            parents.push(key)
        }
    }

    private fun insertArray(): Long {
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key: Long = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> {
                if (isParentNull) wtx.insertArrayAsFirstChild().nodeKey
                else wtx.insertArrayAsRightSibling().nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> {
                if (isParentNull) wtx.insertArrayAsLastChild().nodeKey
                else wtx.insertArrayAsRightSibling().nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> {
                check(!(wtx.kind === NodeKind.JSON_DOCUMENT || wtx.parentKey == Fixed.DOCUMENT_NODE_KEY.standardProperty))
                val k = wtx.insertArrayAsLeftSibling().nodeKey
                insert = InsertPosition.AS_FIRST_CHILD
                k
            }
            InsertPosition.AS_RIGHT_SIBLING -> {
                check(!(wtx.kind === NodeKind.JSON_DOCUMENT || wtx.parentKey == Fixed.DOCUMENT_NODE_KEY.standardProperty))
                val k = wtx.insertArrayAsRightSibling().nodeKey
                insert = InsertPosition.AS_FIRST_CHILD
                k
            }
            else -> throw AssertionError()
        }
        if (!parents.isEmpty) parents.popLong()
        parents.push(key)
        parents.push(Fixed.NULL_NODE_KEY.standardProperty)
        return key
    }

    private fun addObject(): Long {
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key: Long = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> {
                if (isParentNull) wtx.insertObjectAsFirstChild().nodeKey
                else wtx.insertObjectAsRightSibling().nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> {
                if (isParentNull) wtx.insertObjectAsLastChild().nodeKey
                else wtx.insertObjectAsRightSibling().nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> {
                check(!(wtx.kind === NodeKind.JSON_DOCUMENT || wtx.parentKey == Fixed.DOCUMENT_NODE_KEY.standardProperty))
                val k = wtx.insertObjectAsLeftSibling().nodeKey
                insert = InsertPosition.AS_FIRST_CHILD
                k
            }
            InsertPosition.AS_RIGHT_SIBLING -> {
                check(!(wtx.kind === NodeKind.JSON_DOCUMENT || wtx.parentKey == Fixed.DOCUMENT_NODE_KEY.standardProperty))
                val k = wtx.insertObjectAsRightSibling().nodeKey
                insert = InsertPosition.AS_FIRST_CHILD
                k
            }
            else -> throw AssertionError()
        }
        if (!parents.isEmpty) parents.popLong()
        parents.push(key)
        parents.push(Fixed.NULL_NODE_KEY.standardProperty)
        return key
    }

    private fun addObjectRecord(name: String?, objectValue: Any, isNextTokenParentToken: Boolean) {
        assert(name != null)
        val value: ObjectRecordValue<*> = getObjectRecordValue(objectValue)
        val isParentNull = parents.isEmpty || parents.peekLong(0) == Fixed.NULL_NODE_KEY.standardProperty
        val key: Long = when (insert) {
            InsertPosition.AS_FIRST_CHILD -> if (isParentNull) {
                wtx.insertObjectRecordAsFirstChild(name, value).nodeKey
            } else {
                wtx.insertObjectRecordAsRightSibling(name, value).nodeKey
            }
            InsertPosition.AS_LAST_CHILD -> if (isParentNull) {
                wtx.insertObjectRecordAsLastChild(name, value).nodeKey
            } else {
                wtx.insertObjectRecordAsRightSibling(name, value).nodeKey
            }
            InsertPosition.AS_LEFT_SIBLING -> wtx.insertObjectRecordAsLeftSibling(name, value).nodeKey
            InsertPosition.AS_RIGHT_SIBLING -> wtx.insertObjectRecordAsRightSibling(name, value).nodeKey
            else -> throw AssertionError()
        }
        if (!parents.isEmpty) {
            parents.popLong()
        }
        parents.push(wtx.parentKey)
        parents.push(Fixed.NULL_NODE_KEY.standardProperty)
        if (wtx.kind === NodeKind.OBJECT || wtx.kind === NodeKind.ARRAY) {
            if (!parents.isEmpty) {
                parents.popLong()
            }
            parents.push(key)
            parents.push(Fixed.NULL_NODE_KEY.standardProperty)
        } else {
            adaptTrxPosAndStack(isNextTokenParentToken, key)
        }
    }

    private fun getObjectRecordValue(objVal: Any?): ObjectRecordValue<*> {
        return when (objVal) {
            is JsonObject -> ObjectValue()
            is JsonArray -> ArrayValue()
            is Boolean -> BooleanValue(objVal)
            is String -> StringValue(objVal)
            is NullValue -> NullValue()
            is Number -> NumberValue(objVal)
            else -> throw AssertionError("Unknown object value type: ${objVal?.javaClass}")
        }
    }

    private data class KeyValue(val field: String, val value: Any?)
}
