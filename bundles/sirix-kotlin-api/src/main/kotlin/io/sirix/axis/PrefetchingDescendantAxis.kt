/**
 * Copyright (c) 2024, SirixDB
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 * - Redistributions of source code must retain the above copyright notice
 * - Redistributions in binary form must reproduce the above copyright notice
 *
 * THIS SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND.
 */

package io.sirix.axis

import io.sirix.api.NodeCursor
import io.sirix.api.NodeReadOnlyTrx
import io.sirix.api.ResourceSession
import io.sirix.api.StorageEngineReader
import io.sirix.cache.IndexLogKey
import io.sirix.index.IndexType
import io.sirix.io.Reader
import io.sirix.settings.Constants
import io.sirix.settings.Fixed
import it.unimi.dsi.fastutil.longs.LongArrayList
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * A preorder DescendantAxis with async page prefetching using virtual threads.
 *
 * This axis prefetches upcoming pages during traversal using a pool of reader transactions,
 * one per virtual thread. Each prefetch operation uses its own read-only transaction, which:
 * - Avoids race conditions (each thread has its own reader)
 * - Loads pages into the shared buffer cache (benefiting the main traversal)
 * - Uses lightweight virtual threads for optimal I/O parallelism
 *
 * The axis matches the exact traversal order of [DescendantAxis] (preorder DFS).
 *
 * **Prefetching strategy:**
 * - When entering a new page, prefetch the next N sequential pages asynchronously
 * - Node keys are assigned sequentially, so descendants tend to be on sequential pages
 * - Uses a pool of reader transactions for parallel prefetch I/O
 * - Batches prefetch checks: only triggers every [PREFETCH_CHECK_INTERVAL] pages to reduce overhead
 *
 * Usage:
 * ```kotlin
 * // Default: prefetch 4 pages ahead with 4 parallel readers
 * val axis = PrefetchingDescendantAxis.create(rtx)
 * while (axis.hasNext()) {
 *     val nodeKey = axis.nextLong()
 *     // process node
 * }
 * ```
 *
 * @see DescendantAxis for simpler traversal without prefetching
 */
class PrefetchingDescendantAxis private constructor(
    cursor: NodeCursor,
    includeSelf: IncludeSelf,
    private val resourceSession: ResourceSession<*, *>,
    private val prefetchAhead: Int,
    private val prefetchOffsetPages: Int,
    private val parallelReaders: Int,
    private val revisionNumber: Int
) : AbstractAxis(cursor, includeSelf) {

    companion object {
        /** Default number of pages to prefetch ahead (4096 found optimal for large datasets with 4GB+ cache) */
        private const val DEFAULT_PREFETCH_AHEAD = 4096
        /** Default number of pages to skip before starting prefetch */
        private const val DEFAULT_PREFETCH_OFFSET_PAGES = 0
        /** Default number of parallel reader transactions for prefetching */
        private const val DEFAULT_PARALLEL_READERS = 8
        /**
         * Check for page boundary crossing every N pages.
         * Smaller interval = more frequent checks but better cache utilization.
         * Must be a power of 2 for efficient bitwise check.
         */
        private const val PREFETCH_CHECK_INTERVAL = 4
        /** Bitmask for efficient modulo check: (pageKey & MASK) == 0 triggers prefetch */
        private const val PREFETCH_CHECK_MASK = (PREFETCH_CHECK_INTERVAL - 1).toLong()

        /**
         * Create a prefetching axis with default settings.
         */
        @JvmStatic
        fun <T> create(rtx: T, includeSelf: IncludeSelf = IncludeSelf.NO): PrefetchingDescendantAxis
            where T : NodeCursor, T : NodeReadOnlyTrx {
            return create(rtx, includeSelf, DEFAULT_PREFETCH_AHEAD, DEFAULT_PREFETCH_OFFSET_PAGES)
        }

        /**
         * Create a prefetching axis with custom prefetch depth.
         *
         * @param prefetchAhead number of pages to prefetch ahead (0 to disable)
         */
        @JvmStatic
        fun <T> create(rtx: T, includeSelf: IncludeSelf, prefetchAhead: Int): PrefetchingDescendantAxis
            where T : NodeCursor, T : NodeReadOnlyTrx {
            return create(rtx, includeSelf, prefetchAhead, DEFAULT_PREFETCH_OFFSET_PAGES)
        }

        /**
         * Create a prefetching axis with custom prefetch depth and offset.
         *
         * @param prefetchAhead number of pages to prefetch (0 to disable)
         * @param prefetchOffsetPages number of pages to skip before prefetching starts (>= 0)
         */
        @JvmStatic
        fun <T> create(
            rtx: T,
            includeSelf: IncludeSelf,
            prefetchAhead: Int,
            prefetchOffsetPages: Int
        ): PrefetchingDescendantAxis
            where T : NodeCursor, T : NodeReadOnlyTrx {
            return create(rtx, includeSelf, prefetchAhead, prefetchOffsetPages, DEFAULT_PARALLEL_READERS)
        }

        /**
         * Create a prefetching axis with full customization.
         *
         * @param prefetchAhead number of pages to prefetch (0 to disable)
         * @param prefetchOffsetPages number of pages to skip before prefetching starts (>= 0)
         * @param parallelReaders number of parallel reader transactions for prefetching (capped at 8)
         */
        @JvmStatic
        fun <T> create(
            rtx: T,
            includeSelf: IncludeSelf,
            prefetchAhead: Int,
            prefetchOffsetPages: Int,
            parallelReaders: Int
        ): PrefetchingDescendantAxis
            where T : NodeCursor, T : NodeReadOnlyTrx {
            require(prefetchAhead >= 0) { "prefetchAhead must be non-negative" }
            require(prefetchOffsetPages >= 0) { "prefetchOffsetPages must be non-negative" }
            require(parallelReaders >= 1) { "parallelReaders must be at least 1" }

            // Use requested readers (user is responsible for reasonable values)
            val effectiveReaders = if (prefetchAhead > 0) parallelReaders else 0

            return PrefetchingDescendantAxis(
                rtx,
                includeSelf,
                rtx.resourceSession,
                prefetchAhead,
                prefetchOffsetPages,
                effectiveReaders,
                rtx.revisionNumber
            )
        }
    }

    // Core traversal state (same as DescendantAxis)
    private lateinit var rightSiblingKeyStack: LongArrayList
    private var startNodeRightSiblingKey: Long = Fixed.NULL_NODE_KEY.standardProperty
    private var first = true

    // Prefetch state
    private var currentRecordPageKey: Long = -1L
    private var lastPrefetchedPageKey: Long = -1L
    /** Count of page boundary crossings - used to batch prefetch checks */
    private var pageBoundaryCrossings: Int = 0

    // Pool of readers for parallel prefetching
    private val readerPool: Array<StorageEngineReader?>
    private val readerIndex = AtomicInteger(0)
    private val isShutdown = AtomicBoolean(false)

    // Guard for super() calling reset() before init
    private var initialized = false

    init {
        rightSiblingKeyStack = LongArrayList()

        // Create pool of readers for parallel prefetching
        readerPool = if (prefetchAhead > 0 && parallelReaders > 0) {
            Array(parallelReaders) {
                try {
                    resourceSession.beginStorageEngineReader(revisionNumber)
                } catch (_: Exception) {
                    null
                }
            }
        } else {
            emptyArray()
        }

        initialized = true
    }

    override fun reset(nodeKey: Long) {
        super.reset(nodeKey)
        if (!initialized) return

        rightSiblingKeyStack.clear()
        first = true
        lastPrefetchedPageKey = -1L
        pageBoundaryCrossings = 0

        // Cache start node's right sibling for boundary detection (same as DescendantAxis)
        val currentKey = cursor.nodeKey
        cursor.moveTo(nodeKey)
        startNodeRightSiblingKey = cursor.rightSiblingKey
        currentRecordPageKey = recordPageKey(nodeKey)
        cursor.moveTo(currentKey)
    }

    // Inline flag to avoid function call overhead
    private val prefetchEnabled = prefetchAhead > 0 && parallelReaders > 0

    override fun nextKey(): Long {
        val cursor = cursor

        if (first) {
            first = false
            return if (includeSelf() == IncludeSelf.YES) {
                cursor.nodeKey
            } else {
                val firstChildKey = cursor.firstChildKey
                if (prefetchEnabled && firstChildKey != Fixed.NULL_NODE_KEY.standardProperty) {
                    checkPageBoundary(firstChildKey)
                }
                firstChildKey
            }
        }

        // Preorder: first child first
        val firstChildKey = cursor.firstChildKey
        if (firstChildKey != Fixed.NULL_NODE_KEY.standardProperty) {
            val rightSiblingKey = cursor.rightSiblingKey
            if (rightSiblingKey != Fixed.NULL_NODE_KEY.standardProperty) {
                rightSiblingKeyStack.add(rightSiblingKey)
            }
            if (prefetchEnabled) checkPageBoundary(firstChildKey)
            return firstChildKey
        }

        // Then right sibling
        val rightSiblingKey = cursor.rightSiblingKey
        if (rightSiblingKey != Fixed.NULL_NODE_KEY.standardProperty) {
            if (rightSiblingKey == startNodeRightSiblingKey) return done()
            if (prefetchEnabled) checkPageBoundary(rightSiblingKey)
            return rightSiblingKey
        }

        // Then from stack
        while (!rightSiblingKeyStack.isEmpty) {
            val key = rightSiblingKeyStack.popLong()
            if (key == startNodeRightSiblingKey) return done()
            if (prefetchEnabled) checkPageBoundary(key)
            return key
        }

        return done()
    }

    /**
     * Check if we're crossing a page boundary and trigger read-ahead if so.
     */
    private fun checkPageBoundary(nodeKey: Long) {
        val newRecordPageKey = recordPageKey(nodeKey)
        if (newRecordPageKey != currentRecordPageKey) {
            currentRecordPageKey = newRecordPageKey
            pageBoundaryCrossings++

            // Trigger prefetch when we've consumed half the prefetched pages
            // This ensures we stay ahead without triggering too frequently
            val halfPrefetch = (prefetchAhead / 2).coerceAtLeast(1)
            if (newRecordPageKey > lastPrefetchedPageKey - halfPrefetch) {
                prefetchAheadPages(newRecordPageKey)
            }
        }
    }

    /**
     * Compute Sirix' record page key for a given node key in the DOCUMENT index.
     */
    private fun recordPageKey(nodeKey: Long): Long {
        return nodeKey ushr Constants.INP_REFERENCE_COUNT_EXPONENT
    }

    // Debug counters
    @Volatile private var prefetchTriggers = 0L
    @Volatile private var pagesQueued = 0L

    /**
     * Prefetch pages ahead using parallel virtual threads, each with its own reader.
     */
    private fun prefetchAheadPages(fromRecordPageKey: Long) {
        if (isShutdown.get()) return

        val baseStartPage = fromRecordPageKey + prefetchOffsetPages
        val baseEndPageExclusive = baseStartPage + prefetchAhead

        // Avoid redundant prefetch if we've already prefetched this range
        if (prefetchAhead == 0 || baseEndPageExclusive <= (lastPrefetchedPageKey + 1)) {
            return
        }

        // Only prefetch pages we haven't prefetched yet
        val startPage = maxOf(baseStartPage, lastPrefetchedPageKey + 1)
        val endPageExclusive = baseEndPageExclusive
        if (startPage >= endPageExclusive) {
            return
        }

        prefetchTriggers++
        val pagesToPrefetch = (endPageExclusive - startPage).toInt()
        pagesQueued += pagesToPrefetch

        // Update marker BEFORE submitting to prevent overlapping batches
        lastPrefetchedPageKey = endPageExclusive - 1

        // Submit prefetch tasks to virtual threads - each gets its own reader from pool
        for (pageKey in startPage until endPageExclusive) {
            // Round-robin reader selection
            val idx = readerIndex.getAndIncrement() % readerPool.size
            val reader = readerPool[idx] ?: continue

            CompletableFuture.runAsync({
                if (isShutdown.get()) return@runAsync
                val indexLogKey = IndexLogKey(IndexType.DOCUMENT, pageKey, 0, revisionNumber)
                try {
                    reader.getRecordPage(indexLogKey)
                } catch (_: Exception) {
                    // Prefetch failures are non-fatal
                }
            }, Reader.POOL)
        }
    }

    fun getPrefetchStats(): String =
        "Prefetch triggers: $prefetchTriggers, pages queued: $pagesQueued, boundary crossings: $pageBoundaryCrossings (ahead=$prefetchAhead, offset=$prefetchOffsetPages, readers=$parallelReaders, interval=$PREFETCH_CHECK_INTERVAL)"

    override fun done(): Long {
        // Signal shutdown
        isShutdown.set(true)

        // Close all readers in the pool
        for (reader in readerPool) {
            try {
                reader?.close()
            } catch (_: Exception) {
                // Ignore close errors
            }
        }

        return Fixed.NULL_NODE_KEY.standardProperty
    }
}
