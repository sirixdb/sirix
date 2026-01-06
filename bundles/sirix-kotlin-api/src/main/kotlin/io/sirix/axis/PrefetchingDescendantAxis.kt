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
import io.sirix.api.StorageEngineReader
import io.sirix.access.trx.page.NodeStorageEngineReader
import io.sirix.cache.IndexLogKey
import io.sirix.index.IndexType
import io.sirix.settings.Constants
import io.sirix.settings.Fixed
import it.unimi.dsi.fastutil.longs.LongArrayList
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import java.util.concurrent.Executors

/**
 * A preorder DescendantAxis with async page prefetching using Kotlin coroutines.
 *
 * This axis prefetches upcoming pages during traversal using coroutines on [Dispatchers.IO].
 * Each prefetch operation uses a single reusable read-only transaction, which:
 * - Avoids race conditions with the main traversal's page guards
 * - Loads pages into the shared buffer cache (benefiting the main traversal)
 * - Uses lightweight coroutines instead of heavyweight threads
 *
 * The axis matches the exact traversal order of [DescendantAxis] (preorder DFS).
 *
 * **Prefetching strategy:**
 * - When entering a new page, prefetch the next N sequential pages asynchronously
 * - Node keys are assigned sequentially, so descendants tend to be on sequential pages
 * - Uses a single prefetch transaction for all prefetch operations (minimal overhead)
 *
 * Usage:
 * ```kotlin
 * // Default: prefetch 4 pages ahead
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
    private val prefetchTrx: StorageEngineReader?,
    private val prefetchAhead: Int,
    private val prefetchOffsetPages: Int,
    private val scope: CoroutineScope?,
    private val executor: java.util.concurrent.ExecutorService?
) : AbstractAxis(cursor, includeSelf) {

    companion object {
        /** Default number of pages to prefetch ahead */
        private const val DEFAULT_PREFETCH_AHEAD = 4
        /** Default number of pages to skip before starting prefetch */
        private const val DEFAULT_PREFETCH_OFFSET_PAGES = 0
        
        /**
         * Create a prefetching axis with default settings (4 pages ahead).
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
         * Create a prefetching axis with custom prefetch depth and an offset.
         *
         * The offset is useful to avoid competing with the main traversal and the OS readahead:
         * instead of prefetching the immediately-next pages, start prefetching a bit further ahead.
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
            require(prefetchAhead >= 0) { "prefetchAhead must be non-negative" }
            require(prefetchOffsetPages >= 0) { "prefetchOffsetPages must be non-negative" }
            
            // Create ONE prefetch transaction to reuse for all prefetches
            // Use a SINGLE-THREADED executor to avoid race conditions on currentPageGuard
            val (prefetchTrx, scope, executor) = if (prefetchAhead > 0) {
                val trx = rtx.resourceSession.beginPageReadOnlyTrx(rtx.revisionNumber)
                val exec = Executors.newSingleThreadExecutor { r ->
                    Thread(r, "prefetch-worker").apply { isDaemon = true }
                }
                val scope = CoroutineScope(exec.asCoroutineDispatcher() + SupervisorJob())
                Triple(trx, scope, exec)
            } else {
                Triple(null, null, null)
            }
            
            return PrefetchingDescendantAxis(
                rtx,
                includeSelf,
                prefetchTrx,
                prefetchAhead,
                prefetchOffsetPages,
                scope,
                executor
            )
        }
    }

    // Core traversal state (same as DescendantAxis)
    private lateinit var rightSiblingKeyStack: LongArrayList
    private var startNodeRightSiblingKey: Long = Fixed.NULL_NODE_KEY.standardProperty
    private var first = true
    
    // Prefetch state
    private var currentRecordPageKey: Long = -1L
    private var prefetchedUpToRecordPageKey: Long = -1L
    private var pendingJobs: MutableList<Job>? = null
    
    // Guard for super() calling reset() before init
    private var initialized = false
    
    init {
        rightSiblingKeyStack = LongArrayList()
        if (prefetchTrx != null) {
            pendingJobs = mutableListOf()
        }
        initialized = true
    }

    override fun reset(nodeKey: Long) {
        super.reset(nodeKey)
        if (!initialized) return
        
        rightSiblingKeyStack.clear()
        cancelPendingPrefetches()
        first = true
        prefetchedUpToRecordPageKey = -1L
        
        // Cache start node's right sibling for boundary detection (same as DescendantAxis)
        val currentKey = cursor.nodeKey
        cursor.moveTo(nodeKey)
        startNodeRightSiblingKey = cursor.rightSiblingKey
        currentRecordPageKey = recordPageKey(nodeKey)
        cursor.moveTo(currentKey)
    }

    // Inline flag to avoid function call overhead (309M calls!)
    private val prefetchEnabled = prefetchTrx != null
    
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
        // PERF: Avoid calling hasFirstChild()+firstChildKey and hasRightSibling()+rightSiblingKey
        // (reads the same flyweight fields twice).
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
     * 
     * For freshly shredded data, node keys are assigned in preorder, so pages
     * are visited sequentially. We prefetch the NEXT pages when entering a new page.
     */
    private fun checkPageBoundary(nodeKey: Long) {
        val newRecordPageKey = recordPageKey(nodeKey)
        if (newRecordPageKey != currentRecordPageKey) {
            currentRecordPageKey = newRecordPageKey
            prefetchAheadPages(newRecordPageKey)
        }
    }

    /**
     * Compute Sirix' record page key for a given node key in the DOCUMENT index.
     *
     * IMPORTANT: This is *not* the same as `nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT`.
     * Document records are stored in a KeyValueLeafPage tree keyed by
     * `StorageEngineReader.pageKey(recordKey, IndexType.DOCUMENT)` (INP exponent).
     */
    private fun recordPageKey(nodeKey: Long): Long {
        // PERF: DOCUMENT record pages use INP exponent.
        // Avoid virtual call to StorageEngineReader.pageKey(...) in the hottest loop.
        return nodeKey ushr Constants.INP_REFERENCE_COUNT_EXPONENT
    }
    
    // Debug counters
    @Volatile private var prefetchHits = 0L
    @Volatile private var prefetchMisses = 0L
    
    /**
     * Launch async prefetch coroutine for upcoming pages.
     */
    private fun prefetchAheadPages(fromRecordPageKey: Long) {
        val trx = prefetchTrx ?: return
        val coroutineScope = scope ?: return
        val jobs = pendingJobs ?: return
        
        val baseStartPage = fromRecordPageKey + prefetchOffsetPages
        // We prefetch 'prefetchAhead' pages starting at baseStartPage.
        val baseEndPageExclusive = baseStartPage + prefetchAhead

        // Avoid redundant prefetch if we've already prefetched this range.
        if (prefetchAhead == 0 || baseEndPageExclusive <= (prefetchedUpToRecordPageKey + 1)) {
            return
        }

        // Only prefetch pages we haven't prefetched yet.
        val startPage = maxOf(baseStartPage, prefetchedUpToRecordPageKey + 1)
        val endPageExclusive = baseEndPageExclusive
        if (startPage >= endPageExclusive) {
            return
        }
        
        // Check if previous prefetch completed (first page we need should be ready)
        val pendingCount = jobs.count { !it.isCompleted }
        if (pendingCount > 0) {
            prefetchMisses++
        } else {
            prefetchHits++
        }
        
        // Launch coroutine on single-threaded executor
        val job = coroutineScope.launch {
            if (trx is NodeStorageEngineReader) {
                for (pageKey in startPage until endPageExclusive) {
                    // Use getRecordPage directly - it loads into cache
                    // The page gets HOT bit set, protecting it from immediate eviction
                    val indexLogKey = IndexLogKey(IndexType.DOCUMENT, pageKey, 0, trx.revisionNumber)
                    try {
                        trx.getRecordPage(indexLogKey)
                        // Guard is managed by prefetch trx's currentPageGuard - released on next call
                    } catch (e: Exception) {
                        // Prefetch failures are non-fatal
                    }
                }
            }
        }
        jobs.add(job)
        prefetchedUpToRecordPageKey = endPageExclusive - 1
        
        // Periodic cleanup of completed jobs
        if (jobs.size > 16) {
            jobs.removeAll { it.isCompleted }
        }
    }
    
    fun getPrefetchStats(): String =
        "Prefetch hits: $prefetchHits, misses: $prefetchMisses (ahead=$prefetchAhead, offset=$prefetchOffsetPages)"
    
    override fun done(): Long {
        cancelPendingPrefetches()
        // Cancel the coroutine scope
        scope?.cancel()
        // Shut down the executor
        executor?.shutdown()
        // Close the prefetch transaction
        try {
            prefetchTrx?.close()
        } catch (_: Exception) {
            // Ignore close errors
        }
        return Fixed.NULL_NODE_KEY.standardProperty
    }
    
    private fun cancelPendingPrefetches() {
        pendingJobs?.forEach { it.cancel() }
        pendingJobs?.clear()
    }
}
