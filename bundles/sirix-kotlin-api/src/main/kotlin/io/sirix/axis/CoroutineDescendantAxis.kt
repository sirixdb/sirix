/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.axis

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import io.sirix.api.NodeCursor
import io.sirix.api.NodeReadOnlyTrx
import io.sirix.api.NodeTrx
import io.sirix.api.PageReadOnlyTrx
import io.sirix.api.ResourceSession
import io.sirix.index.IndexType
import io.sirix.settings.Fixed
import io.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

/**
 * <h1>CoroutineDescendantAxis</h1>
 * <p>
 * DescendantAxis which simultaneously compute all right sibling results in another task.
 * The CoroutineDescendantAxis gets the computed right sibling results from a common producer consumer channel
 * after it finishes left node processing. As soon as the end of the computed result sequence is reached (marked by the
 * NULL_NODE_KEY), the CoroutineDescendantAxis returns <code>false</code>.
 * </p>
 */
class CoroutineDescendantAxis<R, W> :
    AbstractAxis where R : NodeReadOnlyTrx, R : NodeCursor, W : NodeTrx, W : NodeCursor {

    /** Logger */
    private val logger = LogWrapper(LoggerFactory.getLogger(CoroutineDescendantAxis::class.java))
    /** Flag indicating if parallel processing is active */

    private val parallelActive = AtomicBoolean(false)

    /** Resource session for creating transactions */
    private val resourceSession: ResourceSession<R, W>

    /** Coroutine scope for managing parallel tasks */
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    /** Channel for results from parallel producers */
    private val resultChannel = Channel<Long>(Channel.UNLIMITED)

    /** Active parallel tasks */
    private val activeTasks = mutableListOf<Job>()


    /** Minimum descendant count to trigger parallelization */
    private val PARALLELIZATION_THRESHOLD = 1000

    /** Current position in traversal */
    private var currentPosition = 0L
    private var isFirst = true
    private var mainTraversalComplete = false

    /** Queue for storing results in order */
    private val resultQueue = java.util.concurrent.ConcurrentLinkedQueue<Long>()

    /**
     * Constructor initializing internal state.
     */
    constructor(resourceSession: ResourceSession<R, W>) : super(resourceSession.beginNodeReadOnlyTrx()) {
        this.resourceSession = resourceSession
        logger.debug("Initialized CoroutineDescendantAxis with resource session")
    }

    constructor(
        resourceSession: ResourceSession<R, W>,
        includeSelf: IncludeSelf
    ) : super(resourceSession.beginNodeReadOnlyTrx(), includeSelf) {
        this.resourceSession = resourceSession
        logger.debug("Initialized CoroutineDescendantAxis with resource session and includeSelf: {}", includeSelf)
    }

    constructor(
        resourceSession: ResourceSession<R, W>,
        includeSelf: IncludeSelf,
        cursor: NodeCursor
    ) : super(cursor, includeSelf) {
        this.resourceSession = resourceSession
        logger.debug("Initialized CoroutineDescendantAxis with resource session, includeSelf: {}, and custom cursor", includeSelf)
    }

    override fun reset(nodeKey: Long) {
        logger.debug("Resetting axis to node key: {}", nodeKey)
        super.reset(nodeKey)
        cleanup()
        isFirst = true
        mainTraversalComplete = false
        currentPosition = 0L
    }

    override fun nextKey(): Long {
        // Handle first call
        if (isFirst) {
            logger.debug("Handling first call")
            isFirst = false
            return handleFirstCall()
        }

        // Check if we have results in the queue
        val queuedResult = resultQueue.poll()
        if (queuedResult != null) {
            logger.trace("Returning queued result: {}", queuedResult)
            return queuedResult
        }

        // Continue main traversal
        if (!mainTraversalComplete) {
            logger.trace("Continuing main traversal")
            return continueMainTraversal()
        }

        // Wait for any remaining parallel tasks
        if (parallelActive.get()) {
            logger.trace("Waiting for parallel results")
            return waitForParallelResults()
        }

        logger.debug("Traversal complete, returning done")
        return done()
    }

    private fun handleFirstCall(): Long {
        return if (includeSelf() == IncludeSelf.YES) {
            logger.debug("Including self in traversal")
            currentPosition = cursor.nodeKey
            considerParallelProcessing()
            currentPosition
        } else {
            if (cursor.moveToFirstChild()) {
                logger.debug("Moving to first child: {}", cursor.nodeKey)
                currentPosition = cursor.nodeKey
                considerParallelProcessing()
                currentPosition
            } else {
                logger.debug("No children found, returning done")
                done()
            }
        }
    }

    private fun continueMainTraversal(): Long {
        // Try to move to first child
        if (cursor.moveToFirstChild()) {
            logger.trace("Moving to first child: {}", cursor.nodeKey)
            currentPosition = cursor.nodeKey
            considerParallelProcessing()
            return currentPosition
        }

        // Try to move to right sibling
        if (cursor.moveToRightSibling()) {
            logger.trace("Moving to right sibling: {}", cursor.nodeKey)
            currentPosition = cursor.nodeKey
            considerParallelProcessing()
            return currentPosition
        }

        logger.trace("No more children or siblings, moving up")
        return moveUpAndFindNextSibling()
    }

    private fun moveUpAndFindNextSibling(): Long {
        while (cursor.hasParent() && cursor.parentKey != startKey) {
            cursor.moveTo(cursor.parentKey)
            logger.trace("Moving up to parent: {}", cursor.nodeKey)

            if (cursor.moveToRightSibling()) {
                logger.trace("Found right sibling: {}", cursor.nodeKey)
                currentPosition = cursor.nodeKey
                considerParallelProcessing()
                return currentPosition
            }
        }

        logger.debug("Main traversal complete")
        mainTraversalComplete = true
        return if (parallelActive.get()) waitForParallelResults() else done()
    }

    private fun considerParallelProcessing() {
        if (!cursor.hasRightSibling()) {
            logger.trace("No right sibling to parallelize")
            return
        }

        val rightSiblingKey = cursor.rightSiblingKey
        logger.trace("Considering parallelization for right sibling: {}", rightSiblingKey)

        if (shouldParallelizeSubtree(rightSiblingKey)) {
            logger.debug("Launching parallel traversal for right sibling: {}", rightSiblingKey)
            launchParallelTraversal(rightSiblingKey)
        } else {
            logger.trace("Skipping parallelization for right sibling: {}", rightSiblingKey)
        }
    }

    private fun shouldParallelizeSubtree(nodeKey: Long): Boolean {
        logger.trace("Checking if subtree should be parallelized: {}", nodeKey)
        val tempCursor = resourceSession.beginNodeReadOnlyTrx()
        try {
            tempCursor.moveTo(nodeKey)

            val currentPageKey = (this.cursor as PageReadOnlyTrx).pageKey(this.cursor.nodeKey, IndexType.DOCUMENT)
            val targetPageKey = (tempCursor as PageReadOnlyTrx).pageKey(nodeKey, IndexType.DOCUMENT)

            if (currentPageKey == targetPageKey) {
                logger.trace("Skipping parallelization - same page: {}", currentPageKey)
                return false
            }

            val descendantCount = tempCursor.descendantCount
            val shouldParallelize = descendantCount >= PARALLELIZATION_THRESHOLD
            logger.trace("Subtree descendant count: {}, should parallelize: {}", descendantCount, shouldParallelize)
            return shouldParallelize

        } finally {
            tempCursor.close()
        }
    }

    private fun launchParallelTraversal(startKey: Long) {
        if (parallelActive.get()) {
            logger.trace("Parallel processing already active, skipping")
            return
        }

        logger.debug("Starting parallel traversal from node: {}", startKey)
        parallelActive.set(true)
        val job = scope.launch {
            val producerCursor = resourceSession.beginNodeReadOnlyTrx()
            try {
                traverseSubtreeParallel(producerCursor, startKey)
            } catch (e: Exception) {
                logger.error("Error in parallel traversal", e)
            } finally {
                producerCursor.close()
                parallelActive.set(false)
                logger.debug("Parallel traversal completed for node: {}", startKey)
            }
        }
        activeTasks.add(job)
    }

    private suspend fun traverseSubtreeParallel(cursor: NodeCursor, startKey: Long) {
        logger.trace("Starting parallel subtree traversal from: {}", startKey)
        cursor.moveTo(startKey)
        val stack = mutableListOf<Long>()
        stack.add(startKey)

        while (stack.isNotEmpty() && parallelActive.get()) {
            val nodeKey = stack.removeAt(stack.size - 1)
            cursor.moveTo(nodeKey)

            resultQueue.add(nodeKey)
            logger.trace("Added node to result queue: {}", nodeKey)

            val children = mutableListOf<Long>()
            if (cursor.moveToFirstChild()) {
                children.add(cursor.nodeKey)

                while (cursor.moveToRightSibling()) {
                    children.add(cursor.nodeKey)
                }
            }

            for (i in children.size - 1 downTo 0) {
                stack.add(children[i])
            }

            if (stack.size % 100 == 0) {
                logger.trace("Yielding after processing {} nodes", stack.size)
                yield()
            }
        }
        logger.debug("Completed parallel subtree traversal from: {}", startKey)
    }

    private fun waitForParallelResults(): Long {
        val result = resultQueue.poll()
        if (result != null) {
            logger.trace("Found parallel result: {}", result)
            return result
        }

        return if (!parallelActive.get()) {
            logger.debug("No more parallel results, returning done")
            done()
        } else {
            logger.trace("No immediate results, returning done")
            done()
        }
    }

    override fun done(): Long {
        logger.debug("Traversal done")
        cleanup()
        return Fixed.NULL_NODE_KEY.standardProperty
    }

    private fun cleanup() {
        logger.debug("Cleaning up resources")
        parallelActive.set(false)

        activeTasks.forEach { 
            logger.trace("Cancelling active task")
            it.cancel() 
        }
        activeTasks.clear()
        resultChannel.close()
    }

    fun close() {
        logger.debug("Closing CoroutineDescendantAxis")
        cleanup()
        scope.cancel()
    }
}