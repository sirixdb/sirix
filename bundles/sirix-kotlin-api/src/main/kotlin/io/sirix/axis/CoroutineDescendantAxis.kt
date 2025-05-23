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
import io.sirix.api.NodeCursor
import io.sirix.api.NodeReadOnlyTrx
import io.sirix.api.NodeTrx
import io.sirix.api.ResourceSession
import io.sirix.settings.Fixed
import io.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue
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

    /** Resource session for creating transactions */
    private val resourceSession: ResourceSession<R, W>

    /** Coroutine scope for managing parallel tasks */
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    /** Thread safe Queue for results from parallel producers */
    private val resultQueue = ConcurrentLinkedQueue<Long>()

    /** Active parallel tasks */
    private val activeTasks = mutableListOf<Job>()

    /** Flag indicating if parallel processing is active */
    private val parallelActive = AtomicBoolean(false)

    /** Minimum descendant count to trigger parallelization */
    private val PARELLELIZATION_THRESHOLD = 1000

    /** Current position in traversal */
    private var currentPosition = 0L
    private var isFirst = true
    private var mainTraversalComplete = false

    /**
     * Constructor initializing internal state.
     */
    constructor(resourceSession: ResourceSession<R, W>) : super(resourceSession.beginNodeReadOnlyTrx()) {
        this.resourceSession = resourceSession
    }

    constructor(
        resourceSession: ResourceSession<R, W>,
        includeSelf: IncludeSelf
    ) : super(resourceSession.beginNodeReadOnlyTrx(), includeSelf) {
        this.resourceSession = resourceSession
    }

    constructor(
        resourceSession: ResourceSession<R, W>,
        includeSelf: IncludeSelf,
        cursor: NodeCursor
    ) : super(cursor, includeSelf) {
        this.resourceSession = resourceSession
    }

    override fun reset(nodeKey: Long) {
        super.reset(nodeKey)
        cleanup()
        isFirst = true
        mainTraversalComplete = false
        currentPosition = 0L
    }
 //Main traversal logic
    //Execution Priority:
 //1- Handle first call
 //2- Return results from the parallel queue
 //3- Continue main preorder traversal
 //4- Wait for parallel results
 //5- Mark traversal as done
    override fun nextKey(): Long {
        // Handle first call
        if (isFirst) {
            isFirst = false
            return handleFirstCall()
        }

        // Check if we have parallel results ready
        if (!resultQueue.isEmpty()) {
            return resultQueue.poll()
        }

        // Continue main traversal
        if (!mainTraversalComplete) {
            return continueMainTraversal()
        }

        // Wait for any remaining parallel tasks
        if (parallelActive.get()) {
            return waitForParallelResults()
        }

        return done()
    }

    private fun handleFirstCall(): Long {
        return if (includeSelf() == IncludeSelf.YES) {
            currentPosition = cursor.nodeKey
            // Start parallel processing for siblings if beneficial
            considerParallelProcessing()
            currentPosition
        } else {
            if (cursor.hasFirstChild()) {
                cursor.moveTo(cursor.firstChildKey)
                currentPosition = cursor.nodeKey
                considerParallelProcessing()
                currentPosition
            } else {
                done()
            }
        }
    }

    private fun continueMainTraversal(): Long {
        // Try to move to first child
        if (cursor.hasFirstChild()) {
            val firstChildKey = cursor.firstChildKey
            cursor.moveTo(firstChildKey)
            currentPosition = firstChildKey
            considerParallelProcessing()
            return currentPosition
        }

        // Try to move to right sibling
        if (cursor.hasRightSibling()) {
            val rightSiblingKey = cursor.rightSiblingKey
            cursor.moveTo(rightSiblingKey)
            currentPosition = rightSiblingKey
            considerParallelProcessing()
            return currentPosition
        }

        // Move up and try right siblings of ancestors
        return moveUpAndFindNextSibling()
    }

    private fun moveUpAndFindNextSibling(): Long {
        var currentKey = cursor.nodeKey

        while (cursor.hasParent() && cursor.parentKey != startKey) {
            cursor.moveTo(cursor.parentKey)

            if (cursor.hasRightSibling()) {
                val rightSiblingKey = cursor.rightSiblingKey
                cursor.moveTo(rightSiblingKey)
                currentPosition = rightSiblingKey
                considerParallelProcessing()
                return currentPosition
            }
        }

        mainTraversalComplete = true
        return if (parallelActive.get()) waitForParallelResults() else done()
    }
 // the next 2 functions are used for parallelization criteria
 // Page locality: avoids parallelizing siblings on the same page
 //Subtree size: must exceed parallelizationThreshold
 //Only one active parallel task at a time
    private fun considerParallelProcessing() {
        if (!cursor.hasRightSibling()) return

        val rightSiblingKey = cursor.rightSiblingKey

        // Check if right sibling is on a different page and has enough descendants
        if (shouldParallelizeSubtree(rightSiblingKey)) {
            launchParallelTraversal(rightSiblingKey)
        }
    }

    private fun shouldParallelizeSubtree(nodeKey: Long): Boolean {
        // Create a temporary cursor to check the subtree
        val tempCursor = resourceSession.beginNodeReadOnlyTrx()
        try {
            tempCursor.moveTo(nodeKey)

            // Check if it's on a different page (simplified check)
            val currentPage = cursor.nodeKey / 1000  // Simplified page calculation
            val targetPage = nodeKey / 1000

            if (currentPage == targetPage) {
                return false  // Same page, no benefit from parallelization
            }

            // Check descendant count if available
            val descendantCount = getEstimatedDescendantCount(tempCursor)
            return descendantCount >= PARELLELIZATION_THRESHOLD

        } finally {
            tempCursor.close()
        }
    }

    private fun getEstimatedDescendantCount(cursor: NodeCursor): Int {
        // Try to get descendant count from node metadata if available
        // This is a simplified estimation - in reality, you'd use actual node metadata
        var count = 0
        val startKey = cursor.nodeKey

        if (cursor.hasFirstChild()) {
            cursor.moveTo(cursor.firstChildKey)
            count = 1

            // Quick scan to estimate size (limit to avoid blocking)
            var scanned = 0
            while (cursor.hasRightSibling() && scanned < 100) {
                cursor.moveTo(cursor.rightSiblingKey)
                count++
                scanned++

                if (cursor.hasFirstChild()) {
                    count += 10  // Rough estimate for child subtrees
                }
            }
        }

        cursor.moveTo(startKey)
        return count
    }

    private fun launchParallelTraversal(startKey: Long) {
        if (parallelActive.get()) return  // Already have parallel processing running

        parallelActive.set(true)
        val job = scope.launch {
            val producerCursor = resourceSession.beginNodeReadOnlyTrx()
            try {
                traverseSubtreeParallel(producerCursor, startKey)
            } finally {
                producerCursor.close()
                parallelActive.set(false)
            }
        }
        activeTasks.add(job)
    }

    private suspend fun traverseSubtreeParallel(cursor: NodeCursor, startKey: Long) {
        cursor.moveTo(startKey)
        val stack = mutableListOf<Long>()
        stack.add(startKey)

        while (stack.isNotEmpty() && parallelActive.get()) {
            val nodeKey = stack.removeAt(stack.size - 1)
            cursor.moveTo(nodeKey)

            // Add to result queue
            resultQueue.offer(nodeKey)

            // Add children to stack (in reverse order for preorder traversal)
            val children = mutableListOf<Long>()
            if (cursor.hasFirstChild()) {
                cursor.moveTo(cursor.firstChildKey)
                children.add(cursor.nodeKey)

                while (cursor.hasRightSibling()) {
                    cursor.moveTo(cursor.rightSiblingKey)
                    children.add(cursor.nodeKey)
                }
            }

            // Add in reverse order to maintain preorder traversal
            for (i in children.size - 1 downTo 0) {
                stack.add(children[i])
            }

            // Yield occasionally to avoid blocking
            if (stack.size % 100 == 0) {
                yield()
            }
        }
    }

    private fun waitForParallelResults(): Long {
        // Check queue first
        if (!resultQueue.isEmpty()) {
            return resultQueue.poll()
        }

        // If no results and parallel processing is done, we're finished
        if (!parallelActive.get()) {
            return done()
        }

        // Wait briefly for results
        Thread.sleep(1)  // Very short wait
        return if (!resultQueue.isEmpty()) {
            resultQueue.poll()
        } else {
            done()
        }
    }

    override fun done(): Long {
        cleanup()
        return Fixed.NULL_NODE_KEY.standardProperty
    }

    private fun cleanup() {

        parallelActive.set(false)

        // Cancel all active tasks
        activeTasks.forEach { it.cancel() }
        activeTasks.clear()

        // Clear result queue
        resultQueue.clear()
    }

    // Implement Closeable to properly cleanup resources
    fun close() {
        cleanup()
        scope.cancel()
    }
}
