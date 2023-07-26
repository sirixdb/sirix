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
import org.checkerframework.checker.index.qual.NonNegative
import io.sirix.api.NodeCursor
import io.sirix.api.NodeReadOnlyTrx
import io.sirix.api.NodeTrx
import io.sirix.api.ResourceSession
import io.sirix.settings.Fixed
import io.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.coroutines.CoroutineContext

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
    /** Logger  */
    private val logger =
        LogWrapper(LoggerFactory.getLogger(CoroutineDescendantAxis::class.java))

    /** Right page coroutine producer  */
    private var producer: Producer

    /** Axis current resource session  */
    private var resourceSession: ResourceSession<R, W>

    /** Determines if it's the first call to hasNext().  */
    private var first = false

    /** Determines if left page still has any results to compute.  */
    private var left = true

    /** Capacity of the results queue.  */
    private val capacity = 200

    /**
     * Constructor initializing internal state.
     *
     * @param resourceSession to retrieve cursors for pages
     */
    constructor(resourceSession: ResourceSession<R, W>) : super(resourceSession.beginNodeReadOnlyTrx()) {
        this.resourceSession = resourceSession
        producer = Producer()
    }

    /**
     * Constructor initializing internal state.
     *
     * @param resourceSession to retrieve cursors for pages
     * @param includeSelf determines if current node is included or not
     */
    constructor(
        resourceSession: ResourceSession<R, W>,
        includeSelf: IncludeSelf
    ) : super(resourceSession.beginNodeReadOnlyTrx(), includeSelf) {
        this.resourceSession = resourceSession
        producer = Producer()
    }

    /**
     * Constructor initializing internal state.
     *
     * @param resourceSession to retrieve cursor for right page
     * @param includeSelf determines if current node is included or not
     * @param cursor to iterate through left page
     */
    constructor(resourceSession: ResourceSession<R, W>, includeSelf: IncludeSelf, cursor: NodeCursor) : super(
        cursor,
        includeSelf
    ) {
        this.resourceSession = resourceSession
        producer = Producer()
    }

    init {
        first = true
        left = true
    }

    override fun reset(nodeKey: Long) {
        super.reset(nodeKey)
        first = true
        left = true
    }

    @InternalCoroutinesApi
    override fun nextKey(): Long {
        var key: Long

        if (left) {
            // Determines if first call to hasNext().
            if (first) {
                first = false
                return nextKeyForFirst()
            }
            // Always follow first child if there is one.
            if (cursor.hasFirstChild()) {
                return nextKeyForFirstChild()
            }
            // Then follow right sibling if there is one.
            if (cursor.hasRightSibling()) {
                return nextKeyForRightSibling()
            }

            // End of left node, any other results are computed in coroutine
            left = false
        }

        // Follow right node when coroutine is enabled.
        if (producer.running) {

            // Then follow right sibling on from coroutine.
            runBlocking { key = producer.receive() }

            if (!isDone(key)) {
                val currKey = cursor.nodeKey
                return hasNextNode(cursor, key, currKey)
            }
        }

        return done()
    }

    private fun nextKeyForFirst(): Long {
        return if (includeSelf() == IncludeSelf.YES) {
            cursor.nodeKey
        } else {
            cursor.firstChildKey
        }
    }

    @InternalCoroutinesApi
    private fun nextKeyForFirstChild(): Long {
        if (cursor.hasRightSibling()) {
            producer.run(cursor.rightSiblingKey)
        }
        return cursor.firstChildKey
    }

    private fun nextKeyForRightSibling(): Long {
        val currKey = cursor.nodeKey
        val key = cursor.rightSiblingKey
        return hasNextNode(cursor, key, currKey)
    }

    /**
     * Determines if the subtree-traversal is finished.
     * @param cursor right or left page current cursor
     * @param key next key
     * @param currKey current node key
     * @return `false` if finished, `true` if not
     */
    private fun hasNextNode(cursor: NodeCursor, key: @NonNegative Long, currKey: @NonNegative Long): Long {
        cursor.moveTo(key)
        return if (cursor.leftSiblingKey == startKey) {
            done()
        } else {
            cursor.moveTo(currKey)
            key
        }
    }

    private fun isDone(key: Long): Boolean {
        return key == Fixed.NULL_NODE_KEY.standardProperty
    }

    /**
     * Signals that axis traversal is done, that is `hasNext()` must return false. Is callable
     * from subclasses which implement [.nextKey] to signal that the axis-traversal is done and
     * [.hasNext] must return false.
     *
     * @return null node key to indicate that the travesal is done
     */
    override fun done(): Long {
        // End of results also from right page
        left = true
        // Cancel all coroutine tasks
        producer.close()
        return Fixed.NULL_NODE_KEY.standardProperty
    }

    private inner class Producer : CoroutineScope {
        /** Stack for remembering next nodeKey of right siblings.  */
        private val rightSiblingKeyStack: Deque<Long> = ArrayDeque()

        /**
         * Channel that stores result keys already computed by the producer. End of the result sequence is
         * marked by the NULL_NODE_KEY.
         */
        private var results: Channel<Long> = Channel(capacity)

        /**
         * Actual coroutine job
         */
        private var producingTask: Job? = null

        /** Determines if right node coroutine is running.  */
        var running = false

        /** Right page cursor  */
        private val cursor: NodeCursor = resourceSession.beginNodeReadOnlyTrx()

        override val coroutineContext: CoroutineContext
            get() = CoroutineName("CoroutineDescendantAxis") + Dispatchers.IO

        fun close() {
            producingTask?.cancel()
            results.close()
            running = false
            results = Channel(capacity)
            cursor.close()
        }

        suspend fun receive(): Long {
            return results.receive()
        }

        @InternalCoroutinesApi
        fun run(key: Long) {
            // wait for last coroutine end
            runBlocking { producingTask?.join() }
            producingTask = launch { produce(key) }
            running = true
        }

        @InternalCoroutinesApi
        suspend fun produce(key: Long) {
            produceAll(key)
            produceFinishSign()
        }

        @InternalCoroutinesApi
        private suspend fun produceAll(startKey: Long) {
            var key = startKey
            results.send(key)
            // Compute all results from given start key
            while (true) {
                cursor.moveTo(key)

                // Always follow first child if there is one.
                if (cursor.hasFirstChild()) {
                    key = cursor.firstChildKey
                    if (cursor.hasRightSibling()) {
                        rightSiblingKeyStack.push(cursor.rightSiblingKey)
                    }
                    results.send(key)
                    continue
                }

                // Then follow right sibling if there is one.
                if (cursor.hasRightSibling()) {
                    val currKey: Long = cursor.node.nodeKey
                    key = hasNextNode(cursor, cursor.rightSiblingKey, currKey)
                    results.send(key)
                    continue
                }

                // Then follow right sibling on stack.
                if (rightSiblingKeyStack.size > 0) {
                    val currKey: Long = cursor.node.nodeKey
                    key = hasNextNode(cursor, rightSiblingKeyStack.pop(), currKey)
                    results.send(key)
                    continue
                }
                break
            }
        }

        @InternalCoroutinesApi
        private suspend fun produceFinishSign() {
            try {
                // Mark end of result sequence by the NULL_NODE_KEY only if channel is not already closed
                withContext(NonCancellable) {
                    results.send(Fixed.NULL_NODE_KEY.standardProperty)
                }
            } catch (e: InterruptedException) {
                logger.error(e.message, e)
            }
        }
    }
}
