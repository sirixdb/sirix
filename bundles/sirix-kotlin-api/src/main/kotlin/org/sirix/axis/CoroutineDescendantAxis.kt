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

package org.sirix.axis

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.sirix.api.NodeCursor
import org.sirix.api.NodeReadOnlyTrx
import org.sirix.api.NodeTrx
import org.sirix.api.ResourceManager
import org.sirix.settings.Fixed
import org.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory
import java.util.*
import javax.annotation.Nonnegative
import kotlin.coroutines.CoroutineContext

/**
 * <h1>CoroutineDescendantAxis</h1>
 * <p>
 * DescendantAxis which simultaneously compute all right page results in another task.
 * The CoroutineDescendantAxis gets the computed right page results from common producer consumer channel
 * after it finish left page processing. As soon as the end of the computed result sequence is reached (marked by the
 * NULL_NODE_KEY), the CoroutineDescendantAxis returns <code>false</code>.
 * </p>
 */
class CoroutineDescendantAxis<R,W>: AbstractAxis where R: NodeReadOnlyTrx, R: NodeCursor, W: NodeTrx, W: NodeCursor {
    /** Logger  */
    private val LOGGER = LogWrapper(LoggerFactory.getLogger(CoroutineDescendantAxis::class.java))

    /** Right page coroutine producer  */
    private var mProducer: Producer

    /** Axis current resource manager  */
    private var mResourceManager: ResourceManager<R, W>

    /** Determines if it's the first call to hasNext().  */
    private var mFirst = false

    /** Determines if left page still has any results to compute.  */
    private var mLeft = true

    /** Capacity of the mResults queue.  */
    private val M_CAPACITY = 200

    /**
     * Constructor initializing internal state.
     *
     * @param resourceManager to retrieve cursors for pages
     */
    constructor(resourceManager: ResourceManager<R, W>) : super(resourceManager.beginNodeReadOnlyTrx()) {
        mResourceManager = resourceManager
        mProducer = Producer()
    }
    /**
     * Constructor initializing internal state.
     *
     * @param resourceManager to retrieve cursors for pages
     * @param includeSelf determines if current node is included or not
     */
    constructor(resourceManager: ResourceManager<R, W>, includeSelf: IncludeSelf) : super(resourceManager.beginNodeReadOnlyTrx(), includeSelf) {
        mResourceManager = resourceManager
        mProducer = Producer()
    }
    /**
     * Constructor initializing internal state.
     *
     * @param resourceManager to retrieve cursor for right page
     * @param includeSelf determines if current node is included or not
     * @param cursor to iterate through left page
     */
    constructor(resourceManager: ResourceManager<R, W>, includeSelf: IncludeSelf, cursor: NodeCursor) : super(cursor, includeSelf) {
        mResourceManager = resourceManager
        mProducer = Producer()
    }

    init {
        mFirst = true
        mLeft = true
    }

    override fun reset(nodeKey: Long) {
        super.reset(nodeKey)
        mFirst = true
        mLeft = true
        if (mProducer != null) {
            mProducer.close()
        }
    }

    @InternalCoroutinesApi
    override fun nextKey(): Long {
        var key = Fixed.NULL_NODE_KEY.standardProperty

        if (mLeft) {
            // Determines if first call to hasNext().
            if (mFirst) {
                mFirst = false
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

            // End of left page, any other results are computed in coroutine
            mLeft = false
        }

        // Follow right page when coroutine is enabled.
        if (mProducer.mRunning && !mLeft) {

            // Then follow right sibling on from coroutine.
            runBlocking { key = mProducer.receive() }

            if (!isDone(key)) {
                val currKey = cursor.nodeKey
                return hasNextNode(cursor, key, currKey)
            }
        }

        return done()
    }

    private fun nextKeyForFirst(): Long {
        return if (isSelfIncluded == IncludeSelf.YES) {
            cursor.nodeKey
        } else {
            cursor.firstChildKey
        }
    }

    @InternalCoroutinesApi
    private fun nextKeyForFirstChild(): Long {
        if (cursor.hasRightSibling()) {
            mProducer.run(cursor.rightSiblingKey)
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
    private fun hasNextNode(cursor: NodeCursor, @Nonnegative key: Long, @Nonnegative currKey: Long): Long {
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
        mLeft = true
        // Cancel all coroutine tasks
        mProducer.close()
        return Fixed.NULL_NODE_KEY.standardProperty
    }

     private inner class Producer: CoroutineScope {
        /** Stack for remembering next nodeKey in right page.  */
        private val mRightSiblingKeyStack: Deque<Long> = ArrayDeque<Long>()
        /**
         * Channel that stores result keys already computed by the producer. End of the result sequence is
         * marked by the NULL_NODE_KEY.
         */
        private var mResults: Channel<Long> = Channel(M_CAPACITY)
         /**
         * Actual coroutine job
         */
        private var mProducingTask: Job? = null
        /** Determines if right page coroutine is running.  */
        var mRunning = false
        /** Right page cursor  */
        private val cursor: NodeCursor = mResourceManager.beginNodeReadOnlyTrx()

        override val coroutineContext: CoroutineContext
            get() = CoroutineName("CoroutineDescendantAxis") + Dispatchers.Default

        fun close() {
            mProducingTask?.cancel()
            mResults.close()
            mRunning = false
            mResults = Channel(M_CAPACITY)
            cursor.close()
        }
        suspend fun receive(): Long {
            return mResults.receive()
        }

        @InternalCoroutinesApi
        fun run(key: Long) {
            // wait for last coroutine end
            runBlocking { mProducingTask?.join() }
            mProducingTask = launch { produce(key) }
            mRunning = true
        }

         @InternalCoroutinesApi
         suspend fun produce(key: Long) {
             produceAll(key)
             produceFinishSign()
         }

         @InternalCoroutinesApi
         private suspend fun produceAll(startKey: Long) {
             var key = startKey
             mResults.send(key)
             // Compute all results from given start key
             while (NonCancellable.isActive) {
                 cursor.moveTo(key)

                 // Always follow first child if there is one.
                 if (cursor.hasFirstChild()) {
                     key = cursor.firstChildKey
                     if (cursor.hasRightSibling()) {
                         mRightSiblingKeyStack.push(cursor.rightSiblingKey)
                     }
                     mResults.send(key)
                     continue
                 }

                 // Then follow right sibling if there is one.
                 if (cursor.hasRightSibling()) {
                     val currKey: Long = cursor.node.nodeKey
                     key = hasNextNode(cursor, cursor.rightSiblingKey, currKey)
                     mResults.send(key)
                     continue
                 }

                 // Then follow right sibling on stack.
                 if (mRightSiblingKeyStack.size > 0) {
                     val currKey: Long = cursor.node.nodeKey
                     key = hasNextNode(cursor, mRightSiblingKeyStack.pop(), currKey)
                     mResults.send(key)
                     continue
                 }
                 break
             }
         }

         @InternalCoroutinesApi
         private suspend fun produceFinishSign() {
             try {
                 // Mark end of result sequence by the NULL_NODE_KEY only if channel is not already closed
                 if (NonCancellable.isActive) {
                     mResults.send(Fixed.NULL_NODE_KEY.standardProperty)
                 }
             } catch (e: InterruptedException) {
                 LOGGER.error(e.message, e)
             }
         }
     }
}
