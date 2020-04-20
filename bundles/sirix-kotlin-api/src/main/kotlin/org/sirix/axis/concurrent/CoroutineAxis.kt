package org.sirix.axis.concurrent

import com.google.common.base.Preconditions

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.sirix.api.Axis
import org.sirix.api.NodeCursor
import org.sirix.api.NodeReadOnlyTrx
import org.sirix.axis.AbstractAxis
import org.sirix.settings.Fixed
import org.sirix.utils.LogWrapper

import org.slf4j.LoggerFactory
import javax.annotation.Nonnegative
import kotlin.coroutines.CoroutineContext

/**
 * <h1>CoroutineAxis</h1>
 * <p>
 * Realizes in combination with the <code>CoroutineAxisHelper</code> the concurrent evaluation of
 * pipeline steps. The given axis is embedded in a Coroutine that uses its own transaction and stores all
 * the results in channel. The CoroutineAxis gets the computed results from that channel
 * one by one on every hasNext() call and sets the main-transaction to it.
 * As soon as the end of the computed result sequence is reached (marked by the
 * NULL_NODE_KEY), the CoroutineAxis returns <code>false</code>.
 * </p>
 * <p>
 * This framework is working according to the producer-consumer-principle, where the
 * CoroutineAxisHelper and its encapsulated axis is the producer and the CoroutineAxis
 * is the consumer. This can be used by any class that implements the IAxis interface. Note:
 * Make sure that the used class is thread-safe.
 * </p>
 */

class CoroutineAxis<R>(rtx: R, childAxis: Axis) : AbstractAxis(rtx), CoroutineScope where R : NodeCursor, R : NodeReadOnlyTrx {
    /** Logger.  */
    private val LOGGER = LogWrapper(LoggerFactory.getLogger(CoroutineAxis::class.java))

    /** Axis that is running in coroutine and produces results for this axis.  */
    private var mProducerAxis: Axis?

    /**
     * Producer which runs axis in coroutine
     */
    private var mProducer: CoroutineAxisHelper

    /**
     * Channel that stores result keys already computed by the producer. End of the result sequence is
     * marked by the NULL_NODE_KEY.
     */
    private var mResults: Channel<Long>

    /**
     * Producing task which put results in the channel
     */
    private var mProducingTask: Job? = null

    /** Capacity of the mResults queue.  */
    private val M_CAPACITY = 200

    /** Has axis already been called?  */
    private var mFirst: Boolean

    /** Is axis already finished and has no results left?  */
    private var mFinished: Boolean

    init {
        require(rtx.id != childAxis.trx.id) { "The filter must be bound to another transaction but on the same revision/node!" }
        mProducerAxis = Preconditions.checkNotNull(childAxis)
        mResults = Channel(M_CAPACITY)
        mProducer = CoroutineAxisHelper(mProducerAxis!!, mResults)
        mFirst = true
        mFinished = false
    }

    override val coroutineContext: CoroutineContext
        get() = CoroutineName("CoroutineAxis") + Dispatchers.Default

    @Synchronized
    override fun reset(@Nonnegative nodeKey: Long) {
        super.reset(nodeKey)
        mFirst = true
        mFinished = false
        mProducingTask?.cancel()
        mProducerAxis?.let { producerAxis ->
            producerAxis.reset(nodeKey)
            mResults.close()
            mResults = Channel(M_CAPACITY)
            mProducer = CoroutineAxisHelper(producerAxis, mResults)
        }
    }

    @InternalCoroutinesApi
    override fun nextKey(): Long {
        if (mFirst) {
            mFirst = false
            runProducer()
        }

        if (isFinished()) {
            return done()
        }
        val result = getResult()

        if (!isEndOfStream(result)) {
            return result
        }

        mFinished = true
        return done()
    }

    /**
     * Runs producer task in new coroutine and holds task
     */
    @InternalCoroutinesApi
    private fun runProducer() {
        mProducingTask = launch { mProducer.produce() }
    }

    /**
     * Gets current producer result from coroutine channel
     * @return producer result
     */
    private fun getResult(): Long {
        var result = Fixed.NULL_NODE_KEY.standardProperty
        try {
            // Get result from producer as soon as it is available.
            runBlocking { result = mResults.receive() }
        } catch (e: InterruptedException) {
            LOGGER.warn(e.message, e)
        }
        return result
    }

    /**
     * Determines if producer will send another results
     *
     * @return `true`, if producer send finish sign, `false` otherwise
     */
    private fun isEndOfStream(result: Long): Boolean {
        // NULL_NODE_KEY marks end of the sequence computed by the producer.
        return result == Fixed.NULL_NODE_KEY.standardProperty
    }

    /**
     * Signals that axis traversal is done, that is `hasNext()` must return false. Is callable
     * from subclasses which implement [.nextKey] to signal that the axis-traversal is done and
     * [.hasNext] must return false.
     *
     * @return null node key to indicate that the travesal is done
     */
    override fun done(): Long {
        mProducingTask?.cancel()
        mResults.close()
        return Fixed.NULL_NODE_KEY.standardProperty
    }

    /**
     * Determines if axis has more results to deliver or not.
     *
     * @return `true`, if axis still has results left, `false` otherwise
     */
    fun isFinished(): Boolean {
        return mFinished
    }
}
