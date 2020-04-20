package org.sirix.axis.concurrent

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.sirix.api.Axis
import org.sirix.settings.Fixed
import org.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory

class CoroutineAxisHelper(axis: Axis, channel: Channel<Long>) {
    /** Logger  */
    private val LOGGER = LogWrapper(LoggerFactory.getLogger(CoroutineAxisHelper::class.java))

    /** [Axis] that computes the results.  */
    private val mAxis: Axis = axis

    /** [Channel] shared with consumer.  */
    private val mChannel: Channel<Long> = channel

    @InternalCoroutinesApi
    suspend fun produce() {
        produceAll()
        produceFinishSign()
    }

    @InternalCoroutinesApi
    private suspend fun produceAll() {
        // Compute all results of the given axis
        while (NonCancellable.isActive && mAxis.hasNext()) {
            val nodeKey = mAxis.next()
            try {
                // Send result to consumer
                mChannel.send(nodeKey)
            } catch (e: InterruptedException) {
                LOGGER.error(e.message, e)
            }
        }
    }

    @InternalCoroutinesApi
    private suspend fun produceFinishSign() {
        try {
            // Mark end of result sequence by the NULL_NODE_KEY.
            if (NonCancellable.isActive) {
                mChannel.send(Fixed.NULL_NODE_KEY.standardProperty)
            }
        } catch (e: InterruptedException) {
            LOGGER.error(e.message, e)
        }
    }
}
