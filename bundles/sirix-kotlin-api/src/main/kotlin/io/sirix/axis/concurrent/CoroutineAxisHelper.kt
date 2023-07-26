package io.sirix.axis.concurrent

import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import io.sirix.api.Axis
import io.sirix.settings.Fixed
import io.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory

class CoroutineAxisHelper(
    /** [Axis] that computes the results.  */
    private val axis: Axis,
    /** [Channel] shared with consumer.  */
    private val channel: Channel<Long>
) {
    /** Logger  */
    private val logger =
        LogWrapper(LoggerFactory.getLogger(CoroutineAxisHelper::class.java))

    @InternalCoroutinesApi
    suspend fun produce() {
        produceAll()
        produceFinishSign()
    }

    @InternalCoroutinesApi
    private suspend fun produceAll() {
        // Compute all results of the given axis
        while (NonCancellable.isActive && axis.hasNext()) {
            val nodeKey = axis.nextLong()
            try {
                // Send result to consumer
                channel.send(nodeKey)
            } catch (e: InterruptedException) {
                logger.error(e.message, e)
            }
        }
    }

    @InternalCoroutinesApi
    private suspend fun produceFinishSign() {
        try {
            // Mark end of result sequence by the NULL_NODE_KEY.
            if (NonCancellable.isActive) {
                channel.send(Fixed.NULL_NODE_KEY.standardProperty)
            }
        } catch (e: InterruptedException) {
            logger.error(e.message, e)
        }
    }
}
