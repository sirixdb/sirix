package io.sirix.rest.crud

import io.brackit.query.atomic.Int32
import io.brackit.query.jdm.Expr
import io.brackit.query.jdm.Item
import io.brackit.query.jdm.Iter
import io.brackit.query.jdm.Sequence
import io.brackit.query.module.Module
import io.brackit.query.util.serialize.Serializer
import io.sirix.api.json.JsonNodeReadOnlyTrx
import io.sirix.api.json.JsonResourceSession
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext
import io.sirix.query.json.PrefetchingChildAxis
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.oauth2.OAuth2Auth
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mockito
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

@Tag("offline")
@Timeout(10)
class QuerySerializerLifecycleTest {
    @ParameterizedTest
    @ValueSource(strings = ["bounded", "pastEnd", "exhausted", "nextFailure", "serializeFailure"])
    fun closesIteratorOnEveryExitAndSuppressesCleanupFailures(mode: String) {
        val iterator = Mockito.mock(Iter::class.java)
        val serializer = Mockito.mock(Serializer::class.java)
        val first = IllegalStateException("iteration or serialization")
        val closeFailure = IllegalStateException("iterator close")
        val serializerFailure = IllegalStateException("serializer close")
        Mockito.`when`(iterator.next()).thenReturn(Int32.ONE, null)
        val failing = mode.endsWith("Failure")
        if (mode == "nextFailure") Mockito.`when`(iterator.next()).thenThrow(first)
        if (failing) {
            Mockito.doThrow(closeFailure).`when`(iterator).close()
            Mockito.doThrow(serializerFailure).`when`(serializer).close()
        }
        val serialize = {
            paginate(iterator, serializer, if (mode == "pastEnd") 10 else 0,
                if (mode == "bounded") 0 else null) { _, _ ->
                if (mode == "serializeFailure") throw first
            }
        }
        if (failing) {
            assertSame(first, assertThrows(IllegalStateException::class.java) { serialize() })
            assertEquals(listOf(closeFailure, serializerFailure), first.suppressed.toList())
        } else {
            serialize()
        }
        Mockito.verify(iterator).close()
        Mockito.verify(serializer).close()
    }

    @ParameterizedTest
    @CsvSource("0,0", "2,3", "4,4")
    fun boundedWindowSkipsThePrefixAndIncludesItsEnd(start: Int, end: Int) {
        val iterator = Mockito.mock(Iter::class.java)
        val next = AtomicInteger()
        Mockito.`when`(iterator.next()).thenAnswer {
            val index = next.getAndIncrement()
            if (index < 5) Int32(index) else null
        }
        val items = ArrayList<Item?>()
        paginate(iterator, Mockito.mock(Serializer::class.java), start.toLong(), end.toLong()) { _, item -> items.add(item) }
        assertEquals((start..end).map { Int32(it) }, items)
        assertEquals(end + 1, next.get())
        Mockito.verify(iterator).close()
    }

    @Test
    fun abandoningBoundedResultClosesPrefetchAndItsReaders() {
        val session = Mockito.mock(JsonResourceSession::class.java)
        val cursor = Mockito.mock(JsonNodeReadOnlyTrx::class.java)
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val readers = ConcurrentLinkedQueue<JsonNodeReadOnlyTrx>()
        Mockito.`when`(cursor.resourceSession).thenReturn(session)
        Mockito.`when`(cursor.revisionNumber).thenReturn(1)
        Mockito.`when`(cursor.maxNodeKey).thenReturn(32768L)
        Mockito.`when`(cursor.childCount).thenReturn(32768L)
        Mockito.`when`(cursor.hasFirstChild()).thenReturn(true)
        Mockito.`when`(cursor.firstChildKey).thenReturn(1L)
        Mockito.`when`(cursor.hasRightSibling()).thenReturn(true)
        Mockito.`when`(cursor.rightSiblingKey).thenReturn(2L)
        Mockito.`when`(cursor.moveTo(Mockito.anyLong())).thenReturn(true)
        Mockito.`when`(session.beginNodeReadOnlyTrx(1)).thenAnswer {
            val reader = Mockito.mock(JsonNodeReadOnlyTrx::class.java)
            Mockito.`when`(reader.moveTo(Mockito.anyLong())).thenAnswer {
                entered.countDown()
                release.await()
                true
            }
            readers.add(reader)
            reader
        }
        val axis = PrefetchingChildAxis(cursor)
        val iterator = Mockito.mock(Iter::class.java)
        Mockito.`when`(iterator.next()).thenAnswer {
            axis.nextLong()
            Int32.ONE
        }
        Mockito.doAnswer {
            axis.close()
            cursor.close()
            null
        }.`when`(iterator).close()
        val prefetchField = PrefetchingChildAxis::class.java.getDeclaredField("prefetcher")
        prefetchField.isAccessible = true
        var prefetch: Any? = null
        var emitted = 0
        try {
            paginate(iterator, Mockito.mock(Serializer::class.java), 0, 1) { _, _ ->
                if (++emitted == 2) {
                    entered.await() // The result is abandoned while a real prefetch reader is active.
                    prefetch = prefetchField.get(axis)
                }
            }
            assertEquals(2, emitted)
            assertNull(prefetchField.get(axis), "iterator close must detach its prefetcher")
            Mockito.verify(cursor).close()
        } finally {
            release.countDown()
            axis.close()
        }
        val running = requireNotNull(prefetch)
        val outstandingField = running.javaClass.getDeclaredField("outstanding")
        outstandingField.isAccessible = true
        val outstanding = outstandingField.get(running) as AtomicInteger
        val readersField = running.javaClass.getDeclaredField("workerTrxs")
        readersField.isAccessible = true
        val workerReaders = readersField.get(running) as Map<*, *>
        // The final worker owns deferred release. No timing threshold is part of the assertion;
        // the test timeout is only a deadlock guard.
        while (outstanding.get() != 0 || workerReaders.isNotEmpty()) {
            if (Thread.currentThread().isInterrupted) throw InterruptedException()
            Thread.yield()
        }
        assertTrue(readers.isNotEmpty())
        readers.forEach { Mockito.verify(it).close() }
        Mockito.verify(iterator).close()
    }

    private fun paginate(
        iterator: Iter,
        serializer: Serializer,
        start: Long,
        end: Long?,
        serialize: (Serializer, Item?) -> Unit
    ) {
        val sequence = Mockito.mock(Sequence::class.java)
        Mockito.`when`(sequence.iterate()).thenReturn(iterator)
        val body = Mockito.mock(Expr::class.java)
        Mockito.`when`(body.evaluate(Mockito.any(), Mockito.any())).thenReturn(sequence)
        val module = Mockito.mock(Module::class.java)
        Mockito.`when`(module.body).thenReturn(body)
        val chain = Mockito.mock(SirixCompileChain::class.java)
        Mockito.`when`(chain.compile("test")).thenReturn(module)
        QuerySerializer.serializePaginated(chain, "test", Mockito.mock(SirixQueryContext::class.java), start, end,
            Mockito.mock(OAuth2Auth::class.java), Mockito.mock(AuthorizationProvider::class.java),
            User.fromName("reader"), serializer, serialize)
    }
}
