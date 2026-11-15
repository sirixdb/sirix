package io.sirix.rest.crud

import io.brackit.query.atomic.Int32
import io.brackit.query.jdm.Expr
import io.brackit.query.jdm.Item
import io.brackit.query.jdm.Iter
import io.brackit.query.jdm.Sequence
import io.brackit.query.module.Module
import io.brackit.query.sequence.ItemSequence
import io.brackit.query.util.serialize.Serializer
import io.sirix.api.json.JsonNodeReadOnlyTrx
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext
import io.sirix.api.Axis
import io.sirix.axis.concurrent.ConcurrentAxis
import io.sirix.io.StorageType
import io.sirix.query.json.BasicJsonDBStore
import io.sirix.query.json.JsonDBItem
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.io.TempDir
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.oauth2.OAuth2Auth
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mockito
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

    @TempDir
    lateinit var directory: Path

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    @Timeout(30)
    fun abandoningRealDescendantQueryFencesProducersBeforeReadersClose(sequenceInput: Boolean) {
        BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL)
            .buildPathSummary(true).build().use { store ->
            val records = (1..600).joinToString(",") { """{"left":{"match":1},"right":{"match":2}}""" }
            store.create("lifecycle", "records", """{"root":{"deep":{"rows":[$records]}}}""")
            SirixQueryContext.createWithJsonStore(store).use { context ->
                SirixCompileChain.createWithJsonStore(store).use { chain ->
                    val document = "jn:doc('lifecycle','records')"
                    val query = "$document=>>match"
                    val module = chain.compile(query)
                    if (sequenceInput) {
                        val input = module.body.javaClass.getDeclaredField("context")
                        input.isAccessible = true
                        val contextExpression = Mockito.spy(input.get(module.body) as Expr)
                        Mockito.doAnswer { invocation ->
                            ItemSequence(invocation.callRealMethod() as Item)
                        }.`when`(contextExpression).evaluate(Mockito.any(), Mockito.any())
                        input.set(module.body, contextExpression)
                    }
                    val body = Mockito.spy(module.body)
                    val result = AtomicReference<Sequence>()
                    val iterator = AtomicReference<Iter>()
                    Mockito.doAnswer { invocation ->
                        val sequence = invocation.callRealMethod() as Sequence
                        result.set(sequence)
                        val observedSequence = Mockito.spy(sequence)
                        Mockito.doAnswer { iteration ->
                            (iteration.callRealMethod() as Iter).also { iterator.set(it) }
                        }.`when`(observedSequence).iterate()
                        observedSequence
                    }.`when`(body).evaluate(Mockito.any(), Mockito.any())
                    val observedModule = Mockito.spy(module)
                    Mockito.doReturn(body).`when`(observedModule).body
                    val observedChain = Mockito.spy(chain)
                    Mockito.doReturn(observedModule).`when`(observedChain).compile(query)
                    val producers = ArrayList<ConcurrentAxis<*>>()
                    val readers = ArrayList<JsonNodeReadOnlyTrx>()
                    var emitted = 0
                    val serializer = Mockito.mock(Serializer::class.java)
                    Mockito.doAnswer {
                        assertTrue(producers.isNotEmpty(), "the real descendant plan must start a producer")
                        producers.forEach { assertTrue(it.executorService.isTerminated) }
                        assertTrue(readers.none { it.isClosed }, "producer fencing precedes session teardown")
                        null
                    }.`when`(serializer).close()
                    QuerySerializer.serializePaginated(observedChain, query, context, 0, 0,
                        Mockito.mock(OAuth2Auth::class.java), Mockito.mock(AuthorizationProvider::class.java),
                        User.fromName("reader"), serializer) { _, item ->
                        emitted++
                        assertEquals("1", item.toString())
                        val plans = if (sequenceInput) {
                            val cache = iterator.get().javaClass.getDeclaredField("nodeKindToLazySequence")
                            cache.isAccessible = true
                            (cache.get(iterator.get()) as Map<*, *>).values.filterNotNull()
                        } else listOf(result.get())
                        plans.forEach { sequence ->
                            val getAxis = sequence.javaClass.getDeclaredMethod("getAxis")
                            getAxis.isAccessible = true
                            collectProducers(getAxis.invoke(sequence) as Axis, producers)
                        }
                        val session = (item as JsonDBItem).trx.resourceSession
                        val mapField = session.javaClass.superclass.getDeclaredField("nodeTrxMap")
                        mapField.isAccessible = true
                        @Suppress("UNCHECKED_CAST")
                        readers.addAll((mapField.get(session) as Map<Int, JsonNodeReadOnlyTrx>).values)
                    }
                    assertEquals(1, emitted)
                    assertTrue(readers.isNotEmpty())
                    readers.first().resourceSession.close()
                    assertTrue(readers.all { it.isClosed })
                }
            }
        }
    }

    private fun collectProducers(axis: Axis, producers: MutableList<ConcurrentAxis<*>>) {
        if (axis is ConcurrentAxis<*>) producers.add(axis)
        axis.javaClass.declaredFields.forEach { field ->
            if (Axis::class.java.isAssignableFrom(field.type)) {
                field.isAccessible = true
                collectProducers(field.get(axis) as Axis, producers)
            }
        }
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
