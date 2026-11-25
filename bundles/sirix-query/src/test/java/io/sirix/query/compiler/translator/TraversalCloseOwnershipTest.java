package io.sirix.query.compiler.translator;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.api.Axis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.concurrent.ConcurrentAxis;
import io.sirix.node.NodeKind;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.node.XmlDBCollection;
import io.sirix.query.stream.json.SirixJsonStream;
import io.sirix.query.stream.node.SirixNodeStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Ownership oracle: closing a partial traversal fences real producers, even when cleanup fails. */
@Timeout(15)
final class TraversalCloseOwnershipTest {
  @ParameterizedTest
  @ValueSource(strings = {"json", "xml", "lazy"})
  void closingAStreamOrLazyIteratorTerminatesItsProducer(final String wrapper) {
    final ConcurrentAxis<JsonNodeReadOnlyTrx> axis = runningProducer(null);
    try {
      switch (wrapper) {
        case "xml" -> new SirixNodeStream(axis, mock(XmlDBCollection.class)).close();
        case "lazy" -> sequence(axis).iterate().close();
        default -> new SirixJsonStream(axis, mock(JsonDBCollection.class)).close();
      }
      assertTrue(axis.executorService.isTerminated(), "close must fence the running producer");
    } finally {
      axis.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void descendantCloseReleasesEveryCachedProducerAndUpstreamAfterFailure(final boolean fail) throws Exception {
    final IllegalStateException primary = fail
        ? new IllegalStateException("producer close")
        : null;
    final AssertionError secondary = new AssertionError("upstream close");
    final AtomicInteger upstreamReleased = new AtomicInteger();
    final Iter upstream = new BaseIter() {
      @Override
      public Item next() {
        return null;
      }

      @Override
      public void close() {
        upstreamReleased.incrementAndGet();
        if (fail) {
          throw secondary;
        }
      }
    };
    final Expr context = mock(Expr.class);
    when(context.evaluate(null, null)).thenReturn(new LazySequence() {
      @Override
      public Iter iterate() {
        return upstream;
      }
    });
    final Expr field = mock(Expr.class);
    when(field.evaluateToItem(null, null)).thenReturn(new Str("value"));
    final Iter result = new DerefDescendantExpr(context, field).evaluate(null, null).iterate();
    final ConcurrentAxis<JsonNodeReadOnlyTrx> first = runningProducer(primary);
    final ConcurrentAxis<JsonNodeReadOnlyTrx> second = runningProducer(null);
    // Fault-inject owned cache state, without adding a successful-row probe to production.
    // Two node kinds and a missing match exercise every cached-owner close branch independently.
    final Field cache = result.getClass().getDeclaredField("nodeKindToLazySequence");
    cache.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<NodeKind, SirixJsonLazySequence> owners = (Map<NodeKind, SirixJsonLazySequence>) cache.get(result);
    owners.put(NodeKind.OBJECT, sequence(first));
    owners.put(NodeKind.ARRAY, sequence(second));
    owners.put(NodeKind.NULL_VALUE, null);
    try {
      if (fail) {
        assertSame(primary, assertThrows(IllegalStateException.class, result::close));
        assertEquals(1, primary.getSuppressed().length);
        assertSame(secondary, primary.getSuppressed()[0]);
      } else {
        result.close();
      }
      assertEquals(1, upstreamReleased.get(), "upstream ownership must be released despite axis failure");
      assertTrue(first.executorService.isTerminated());
      assertTrue(second.executorService.isTerminated(), "a failing sibling cannot skip another producer");
    } finally {
      try {
        first.close();
      } catch (final IllegalStateException failure) {
        assertSame(primary, failure);
      }
      second.close();
    }
  }

  @Test
  void descendantCloseWithoutCachedAxesPreservesAnUpstreamError() {
    final AssertionError failure = new AssertionError("upstream");
    final Expr context = mock(Expr.class);
    when(context.evaluate(null, null)).thenReturn(new LazySequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          @Override
          public Item next() {
            return null;
          }

          @Override
          public void close() {
            throw failure;
          }
        };
      }
    });
    final Iter result = new DerefDescendantExpr(context, mock(Expr.class)).evaluate(null, null).iterate();
    assertSame(failure, assertThrows(AssertionError.class, result::close));
  }

  private static SirixJsonLazySequence sequence(final Axis axis) {
    return new SirixJsonLazySequence(new SirixJsonStream(axis, mock(JsonDBCollection.class)));
  }

  private static ConcurrentAxis<JsonNodeReadOnlyTrx> runningProducer(final RuntimeException cleanupFailure) {
    final JsonNodeReadOnlyTrx cursor = mock(JsonNodeReadOnlyTrx.class);
    when(cursor.moveTo(anyLong())).thenReturn(true);
    final Axis source = mock(Axis.class);
    when(source.hasNext()).thenReturn(true);
    when(source.nextLong()).thenReturn(1L);
    if (cleanupFailure != null) {
      doAnswer(invocation -> {
        throw cleanupFailure;
      }).when(source).close();
    }
    final ConcurrentAxis<JsonNodeReadOnlyTrx> axis = new ConcurrentAxis<>(cursor, source);
    assertTrue(axis.hasNext(), "a real producer must start before testing abandonment");
    axis.nextLong();
    return axis;
  }
}
