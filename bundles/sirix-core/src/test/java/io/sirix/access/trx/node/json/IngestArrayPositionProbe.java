package io.sirix.access.trx.node.json;

import io.sirix.api.json.JsonNodeTrx;
import io.sirix.access.trx.node.AbstractNodeTrxImpl;
import io.sirix.diff.DiffTuple;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntMaps;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Reads transient transaction state without exposing the hints through the public writer API. */
public final class IngestArrayPositionProbe {
  private static final Field POSITIONS = positionsField();

  private IngestArrayPositionProbe() {
  }

  public static Long2IntMap snapshot(final JsonNodeTrx trx) {
    try {
      final Long2IntMap positions = (Long2IntMap) POSITIONS.get(trx);
      return positions == null ? Long2IntMaps.EMPTY_MAP : new Long2IntOpenHashMap(positions);
    } catch (final IllegalAccessException e) {
      throw new AssertionError(e);
    }
  }

  /** Captures the actual pending tuples, including stale tuples that serialization must skip. */
  public static List<DiffTuple> pendingDiffs(final JsonNodeTrx trx, final boolean ordered) {
    try {
      final Field field = AbstractNodeTrxImpl.class.getDeclaredField(ordered
          ? "updateOperationsOrdered" : "updateOperationsUnordered");
      field.setAccessible(true);
      final Map<?, ?> operations = (Map<?, ?>) field.get(trx);
      final List<DiffTuple> diffs = new ArrayList<>(operations.size());
      for (final Object value : operations.values()) {
        diffs.add((DiffTuple) value);
      }
      return diffs;
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private static Field positionsField() {
    try {
      final Field field = JsonNodeTrxImpl.class.getDeclaredField("ingestArrayPositions");
      field.setAccessible(true);
      return field;
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }
}
