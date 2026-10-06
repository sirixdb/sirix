package io.sirix.access.trx.node.json;

import io.sirix.access.trx.node.AbstractNodeTrxImpl;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.DataRecord;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

/** Decorates only the real hash-maintenance writer, restoring the displaced dependency on close. */
public final class JsonHashingWorkProbe implements AutoCloseable {
  private final JsonHashingMutation mutation;
  private final Field writerField;
  private final StorageEngineWriter original;
  private final LongSet prefixKeys = new LongOpenHashSet();
  private final LongSet newKeysWritten = new LongOpenHashSet();
  private final LongSet preparedKeys = new LongOpenHashSet();
  private final LongSet writtenPages = new LongOpenHashSet();
  private final LongArrayList states;
  private final LongArrayList ready;
  private final Long2IntOpenHashMap offsets;
  private long preparations;
  private long writes;
  private long reads;

  public JsonHashingWorkProbe(final JsonNodeTrx transaction, final long frontier) {
    try {
      final Field hashingField = AbstractNodeTrxImpl.class.getDeclaredField("nodeHashing");
      hashingField.setAccessible(true);
      mutation = ((JsonNodeHashing) hashingField.get(transaction)).mutation();
      writerField = JsonHashingMutation.class.getDeclaredField("writer");
      writerField.setAccessible(true);
      original = (StorageEngineWriter) writerField.get(mutation);
      states = (LongArrayList) field(JsonHashingMutation.class, "states").get(mutation);
      ready = (LongArrayList) field(JsonHashingMutation.class, "ready").get(mutation);
      offsets = (Long2IntOpenHashMap) field(JsonHashingMutation.class, "offsets").get(mutation);
      final StorageEngineWriter counted =
          (StorageEngineWriter) Proxy.newProxyInstance(StorageEngineWriter.class.getClassLoader(),
              new Class<?>[] {StorageEngineWriter.class}, (proxy, method, arguments) -> {
                if (method.getName().equals("getRecord") && arguments[1] == IndexType.DOCUMENT) {
                  reads++;
                  final long key = (long) arguments[0];
                  if (key > 1 && key <= frontier) {
                    prefixKeys.add(key);
                  }
                } else if (method.getName().equals("persistRecord") && arguments[1] == IndexType.DOCUMENT) {
                  final long key = ((DataRecord) arguments[0]).getNodeKey();
                  writes++;
                  writtenPages.add(original.pageKey(key, IndexType.DOCUMENT));
                  if (key > frontier) {
                    newKeysWritten.add(key);
                  }
                } else if (method.getName().equals("prepareRecordForModification")
                    && arguments[1] == IndexType.DOCUMENT) {
                  preparations++;
                  preparedKeys.add((long) arguments[0]);
                }
                try {
                  return method.invoke(original, arguments);
                } catch (final InvocationTargetException failure) {
                  throw failure.getCause();
                }
              });
      writerField.set(mutation, counted);
    } catch (final ReflectiveOperationException failure) {
      throw new AssertionError(failure);
    }
  }

  public long reads() {
    return reads;
  }

  public long prefixKeys() {
    return prefixKeys.size();
  }

  public long newKeysWritten() {
    return newKeysWritten.size();
  }

  public long preparations() {
    return preparations;
  }

  public long writes() {
    return writes;
  }

  public long writtenPages() {
    return writtenPages.size();
  }

  public long capturedKeys() {
    return offsets.size();
  }

  public long scratchBytes() {
    try {
      final long[] keys = (long[]) field(Long2IntOpenHashMap.class, "key").get(offsets);
      final int[] values = (int[]) field(Long2IntOpenHashMap.class, "value").get(offsets);
      return (long) (states.elements().length + ready.elements().length + keys.length) * Long.BYTES
          + (long) values.length * Integer.BYTES;
    } catch (final ReflectiveOperationException failure) {
      throw new AssertionError(failure);
    }
  }

  public long preparedBetween(final long first, final long last) {
    return preparedKeys.longStream().filter(key -> key >= first && key <= last).count();
  }

  private static Field field(final Class<?> owner, final String name) throws NoSuchFieldException {
    final Field field = owner.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }

  @Override
  public void close() {
    try {
      writerField.set(mutation, original);
    } catch (final IllegalAccessException failure) {
      throw new AssertionError(failure);
    }
  }
}
