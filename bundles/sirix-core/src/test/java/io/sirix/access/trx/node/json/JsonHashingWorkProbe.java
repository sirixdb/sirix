package io.sirix.access.trx.node.json;

import io.sirix.access.trx.node.AbstractNodeTrxImpl;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.DataRecord;
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
  private long reads;

  public JsonHashingWorkProbe(final JsonNodeTrx transaction, final long frontier) {
    try {
      final Field hashingField = AbstractNodeTrxImpl.class.getDeclaredField("nodeHashing");
      hashingField.setAccessible(true);
      mutation = ((JsonNodeHashing) hashingField.get(transaction)).mutation();
      writerField = JsonHashingMutation.class.getDeclaredField("writer");
      writerField.setAccessible(true);
      original = (StorageEngineWriter) writerField.get(mutation);
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
                  if (key > frontier) {
                    newKeysWritten.add(key);
                  }
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

  @Override
  public void close() {
    try {
      writerField.set(mutation, original);
    } catch (final IllegalAccessException failure) {
      throw new AssertionError(failure);
    }
  }
}
