package org.sirix.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import sun.misc.Unsafe;

public final class TestSerialisationPerf {
  public static final int REPETITIONS = 1 * 1000 * 1000;

  private static ObjectToBeSerialised ITEM = new ObjectToBeSerialised(1010L,
                                                                      true, 777, 99, new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7,
      0.8, 0.9, 1.0 },
                                                                      new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });

  public static void main(final String[] arg) throws Exception {
    for (final PerformanceTestCase testCase : testCases) {
      for (int i = 0; i < 5; i++) {
        testCase.performTest();

        System.out.format(
            "%d %s\twrite=%,dns read=%,dns total=%,dns\n",
            i,
            testCase.getName(),
            testCase.getWriteTimeNanos(),
            testCase.getReadTimeNanos(),
            testCase.getWriteTimeNanos()
                + testCase.getReadTimeNanos());

        if (!ITEM.equals(testCase.getTestOutput())) {
          throw new IllegalStateException("Objects do not match");
        }

        System.gc();
        Thread.sleep(3000);
      }
    }
  }

  private static final PerformanceTestCase[] testCases = {
      new PerformanceTestCase("Serialisation", REPETITIONS, ITEM) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        @Override
        public void testWrite(ObjectToBeSerialised item)
            throws Exception {
          for (int i = 0; i < REPETITIONS; i++) {
            baos.reset();

            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(item);
            oos.close();
          }
        }

        @Override
        public ObjectToBeSerialised testRead() throws Exception {
          ObjectToBeSerialised object = null;
          for (int i = 0; i < REPETITIONS; i++) {
            ByteArrayInputStream bais = new ByteArrayInputStream(
                baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            object = (ObjectToBeSerialised) ois.readObject();
          }

          return object;
        }
      },

      new PerformanceTestCase("ByteBuffer", REPETITIONS, ITEM) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

        @Override
        public void testWrite(ObjectToBeSerialised item)
            throws Exception {
          for (int i = 0; i < REPETITIONS; i++) {
            byteBuffer.clear();
            item.write(byteBuffer);
          }
        }

        @Override
        public ObjectToBeSerialised testRead() throws Exception {
          ObjectToBeSerialised object = null;
          for (int i = 0; i < REPETITIONS; i++) {
            byteBuffer.flip();
            object = ObjectToBeSerialised.read(byteBuffer);
          }

          return object;
        }
      },

      new PerformanceTestCase("ByteBufferImproved", REPETITIONS, ITEM) {
        // Native order
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024).order(
            ByteOrder.nativeOrder());

        @Override
        public void testWrite(ObjectToBeSerialised item)
            throws Exception {
          for (int i = 0; i < REPETITIONS; i++) {
            byteBuffer.clear();
            item.writeImproved(byteBuffer);
          }
        }

        @Override
        public ObjectToBeSerialised testRead() throws Exception {
          ObjectToBeSerialised object = null;
          for (int i = 0; i < REPETITIONS; i++) {
            byteBuffer.flip();
            object = ObjectToBeSerialised.readImproved(byteBuffer);
          }

          return object;
        }
      },

      new PerformanceTestCase("UnsafeMemory", REPETITIONS, ITEM) {
        UnsafeMemory buffer = new UnsafeMemory(new byte[1024]);

        @Override
        public void testWrite(ObjectToBeSerialised item)
            throws Exception {
          for (int i = 0; i < REPETITIONS; i++) {
            buffer.reset();
            item.write(buffer);
          }
        }

        @Override
        public ObjectToBeSerialised testRead() throws Exception {
          ObjectToBeSerialised object = null;
          for (int i = 0; i < REPETITIONS; i++) {
            buffer.reset();
            object = ObjectToBeSerialised.read(buffer);
          }

          return object;
        }
      }, };
}

abstract class PerformanceTestCase {
  private final String name;
  private final int repetitions;
  private final ObjectToBeSerialised testInput;
  private ObjectToBeSerialised testOutput;
  private long writeTimeNanos;
  private long readTimeNanos;

  public PerformanceTestCase(final String name, final int repetitions,
      final ObjectToBeSerialised testInput) {
    this.name = name;
    this.repetitions = repetitions;
    this.testInput = testInput;
  }

  public String getName() {
    return name;
  }

  public ObjectToBeSerialised getTestOutput() {
    return testOutput;
  }

  public long getWriteTimeNanos() {
    return writeTimeNanos;
  }

  public long getReadTimeNanos() {
    return readTimeNanos;
  }

  public void performTest() throws Exception {
    final long startWriteNanos = System.nanoTime();
    testWrite(testInput);
    writeTimeNanos = (System.nanoTime() - startWriteNanos) / repetitions;

    final long startReadNanos = System.nanoTime();
    testOutput = testRead();
    readTimeNanos = (System.nanoTime() - startReadNanos) / repetitions;
  }

  public abstract void testWrite(ObjectToBeSerialised item) throws Exception;

  public abstract ObjectToBeSerialised testRead() throws Exception;
}

class ObjectToBeSerialised implements Serializable {
  private static final long serialVersionUID = 10275539472837495L;

  private final long sourceId;
  private final boolean special;
  private final int orderCode;
  private final int priority;
  private final double[] prices;
  private final long[] quantities;

  public ObjectToBeSerialised(final long sourceId, final boolean special,
      final int orderCode, final int priority, final double[] prices,
      final long[] quantities) {
    this.sourceId = sourceId;
    this.special = special;
    this.orderCode = orderCode;
    this.priority = priority;
    this.prices = prices;
    this.quantities = quantities;
  }

  public void write(final ByteBuffer byteBuffer) {
    byteBuffer.putLong(sourceId);
    byteBuffer.put((byte) (special ? 1 : 0));
    byteBuffer.putInt(orderCode);
    byteBuffer.putInt(priority);

    byteBuffer.putInt(prices.length);
    for (final double price : prices) {
      byteBuffer.putDouble(price);
    }

    byteBuffer.putInt(quantities.length);
    for (final long quantity : quantities) {
      byteBuffer.putLong(quantity);
    }
  }

  public void writeImproved(final ByteBuffer byteBuffer) {
    byteBuffer.putLong(sourceId);
    byteBuffer.put((byte) (special ? 1 : 0));
    byteBuffer.putInt(orderCode);
    byteBuffer.putInt(priority);

    byteBuffer.putInt(prices.length);
    byteBuffer.asDoubleBuffer().put(prices);
    byteBuffer.position(byteBuffer.position() + prices.length * 8);

    byteBuffer.putInt(quantities.length);
    byteBuffer.asLongBuffer().put(quantities);
    byteBuffer.position(byteBuffer.position() + quantities.length * 8);
  }

  public static ObjectToBeSerialised read(final ByteBuffer byteBuffer) {
    final long sourceId = byteBuffer.getLong();
    final boolean special = 0 != byteBuffer.get();
    final int orderCode = byteBuffer.getInt();
    final int priority = byteBuffer.getInt();

    final int pricesSize = byteBuffer.getInt();
    final double[] prices = new double[pricesSize];
    for (int i = 0; i < pricesSize; i++) {
      prices[i] = byteBuffer.getDouble();
    }

    final int quantitiesSize = byteBuffer.getInt();
    final long[] quantities = new long[quantitiesSize];
    for (int i = 0; i < quantitiesSize; i++) {
      quantities[i] = byteBuffer.getLong();
    }

    return new ObjectToBeSerialised(sourceId, special, orderCode, priority,
                                    prices, quantities);
  }

  public static ObjectToBeSerialised readImproved(final ByteBuffer byteBuffer) {
    final long sourceId = byteBuffer.getLong();
    final boolean special = 0 != byteBuffer.get();
    final int orderCode = byteBuffer.getInt();
    final int priority = byteBuffer.getInt();

    final int pricesSize = byteBuffer.getInt();
    final double[] prices = new double[pricesSize];
    byteBuffer.asDoubleBuffer().get(prices);
    byteBuffer.position(byteBuffer.position() + pricesSize * 8);

    final int quantitiesSize = byteBuffer.getInt();
    final long[] quantities = new long[quantitiesSize];
    byteBuffer.asLongBuffer().get(quantities);
    byteBuffer.position(byteBuffer.position() + quantitiesSize * 8);

    return new ObjectToBeSerialised(sourceId, special, orderCode, priority,
                                    prices, quantities);
  }

  public void write(final UnsafeMemory buffer) {
    buffer.putLong(sourceId);
    buffer.putBoolean(special);
    buffer.putInt(orderCode);
    buffer.putInt(priority);
    buffer.putDoubleArray(prices);
    buffer.putLongArray(quantities);
  }

  public static ObjectToBeSerialised read(final UnsafeMemory buffer) {
    final long sourceId = buffer.getLong();
    final boolean special = buffer.getBoolean();
    final int orderCode = buffer.getInt();
    final int priority = buffer.getInt();
    final double[] prices = buffer.getDoubleArray();
    final long[] quantities = buffer.getLongArray();

    return new ObjectToBeSerialised(sourceId, special, orderCode, priority,
                                    prices, quantities);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ObjectToBeSerialised that = (ObjectToBeSerialised) o;

    if (orderCode != that.orderCode) {
      return false;
    }
    if (priority != that.priority) {
      return false;
    }
    if (sourceId != that.sourceId) {
      return false;
    }
    if (special != that.special) {
      return false;
    }
    if (!Arrays.equals(prices, that.prices)) {
      return false;
    }
    if (!Arrays.equals(quantities, that.quantities)) {
      return false;
    }

    return true;
  }
}

class UnsafeMemory {
  private static final Unsafe unsafe;
  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe) field.get(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final long byteArrayOffset = unsafe
      .arrayBaseOffset(byte[].class);
  private static final long longArrayOffset = unsafe
      .arrayBaseOffset(long[].class);
  private static final long doubleArrayOffset = unsafe
      .arrayBaseOffset(double[].class);

  private static final int SIZE_OF_BOOLEAN = 1;
  private static final int SIZE_OF_INT = 4;
  private static final int SIZE_OF_LONG = 8;

  private int pos = 0;
  private final byte[] buffer;

  public UnsafeMemory(final byte[] buffer) {
    if (null == buffer) {
      throw new NullPointerException("buffer cannot be null");
    }

    this.buffer = buffer;
  }

  public void reset() {
    this.pos = 0;
  }

  public void putBoolean(final boolean value) {
    unsafe.putBoolean(buffer, byteArrayOffset + pos, value);
    pos += SIZE_OF_BOOLEAN;
  }

  public boolean getBoolean() {
    boolean value = unsafe.getBoolean(buffer, byteArrayOffset + pos);
    pos += SIZE_OF_BOOLEAN;

    return value;
  }

  public void putInt(final int value) {
    unsafe.putInt(buffer, byteArrayOffset + pos, value);
    pos += SIZE_OF_INT;
  }

  public int getInt() {
    int value = unsafe.getInt(buffer, byteArrayOffset + pos);
    pos += SIZE_OF_INT;

    return value;
  }

  public void putLong(final long value) {
    unsafe.putLong(buffer, byteArrayOffset + pos, value);
    pos += SIZE_OF_LONG;
  }

  public long getLong() {
    long value = unsafe.getLong(buffer, byteArrayOffset + pos);
    pos += SIZE_OF_LONG;

    return value;
  }

  public void putLongArray(final long[] values) {
    putInt(values.length);

    long bytesToCopy = values.length << 3;
    unsafe.copyMemory(values, longArrayOffset, buffer, byteArrayOffset
        + pos, bytesToCopy);
    pos += bytesToCopy;
  }

  public long[] getLongArray() {
    int arraySize = getInt();
    long[] values = new long[arraySize];

    long bytesToCopy = values.length << 3;
    unsafe.copyMemory(buffer, byteArrayOffset + pos, values,
                      longArrayOffset, bytesToCopy);
    pos += bytesToCopy;

    return values;
  }

  public void putDoubleArray(final double[] values) {
    putInt(values.length);

    long bytesToCopy = values.length << 3;
    unsafe.copyMemory(values, doubleArrayOffset, buffer, byteArrayOffset
        + pos, bytesToCopy);
    pos += bytesToCopy;
  }

  public double[] getDoubleArray() {
    int arraySize = getInt();
    double[] values = new double[arraySize];

    long bytesToCopy = values.length << 3;
    unsafe.copyMemory(buffer, byteArrayOffset + pos, values,
                      doubleArrayOffset, bytesToCopy);
    pos += bytesToCopy;

    return values;
  }
}
