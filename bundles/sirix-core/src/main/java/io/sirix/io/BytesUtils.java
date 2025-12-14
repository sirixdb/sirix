package io.sirix.io;

import io.sirix.node.BytesOut;

import java.nio.ByteBuffer;

public final class BytesUtils {
  private BytesUtils() {
    throw new AssertionError();
  }

  public static void doWrite(BytesOut<?> bytes, ByteBuffer toWrite) {
    // Write ByteBuffer content to BytesOut
    byte[] buffer = new byte[toWrite.remaining()];
    toWrite.get(buffer);
    bytes.write(buffer);
  }

  public static void doWrite(BytesOut<?> bytes, byte[] toWrite) {
    bytes.write(toWrite);
  }

  public static ByteBuffer doRead(BytesOut<?> bytes) {
    // Get underlying destination from BytesOut
    Object destination = bytes.getDestination();
    if (destination instanceof ByteBuffer) {
      return (ByteBuffer) destination;
    }
    // Fallback for other destination types
    throw new UnsupportedOperationException("Unsupported destination type: " + destination.getClass());
  }

  /**
   * Convert the byte[] into a String to be used for logging and debugging.
   *
   * @param bytes the byte[] to be dumped
   * @return the String representation
   */
  public static String dumpBytes(byte[] bytes) {
    StringBuilder buffer = new StringBuilder("byte[");
    buffer.append(bytes.length);
    buffer.append("]: [");
    for (byte aByte : bytes) {
      buffer.append(aByte);
      buffer.append(" ");
    }
    buffer.append("]");
    return buffer.toString();
  }

  /**
   * Convert the byteBuffer into a String to be used for logging and debugging.
   *
   * @param byteBuffer the byteBuffer to be dumped
   * @return the String representation
   */
  public static String dumpBytes(ByteBuffer byteBuffer) {
    byteBuffer.mark();
    int length = byteBuffer.limit() - byteBuffer.position();
    byte[] dst = new byte[length];
    byteBuffer.get(dst);
    byteBuffer.reset();
    return dumpBytes(dst);
  }
}
