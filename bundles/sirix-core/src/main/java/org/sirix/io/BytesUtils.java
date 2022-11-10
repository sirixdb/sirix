package org.sirix.io;

import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

public final class BytesUtils {
  private BytesUtils() {
    throw new AssertionError();
  }

  public static void doWrite(Bytes<?> bytes, ByteBuffer toWrite) {
    // No garbage when writing to Bytes from ByteBuffer.
    bytes.clear();
    bytes.write(bytes.writePosition(), toWrite, toWrite.position(), toWrite.limit());
  }

  public static void doWrite(Bytes<?> bytes, byte[] toWrite) {
    bytes.clear();
    bytes.write(toWrite);
  }

  public static ByteBuffer doRead(Bytes bytes) {
    // No garbage when getting the underlying ByteBuffer.
    assert bytes.underlyingObject() instanceof ByteBuffer;
    final var byteBuffer = (ByteBuffer) bytes.underlyingObject();
    return byteBuffer;
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
