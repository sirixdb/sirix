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
}
