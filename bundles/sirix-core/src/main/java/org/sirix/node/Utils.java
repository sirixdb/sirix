package org.sirix.node;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

/**
 * Copied from Kafka ByteUtils class.
 */
public final class Utils {

  private Utils() {
  }

  /**
   * Store a "compressed" variable-length long value.
   * 
   * @param output {@link ByteArrayDataOutput} reference
   * @param value long value
   */
  public static void putVarLong(final Bytes<ByteBuffer> output, long value) {
    output.writeStopBit(value);
  }

  /**
   * Get a "compressed" variable-length long value.
   * 
   * @param input {@link ByteArrayDataInput} reference
   * @return long value
   */
  public static long getVarLong(final Bytes<ByteBuffer> input)  {
    return input.readStopBit();
  }
}
