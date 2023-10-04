package io.sirix.node;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

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
  public static void putVarLong(final BytesOut<?> output, long value) {
    output.writeStopBit(value);
  }

  /**
   * Get a "compressed" variable-length long value.
   *
   * @param input {@link ByteArrayDataInput} reference
   * @return long value
   */
  public static long getVarLong(final BytesIn<?> input)  {
    return input.readStopBit();
  }
}
