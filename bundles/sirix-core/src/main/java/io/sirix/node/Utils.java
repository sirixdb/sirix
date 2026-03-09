package io.sirix.node;

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;

/**
 * Copied from Kafka ByteUtils class.
 */
public final class Utils {

  private Utils() {}

  /**
   * Store a "compressed" variable-length long value.
   *
   * @param output {@link BytesOut} reference
   * @param value long value
   */
  public static void putVarLong(final BytesOut<?> output, long value) {
    output.writeStopBit(value);
  }

  /**
   * Get a "compressed" variable-length long value.
   *
   * @param input {@link BytesIn} reference
   * @return long value
   */
  public static long getVarLong(final BytesIn<?> input) {
    return input.readStopBit();
  }
}
