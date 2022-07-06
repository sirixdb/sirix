package org.sirix.node;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

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
  public static void putVarLong(final DataOutput output, long value) throws IOException {
    long v = (value << 1) ^ (value >> 63);
    while ((v & 0xffffffffffffff80L) != 0L) {
      output.writeByte(((int) v & 0x7f) | 0x80);
      v >>>= 7;
    }
    output.writeByte((byte) v);
  }

  /**
   * Get a "compressed" variable-length long value.
   * 
   * @param input {@link ByteArrayDataInput} reference
   * @return long value
   */
  public static long getVarLong(final DataInput input) throws IOException {
    long value = 0L;
    int i = 0;
    long b;
    while (((b = input.readByte()) & 0x80) != 0) {
      value |= (b & 0x7f) << i;
      i += 7;
      if (i > 63)
        throw illegalVarlongException(value);
    }
    value |= b << i;
    return (value >>> 1) ^ -(value & 1);
  }

  private static IllegalArgumentException illegalVarlongException(long value) {
    throw new IllegalArgumentException("Varlong is too long, most significant bit in the 10th byte is set, " +
                                           "converted value: " + Long.toHexString(value));
  }
}
