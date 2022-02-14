package org.sirix.node;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class Utils {

  /**
   * Store a "compressed" variable-length long value.
   * 
   * @param output {@link ByteArrayDataOutput} reference
   * @param value long value
   */
  public static final void putVarLong(final DataOutput output, long value) throws IOException {
    while ((value & ~0x7F) != 0) {
      output.write(((byte) ((value & 0x7f) | 0x80)));
      value >>>= 7;
    }
    output.write((byte) value);
  }

  /**
   * Get a "compressed" variable-length long value.
   * 
   * @param input {@link ByteArrayDataInput} reference
   * @return long value
   */
  public static final long getVarLong(final DataInput input) throws IOException {
    byte singleByte = input.readByte();
    long value = singleByte & 0x7F;
    for (int shift = 7; (singleByte & 0x80) != 0; shift += 7) {
      singleByte = input.readByte();
      value |= (singleByte & 0x7FL) << shift;
    }
    return value;
  }
}
