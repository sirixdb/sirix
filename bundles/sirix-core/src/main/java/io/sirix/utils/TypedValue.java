/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.utils;

import io.sirix.exception.SirixRuntimeException;
import io.sirix.settings.Constants;

/**
 * 
 * <p>
 * Util to efficiently convert byte arrays to various Java types and vice versa. It also provides
 * efficient comparison and hash methods.
 * </p>
 */
public final class TypedValue {

  /** Empty string. */
  public static final byte[] EMPTY = new byte[0];

  /**
   * Hidden constructor.
   */
  private TypedValue() {
    // Hidden.
  }

  /**
   * Parse int from given UTF-8 byte array.
   * 
   * @param mBytes Byte array to parse int from.
   * @return Int.
   */
  public static int parseInt(final byte[] mBytes) {
    try {
      int value = (((mBytes[0] & 0xFF) << 24)
                  | ((mBytes[1] & 0xFF) << 16)
                  | ((mBytes[2] & 0xFF)  << 8)
                  | (mBytes[3] & 0xFF));
      return value;
    } catch(final Exception e) {
      throw new SirixRuntimeException(e.getLocalizedMessage());
    }
  }

  /**
   * Parse long from given UTF-8 byte array.
   * 
   * @param mBytes Byte array to parse long from.
   * @return Long.
   */
  public static long parseLong(final byte[] mBytes) {
    try {
      long value = ((((long) mBytes[0] & 0xFF) << 56)
                    | (((long) mBytes[1] & 0xFF) << 48)
                    | (((long) mBytes[2] & 0xFF) << 40)
                    | (((long) mBytes[3] & 0xFF) << 32)
                    | (((long) mBytes[4] & 0xFF) << 24)
                    | (((long) mBytes[5] & 0xFF) << 16)
                    | (((long) mBytes[6] & 0xFF) << 8)
                    | (((long) mBytes[7] & 0xFF)));
      return value;
    } catch (final Exception e) {
      throw new SirixRuntimeException(e.getLocalizedMessage());
    }
  }

  /**
   * Get UTF-8 byte array from boolean. The given byte array yields a string representation if read with
   * parseString().
   * 
   * @param mValue Boolean to encode as UTF-8 byte array.
   * @return UTF-8-encoded byte array of boolean.
   */
  public static byte[] getBytes(final boolean mValue) {
    final byte[] bytes = new byte[1];
    if (mValue) {
      bytes[0] = 1;
    } else {
      bytes[0] = 0;
    }
    return bytes;
  }

  /**
   * Get UTF-8 byte array from int. The given byte array yields a string representation if read with
   * parseString().
   * 
   * @param mValue Int to encode as UTF-8 byte array.
   * @return UTF-8-encoded byte array of int.
   */
  public static byte[] getBytes(final int mValue) {
    final byte[] bytes = new byte[4];
    try {
      bytes[0] = (byte) (mValue >> 24);
      bytes[1] = (byte) (mValue >> 16);
      bytes[2] = (byte) (mValue >> 8);
      bytes[3] = (byte) mValue;
    } catch (final Exception e) {
      throw new SirixRuntimeException(e.getLocalizedMessage());
    }
    return bytes;
  }

  /**
   * Get UTF-8 byte array from long. The given byte array yields a string representation if read
   * with parseString().
   * 
   * @param value Long to encode as UTF-8 byte array.
   * @return UTF-8-encoded byte array of long.
   */
  public static byte[] getBytes(final long value) {
    final byte[] bytes = new byte[8];
    try {
      bytes[0] = (byte) (value >> 56);
      bytes[1] = (byte) (value >> 48);
      bytes[2] = (byte) (value >> 40);
      bytes[3] = (byte) (value >> 32);
      bytes[4] = (byte) (value >> 24);
      bytes[5] = (byte) (value >> 16);
      bytes[6] = (byte) (value >> 8);
      bytes[7] = (byte) value;
    } catch (final Exception e) {
      throw new SirixRuntimeException(e.getLocalizedMessage());
    }
    return bytes;
  }

  /**
   * Get UTF-8 byte array from string. The given byte array yields a int if read with parseInt().
   * 
   * @param mValue String to encode as UTF-8 byte array.
   * @return UTF-8-encoded byte array of string.
   */
  public static byte[] getBytes(final String mValue) {
    byte[] bytes = null;
    try {
      if (mValue == null || mValue.length() == 0) {
        bytes = EMPTY;
      } else {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < mValue.length(); i++) {
          switch (mValue.charAt(i)) {
            case '&':
              builder.append("&amp;");
              break;
            case '<':
              builder.append("&lt;");
              break;
            default:
              builder.append(mValue.charAt(i));
          }
        }
        bytes = builder.toString().getBytes(Constants.DEFAULT_ENCODING);
      }
      return bytes;
    } catch (final Exception e) {
      throw new SirixRuntimeException("Could not convert String to byte[]: " + e.getLocalizedMessage());
    }
  }

  public static boolean equals(final byte[] mValue1, final byte[] mValue2) {
    // Fail if one is null.
    if ((mValue1 == null) || (mValue2 == null)) {
      return false;
    }
    // Fail if the values are not of equal length.
    if (mValue1.length != mValue2.length) {
      return false;
    }
    // Fail if a single byte does not match.
    for (int i = 0, l = mValue1.length; i < l; i++) {
      if (mValue1[i] != mValue2[i]) {
        return false;
      }
    }
    // Values must be equal if we reach here.
    return true;
  }

  public static boolean equals(final byte[] mValue1, final String mValue2) {
    return equals(mValue1, TypedValue.getBytes(mValue2));
  }

  public static boolean equals(final String mValue1, final byte[] mValue2) {
    return equals(TypedValue.getBytes(mValue1), mValue2);
  }

  public static boolean equals(final String mValue1, final String mValue2) {
    return equals(TypedValue.getBytes(mValue1), TypedValue.getBytes(mValue2));
  }

  /**
   * Get UTF-8 byte array from double. The given byte array yields a double if read with
   * parseDouble().
   * 
   * @param mValue double value to encode as UTF-8 byte array.
   * @return UTF-8-encoded byte array of double.
   */
  public static byte[] getBytes(final Double mValue) {
    return mValue.toString().getBytes();
  }

  /**
   * Get UTF-8 byte array from float. The given byte array yields a float if read with parseFloat().
   * 
   * @param mValue float to encode as UTF-8 byte array.
   * @return UTF-8-encoded byte array of float.
   */
  public static byte[] getBytes(final Float mValue) {
    return mValue.toString().getBytes();
  }

}
