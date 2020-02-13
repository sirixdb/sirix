package org.sirix.utils;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Swiss army knife for encoding/decoding of basic data types.
 * 
 * @author Sebastian Baechle
 * 
 */
public final class Calc {

  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final int NONENULL = Integer.MAX_VALUE;
  public static byte[] fromBigDecimal(BigDecimal i) {
    BigInteger bi = i.unscaledValue();
    byte[] tmp = bi.toByteArray();
    int scale = i.scale();
    if ((scale <= 0x3F) && (scale > -0x3F)) {
      // 6 bits are sufficient to encode
      // scale unsigned
      // encode sign
      byte[] b = new byte[1 + tmp.length];
      b[0] = (byte) ((scale >= 0) ? 0x0 : 0x40);
      b[0] |= (byte) (scale & 0x3F);
      System.arraycopy(tmp, 0, b, 1, tmp.length);
      return b;
    }
    byte[] b = new byte[5 + tmp.length];
    b[0] = (byte) 0x80; // set bit 0
    fromInt(scale, b, 1);
    System.arraycopy(tmp, 0, b, 5, tmp.length);
    return b;
  }

  public static BigDecimal toBigDecimal(byte[] b) {
    if ((b[0] & 0x80) == 0) {
      // bit 0 not set: 6 bits are sufficient
      // to encode scale unsigned
      // bit 1 encodes sign
      int sign = ((b[0] & 0x40) == 0) ? 1 : -1;
      // bits 2-7 encode unsigned scale
      int scale = sign * (b[0] & 0x3F);
      BigInteger bi = toBigInteger(b, 1, b.length - 1);
      return new BigDecimal(bi, scale);
    }
    int scale = toInt(b, 1);
    BigInteger bi = toBigInteger(b, 5, b.length - 5);
    return new BigDecimal(bi, scale);
  }

  public static BigDecimal toBigDecimal(byte[] b, int off, int len) {
    if ((b[off] & 0x80) == 0) {
      // bit 0 not set: 6 bits are sufficient to encode scale
      // bit 1 encodes sign
      int sign = ((b[off] & 0x40) == 0) ? 1 : -1;
      // bits 2-7 encode unsigned scale
      int scale = sign * (b[off] & 0x3F);
      BigInteger bi = toBigInteger(b, off + 1, len - 1);
      return new BigDecimal(bi, scale);
    }
    int scale = toInt(b, 1);
    BigInteger bi = toBigInteger(b, off + 5, len - 5);
    return new BigDecimal(bi, scale);
  }

  public static BigInteger toBigInteger(byte[] b) {
    return new BigInteger(b);
  }

  public static BigInteger toBigInteger(byte[] b, int off, int len) {
    return new BigInteger(Arrays.copyOfRange(b, off, off + len));
  }

  public static byte[] fromBigInteger(BigInteger i) {
    return i.toByteArray();
  }

  public static void fromBigInteger(BigInteger i, byte[] b, int off) {
    byte[] tmp = i.toByteArray();
    System.arraycopy(tmp, 0, b, off, tmp.length);
  }

  public static String toString(byte[] s) {
    return new String(s, UTF8);
  }

  public static String toString(byte[] s, int off, int len) {
    return new String(s, off, len, UTF8);
  }

  public static byte[] fromString(String s) {
    return s.getBytes(UTF8);
  }

  public static void fromString(String s, byte[] b, int off) {
    byte[] tmp = s.getBytes(UTF8);
    System.arraycopy(tmp, 0, b, off, tmp.length);
  }

  public static int toUIntVar(byte[] b) {
    int len = b.length;
    if (len == 1) {
      return b[0] & 0xFF;
    }
    if (len == 2) {
      return ((b[0] & 0xFF) << 8) | b[1] & 0xFF;
    }
    if (len == 3) {
      return ((b[0] & 0xFF) << 16) | ((b[1] & 0xFF) << 8) | b[2] & 0xFF;
    }
    return ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | b[3] & 0xFF;
  }

  public static int toInt(byte[] b, int off, int len) {
    if (len == 1) {
      return b[off] & 0xFF;
    }
    if (len == 2) {
      return ((b[off++] & 0xFF) << 8) | b[off] & 0xFF;
    }
    if (len == 3) {
      return ((b[off++] & 0xFF) << 16) | ((b[off++] & 0xFF) << 8) | b[off++] & 0xFF;
    }
    return ((b[off++] & 0xFF) << 24) | ((b[off++] & 0xFF) << 16) | ((b[off++] & 0xFF) << 8)
        | b[off] & 0xFF;
  }

  public static byte[] fromUIntVar(int i) {
    if ((i & 0xFFFFFF00) == 0) {
      return new byte[] {(byte) i};
    }
    if ((i & 0xFFFF0000) == 0) {
      return new byte[] {(byte) ((i >> 8) & 0xFF), (byte) i};
    }
    if ((i & 0xFF000000) == 0) {
      return new byte[] {(byte) ((i >> 16) & 0xFF), (byte) ((i >> 8) & 0xFF), (byte) i};
    }
    if (i < 0) {
      throw new IllegalArgumentException(String.valueOf(i));
    }
    return new byte[] {(byte) ((i >> 24) & 0xFF), (byte) ((i >> 16) & 0xFF),
        (byte) ((i >> 8) & 0xFF), (byte) i};
  }

  public static int toInt(byte[] b) {
    return ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | b[3] & 0xFF;
  }

  public static int toInt(byte[] b, int off) {
    return ((b[off++] & 0xFF) << 24) | ((b[off++] & 0xFF) << 16) | ((b[off++] & 0xFF) << 8)
        | (b[off++] & 0xFF);
  }

  public static byte[] fromInt(int i) {
    return new byte[] {(byte) ((i >> 24) & 0xFF), (byte) ((i >> 16) & 0xFF),
        (byte) ((i >> 8) & 0xFF), (byte) i};
  }

  public static void fromInt(int i, byte[] b, int off) {
    b[off++] = (byte) ((i >> 24) & 0xFF);
    b[off++] = (byte) ((i >> 16) & 0xFF);
    b[off++] = (byte) ((i >> 8) & 0xFF);
    b[off] = (byte) i;
  }

  public static long toLong(byte[] b) {
    return ((((long) b[0] & 0xFF) << 56) | (((long) b[1] & 0xFF) << 48)
        | (((long) b[2] & 0xFF) << 40) | (((long) b[3] & 0xFF) << 32) | (((long) b[4] & 0xFF) << 24)
        | (((long) b[5] & 0xFF) << 16) | (((long) b[6] & 0xFF) << 8) | ((long) b[7] & 0xFF));
  }

  public static long toLong(byte[] b, int off) {
    return ((((long) b[off++] & 0xFF) << 56) | (((long) b[off++] & 0xFF) << 48)
        | (((long) b[off++] & 0xFF) << 40) | (((long) b[off++] & 0xFF) << 32)
        | (((long) b[off++] & 0xFF) << 24) | (((long) b[off++] & 0xFF) << 16)
        | (((long) b[off++] & 0xFF) << 8) | ((long) b[off++] & 0xFF));
  }

  public static byte[] fromLong(long i) {
    return new byte[] {(byte) ((i >> 56) & 0xFF), (byte) ((i >> 48) & 0xFF),
        (byte) ((i >> 40) & 0xFF), (byte) ((i >> 32) & 0xFF), (byte) ((i >> 24) & 0xFF),
        (byte) ((i >> 16) & 0xFF), (byte) ((i >> 8) & 0xFF), (byte) i};
  }

  public static void fromLong(long i, byte[] b, int off) {
    b[off++] = (byte) ((i >> 56) & 0xFF);
    b[off++] = (byte) ((i >> 48) & 0xFF);
    b[off++] = (byte) ((i >> 40) & 0xFF);
    b[off++] = (byte) ((i >> 32) & 0xFF);
    b[off++] = (byte) ((i >> 24) & 0xFF);
    b[off++] = (byte) ((i >> 16) & 0xFF);
    b[off++] = (byte) ((i >> 8) & 0xFF);
    b[off++] = (byte) (i);
  }

  public static double toDouble(byte[] b) {
    return Double.longBitsToDouble(toLong(b));
  }

  public static double toDouble(byte[] b, int off) {
    return Double.longBitsToDouble(toLong(b, off));
  }

  public static byte[] fromDouble(double i) {
    return fromLong(Double.doubleToRawLongBits(i));
  }

  public static void fromDouble(double i, byte[] b, int off) {
    fromLong(Double.doubleToRawLongBits(i), b, off);
  }

  public static float toFloat(byte[] b) {
    return Float.intBitsToFloat(toInt(b));
  }

  public static float toFloat(byte[] b, int off) {
    return Float.intBitsToFloat(toInt(b, off));
  }

  public static byte[] fromFloat(float i) {
    return fromLong(Float.floatToRawIntBits(i));
  }

  public static void fromDouble(float i, byte[] b, int off) {
    fromInt(Float.floatToRawIntBits(i), b, off);
  }

  public static int compare(byte[] v1, byte[] v2) {
    // a null value is interpreted as EOF (= highest possible value)
    if(comparsionOfNull(v1, v2) != NONENULL)
      return comparsionOfNull(v1, v2);

    int len1 = v1.length;
    int len2 = v2.length;
    int len = Math.min(len1, len2);
    int pos = -1;
    while (++pos < len) {
      byte b1 = v1[pos];
      byte b2 = v2[pos];
      if (b1 != b2) {
        return b1 - b2;
      }
    }
    return len1 - len2;

  }

  public static int compare(byte[] v1, int off1, int len1, byte[] v2, int off2, int len2) {
    // a null value is interpreted as EOF (= highest possible value)
    if(comparsionOfNull(v1, v2) != NONENULL)
      return comparsionOfNull(v1, v2);
    int len = Math.min(len1, len2);
    int pos = -1;
    while (++pos < len) {
      byte b1 = v1[off1 + pos];
      byte b2 = v2[off2 + pos];
      if (b1 != b2) {
        return b1 - b2;
      }
    }
    return len1 - len2;

  }

  /**
   * Nulls in this project are viewed as EOF, thus it is often of interest when comparing two values which one is
   * greatest to check if one is null or possible both and return which.
   * @param v1 the first value to be tested.
   * @param v2 the second value to be tested.
   * @return -1, if v2 is null but not v1, 1 for vice versa, 0 for both being null, and int max for when neither is null.
   */
  private final static int comparsionOfNull(byte[] v1, byte[] v2){
    if(v1 == null && v2 == null)
      return 0;
    else if(v1 != null && v2 == null)
      return -1;
    else if(v1 == null && v2 != null)
      return 1;
    else
      return NONENULL;
  }

  public final static int compareAsPrefix(byte[] v1, byte[] v2) {
    // a null value is interpreted as EOF (= highest possible value)
    if(comparsionOfNull(v1, v2) != NONENULL)
      return comparsionOfNull(v1, v2);

    int len1 = v1.length;
    int len2 = v2.length;
    int len = Math.min(len1, len2);
    int pos = -1;
    while (++pos < len) {
      if (v1[pos] != v2[pos]) {
        return v1[pos] - v2[pos];
      }
    }
    return (len1 <= len2) ? 0 : 1;
  }


  public static int compareU(byte[] v1, byte[] v2) {
    // a null value is interpreted as EOF (= highest possible value)
    return compareU(v1, 0, v1.length, v2, 0, v2.length);
  }

  public static int compareU(byte[] v1, int off1, int len1, byte[] v2, int off2, int len2) {
    // a null value is interpreted as EOF (= highest possible value)
    if(comparsionOfNull(v1, v2) != NONENULL)
      return comparsionOfNull(v1, v2);
    int len = Math.min(len1, len2);
    int pos = -1;
    while (++pos < len) {
      int b1 = v1[off1 + pos] & 0xFF;
      int b2 = v2[off2 + pos] & 0xFF;
      if (b1 != b2) {
        return b1 - b2;
      }
    }
    return len1 - len2;
  }

  public final static int compareUAsPrefix(byte[] v1, byte[] v2) {
    // a null value is interpreted as EOF (= highest possible value)
    if(comparsionOfNull(v1, v2) != Integer.MAX_VALUE)
      return comparsionOfNull(v1, v2);
    int len1 = v1.length;
    int len2 = v2.length;
    int len = Math.min(len1, len2);
    int pos = -1;
    while (++pos < len) {
      int b1 = v1[pos] & 0xFF;
      int b2 = v2[pos] & 0xFF;
      if (b1 != b2) {
        return b1 - b2;
      }
    }
    return (len1 <= len2) ? 0 : 1;
  }

  public static int compareUIntVar(byte[] v1, byte[] v2) {
    // a null value is interpreted as EOF (= highest possible value)
    if(comparsionOfNull(v1, v2) != NONENULL)
      return comparsionOfNull(v1, v2);
    int i1 = toUIntVar(v1);
    int i2 = toUIntVar(v2);
    return (i1 < i2) ? -1 : (i1 == i2) ? 0 : 1;
  }

  public static int compareInt(byte[] v1, byte[] v2) {
    // a null value is interpreted as EOF (= highest possible value)
    return compareInt(v1, 0, v2, 0);
  }

  public static int compareInt(byte[] v1, int off1, byte[] v2, int off2) {
    // a null value is interpreted as EOF (= highest possible value)
    if(comparsionOfNull(v1, v2) != NONENULL)
      return comparsionOfNull(v1, v2);
    int i1 = toInt(v1, off1);
    int i2 = toInt(v2, off2);
    return (i1 < i2) ? -1 : (i1 == i2) ? 0 : 1;
  }

  public static int compareLong(byte[] v1, byte[] v2) {
    // a null value is interpreted as EOF (= highest possible value)
    return compareLong(v1, 0, v2, 0);
  }

  public static int compareLong(byte[] v1, int off1, byte[] v2, int off2) {
    // a null value is interpreted as EOF (= highest possible value)
    if(comparsionOfNull(v1, v2) != NONENULL)
      return comparsionOfNull(v1, v2);
    long i1 = toLong(v1, off1);
    long i2 = toLong(v2, off2);
    return (i1 < i2) ? -1 : (i1 == i2) ? 0 : 1;
  }

  public static int compareDouble(byte[] v1, byte[] v2) {
    // a null value is interpreted as EOF (= highest possible value)
    if(comparsionOfNull(v1, v2) != NONENULL)
      return comparsionOfNull(v1, v2);
    double d1 = toDouble(v1);
    double d2 = toDouble(v2);
    return Double.compare(d1, d2);
  }

  public static int compareFloat(byte[] v1, byte[] v2) {
    // a null value is interpreted as EOF (= highest possible value)
    if(comparsionOfNull(v1, v2) != NONENULL)
      return comparsionOfNull(v1, v2);
    float f1 = toFloat(v1);
    float f2 = toFloat(v2);
    return Float.compare(f1, f2);
  }

  public static int compareBigInteger(byte[] v1, byte[] v2) {
    // a null value is interpreted as EOF (= highest possible value)
   if(comparsionOfNull(v1, v2) != NONENULL)
     return comparsionOfNull(v1, v2);
    BigInteger i1 = toBigInteger(v1);
    BigInteger i2 = toBigInteger(v2);
    return i1.compareTo(i2);
  }

  public static int compareBigDecimal(byte[] v1, byte[] v2) {
    // a null value is interpreted as EOF (= highest possible value)
    if (comparsionOfNull(v1, v2) != NONENULL)
      return comparsionOfNull(v1, v2);
    BigDecimal i1 = toBigDecimal(v1);
    BigDecimal i2 = toBigDecimal(v2);
    return i1.compareTo(i2);
  }
}
