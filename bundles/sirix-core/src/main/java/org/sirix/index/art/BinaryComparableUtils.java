package org.sirix.index.art;

/**
 * Contains utilities to help with {@link BinaryComparable} key transformation.
 */
class BinaryComparableUtils {
  private BinaryComparableUtils() {
    // Effective Java Item 4
    throw new AssertionError();
  }

  // 2^7 = 128
  private static final int BYTE_SHIFT = 1 << Byte.SIZE - 1;

  /**
   * For signed types to be interpreted as unsigned
   */
  static byte[] unsigned(byte[] key) {
    key[0] = unsigned(key[0]);
    return key;
  }

  /**
   * For Node4, Node16 to interpret every byte as unsigned when storing partial keys.
   * Node 48, Node256 simply use {@link Byte#toUnsignedInt(byte)}
   * to index into their key arrays.
   */
  static byte unsigned(byte b) {
    return (byte) (b ^ BYTE_SHIFT);
  }

  // passed b must have been interpreted as unsigned already
  // this is the reverse of unsigned
  static byte signed(byte b) {
    return unsigned(b);
  }
}