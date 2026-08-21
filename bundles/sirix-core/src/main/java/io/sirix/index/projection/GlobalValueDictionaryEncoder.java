package io.sirix.index.projection;

interface GlobalValueDictionaryEncoder {
  int intern(byte[] source, int offset, int length);

  int intern(String value);

  /**
   * Conservative UTF-8 length without allocating the encoded array.  Malformed surrogate code
   * units count as three bytes although the JDK encoder may substitute one byte; over-counting can
   * only decline a malformed value early, never permit an oversized materialisation.
   */
  static int utf8LengthCapped(final String value, final int limit) {
    int length = 0;
    for (int index = 0; index < value.length(); index++) {
      final char character = value.charAt(index);
      if (character < 0x80) {
        length++;
      } else if (character < 0x800) {
        length += 2;
      } else if (Character.isHighSurrogate(character) && index + 1 < value.length()
          && Character.isLowSurrogate(value.charAt(index + 1))) {
        length += 4;
        index++;
      } else {
        length += 3;
      }
      if (length > limit) {
        return limit + 1;
      }
    }
    return length;
  }
}
