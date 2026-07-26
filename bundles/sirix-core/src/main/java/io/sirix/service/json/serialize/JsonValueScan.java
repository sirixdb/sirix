package io.sirix.service.json.serialize;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * Escape pre-scan over a string value's raw UTF-8 bytes for the BYTE output pipeline.
 *
 * <p>{@code StringValue.escape} escapes exactly: the table entries ({@code " \ / \b \f \n \r \t}),
 * all C0 controls (&lt; 0x20), U+007F–U+009F, and U+2000–U+20FF. In UTF-8 those are byte-visible
 * as: bytes &lt; 0x20, {@code 0x22 0x2F 0x5C 0x7F}, lead byte {@code 0xC2} (U+0080–U+00BF ⊇
 * U+0080–U+009F) and lead byte {@code 0xE2} (U+2000–U+2FFF ⊇ U+2000–U+20FF). A value containing
 * NONE of those bytes is guaranteed escape-free and can be bulk-copied to the byte sink with no
 * String construction and no re-encoding. (The {@code 0xC2}/{@code 0xE2} flags over-approximate;
 * flagged values just take the exact escape path.)
 *
 * <p>Vector lanes engage only when at least one full species width fits — short values use the
 * scalar flag scan (measured: vector setup does not pay below one lane).
 */
final class JsonValueScan {

  private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;

  private JsonValueScan() {
  }

  /** {@code true} if the UTF-8 bytes MAY contain a JSON-escapable character. */
  static boolean mayNeedJsonEscape(final byte[] utf8) {
    final int len = utf8.length;
    int i = 0;
    final int upper = len >= SPECIES.length() ? SPECIES.loopBound(len) : 0;
    for (; i < upper; i += SPECIES.length()) {
      final ByteVector v = ByteVector.fromArray(SPECIES, utf8, i);
      final VectorMask<Byte> flagged = v.compare(VectorOperators.ULT, (byte) 0x20)
                                        .or(v.eq((byte) '"'))
                                        .or(v.eq((byte) '\\'))
                                        .or(v.eq((byte) '/'))
                                        .or(v.eq((byte) 0x7F))
                                        .or(v.eq((byte) 0xC2))
                                        .or(v.eq((byte) 0xE2));
      if (flagged.anyTrue()) {
        return true;
      }
    }
    for (; i < len; i++) {
      final int b = utf8[i] & 0xFF;
      if (b < 0x20 || b == '"' || b == '\\' || b == '/' || b == 0x7F || b == 0xC2 || b == 0xE2) {
        return true;
      }
    }
    return false;
  }
}
