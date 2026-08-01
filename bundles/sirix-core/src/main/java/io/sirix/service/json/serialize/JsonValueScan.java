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
 * <p>Vector lanes engage only when at least one full species width fits — short values, and the
 * tail of long ones, use the scalar scan (measured: vector setup does not pay below one lane).
 * Object keys and typical field values are well under one lane wide, so the scalar path is the one
 * that runs in practice and it reads a 256-entry classification table rather than re-deriving the
 * predicate per byte: one indexed load and one perfectly-predicted branch, instead of five or
 * seven compares.
 */
final class JsonValueScan {

  private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;

  /** Byte values that {@link #isPlainAscii} rejects — anything outside 0x20–0x7E, plus {@code " \ /}. */
  private static final boolean[] NOT_PLAIN_ASCII = new boolean[256];

  /** Byte values that {@link #mayNeedJsonEscape} flags; see the class comment for the derivation. */
  private static final boolean[] MAY_NEED_ESCAPE = new boolean[256];

  static {
    for (int b = 0; b < 256; b++) {
      NOT_PLAIN_ASCII[b] = b < 0x20 || b > 0x7E || b == '"' || b == '\\' || b == '/';
      MAY_NEED_ESCAPE[b] = b < 0x20 || b == '"' || b == '\\' || b == '/' || b == 0x7F || b == 0xC2 || b == 0xE2;
    }
  }

  private JsonValueScan() {
  }

  /**
   * {@code true} if every byte is plain ASCII that JSON emits verbatim: {@code 0x20}–{@code 0x7E}
   * minus {@code " \ /}. Strictly stronger than {@code !}{@link #mayNeedJsonEscape(byte[])} — it
   * additionally rules out multi-byte sequences — which is what lets the CHAR sink widen the bytes
   * to chars instead of decoding them, since for ASCII the widening is the decode.
   *
   * @param utf8 the stored UTF-8 bytes
   * @return {@code true} when the bytes can be copied to either sink verbatim
   */
  static boolean isPlainAscii(final byte[] utf8) {
    final int len = utf8.length;
    int i = 0;
    final int upper = len >= SPECIES.length() ? SPECIES.loopBound(len) : 0;
    for (; i < upper; i += SPECIES.length()) {
      final ByteVector v = ByteVector.fromArray(SPECIES, utf8, i);
      // Unsigned compares: ULT 0x20 flags the C0 controls, UGT 0x7E flags DEL and every
      // continuation/lead byte of a multi-byte sequence — together, everything outside 0x20–0x7E.
      final VectorMask<Byte> flagged = v.compare(VectorOperators.ULT, (byte) 0x20)
                                        .or(v.compare(VectorOperators.UGT, (byte) 0x7E))
                                        .or(v.eq((byte) '"'))
                                        .or(v.eq((byte) '\\'))
                                        .or(v.eq((byte) '/'));
      if (flagged.anyTrue()) {
        return false;
      }
    }
    for (; i < len; i++) {
      if (NOT_PLAIN_ASCII[utf8[i] & 0xFF]) {
        return false;
      }
    }
    return true;
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
      if (MAY_NEED_ESCAPE[utf8[i] & 0xFF]) {
        return true;
      }
    }
    return false;
  }
}
