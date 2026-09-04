package io.sirix.index.projection;

import io.sirix.node.ValueDictionaryEntryNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bounds of the zero-copy dictionary read path.
 *
 * <p>
 * Packing changed a latent assumption: values used to own their arrays, so an array bound WAS a
 * value bound. Now many values share one backing array, and every range operation has to respect
 * the slice rather than the array. These tests pin the two ways that went wrong.
 */
final class DictionaryRangeOperationBoundsTest {

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("a start before the value is refused, not read from the preceding bytes")
  void startBeforeTheValueIsRefused() {
    // "AAAA" then "2013-07-14T20:38:47": with start = 0 the 1-based window opened one byte EARLY and
    // read the tail of the previous packed value.
    final byte[] packed = utf8("AAAA2013-07-14T20:38:47");
    final int off = 4;
    final int len = packed.length - off;
    assertEquals(Long.MIN_VALUE, ProjectionIndexByteScan.packIsoMinuteSubstring(packed, off, len, 0, 16));
    assertEquals(Long.MIN_VALUE, ProjectionIndexByteScan.packIsoMinuteSubstring(packed, off, len, -5, 16));
    assertEquals(Long.MIN_VALUE, ProjectionIndexByteScan.xsIntegerOfSubstring(packed, off, len, 0, 4));
    assertEquals(Long.MIN_VALUE, ProjectionIndexByteScan.xsIntegerOfSubstring(packed, off, len, -1, 4));
    // The correct 1-based start still works, so the guard refuses only what it should.
    assertTrue(ProjectionIndexByteScan.packIsoMinuteSubstring(packed, off, len, 1, 16) != Long.MIN_VALUE);
  }

  @Test
  @DisplayName("a negative substring length is refused")
  void negativeLengthIsRefused() {
    final byte[] value = utf8("12345");
    assertEquals(Long.MIN_VALUE, ProjectionIndexByteScan.xsIntegerOfSubstring(value, 0, value.length, 1, -1));
    assertEquals(Long.MIN_VALUE,
        ProjectionIndexByteScan.xsIntegerOfSubstring(value, 0, value.length, 1, Integer.MIN_VALUE));
  }

  @Test
  @DisplayName("an extreme start cannot wrap into a window that looks in range")
  void extremeStartDoesNotOverflow() {
    final byte[] value = utf8("2013-07-14T20:38:47");
    assertEquals(Long.MIN_VALUE,
        ProjectionIndexByteScan.packIsoMinuteSubstring(value, 0, value.length, Integer.MAX_VALUE, 16));
    assertEquals(Long.MIN_VALUE,
        ProjectionIndexByteScan.xsIntegerOfSubstring(value, 0, value.length, Integer.MAX_VALUE, 4));
    assertEquals(Long.MIN_VALUE,
        ProjectionIndexByteScan.xsIntegerOfSubstring(value, 0, value.length, Integer.MAX_VALUE - 1, Integer.MAX_VALUE));
  }

  @Test
  @DisplayName("comparison never reads past a slice into the next packed value")
  void comparisonDoesNotReadAcrossSlices() {
    // A packed pair whose FIRST value ends in a multi-byte lead byte and whose second begins with
    // bytes that look like continuations. Decoding against the array bound would consume them and
    // compare against bytes belonging to another value; decoding against the SLICE bound refuses.
    // The slices must SHARE a leading character, or the comparison decides on the first unit and
    // never reaches the truncation — which is exactly how a first attempt at this test passed
    // vacuously against the unfixed decode.
    final byte[] packed = new byte[] {'a', (byte) 0xE2, (byte) 0x82, (byte) 0xAC, 'a', 'b'};
    assertThrows(IllegalStateException.class,
        () -> ValueDictionaryEntryNode.compareUtf16Range(packed, 0, 2, packed, 4, 2),
        "a truncated sequence must fail closed, never borrow the neighbour's bytes");
    // The same bytes sliced to their true extent compare fine, so the guard is about the BOUND and
    // not about the content: "a\u20AC" orders after "ab".
    assertTrue(ValueDictionaryEntryNode.compareUtf16Range(packed, 0, 4, packed, 4, 2) > 0);
  }

  @Test
  @DisplayName("range comparison validates its window against the backing array")
  void rangeComparisonValidatesItsWindow() {
    final byte[] value = utf8("abc");
    assertThrows(NullPointerException.class, () -> ValueDictionaryEntryNode.compareUtf16Range(null, 0, 0, value, 0, 3));
    assertThrows(IndexOutOfBoundsException.class,
        () -> ValueDictionaryEntryNode.compareUtf16Range(value, 0, 4, value, 0, 3));
    assertThrows(IndexOutOfBoundsException.class,
        () -> ValueDictionaryEntryNode.compareUtf16Range(value, -1, 2, value, 0, 3));
  }

  @Test
  @DisplayName("range comparison agrees with UTF-16 ordering, including supplementary planes")
  void rangeComparisonMatchesUtf16Ordering() {
    final String[] values = {"", "a", "ab", "b", "！", "𐐀", "zz"};
    for (final String left : values) {
      for (final String right : values) {
        final byte[] l = utf8(left);
        final byte[] r = utf8(right);
        assertEquals(Integer.signum(left.compareTo(right)),
            Integer.signum(ValueDictionaryEntryNode.compareUtf16Range(l, 0, l.length, r, 0, r.length)),
            "\"" + left + "\" vs \"" + right + "\"");
      }
    }
  }

  @Test
  @DisplayName("comparison over an OFFSET slice matches the same value compared standalone")
  void offsetSlicesCompareLikeStandaloneValues() {
    final byte[] packed = utf8("prefix𐐀suffix");
    final int off = utf8("prefix").length;
    final int len = utf8("𐐀").length;
    final byte[] standalone = utf8("！");
    assertEquals(Integer.signum("𐐀".compareTo("！")),
        Integer.signum(ValueDictionaryEntryNode.compareUtf16Range(packed, off, len, standalone, 0, standalone.length)));
  }
}
