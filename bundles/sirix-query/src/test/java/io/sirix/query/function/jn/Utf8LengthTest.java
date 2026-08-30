package io.sirix.query.function.jn;

import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.SequenceType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class Utf8LengthTest {

  private static final Utf8Length FUNCTION =
      new Utf8Length(new Signature(SequenceType.INTEGER, new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne)));

  @Test
  void countsUtf8BytesWithoutAllocatingAnEncodedArray() {
    assertLength(null, 0L);
    assertLength("", 0L);
    assertLength("ascii", 5L);
    assertLength("ünïcode", 9L);
    assertLength("\uD83D\uDE00", 4L);
  }

  @Test
  void matchesTheJdkEncoderReplacementForIsolatedSurrogates() {
    assertLength("\uD800", 1L);
    assertLength("\uDC00", 1L);
  }

  private static void assertLength(final String value, final long expected) {
    final Sequence result = FUNCTION.execute(null, null, new Sequence[] {value == null
        ? null
        : new Str(value)});
    assertEquals(expected, ((Int64) result).longValue());
  }
}
