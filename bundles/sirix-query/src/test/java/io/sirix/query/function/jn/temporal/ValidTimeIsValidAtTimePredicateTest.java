package io.sirix.query.function.jn.temporal;

import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jsonitem.object.AbstractObject;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.sirix.exception.SirixIOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ValidTimeIndexScan#isValidAtTime} — the single validity predicate shared
 * by the linear-scan fallback, the CAS-index verification, and the interval-index re-verification.
 *
 * <p>
 * Data-shape problems (absent fields, non-string values, unparseable dates) must degrade to
 * "unbounded on that side" / "no interval", but genuine failures while reading the fields (e.g.
 * I/O errors) must propagate: a former catch-all mapped them to {@code false}, silently dropping
 * every record from query results instead of surfacing the error.
 * </p>
 */
@DisplayName("ValidTimeIndexScan.isValidAtTime predicate")
final class ValidTimeIsValidAtTimePredicateTest {

  private static final String FROM = "validFrom";
  private static final String TO = "validTo";

  private static final Instant T = Instant.parse("2020-06-01T12:00:00Z");

  private static Object obj(final String validFrom, final String validTo) {
    if (validFrom == null && validTo == null) {
      return new ArrayObject(new QNm[0], new Sequence[0]);
    }
    if (validFrom == null) {
      return new ArrayObject(new QNm[] {new QNm(TO)}, new Sequence[] {new Str(validTo)});
    }
    if (validTo == null) {
      return new ArrayObject(new QNm[] {new QNm(FROM)}, new Sequence[] {new Str(validFrom)});
    }
    return new ArrayObject(new QNm[] {new QNm(FROM), new QNm(TO)},
        new Sequence[] {new Str(validFrom), new Str(validTo)});
  }

  @Test
  @DisplayName("t inside / at boundaries / outside a closed interval")
  void closedInterval() {
    final Object o = obj("2020-06-01T00:00:00Z", "2020-06-02T00:00:00Z");
    assertTrue(ValidTimeIndexScan.isValidAtTime(o, T, FROM, TO));
    assertTrue(ValidTimeIndexScan.isValidAtTime(o, Instant.parse("2020-06-01T00:00:00Z"), FROM, TO));
    assertTrue(ValidTimeIndexScan.isValidAtTime(o, Instant.parse("2020-06-02T00:00:00Z"), FROM, TO));
    assertFalse(ValidTimeIndexScan.isValidAtTime(o, Instant.parse("2020-05-31T23:59:59.999Z"), FROM, TO));
    assertFalse(ValidTimeIndexScan.isValidAtTime(o, Instant.parse("2020-06-02T00:00:00.001Z"), FROM, TO));
  }

  @Test
  @DisplayName("absent or unparseable bounds are unbounded on that side; neither bound never matches")
  void openAndDegenerateIntervals() {
    assertTrue(ValidTimeIndexScan.isValidAtTime(obj("2020-06-01T00:00:00Z", null), T, FROM, TO));
    assertTrue(ValidTimeIndexScan.isValidAtTime(obj(null, "2020-06-02T00:00:00Z"), T, FROM, TO));
    assertFalse(ValidTimeIndexScan.isValidAtTime(obj(null, null), T, FROM, TO));
    // Unparseable dates degrade to "absent", not to an error and not to a hard mismatch.
    assertTrue(ValidTimeIndexScan.isValidAtTime(obj("not-a-date", "2020-06-02T00:00:00Z"), T, FROM, TO));
    assertFalse(ValidTimeIndexScan.isValidAtTime(obj("not-a-date", "junk"), T, FROM, TO));
  }

  @Test
  @DisplayName("failures while reading the fields propagate instead of silently dropping the record")
  void readFailurePropagates() {
    final Object failing = new AbstractObject() {
      @Override
      public Sequence get(final QNm field) {
        throw new SirixIOException("simulated I/O failure reading field " + field);
      }

      @Override
      public Object replace(final QNm field, final Sequence value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object rename(final QNm field, final QNm newFieldName) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object insert(final QNm field, final Sequence value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object remove(final QNm field) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object remove(final IntNumeric index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object remove(final int index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Sequence value(final IntNumeric index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Sequence value(final int index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Array names() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Array values() {
        throw new UnsupportedOperationException();
      }

      @Override
      public QNm name(final IntNumeric index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public QNm name(final int index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public IntNumeric length() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int len() {
        throw new UnsupportedOperationException();
      }
    };

    final SirixIOException e =
        assertThrows(SirixIOException.class, () -> ValidTimeIndexScan.isValidAtTime(failing, T, FROM, TO));
    assertEquals("simulated I/O failure reading field validFrom", e.getMessage());
  }
}
