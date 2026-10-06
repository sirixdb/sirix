package io.sirix.index.interval;

import io.brackit.query.QueryException;
import io.brackit.query.atomic.DateTime;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ValidTimeLexicalBoundTest {
  @ParameterizedTest
  @ValueSource(strings = {"2025-01-01T00:00:00.Z", "2025-01-01T00:00:00.+01:00", "2025-01-01T00:00:00.-05:00"})
  void javaOnlyEmptyFractionRequiresOriginalCastVerification(final String raw) {
    assertNotNull(ValidTimeIntervalIndexWriter.parseInstant(raw));
    assertThrows(QueryException.class, () -> new DateTime(raw));
    assertFalse(ValidTimeIntervalIndexWriter.isExactLexicalBound(raw));
  }

  @ParameterizedTest
  @ValueSource(strings = {"2025-01-01T00:00:00Z", "2025-01-01T00:00:00.000Z", "2025-01-01T00:00:00+01:00",
      "2025-01-01T00:00:00.000-05:00"})
  void sharedWholeMillisecondSpellingsRemainExact(final String raw) {
    assertNotNull(ValidTimeIntervalIndexWriter.parseInstant(raw));
    assertDoesNotThrow(() -> new DateTime(raw));
    assertTrue(ValidTimeIntervalIndexWriter.isExactLexicalBound(raw));
  }
}
