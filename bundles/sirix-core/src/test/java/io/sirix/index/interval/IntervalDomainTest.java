package io.sirix.index.interval;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class IntervalDomainTest {
  @Test
  void exactnessExcludesRoundedAndClampedInstants() {
    final IntervalDomain domain = new IntervalDomain(4, 0);
    assertTrue(domain.isExact(Instant.EPOCH));
    assertTrue(domain.isExact(Instant.ofEpochMilli(14)));
    assertFalse(domain.isExact(Instant.ofEpochMilli(-1)));
    assertFalse(domain.isExact(Instant.ofEpochMilli(15)));
    assertFalse(domain.isExact(Instant.EPOCH.plusNanos(1)));
    assertFalse(domain.isExact(Instant.MAX));
    assertEquals(1, domain.point(Instant.MIN));
    assertEquals(15, domain.point(Instant.MAX));
    assertEquals(15, new IntervalDomain(4, -1).toDomain(Long.MAX_VALUE));
  }

  @Test
  void lexicalExactnessRejectsParserDisagreements() {
    assertTrue(ValidTimeIntervalIndexWriter.isExactLexicalBound("2024-01-01T00:00:00Z"));
    assertTrue(ValidTimeIntervalIndexWriter.isExactLexicalBound("2024-01-01T00:00:00.123000+02:00"));
    assertFalse(ValidTimeIntervalIndexWriter.isExactLexicalBound("2016-12-31T23:59:60Z"));
    assertFalse(ValidTimeIntervalIndexWriter.isExactLexicalBound("0000-01-01T00:00:00Z"));
    assertFalse(ValidTimeIntervalIndexWriter.isExactLexicalBound("+10000-01-01T00:00:00Z"));
    assertFalse(ValidTimeIntervalIndexWriter.isExactLexicalBound("2024-01-01t00:00:00z"));
  }
}
