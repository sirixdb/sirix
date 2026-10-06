package io.sirix.index;

import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.exception.SirixRuntimeException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CASNumericConversionTest {
  @Test
  void integralNumericRepresentationsShareTheIntegerKey() {
    final IndexDef definition = definition();
    for (final Number value : List.<Number>of(1, 1L, 1.0f, 1.0d, new BigDecimal("1.00"), BigInteger.ONE)) {
      assertEquals(1, ((Numeric) AtomicUtil.toIndexType(AtomicUtil.fromNumber(value), definition)).intValue());
    }
    assertTrue(definition.hasNumericValuesOnly());
    assertTrue(definition.hasCompleteNumericCoverage());
  }

  @Test
  void fractionalValuesDoNotBecomeIntegerMatchesAndCoveragePersists() {
    final IndexDef definition = definition();
    final Indexes indexes = new Indexes();
    indexes.add(definition);
    indexes.clearDirty();
    assertThrows(SirixRuntimeException.class, () -> AtomicUtil.toIndexType(new Dbl(1.5), definition));
    assertTrue(indexes.isDirty());
    assertTrue(definition.hasNumericValuesOnly());
    assertFalse(definition.hasCompleteNumericCoverage());
    final IndexDef restored = definition();
    restored.init(definition.materialize());
    assertTrue(restored.hasNumericValuesOnly());
    assertFalse(restored.hasCompleteNumericCoverage());
    assertTrue(restored.hasSameDefinition(definition));
  }

  @Test
  void integerProbeEligibilityExcludesFloatingPromotionAmbiguity() {
    assertTrue(AtomicUtil.isExactIntegerProbe(AtomicUtil.fromNumber(16_777_215)));
    assertFalse(AtomicUtil.isExactIntegerProbe(AtomicUtil.fromNumber(16_777_217L)));
    assertFalse(AtomicUtil.isExactIntegerProbe(new Dbl(1.5)));
  }

  private static IndexDef definition() {
    return IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse("/[]/id", PathParser.Type.JSON)), 0,
        IndexDef.DbType.JSON);
  }
}
