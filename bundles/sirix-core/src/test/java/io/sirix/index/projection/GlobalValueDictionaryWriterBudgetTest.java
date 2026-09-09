package io.sirix.index.projection;

import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.ValueDictionaryEntryNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dictionary writer's runtime bound: the protection that needs no row-count hint.
 *
 * <p>
 * Defect (11) was not a crash. A 100M-row load elected three long, near-unique string columns, and
 * their arenas doubled until the collector took 3.4 cores and the load wrote one megabyte a minute
 * while still reporting itself alive. Nothing threw, so nothing could react. This pins the bound
 * that turns that into a refusal a caller can act on — and pins that it is TYPED, because the
 * caller has to tell "this dictionary got too big", which is expected at scale and recoverable,
 * from a genuine encoding fault, which is not.
 * </p>
 */
final class GlobalValueDictionaryWriterBudgetTest {

  private static byte[] value(final int i) {
    return ("http://example.com/a/rather/long/path/segment?id=" + i).getBytes(StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("A bounded writer refuses once its retained bytes reach the budget")
  void boundedWriterRefusesAtTheBudget() {
    final GlobalValueDictionaryWriter writer =
        new GlobalValueDictionaryWriter(3, GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES + (64L << 10));
    final GlobalDictionaryBudgetExceededException thrown =
        assertThrows(GlobalDictionaryBudgetExceededException.class, () -> {
          for (int i = 0; i < 1_000_000; i++) {
            final byte[] v = value(i);
            writer.intern(v, 0, v.length);
          }
        });

    assertEquals(3, thrown.column(), "the refusal must name the column, or the log cannot say what to fix");
    assertTrue(thrown.entryCount() > 0, "it stopped before interning anything, so the bound is mis-scaled");
    assertTrue(thrown.retainedBytes() <= thrown.budgetBytes() + (64L << 10), "retained " + thrown.retainedBytes()
        + " overshot the " + thrown.budgetBytes() + " budget by more than one growth step — the check runs too late");
    assertTrue(thrown.getMessage().contains("budget"), thrown.getMessage());

    // A byte-budget decline has to carry the term it WEIGHED, not merely what it retains. Every
    // guard here compares retention plus a reservation, and a dictionary whose retention alone had
    // passed the bound would have been refused one admission earlier — so a decline reporting
    // retention states a figure BELOW the budget it announces as breached, and the operator gets no
    // number to raise the budget to.
    assertNotNull(thrown.breachingTerm(), "a byte-budget decline must name the term that tripped it: " + thrown);
    assertTrue(thrown.breachingBytes() > thrown.budgetBytes(),
        "the quantity the decline blames must actually exceed the budget it quotes (" + thrown.breachingBytes()
            + " B vs " + thrown.budgetBytes() + " B, term " + thrown.breachingTerm() + ")");
    assertTrue(thrown.retainedBytes() <= thrown.breachingBytes(),
        "retention cannot exceed the retention-plus-reservation term weighed against the budget");
    assertTrue(
        thrown.getMessage().contains(thrown.breachingTerm())
            && thrown.getMessage().contains(Long.toString(thrown.breachingBytes())),
        "the message must state its own arithmetic, named and valued: " + thrown.getMessage());
    assertTrue(
        thrown.getMessage().contains("Raise the configured byte budget to at least " + thrown.breachingBytes() + " B"),
        "the remedy must quote a budget worth raising to: " + thrown.getMessage());
  }

  @Test
  @DisplayName("A structural ceiling declines without claiming any quantity exceeded the byte budget")
  void structuralDeclinesWeighNoBytesAgainstTheBudget() {
    // The other half of the contract. These ceilings are counts and lengths, not bytes: nothing was
    // compared against the budget, so the decline must not invent a comparison the way the old
    // single-constructor message did.
    final GlobalValueDictionaryWriter atTheChunkCeiling = new GlobalValueDictionaryWriter();
    for (int i = 0; i < GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND; i++) {
      final byte[] bytes = compactValue(i);
      atTheChunkCeiling.intern(bytes, 0, bytes.length);
    }
    final byte[] pastTheChunkCeiling = compactValue(GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND);

    // A fresh writer for the length ceiling, so the entry-count guard cannot shadow it.
    final GlobalValueDictionaryWriter empty = new GlobalValueDictionaryWriter();
    final byte[] pastTheValueCeiling = new byte[GlobalValueDictionaryWriter.MAX_VALUE_BYTES + 1];

    for (final GlobalDictionaryBudgetExceededException structural : new GlobalDictionaryBudgetExceededException[] {
        assertThrows(GlobalDictionaryBudgetExceededException.class,
            () -> atTheChunkCeiling.intern(pastTheChunkCeiling, 0, pastTheChunkCeiling.length)),
        assertThrows(GlobalDictionaryBudgetExceededException.class,
            () -> empty.intern(pastTheValueCeiling, 0, pastTheValueCeiling.length))}) {
      assertNull(structural.breachingTerm(),
          "a structural ceiling weighs no bytes, so it must name no breaching term: " + structural.getMessage());
      assertEquals(structural.retainedBytes(), structural.breachingBytes(),
          "with nothing weighed, the reported quantity is simply what is retained");
      assertNotNull(structural.admissionDetail(), "a structural decline must state which ceiling it hit");
      assertTrue(structural.getMessage().contains(structural.admissionDetail()),
          "the message must state the ceiling, not a byte comparison: " + structural.getMessage());
      assertFalse(structural.getMessage().contains("past the"),
          "a structural decline must not announce a budget breach it never measured: " + structural.getMessage());
      assertTrue(structural.getMessage().contains("raising the byte budget cannot override"),
          "the remedy must say a bigger budget will not help here: " + structural.getMessage());
    }
  }

  @Test
  @DisplayName("An unbounded writer is unaffected below the mandatory structural ceiling")
  void unboundedWriterInternsFreely() {
    // The aggregate byte budget is opt-in. The non-humongous structural ceiling is not.
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
    for (int i = 0; i < 10_000; i++) {
      final byte[] v = value(i);
      writer.intern(v, 0, v.length);
    }
    assertEquals(10_000, writer.entryCount());
  }

  @Test
  @DisplayName("The canonical 16,384-entry chunk fits without a humongous backing array")
  void canonicalChunkFitsAndTheFollowingEntryDeclinesBeforeGrowth() {
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
    for (int i = 0; i < GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND; i++) {
      final byte[] bytes = compactValue(i);
      assertEquals(i + 1, writer.intern(bytes, 0, bytes.length));
    }

    assertEquals(16_384, writer.entryCount());
    assertEquals(32_768, writer.hashTableCapacityForTest(),
        "the 50%-full canonical chunk must not grow the table one insertion early");
    assertTrue(writer.largestBackingArrayPayloadBytesForTest() <= (256 << 10),
        "no geometrically grown backing array may approach G1's minimum humongous threshold");

    final long retainedBefore = writer.retainedBytes();
    final int tableCapacityBefore = writer.hashTableCapacityForTest();
    final byte[] next = compactValue(16_384);
    final GlobalDictionaryBudgetExceededException reservationDecline = assertThrows(
        GlobalDictionaryBudgetExceededException.class, () -> writer.reservationBytesForIntern(next, 0, next.length));
    assertTrue(reservationDecline.admissionDetail().contains("16,385"));
    final GlobalDictionaryBudgetExceededException insertDecline =
        assertThrows(GlobalDictionaryBudgetExceededException.class, () -> writer.intern(next, 0, next.length));
    assertTrue(insertDecline.admissionDetail().contains("16,385"));
    assertEquals(16_384, writer.entryCount());
    assertEquals(retainedBefore, writer.retainedBytes());
    assertEquals(tableCapacityBefore, writer.hashTableCapacityForTest());

    final byte[] duplicate = compactValue(16_383);
    assertEquals(16_384, writer.intern(duplicate, 0, duplicate.length),
        "a hit at the structural ceiling must remain allocation-free and legal");
  }

  @Test
  @DisplayName("Oversized values decline before state mutation or UTF-8 materialisation")
  void oversizedValuesAreRejectedBeforeUnsafeMaterialisation() {
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
    final byte[] maximum = new byte[GlobalValueDictionaryWriter.MAX_VALUE_BYTES];
    assertEquals(1, writer.intern(maximum, 0, maximum.length));
    assertEquals(GlobalValueDictionaryWriter.MAX_VALUE_BYTES, writer.valueBytes(1).length);
    final long retainedBefore = writer.retainedBytes();

    final byte[] oversized = new byte[GlobalValueDictionaryWriter.MAX_VALUE_BYTES + 1];
    final GlobalDictionaryBudgetExceededException byteDecline = assertThrows(
        GlobalDictionaryBudgetExceededException.class, () -> writer.intern(oversized, 0, oversized.length));
    assertTrue(byteDecline.admissionDetail().contains("value length"));
    assertEquals(1, writer.entryCount());
    assertEquals(retainedBefore, writer.retainedBytes());

    final String oversizedUtf8 = "\u20ac".repeat(GlobalValueDictionaryWriter.MAX_VALUE_BYTES / 3 + 1);
    final GlobalDictionaryBudgetExceededException stringDecline =
        assertThrows(GlobalDictionaryBudgetExceededException.class, () -> writer.intern(oversizedUtf8));
    assertTrue(stringDecline.admissionDetail().contains("UTF-8 value"));
    assertEquals(1, writer.entryCount());
    assertEquals(retainedBefore, writer.retainedBytes());
  }

  @Test
  @DisplayName("A corrupt oversized reverse value is rejected from its length prefix")
  void oversizedWireValueIsRejectedBeforePayloadAllocation() {
    final byte[] onlyLengthPrefix =
        ByteBuffer.allocate(Integer.BYTES).putInt(ValueDictionaryEntryNode.MAX_VALUE_LENGTH + 1).array();

    final IllegalStateException failure = assertThrows(IllegalStateException.class,
        () -> NodeKind.VALUE_DICTIONARY_ENTRY.deserialize(new ByteArrayBytesIn(onlyLengthPrefix), 1L, null, null));
    assertTrue(failure.getMessage().contains("safe V0 payload limit"), failure.getMessage());
  }

  @Test
  @DisplayName("Forced-global mode fails closed instead of emitting AUTO's typed decline")
  void forcedGlobalModeFailsClosed() {
    final GlobalValueDictionaryWriter writer =
        new GlobalValueDictionaryWriter(7, Long.MAX_VALUE, GlobalValueDictionaryWriter.AdmissionPolicy.FAIL_CLOSED);
    final byte[] oversized = new byte[GlobalValueDictionaryWriter.MAX_VALUE_BYTES + 1];

    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> writer.intern(oversized, 0, oversized.length));
    assertTrue(failure.getCause() instanceof GlobalDictionaryBudgetExceededException);
    assertEquals(0, writer.entryCount());
  }

  @Test
  @DisplayName("Interning a repeated value is a hit and costs no budget")
  void repeatedValuesDoNotConsumeBudget() {
    // The bound is on DISTINCT values; a column that repeats heavily is exactly the case a
    // resource-wide dictionary is for, and it must not be penalised for row count.
    final GlobalValueDictionaryWriter writer =
        new GlobalValueDictionaryWriter(0, GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES + (64L << 10));
    final byte[] v = value(1);
    final int first = writer.intern(v, 0, v.length);
    for (int i = 0; i < 100_000; i++) {
      assertEquals(first, writer.intern(v, 0, v.length));
    }
    assertEquals(1, writer.entryCount());
  }

  @Test
  @DisplayName("Capacity growth is refused before any backing array grows")
  void capacityGrowthIsPreflightedAgainstTheBudget() {
    final GlobalValueDictionaryWriter calibration = new GlobalValueDictionaryWriter();
    for (int i = 0; i < 2_047; i++) {
      final byte[] value = {(byte) i, (byte) (i >>> 8), (byte) (i >>> 16), (byte) (i >>> 24)};
      calibration.intern(value, 0, value.length);
    }
    final long budget = calibration.estimatedFlushPeakBytes();
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter(2, budget);
    for (int i = 0; i < 2_047; i++) {
      final byte[] value = {(byte) i, (byte) (i >>> 8), (byte) (i >>> 16), (byte) (i >>> 24)};
      assertEquals(i + 1, writer.intern(value, 0, value.length));
    }
    final long retainedBefore = writer.retainedBytes();

    final byte[] next = {(byte) 0xff, (byte) 0xff, 0x07, 0x00};
    assertThrows(GlobalDictionaryBudgetExceededException.class, () -> writer.intern(next, 0, next.length));

    assertEquals(2_047, writer.entryCount());
    assertEquals(retainedBefore, writer.retainedBytes());
    assertTrue(retainedBefore <= budget);
  }

  @Test
  @DisplayName("The budget includes persisted nodes and transaction pages retained during flush")
  void persistedOutputIsIncludedInThePeak() {
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
    for (int i = 0; i < 4_096; i++) {
      final byte[] v = value(i);
      writer.intern(v, 0, v.length);
    }

    assertTrue(writer.estimatedFlushPeakBytes() > writer.retainedBytes() + writer.valueBytes(),
        "the projected peak must include directory nodes, KVL/TIL pages, and encoded output");
  }

  @Test
  @DisplayName("Arena growth saturates without signed overflow")
  void arenaGrowthSaturatesWithoutSignedOverflow() {
    assertEquals(Integer.MAX_VALUE - 8, GlobalValueDictionaryWriter.grownCapacityForTest(1 << 30, (1L << 30) + 1));
    assertThrows(IllegalStateException.class,
        () -> GlobalValueDictionaryWriter.grownCapacityForTest(Integer.MAX_VALUE - 8, (long) Integer.MAX_VALUE - 7));
  }

  @Test
  @DisplayName("Election estimates saturate instead of overflowing into an admission")
  void electionEstimateSaturatesOnOverflow() {
    assertEquals(186L, ProjectionIndexBuilder.projectedGlobalDictionaryBytes(3L, 10L));
    assertEquals(Long.MAX_VALUE, ProjectionIndexBuilder.projectedGlobalDictionaryBytes(Long.MAX_VALUE, 1L));
    assertEquals(Long.MAX_VALUE, ProjectionIndexBuilder.projectedGlobalDictionaryBytes(1L, Long.MAX_VALUE));
    assertThrows(IllegalArgumentException.class, () -> ProjectionIndexBuilder.projectedGlobalDictionaryBytes(-1L, 1L));
  }

  @Test
  @DisplayName("AUTO gives a lone worthwhile candidate the aggregate budget instead of a diluted column slice")
  void autoCandidateUsesTheAggregateBudget() {
    final long aggregate = 512L << 20;
    final long candidateProjection = 71L << 20;
    final long[] projectedBytes = {-1L, -1L, candidateProjection, -1L, -1L, -1L, -1L};
    final long[] benefitScores = {0L, 0L, 16_000L, 0L, 0L, 0L, 0L};

    final long[] allocations =
        ProjectionIndexBuilder.planAutoGlobalDictionaryBudgets(aggregate, projectedBytes, benefitScores, true);

    assertEquals(aggregate, allocations[2],
        "six non-candidates must not dilute the one dictionary AUTO actually elected");
    assertEquals(0L, allocations[0]);
    assertEquals(0L, allocations[6]);
    final long componentBudget = ProjectionIndexBuilder.streamingGlobalDictionaryComponentBudget(allocations[2]);
    assertTrue(componentBudget >= 2L * candidateProjection,
        "both resident streaming structures must retain the existing two-times projection headroom");
  }

  @Test
  @DisplayName("AUTO admits multiple candidates without aggregate overcommit")
  void autoCandidatesShareOneBoundedAggregate() {
    final long aggregate = 768L << 20;
    final long firstProjection = 71L << 20;
    final long secondProjection = 60L << 20;

    final long[] allocations = ProjectionIndexBuilder.planAutoGlobalDictionaryBudgets(aggregate,
        new long[] {firstProjection, secondProjection}, new long[] {16_000L, 15_000L}, true);

    assertTrue(ProjectionIndexBuilder.streamingGlobalDictionaryComponentBudget(allocations[0]) >= 2L * firstProjection);
    assertTrue(
        ProjectionIndexBuilder.streamingGlobalDictionaryComponentBudget(allocations[1]) >= 2L * secondProjection);
    assertEquals(aggregate, Math.addExact(allocations[0], allocations[1]),
        "even simultaneous writer-plus-front peaks must stay inside the configured aggregate");
  }

  @Test
  @DisplayName("AUTO's constrained winner is deterministic by benefit and projection-column order")
  void autoConstrainedSelectionIsDeterministic() {
    final long aggregate = 300L << 20;
    final long projection = 50L << 20;

    final long[] higherBenefitWins = ProjectionIndexBuilder.planAutoGlobalDictionaryBudgets(aggregate,
        new long[] {projection, projection}, new long[] {10L, 20L}, true);
    assertEquals(0L, higherBenefitWins[0]);
    assertEquals(aggregate, higherBenefitWins[1]);

    final long[] lowerColumnBreaksTie = ProjectionIndexBuilder.planAutoGlobalDictionaryBudgets(aggregate,
        new long[] {projection, projection}, new long[] {20L, 20L}, true);
    assertEquals(aggregate, lowerColumnBreaksTie[0]);
    assertEquals(0L, lowerColumnBreaksTie[1]);
  }

  @Test
  @DisplayName("AUTO rejects a streaming candidate whose two disjoint two-times caps exceed the aggregate")
  void autoRejectsAProjectionThatExceedsTheAggregate() {
    final long[] allocations = ProjectionIndexBuilder.planAutoGlobalDictionaryBudgets(512L << 20,
        new long[] {130L << 20}, new long[] {16_000L}, true);

    assertEquals(0L, allocations[0],
        "selection must not weaken either component's conservative margin merely to force an admission");
  }

  @Test
  @DisplayName("AUTO deterministically orders the maximum supported projection width")
  void autoPlannerHandlesMaximumColumnCountWithoutQuadraticSelection() {
    final int columns = RowGroupDescriptor.MAX_COLUMNS;
    final long[] projectedBytes = new long[columns];
    final long[] benefitScores = new long[columns];
    Arrays.fill(projectedBytes, 1L << 20);
    for (int column = 0; column < columns; column++) {
      benefitScores[column] = column % 97L;
    }

    final long aggregate = 512L << 20;
    final long[] first =
        ProjectionIndexBuilder.planAutoGlobalDictionaryBudgets(aggregate, projectedBytes, benefitScores, true);
    final long[] second =
        ProjectionIndexBuilder.planAutoGlobalDictionaryBudgets(aggregate, projectedBytes, benefitScores, true);

    int admitted = 0;
    long allocated = 0L;
    for (int column = 0; column < columns; column++) {
      assertEquals(first[column], second[column], "planner order changed at column " + column);
      if (first[column] != 0L) {
        admitted++;
        allocated = Math.addExact(allocated, first[column]);
      }
    }
    assertEquals(128, admitted);
    assertEquals(aggregate, allocated);
  }

  @Test
  @DisplayName("Maximum-cardinality reverse planning is range arithmetic, not an entry array")
  void maximumCardinalityReversePlanningDoesNotMaterialiseEntries() {
    final long reverseBuckets = ((long) Integer.MAX_VALUE + 255L) >>> 8;
    final long levelTwoNodes = (reverseBuckets + 255L) >>> 8;
    final long levelOneNodes = (levelTwoNodes + 255L) >>> 8;

    assertEquals(reverseBuckets + levelTwoNodes + levelOneNodes + 1L,
        GlobalValueDictionaryRadix.denseReverseRecordCountForTest(0, Integer.MAX_VALUE));
    // entryKeyForLocalId was the per-value key derivation; packing replaced it, so the seam is gone.
  }

  private static byte[] compactValue(final int value) {
    return new byte[] {(byte) value, (byte) (value >>> 8), (byte) (value >>> 16), (byte) (value >>> 24)};
  }
}
