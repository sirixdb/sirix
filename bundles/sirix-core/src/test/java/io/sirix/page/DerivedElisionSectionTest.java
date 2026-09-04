/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.StructuralKeyColumnCodec;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.StringRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.Arrays;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Witness for the derived value- and name-key-elision sections.
 *
 * <p>
 * Both sections used to spell out, per elided slot, a slot gap, a type byte, the original heap
 * width and the value's absolute index in its PAX region — four to five bytes each, measured at
 * 3.65 B per record on a 1M-row load, roughly twice the heap bytes the elision itself removed.
 * Every field is a function of what the page already carries, so the sections now hold membership
 * and the exceptions to the derivation only. See {@link ElisionDeriver}.
 *
 * <p>
 * The assertions below are of three kinds. <em>Bytes</em>: what the section costs, before and
 * after, on the same fixture. <em>Round trip</em>: the derived reader must rebuild the
 * byte-identical heap the per-slot reader rebuilds — compared frame to frame, so a fixture whose
 * values cannot be decoded without a symbol table is covered as fully as one whose can.
 * <em>Mutation</em>: each exception list is proven load-bearing by
 * {@link ElisionDeriver#ASSUME_PREDICTED_FOR_TESTING}, which stages none of them; a fixture
 * carrying a genuine deviation must then corrupt or fail, and does.
 */
@DisplayName("Derived elision sections")
final class DerivedElisionSectionTest {

  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  /** First nameKey of the fixture fields; each distinct key is its own region tag. */
  private static final int FIRST_NAME_KEY = 200;

  /** First pathNodeKey of the path-tagged fixtures. */
  private static final int FIRST_PATH_NODE_KEY = 900;

  /** Stored number subtype for {@code Integer}; see {@code NodeKind.serializeNumber}. */
  private static final byte NUMBER_TYPE_INTEGER = 2;

  /** Stored number subtype for {@code Long}. */
  private static final byte NUMBER_TYPE_LONG = 3;

  private Arena arena;
  private boolean derivedElisionBefore;
  private boolean siblingColumnsBefore;
  private boolean runLengthLaneBefore;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    derivedElisionBefore = PageKind.DERIVED_ELISION_SECTIONS;
    siblingColumnsBefore = PageKind.SIBLING_KEY_COLUMNS_ENABLED;
    runLengthLaneBefore = StructuralKeyColumnCodec.RUN_LENGTH_LANE_ENABLED;
    ElisionDeriver.ASSUME_PREDICTED_FOR_TESTING = false;
  }

  @AfterEach
  void tearDown() {
    PageKind.DERIVED_ELISION_SECTIONS = derivedElisionBefore;
    NumberRegion.setPerTagWidthEnabled(true);
    StringRegion.setPlainLaneEnabled(true);
    NumberRegion.setExternalHeaderEnabled(true);
    PageKind.SIBLING_KEY_COLUMNS_ENABLED = siblingColumnsBefore;
    StructuralKeyColumnCodec.RUN_LENGTH_LANE_ENABLED = runLengthLaneBefore;
    ElisionDeriver.ASSUME_PREDICTED_FOR_TESTING = false;
    if (arena != null) {
      arena.close();
    }
  }

  // ─────────────────────────────────────────────────────────────────── bytes

  @Test
  @DisplayName("the section costs a fraction of a byte per elided slot where it cost four to five")
  void derivedSectionCostsAFractionOfWhatTheTuplesCost() {
    final ResourceConfiguration config = newConfig(false);

    PageKind.DERIVED_ELISION_SECTIONS = false;
    final int tupleValueBytes;
    final int tupleNameKeyBytes;
    final KeyValueLeafPage tuplePage = newPage(config);
    try {
      fillMixed(tuplePage, false);
      serialize(config, tuplePage);
      // Four bytes of count prefix plus, per elided slot, a slot-gap varint, a type byte, a width
      // varint and a region-index varint — four bytes at the very least. Per fused slot the name-key
      // section adds one width byte behind its own count prefix.
      tupleValueBytes = 4 + 4 * MIXED_ELIDABLE_SLOTS;
      tupleNameKeyBytes = 4 + MIXED_SLOTS;
    } finally {
      tuplePage.close();
    }

    PageKind.DERIVED_ELISION_SECTIONS = true;
    final KeyValueLeafPage derivedPage = newPage(config);
    try {
      fillMixed(derivedPage, false);
      serialize(config, derivedPage);
      final ElisionDeriver deriver = PageKind.writerElisionDeriverForTesting();
      final int elided = MIXED_ELIDABLE_SLOTS;
      assertTrue(elided > 100, "fixture must elide enough slots for the per-slot figure to mean something");
      // The tuples cost four to five bytes per elided slot; the derived section is a flag byte plus one
      // BIT per populated slot, and nothing else on a fixture whose derivation is exact.
      assertTrue(tupleValueBytes >= 4 * elided,
          "the per-slot tuples should cost at least four bytes per elided slot, cost " + tupleValueBytes + " for "
              + elided);
      final double perSlot = deriver.plannedValueSectionBytes() / (double) elided;
      assertTrue(perSlot <= 0.2, "the derived value-elision section must cost at most 0.2 B per elided slot, cost "
          + perSlot + " (" + deriver.plannedValueSectionBytes() + " B for " + elided + " slots)");
      assertTrue(deriver.plannedNameKeySectionBytes() < tupleNameKeyBytes,
          "the derived name-key section must be smaller than the per-slot widths it replaces");
      assertEquals(1, deriver.plannedNameKeySectionBytes(),
          "every name-key width on this fixture is the canonical varint width, so only the flag byte is staged");
    } finally {
      derivedPage.close();
    }
  }

  @Test
  @DisplayName("a page whose fused primitives are all elided stages one byte")
  void allCandidatesElidedCollapsesToOneByte() {
    PageKind.DERIVED_ELISION_SECTIONS = true;
    final ResourceConfiguration config = newConfig(false);
    final KeyValueLeafPage page = newPage(config);
    try {
      // No doubles: every fused-primitive slot on the page is elidable, so membership needs no bitmap.
      for (int i = 0; i < 120; i++) {
        writeNumber(page, i, FIRST_NAME_KEY + (i % 12), -1L, Integer.valueOf(i * 3));
      }
      serialize(config, page);
      final ElisionDeriver deriver = PageKind.writerElisionDeriverForTesting();
      assertTrue(deriver.allCandidatesElided(), "every candidate is elided, so the flag must say so");
      assertEquals(1, deriver.plannedValueSectionBytes(),
          "membership is one flag and the derivation is exact, so the section is the flag byte alone");
      assertNoExceptions(deriver);
    } finally {
      page.close();
    }
  }

  @Test
  @DisplayName("the derivation is exact on the mixed fixture — no exception list is written")
  void derivationIsExactOnTheMixedFixture() {
    PageKind.DERIVED_ELISION_SECTIONS = true;
    final ResourceConfiguration config = newConfig(false);
    final KeyValueLeafPage page = newPage(config);
    try {
      fillMixed(page, false);
      serialize(config, page);
      assertNoExceptions(PageKind.writerElisionDeriverForTesting());
      assertFalse(PageKind.writerElisionDeriverForTesting().allCandidatesElided(),
          "the fixture holds doubles, which are not elidable, so membership needs the bitmap");
    } finally {
      page.close();
    }
  }

  // ─────────────────────────────────────────────────────────────── round trip

  @Test
  @DisplayName("the derived reader rebuilds the heap the per-slot reader rebuilds, on every fixture")
  void derivedAndTupleReadersAgreeFrameForFrame() {
    for (final Fixture fixture : Fixture.values()) {
      final ResourceConfiguration config = newConfig(fixture.pathSummary);
      final byte[] tupleWire = wireOf(config, fixture, false);
      final byte[] derivedWire = wireOf(config, fixture, true);
      // Guard against a comparison that proves nothing: every fixture elides something — a value, a
      // name key, or both — so the two forms must produce DIFFERENT wire bytes, and the derived one
      // must be the smaller. Without this the frame comparison below could agree for the trivial
      // reason that the lever never engaged.
      assertNotEquals(hex(tupleWire), hex(derivedWire),
          fixture + ": the derived form must actually change the bytes, or the frame comparison is vacuous");
      assertTrue(derivedWire.length < tupleWire.length, fixture + ": the derived form must be the smaller of the two — "
          + derivedWire.length + " vs " + tupleWire.length);
      final byte[] tupleFrame = frameOfRoundTrip(config, fixture, false);
      final byte[] derivedFrame = frameOfRoundTrip(config, fixture, true);
      assertArrayEqualsWithFixture(fixture, tupleFrame, derivedFrame);
    }
  }

  private byte[] wireOf(final ResourceConfiguration config, final Fixture fixture, final boolean derived) {
    PageKind.DERIVED_ELISION_SECTIONS = derived;
    final KeyValueLeafPage page = newPage(config);
    try {
      fill(page, fixture);
      return serialize(config, page);
    } finally {
      page.close();
    }
  }

  @Test
  @DisplayName("every record reads back with the exact bytes it was written with")
  void recordsRoundTripThroughTheDerivedSections() {
    PageKind.DERIVED_ELISION_SECTIONS = true;
    final ResourceConfiguration config = newConfig(false);
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage back = null;
    try {
      fillMixed(page, false);
      back = roundTrip(config, page);
      for (int slot = 0; slot < MIXED_SLOTS; slot++) {
        assertArrayEquals(page.getSlotAsByteArray(slot), back.getSlotAsByteArray(slot), "slot " + slot);
      }
    } finally {
      if (back != null) {
        back.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("a suppressed string tag keeps its slots out of the elided set")
  void aSuppressedStringTagIsNotElided() {
    PageKind.DERIVED_ELISION_SECTIONS = true;
    final ResourceConfiguration config = newConfig(false);
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage back = null;
    try {
      fillMixed(page, false);
      // Past the fused cap: the value becomes an overflow descriptor and its TAG is suppressed from
      // the string region, so no slot of that tag can be elided — the bitmap has to say so.
      writeString(page, MIXED_SLOTS, FIRST_NAME_KEY + 90, -1L, "X".repeat(4_000));
      writeString(page, MIXED_SLOTS + 1, FIRST_NAME_KEY + 90, -1L, "short one");
      serialize(config, page);
      final ElisionDeriver deriver = PageKind.writerElisionDeriverForTesting();
      assertFalse(deriver.allCandidatesElided(), "the suppressed tag's slots are candidates that are not elided");
      assertNoExceptions(deriver);
      back = roundTrip(config, page);
      assertArrayEquals(page.getSlotAsByteArray(MIXED_SLOTS + 1), back.getSlotAsByteArray(MIXED_SLOTS + 1),
          "the suppressed tag's short value stays inline and must read back unchanged");
    } finally {
      if (back != null) {
        back.close();
      }
      page.close();
    }
  }

  @Test
  @DisplayName("a region-only read of the derived page yields the values the record path yields")
  void regionOnlyReadsAgreeWithTheRecordPath() {
    PageKind.DERIVED_ELISION_SECTIONS = true;
    final ResourceConfiguration config = newConfig(false);
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage back = null;
    RegionsOnlyPage regionsOnly = null;
    try {
      fillMixed(page, false);
      PageKind.resetStickyCodecElectionForCurrentThread();
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      final byte[] wire = sink.toByteArray();

      final BytesIn<?> recordSource = Bytes.elasticOffHeapByteBuffer().write(wire).bytesForRead();
      recordSource.readByte();
      back = (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, recordSource, SerializationType.DATA);

      final BytesIn<?> regionSource = Bytes.elasticOffHeapByteBuffer().write(wire).bytesForRead();
      regionSource.readByte();
      regionsOnly = PageKind.KEYVALUELEAFPAGE.deserializeRegionsOnlyPage(config, regionSource,
          RegionTable.maskOf(RegionTable.KIND_NUMBER), 0);

      final NumberRegion.Header header = regionsOnly.numberHeaderInto(new NumberRegion.Header());
      assertNotNull(header, "the mixed fixture must publish a number region");
      final long[] fromRegion = new long[header.count];
      for (int i = 0; i < header.count; i++) {
        fromRegion[i] = NumberRegion.decodeValueAt(regionsOnly.regionPayload(RegionTable.KIND_NUMBER), header, i);
      }
      // The record path is the oracle: every long-valued number the page holds, taken off the
      // reconstructed heap, must be exactly the multiset the column serves without touching a record.
      final long[] fromRecords = new long[header.count];
      int found = 0;
      for (int slot = 0; slot < MIXED_SLOTS; slot++) {
        final long value = back.getFusedObjectNamedNumberValueLongFromSlot(slot);
        if (value != Long.MIN_VALUE) {
          assertTrue(found < fromRecords.length, "the record path holds more numbers than the region does");
          fromRecords[found++] = value;
        }
      }
      assertEquals(header.count, found, "region and record path must agree on how many numbers the page holds");
      Arrays.sort(fromRegion);
      Arrays.sort(fromRecords);
      assertArrayEquals(fromRegion, fromRecords, "the region-only read must yield the record path's values");
    } finally {
      if (regionsOnly != null) {
        regionsOnly.close();
      }
      if (back != null) {
        back.close();
      }
      page.close();
    }
  }

  // ────────────────────────────────────────────────────────────── exceptions

  @Test
  @DisplayName("a Long boxed inside int range takes a type exception, and mis-reads without it")
  void aLongInsideIntRangeTakesATypeException() {
    PageKind.DERIVED_ELISION_SECTIONS = true;
    final ResourceConfiguration config = newConfig(false);
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage back = null;
    try {
      buildLongInsideIntRange(page);
      serialize(config, page);
      final ElisionDeriver deriver = PageKind.writerElisionDeriverForTesting();
      assertEquals(1, deriver.typeExceptionCount(),
          "the subtype is derived by range, so a Long that fits an int is the one slot that deviates");
      assertEquals(0, deriver.indexExceptionCount());
      assertEquals(0, deriver.widthExceptionCount());
      back = roundTrip(config, page);
      assertArrayEquals(page.getSlotAsByteArray(1), back.getSlotAsByteArray(1),
          "the exception must restore the Long subtype's stored bytes");
      assertEquals(NUMBER_TYPE_LONG, payloadTypeByte(back, 1), "and the stored subtype must be Long, not Integer");
    } finally {
      if (back != null) {
        back.close();
      }
      page.close();
    }

    // Mutation: stage no exceptions and the same slot comes back as an Integer — the value survives,
    // its type does not. A silent change of a stored value's type is exactly what the list prevents.
    ElisionDeriver.ASSUME_PREDICTED_FOR_TESTING = true;
    final KeyValueLeafPage mutated = newPage(config);
    KeyValueLeafPage mutatedBack = null;
    try {
      buildLongInsideIntRange(mutated);
      final byte[] original = mutated.getSlotAsByteArray(1);
      mutatedBack = roundTrip(config, mutated);
      final byte[] reread = mutatedBack.getSlotAsByteArray(1);
      assertNotEquals(hex(original), hex(reread),
          "without the type exception the derived reader must NOT reproduce the record");
      assertEquals(NUMBER_TYPE_INTEGER, payloadTypeByte(mutatedBack, 1),
          "and the way it fails is by handing back the derived subtype — a stored Long read as an Integer");
    } finally {
      if (mutatedBack != null) {
        mutatedBack.close();
      }
      mutated.close();
    }
  }

  @Test
  @DisplayName("a page without the tag source its regions need takes index exceptions, and fails loudly without them")
  void anUnderivableIndexTakesAnIndexException() {
    PageKind.DERIVED_ELISION_SECTIONS = true;
    // Path-tagged regions on a page with too few slots for the pathNodeKey column to pay for itself:
    // the tag of every elided slot is then unreachable without touching the record heap, which the
    // derivation may not do, so every index deviates.
    final ResourceConfiguration config = newConfig(true);
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage back = null;
    try {
      buildUnderivableTags(page);
      serialize(config, page);
      final ElisionDeriver deriver = PageKind.writerElisionDeriverForTesting();
      assertEquals(UNDERIVABLE_SLOTS, deriver.indexExceptionCount(),
          "with no pathNodeKey column on a path-tagged page every index has to be spelled out");
      back = roundTrip(config, page);
      for (int slot = 0; slot < UNDERIVABLE_SLOTS; slot++) {
        assertArrayEquals(page.getSlotAsByteArray(slot), back.getSlotAsByteArray(slot), "slot " + slot);
      }
    } finally {
      if (back != null) {
        back.close();
      }
      page.close();
    }

    // Mutation: stage no exceptions and the reader has nothing to fall back on. It must say so rather
    // than inject a plausible-looking value at a guessed index.
    ElisionDeriver.ASSUME_PREDICTED_FOR_TESTING = true;
    final KeyValueLeafPage mutated = newPage(config);
    try {
      buildUnderivableTags(mutated);
      final RuntimeException failure = assertThrows(RuntimeException.class, () -> {
        final KeyValueLeafPage reread = roundTrip(config, mutated);
        reread.close();
      }, "without the index exceptions the derived reader must refuse the page");
      assertTrue(String.valueOf(failure.getMessage()).contains("no derivable region index"),
          "and refuse it for the right reason, not by accident: " + failure.getMessage());
    } finally {
      mutated.close();
    }
  }

  // ───────────────────────────────────────────────────────────── kill switch

  @Test
  @DisplayName("the kill switch reproduces the pre-change bytes exactly")
  void killSwitchIsByteIdenticalToTheReferenceEncoder() {
    // The pin is of the pre-change encoder's bytes, so EVERY lever landed since has to be off for it
    // to mean what its name says.
    PageKind.DERIVED_ELISION_SECTIONS = false;
    PageKind.SIBLING_KEY_COLUMNS_ENABLED = false;
    StructuralKeyColumnCodec.RUN_LENGTH_LANE_ENABLED = false;
    NumberRegion.setPerTagWidthEnabled(false); // B5-c's per-tag number region, landed after the pin
    StringRegion.setPlainLaneEnabled(false); // B5-c's string-region framing, landed after the pin
    NumberRegion.setExternalHeaderEnabled(false); // B5-c's folded per-tag directory, landed after the pin
    final ResourceConfiguration config = newConfig(false);
    final KeyValueLeafPage page = newPage(config);
    try {
      fillMixed(page, false);
      PageKind.resetStickyCodecElectionForCurrentThread();
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
      assertEquals(REFERENCE_MIXED_PAGE_SHA256, sha256(sink.toByteArray()),
          "the kill switch must reproduce the pre-change encoding byte for byte");
    } finally {
      page.close();
    }
  }

  /**
   * SHA-256 of the mixed fixture's wire bytes as the encoder produced them before this change.
   *
   * <p>
   * Recorded by compiling this fixture against the pre-change classes — the working tree's page
   * encoder as of the codec-election fix and B5-d, with nothing of the derived sections on the
   * classpath — and hashing the same serialization there. A hex pin taken from the new encoder would
   * only prove it agrees with itself.
   */
  private static final String REFERENCE_MIXED_PAGE_SHA256 =
      "3b69ca017411b159958ae3e1f21aa8c398dc316a7f6dc0f0f41261bfcccb48e6";

  // ────────────────────────────────────────────────────────────── fixtures

  /** Slots the mixed fixture populates. */
  private static final int MIXED_SLOTS = 300;

  /** Of those, the ones a region can carry — every fifth slot holds a double, which none can. */
  private static final int MIXED_ELIDABLE_SLOTS = MIXED_SLOTS - MIXED_SLOTS / 5;

  /** Slots the underivable-tag fixture populates. */
  private static final int UNDERIVABLE_SLOTS = 8;

  /**
   * The shapes the two readers are compared on. Each is a page the writer can actually produce, and
   * each stresses a different corner of the derivation.
   */
  private enum Fixture {
    /** Numbers, strings, booleans and doubles under name-tagged regions. */
    MIXED_NAME_TAGGED(false),
    /** The same under path-tagged regions, with the pathNodeKey column carrying the tags. */
    MIXED_PATH_TAGGED(true),
    /** One slot only — the smallest page that can elide anything. */
    SINGLE_SLOT(false),
    /** Only doubles: no value-elision candidate at all, while the name keys still elide. */
    NO_VALUE_ELISION_CANDIDATES(false),
    /** A tag suppressed from the string region by an overflow descriptor. */
    SUPPRESSED_STRING_TAG(false),
    /** A slot whose stored string carries the FSST flag, restored verbatim without a symbol table. */
    COMPRESSED_STRING(false),
    /** A Long inside int range: the one deviation the type exception list exists for. */
    LONG_INSIDE_INT_RANGE(false),
    /** Path-tagged regions with no pathNodeKey column: every region index deviates. */
    UNDERIVABLE_TAGS(true);

    private final boolean pathSummary;

    Fixture(final boolean pathSummary) {
      this.pathSummary = pathSummary;
    }
  }

  private void fill(final KeyValueLeafPage page, final Fixture fixture) {
    switch (fixture) {
      case MIXED_NAME_TAGGED -> fillMixed(page, false);
      case MIXED_PATH_TAGGED -> fillMixed(page, true);
      case SINGLE_SLOT -> writeNumber(page, 0, FIRST_NAME_KEY, -1L, Integer.valueOf(7));
      case NO_VALUE_ELISION_CANDIDATES -> {
        for (int i = 0; i < 16; i++) {
          writeNumber(page, i, FIRST_NAME_KEY + i, -1L, Double.valueOf(i + 0.25));
        }
      }
      case SUPPRESSED_STRING_TAG -> {
        fillMixed(page, false);
        writeString(page, MIXED_SLOTS, FIRST_NAME_KEY + 90, -1L, "X".repeat(4_000));
        writeString(page, MIXED_SLOTS + 1, FIRST_NAME_KEY + 90, -1L, "short one");
      }
      case COMPRESSED_STRING -> {
        for (int i = 0; i < 24; i++) {
          writeString(page, i, FIRST_NAME_KEY + (i % 4), -1L, "plain-" + i);
        }
        writeCompressedString(page, 24, FIRST_NAME_KEY + 4, " pretend-fsst");
      }
      case LONG_INSIDE_INT_RANGE -> buildLongInsideIntRange(page);
      case UNDERIVABLE_TAGS -> buildUnderivableTags(page);
    }
  }

  /**
   * Numbers inside and outside int range, strings sharing tags, booleans, and doubles — the last of
   * which no region can carry, so the page has candidates it does not elide.
   */
  private void fillMixed(final KeyValueLeafPage page, final boolean pathTagged) {
    for (int i = 0; i < MIXED_SLOTS; i++) {
      final int nameKey = FIRST_NAME_KEY + (i % 20);
      final long pathNodeKey = pathTagged
          ? FIRST_PATH_NODE_KEY + (i % 20)
          : -1L;
      switch (i % 5) {
        case 0 -> writeNumber(page, i, nameKey, pathNodeKey, Integer.valueOf(i * 7));
        case 1 -> writeNumber(page, i, nameKey, pathNodeKey, Long.valueOf((1L << 40) + i));
        case 2 -> writeString(page, i, nameKey, pathNodeKey, "v" + (i % 13) + '-' + "x".repeat(12));
        case 3 -> writeBoolean(page, i, nameKey, pathNodeKey, (i & 1) == 0);
        default -> writeNumber(page, i, nameKey, pathNodeKey, Double.valueOf(i + 0.5));
      }
    }
  }

  private void buildLongInsideIntRange(final KeyValueLeafPage page) {
    writeNumber(page, 0, FIRST_NAME_KEY, -1L, Integer.valueOf(41));
    writeNumber(page, 1, FIRST_NAME_KEY + 1, -1L, Long.valueOf(42L));
    for (int i = 2; i < 40; i++) {
      writeNumber(page, i, FIRST_NAME_KEY + (i % 6), -1L, Integer.valueOf(i));
    }
  }

  private void buildUnderivableTags(final KeyValueLeafPage page) {
    for (int i = 0; i < UNDERIVABLE_SLOTS; i++) {
      // A distinct pathNodeKey per slot makes the column's own dictionary cost more than the varints
      // it would replace, so the writer keeps the keys inline and the column is never written.
      writeNumber(page, i, FIRST_NAME_KEY + i, FIRST_PATH_NODE_KEY + i, Integer.valueOf(i * 11));
    }
  }

  // ──────────────────────────────────────────────────────────────── helpers

  private static void assertNoExceptions(final ElisionDeriver deriver) {
    assertEquals(0, deriver.typeExceptionCount(), "no type byte should deviate from its derivation");
    assertEquals(0, deriver.indexExceptionCount(), "no region index should deviate from its derivation");
    assertEquals(0, deriver.widthExceptionCount(), "no heap width should deviate from its derivation");
    assertEquals(0, deriver.nameKeyExceptionCount(), "no name-key width should deviate from its derivation");
  }

  /**
   * Serialize the fixture under one section form, read it back, and return the reconstructed frame —
   * header, bitmap, directory and heap — so the two forms can be compared without decoding a record.
   */
  private byte[] frameOfRoundTrip(final ResourceConfiguration config, final Fixture fixture, final boolean derived) {
    PageKind.DERIVED_ELISION_SECTIONS = derived;
    final KeyValueLeafPage page = newPage(config);
    KeyValueLeafPage back = null;
    try {
      fill(page, fixture);
      back = roundTrip(config, page);
      final MemorySegment frame = back.getSlottedPage();
      final int used = PageLayout.HEAP_START + PageLayout.getHeapUsed(frame);
      final byte[] copy = new byte[used];
      MemorySegment.copy(frame, ValueLayout.JAVA_BYTE, 0L, copy, 0, used);
      return copy;
    } finally {
      if (back != null) {
        back.close();
      }
      page.close();
    }
  }

  private static void assertArrayEqualsWithFixture(final Fixture fixture, final byte[] expected, final byte[] actual) {
    assertEquals(expected.length, actual.length, fixture + ": the two readers rebuilt heaps of different sizes");
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] != actual[i]) {
        assertEquals(expected[i], actual[i], fixture + ": the rebuilt frames differ at byte " + i);
      }
    }
  }

  /**
   * The stored number subtype of a fused {@code OBJECT_NAMED_NUMBER} slot — the first byte of its
   * payload field, which is what the derivation predicts and the type exception list corrects.
   */
  private static byte payloadTypeByte(final KeyValueLeafPage page, final int slot) {
    final byte[] record = page.getSlotAsByteArray(slot);
    final int kindId = record[0] & 0xFF;
    final int fieldCount = NodeFieldLayout.fieldCountForKind(kindId);
    final int payloadOffset = record[1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD] & 0xFF;
    return record[1 + fieldCount + payloadOffset];
  }

  private static String hex(final byte[] bytes) {
    final StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (final byte b : bytes) {
      sb.append(Character.forDigit((b >>> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }

  private static ResourceConfiguration newConfig(final boolean pathSummary) {
    return new ResourceConfiguration.Builder("derivedElision").buildPathSummary(pathSummary).build();
  }

  private KeyValueLeafPage newPage(final ResourceConfiguration config) {
    return new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, arena.allocate(MemorySegmentAllocator.SIXTYFOUR_KB),
        null);
  }

  private static byte[] serialize(final ResourceConfiguration config, final KeyValueLeafPage page) {
    PageKind.resetStickyCodecElectionForCurrentThread();
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
    return sink.toByteArray();
  }

  private static KeyValueLeafPage roundTrip(final ResourceConfiguration config, final KeyValueLeafPage page) {
    PageKind.resetStickyCodecElectionForCurrentThread();
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
    final BytesIn<?> source = sink.bytesForRead();
    source.readByte();
    return (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
  }

  private void writeString(final KeyValueLeafPage page, final long nodeKey, final int nameKey, final long pathNodeKey,
      final String value) {
    final ObjectNamedStringNode node = new ObjectNamedStringNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, pathNodeKey, 0,
        0, 0L, value.getBytes(StandardCharsets.UTF_8), HASH_FN, (byte[]) null, false, null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1)));
  }

  /**
   * A fused string whose stored bytes carry the FSST flag. Production shredders do not compress fused
   * strings today, so this is the only way to exercise the stored-form flag — which the derivation
   * predicts as "raw" and the type exception list has to restore.
   */
  private void writeCompressedString(final KeyValueLeafPage page, final long nodeKey, final int nameKey,
      final String storedForm) {
    final ObjectNamedStringNode node = new ObjectNamedStringNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, -1L, 0, 0, 0L,
        storedForm.getBytes(StandardCharsets.UTF_8), HASH_FN, (byte[]) null, true, null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1)));
  }

  private void writeNumber(final KeyValueLeafPage page, final long nodeKey, final int nameKey, final long pathNodeKey,
      final Number value) {
    final ObjectNamedNumberNode node = new ObjectNamedNumberNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, pathNodeKey, 0,
        0, 0L, value, HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1)));
  }

  private void writeBoolean(final KeyValueLeafPage page, final long nodeKey, final int nameKey, final long pathNodeKey,
      final boolean value) {
    final ObjectNamedBooleanNode node = new ObjectNamedBooleanNode(nodeKey, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), nameKey, pathNodeKey, 0,
        0, 0L, value, HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1)));
  }

  private static String sha256(final byte[] bytes) {
    try {
      final MessageDigest digest = MessageDigest.getInstance("SHA-256");
      final byte[] hash = digest.digest(bytes);
      final StringBuilder sb = new StringBuilder(hash.length * 2);
      for (final byte b : hash) {
        sb.append(Character.forDigit((b >>> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
      }
      return sb.toString();
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
