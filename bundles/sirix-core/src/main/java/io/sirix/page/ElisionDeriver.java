/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.exception.SirixIOException;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.page.pax.BooleanRegion;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.ObjectKeyNameKeyRegion;
import io.sirix.page.pax.PathNodeKeyRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.StringRegion;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

/**
 * Derives the per-slot metadata the value- and name-key-elision sections used to spell out.
 *
 * <p>
 * A record page's elision sections named every elided slot with a four-field tuple — a slot gap, a
 * type byte, the original heap width and the value's absolute index in its PAX region — at four to
 * five bytes per elided slot. Every one of those fields is a function of what the page already
 * carries:
 *
 * <ul>
 * <li><b>which slots</b> — a bitmap over the populated slots, or a single flag when every slot
 * whose compact-directory kind is a fused primitive was elided;</li>
 * <li><b>the region index</b> — a region stores its values grouped by tag and slot-ascending within
 * a tag, so the <i>k</i>-th elided slot of tag {@code T} sits at {@code tagStart[T] + k}. The tag
 * is the slot's pathNodeKey or nameKey (whichever the region's {@code tagKind} says), read from the
 * page's pathNodeKey column or its {@link ObjectKeyNameKeyRegion} — never from the record heap,
 * which is not expanded yet when the derivation runs;</li>
 * <li><b>the type byte</b> — {@code 0} for a boolean and for a raw string, and for a number the
 * subtype the writer picks by range: {@code Integer} inside int range, {@code Long} outside it (see
 * {@code NodeKind.serializeNumber} and the bulk scanner's number classification);</li>
 * <li><b>the width</b> — the exact byte count the injection pass writes back: one type byte plus
 * the value's signed varint for a number, one flag byte plus a length varint plus the dictionary
 * entry for a string, one byte for a boolean;</li>
 * <li><b>the name-key width</b> — the canonical signed-varint width of the nameKey the page's
 * {@link ObjectKeyNameKeyRegion} holds for that slot.</li>
 * </ul>
 *
 * <p>
 * A derivation that is merely usually right would corrupt pages. So the writer runs <em>this same
 * code</em> over the page it is about to emit and compares every derived field against the value it
 * actually holds; each disagreement becomes one entry in a sparse exception list keyed by the
 * slot's ordinal among the elided slots. On the fixtures and on bulk-loaded data those lists are
 * empty, and the whole section is one flag byte. Where they are not — a page whose pathNodeKey
 * column did not pay for itself, a {@code Long}-boxed value inside int range, a string region whose
 * tag also holds array elements — the exception restores the exact original and the page still
 * round-trips.
 *
 * <p>
 * Reader and writer share one instance shape and one code path, so the two cannot drift: an
 * agreement re-derived independently on each side is exactly the desync that kept value elision
 * all-or-nothing before.
 *
 * <p>
 * HFT contract: one instance per thread, rebound per page, no allocation on the hot path beyond the
 * one-off growth of the per-tag rank counters.
 */
final class ElisionDeriver {

  // ────────────────────────────────────────────────────────── section flag bits

  /** Every candidate slot on the page is elided, so no elided-slot bitmap follows. */
  static final int VE_FLAG_ALL_CANDIDATES = 0x01;

  /** A type-byte exception list follows. */
  static final int VE_FLAG_TYPE_EXCEPTIONS = 0x02;

  /** A region-index exception list follows. */
  static final int VE_FLAG_INDEX_EXCEPTIONS = 0x04;

  /**
   * An original-heap-width exception list follows.
   *
   * <p>
   * <b>Empty on every page this writer can produce, and that is a property rather than an
   * accident.</b> Once the type byte and the region value are settled, the width is a pure function
   * of them: one type byte plus {@code DeltaVarIntCodec}'s deterministic signed varint for a number,
   * one flag byte plus a length varint plus the dictionary entry for a string, one byte for a
   * boolean. For a deviation to exist the record heap would have to disagree with the region about
   * the value it holds — which is corruption, not a shape, and the injection pass already refuses it:
   * it recomputes the width as it writes and throws on a mismatch rather than laying bytes down at
   * the recorded one.
   *
   * <p>
   * The list is implemented, sized and parsed all the same. It costs nothing while empty, it keeps
   * the three derived fields symmetric, and the day a heap encoder stops being canonical — a padded
   * payload, a second varint flavour — the format already has the room. What it is NOT is a witnessed
   * path: there is no fixture in the suite that populates it, because fabricating one would mean
   * reaching past the encoder through a seam and pinning a page no writer can emit.
   */
  static final int VE_FLAG_WIDTH_EXCEPTIONS = 0x08;

  /** A name-key width exception list follows. */
  static final int NK_FLAG_WIDTH_EXCEPTIONS = 0x01;

  /** Type byte for {@code Integer} payloads (matches {@code NodeKind.serializeNumber}). */
  static final byte NUMBER_TYPE_INTEGER = 2;

  /** Type byte for {@code Long} payloads. */
  static final byte NUMBER_TYPE_LONG = 3;

  /** Returned by {@link #predictIndex} when the page carries no tag source for the slot. */
  static final int INDEX_UNDERIVABLE = -1;

  /**
   * Test seam: when set, the writer trusts its derivation and stages no exception lists at all.
   *
   * <p>
   * This is the mutation the exception lists exist for — "assume predicted". A fixture that carries a
   * genuine deviation must fail loudly under it, and the witness that it does is what proves the
   * lists are load-bearing rather than decorative. Never set outside a test; the writer reads it once
   * per page.
   */
  static boolean ASSUME_PREDICTED_FOR_TESTING;

  // ─────────────────────────────────────────────────────────────── bound state

  private MemorySegment numberPayload;
  private MemorySegment stringPayload;
  private MemorySegment booleanPayload;
  private MemorySegment nameKeyPayload;
  private byte[] pathNodeKeyColumn;

  private final NumberRegion.Header numberHeader = new NumberRegion.Header();
  private final StringRegion.Header stringHeader = new StringRegion.Header();
  private final BooleanRegion.Header booleanHeader = new BooleanRegion.Header();
  private boolean numberReady;
  private boolean stringReady;
  private boolean booleanReady;

  /**
   * Bulk-decoded number values, non-null only for a delta-encoded region: random access into a delta
   * region is O(index), so a per-slot walk over one would be quadratic.
   */
  private long[] numberValues;

  /** Backing store for {@link #numberValues}; grown, never shrunk, reused across pages. */
  private long[] numberValuesScratch = new long[PageLayout.SLOT_COUNT];

  private int[] numberRank = new int[64];
  private int[] stringRank = new int[64];
  private int[] booleanRank = new int[64];

  /** Tag id the last {@link #predictIndex} resolved, or -1. Feeds the string width derivation. */
  private int predictedTagId;

  /** Region index the last {@link #predictIndex} returned, so a caller can tell a hint apart. */
  private int predictedIndex;

  // ───────────────────────────────────────────────────── per-slot derivation out

  /** Type byte derived by the last {@link #deriveTypeAndWidth}. */
  byte derivedType;

  /** Original heap width derived by the last {@link #deriveTypeAndWidth}. */
  int derivedWidth;

  // ────────────────────────────────────────────────────────── exception buffers

  /**
   * One sparse exception list: the ordinals, among the section's elided slots, whose derived field
   * disagreed with what the writer held, and the values that replace the derivation there.
   *
   * <p>
   * Grown on demand and reused across pages; a page whose derivation is exact leaves every list at
   * {@code count == 0} and stages nothing for it.
   */
  private static final class ExceptionList {
    private int[] ordinals = new int[16];
    private int[] intValues;
    private byte[] byteValues;
    private int count;

    ExceptionList(final boolean byteValued) {
      if (byteValued) {
        byteValues = new byte[16];
      } else {
        intValues = new int[16];
      }
    }

    void clear() {
      count = 0;
    }

    void add(final int ordinal, final int intValue, final byte byteValue) {
      if (count == ordinals.length) {
        ordinals = Arrays.copyOf(ordinals, ordinals.length * 2);
        if (intValues != null) {
          intValues = Arrays.copyOf(intValues, intValues.length * 2);
        } else {
          byteValues = Arrays.copyOf(byteValues, byteValues.length * 2);
        }
      }
      ordinals[count] = ordinal;
      if (intValues != null) {
        intValues[count] = intValue;
      } else {
        byteValues[count] = byteValue;
      }
      count++;
    }

    void ensureCapacity(final int needed) {
      if (ordinals.length < needed) {
        ordinals = new int[needed];
        if (intValues != null) {
          intValues = new int[needed];
        } else {
          byteValues = new byte[needed];
        }
      }
    }
  }

  private final ExceptionList typeExceptions = new ExceptionList(true);
  private final ExceptionList indexExceptions = new ExceptionList(false);
  private final ExceptionList widthExceptions = new ExceptionList(false);
  private final ExceptionList nameKeyExceptions = new ExceptionList(true);

  /** Entry-indexed bitmap of the elided slots; only the first {@code ceil(N/8)} bytes are live. */
  private final byte[] elidedBitmap = new byte[(PageLayout.SLOT_COUNT + 7) >>> 3];

  private boolean allCandidatesElided;
  private int elidedCount;
  private int plannedValueSectionBytes;
  private int plannedNameKeySectionBytes;

  // ─────────────────────────────────────────────────────────────────── binding

  /**
   * Bind a page's regions and its pathNodeKey column, dropping the previous page's state.
   *
   * @param regions the page's region table, or {@code null} when it has none
   * @param pathNodeKeyColumnBytes the raw pathNodeKey column payload, or {@code null} when the page
   *        did not write one — a PATH_NODE-tagged region is then underivable and every one of its
   *        slots takes an index exception
   */
  void bind(final RegionTable regions, final byte[] pathNodeKeyColumnBytes) {
    pathNodeKeyColumn = pathNodeKeyColumnBytes;
    numberPayload = null;
    stringPayload = null;
    booleanPayload = null;
    nameKeyPayload = null;
    numberReady = false;
    stringReady = false;
    booleanReady = false;
    numberValues = null;
    predictedTagId = -1;
    predictedIndex = -1;
    if (regions == null) {
      return;
    }
    nameKeyPayload = nonEmpty(regions.payload(RegionTable.KIND_OBJECT_KEY_NAMEKEY));
    numberPayload = nonEmpty(regions.payload(RegionTable.KIND_NUMBER));
    stringPayload = nonEmpty(regions.payload(RegionTable.KIND_STRING));
    booleanPayload = nonEmpty(regions.payload(RegionTable.KIND_BOOLEAN));
    if (numberPayload != null) {
      // The per-tag directory may live in the zone map rather than in the value region; a page is
      // read whole here, so both are in the table whenever the writer published both. Without the
      // directory the region is unreadable and every slot has to take an index exception, which is
      // what numberReady staying false does.
      final MemorySegment numberDirectory = nonEmpty(regions.payload(RegionTable.KIND_NUMBER_ZONEMAP));
      if (numberDirectory == null && NumberRegion.needsExternalDirectory(numberPayload)) {
        numberPayload = null;
        return;
      }
      numberHeader.parseInto(numberPayload, numberDirectory);
      numberReady = true;
      numberRank = clearedRanks(numberRank, numberHeader.dictSize);
      if (NumberRegion.isDelta(numberHeader.encodingKind)) {
        if (numberValuesScratch.length < numberHeader.count) {
          numberValuesScratch = new long[Math.max(numberHeader.count, numberValuesScratch.length * 2)];
        }
        NumberRegion.decodeAllValues(numberPayload, numberHeader, numberValuesScratch);
        numberValues = numberValuesScratch;
      }
    }
    if (stringPayload != null) {
      stringHeader.parseInto(stringPayload);
      stringReady = true;
      stringRank = clearedRanks(stringRank, stringHeader.parentDictSize);
    }
    if (booleanPayload != null) {
      booleanHeader.parseInto(booleanPayload);
      booleanReady = true;
      booleanRank = clearedRanks(booleanRank, booleanHeader.dictSize);
    }
  }

  private static MemorySegment nonEmpty(final MemorySegment payload) {
    return payload == null || payload.byteSize() == 0
        ? null
        : payload;
  }

  private static int[] clearedRanks(final int[] ranks, final int dictSize) {
    if (ranks.length < dictSize) {
      return new int[Math.max(dictSize, ranks.length * 2)];
    }
    Arrays.fill(ranks, 0, dictSize, 0);
    return ranks;
  }

  /** Whether this page carries the name-key region a name-key width derivation reads. */
  boolean hasNameKeyRegion() {
    return nameKeyPayload != null;
  }

  // ──────────────────────────────────────────────────────────────── derivation

  /**
   * The region index the {@code k}-th elided slot of its tag must hold, advancing that tag's rank.
   *
   * <p>
   * Must be called for every elided slot in ascending slot order and for that slot's kind, on both
   * the writer and the reader: the rank counters are the derivation's only state, and skipping a slot
   * on one side shifts every later slot of the same tag on that side alone.
   *
   * @param slot the elided slot
   * @param kindId the slot's node kind id, as the compact directory carries it
   * @return the predicted absolute region index, or {@link #INDEX_UNDERIVABLE} when the page holds no
   *         tag source for the slot — the caller must then have (writer) or find (reader) an
   *         exception entry for it
   */
  int predictIndex(final int slot, final int kindId) {
    predictedTagId = -1;
    predictedIndex = INDEX_UNDERIVABLE;
    if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
      if (!numberReady) {
        return INDEX_UNDERIVABLE;
      }
      final int tagId = resolveTagId(slot, numberHeader.tagKind, 0);
      if (tagId < 0) {
        return INDEX_UNDERIVABLE;
      }
      predictedTagId = tagId;
      predictedIndex = numberHeader.tagStart[tagId] + numberRank[tagId]++;
      return predictedIndex;
    }
    if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID) {
      if (!stringReady) {
        return INDEX_UNDERIVABLE;
      }
      final int tagId = resolveTagId(slot, stringHeader.tagKind, 1);
      if (tagId < 0) {
        return INDEX_UNDERIVABLE;
      }
      predictedTagId = tagId;
      predictedIndex = stringHeader.tagStart[tagId] + stringRank[tagId]++;
      return predictedIndex;
    }
    if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID) {
      if (!booleanReady) {
        return INDEX_UNDERIVABLE;
      }
      final int tagId = resolveTagId(slot, booleanHeader.tagKind, 2);
      if (tagId < 0) {
        return INDEX_UNDERIVABLE;
      }
      predictedTagId = tagId;
      predictedIndex = booleanHeader.tagStart[tagId] + booleanRank[tagId]++;
      return predictedIndex;
    }
    return INDEX_UNDERIVABLE;
  }

  /**
   * The tag dictionary id of {@code slot} in the region identified by {@code regionSelector}.
   *
   * <p>
   * The tag itself comes from the page's columns, never from the heap: the pathNodeKey column for a
   * PATH_NODE-tagged region, the name-key region for a NAME-tagged one. Both lookups return -1 for a
   * slot they do not carry, and a -1 tag simply fails the dictionary probe — the writer sees the same
   * failure and writes the exception, so an ambiguous sentinel cannot mis-inject.
   */
  private int resolveTagId(final int slot, final byte tagKind, final int regionSelector) {
    final int tag;
    if (tagKind == NumberRegion.TAG_KIND_PATH_NODE) {
      if (pathNodeKeyColumn == null) {
        return -1;
      }
      tag = PathNodeKeyRegion.pathNodeKeyForSlot(pathNodeKeyColumn, slot);
      if (tag < 0) {
        return -1;
      }
    } else {
      if (nameKeyPayload == null) {
        return -1;
      }
      tag = ObjectKeyNameKeyRegion.nameKeyForSlot(nameKeyPayload, slot);
    }
    return switch (regionSelector) {
      case 0 -> NumberRegion.lookupTag(numberHeader, tag);
      case 1 -> StringRegion.lookupTag(stringHeader, tag);
      default -> BooleanRegion.lookupTag(booleanHeader, tag);
    };
  }

  /**
   * Derive the type byte and the original heap width of an elided slot, into {@link #derivedType} and
   * {@link #derivedWidth}.
   *
   * <p>
   * The reader has laid nothing out yet when this runs, so a bad index has to be refused here rather
   * than mis-injected later: an index outside the region's value count, or a string index no tag
   * range contains, throws instead of returning a plausible width.
   *
   * @param slot the elided slot, for diagnostics
   * @param kindId the slot's node kind id
   * @param absIdx the settled absolute region index — the prediction, or an exception's value
   */
  void deriveTypeAndWidth(final int slot, final int kindId, final int absIdx) {
    if (absIdx < 0) {
      throw new SirixIOException("value elision: slot " + slot + " has no derivable region index and no exception");
    }
    if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
      if (!numberReady) {
        throw new SirixIOException("value elision: slot " + slot + " elides a number the page has no region for");
      }
      if (absIdx >= numberHeader.count) {
        throw new SirixIOException("value elision: NUMBER index " + absIdx + " at slot " + slot
            + " is past the region's " + numberHeader.count + " values");
      }
      final long value = numberValues != null
          ? numberValues[absIdx]
          : NumberRegion.decodeValueAt(numberPayload, numberHeader, absIdx);
      if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
        derivedType = NUMBER_TYPE_INTEGER;
        derivedWidth = 1 + DeltaVarIntCodec.computeSignedEncodedWidth((int) value);
      } else {
        derivedType = NUMBER_TYPE_LONG;
        derivedWidth = 1 + DeltaVarIntCodec.computeSignedLongEncodedWidth(value);
      }
      return;
    }
    if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID) {
      if (!stringReady) {
        throw new SirixIOException("value elision: slot " + slot + " elides a string the page has no region for");
      }
      if (absIdx >= stringHeader.count) {
        throw new SirixIOException("value elision: STRING index " + absIdx + " at slot " + slot
            + " is past the region's " + stringHeader.count + " values");
      }
      final int tagId = absIdx == predictedIndex && predictedTagId >= 0
          ? predictedTagId
          : tagIdForAbsoluteIndex(stringHeader.tagStart, stringHeader.tagCount, stringHeader.parentDictSize, absIdx);
      if (tagId < 0) {
        throw new SirixIOException("value elision: no STRING tag range contains index " + absIdx + " at slot " + slot);
      }
      final int dictId = StringRegion.decodeDictIdAt(stringPayload, stringHeader, absIdx);
      final int length = StringRegion.decodeStringLength(stringPayload, stringHeader, tagId, dictId);
      if (length < 0) {
        throw new SirixIOException(
            "value elision: STRING entry " + absIdx + " at slot " + slot + " has length " + length);
      }
      derivedType = 0;
      derivedWidth = 1 + DeltaVarIntCodec.computeSignedEncodedWidth(length) + length;
      return;
    }
    if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID) {
      if (!booleanReady) {
        throw new SirixIOException("value elision: slot " + slot + " elides a boolean the page has no region for");
      }
      if (absIdx >= booleanHeader.count) {
        throw new SirixIOException("value elision: BOOLEAN index " + absIdx + " at slot " + slot
            + " is past the region's " + booleanHeader.count + " values");
      }
      derivedType = 0;
      derivedWidth = 1;
      return;
    }
    throw new SirixIOException(
        "value elision names slot " + slot + ", whose kind " + kindId + " has no fused-primitive payload");
  }

  /**
   * The canonical signed-varint width of the nameKey the page's name-key region holds for
   * {@code slot} — what the name-key elision section used to spell out per elided slot.
   */
  int nameKeyWidthForSlot(final int slot) {
    if (nameKeyPayload == null) {
      throw new SirixIOException("name-key elision: the page elides name keys but carries no name-key region");
    }
    return DeltaVarIntCodec.computeSignedEncodedWidth(ObjectKeyNameKeyRegion.nameKeyForSlot(nameKeyPayload, slot));
  }

  /**
   * The tag whose half-open value range {@code [tagStart, tagStart + tagCount)} contains
   * {@code absIdx}, or -1. Linear over the tag dictionary, and only ever reached for an exception
   * entry.
   */
  private static int tagIdForAbsoluteIndex(final int[] tagStart, final int[] tagCount, final int dictSize,
      final int absIdx) {
    for (int t = 0; t < dictSize; t++) {
      if (absIdx >= tagStart[t] && absIdx < tagStart[t] + tagCount[t]) {
        return t;
      }
    }
    return -1;
  }

  /** Whether {@code kindId} is one of the three fused primitives value elision can cover. */
  static boolean isCandidateKindId(final int kindId) {
    return kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID
        || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID
        || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID;
  }

  // ──────────────────────────────────────────────────────────── writer: sizing

  /**
   * Plan the derived value-elision section for the page the writer is staging.
   *
   * <p>
   * Runs the reader's derivation over the writer's own page and records an exception wherever the two
   * disagree, so the section's size is exact before a byte of it is staged.
   *
   * @param populatedCount number of populated entries
   * @param slotKindIds per-entry node kind id
   * @param slotBits per-entry slot id
   * @param slotValueElided per-entry elision marker; zero when the entry keeps its value inline
   * @param slotDiskTypes per-entry type byte the legacy tuple carried
   * @param slotValueWidths per-entry original heap width
   * @param slotRegionAbsIdx slot-indexed absolute region index assigned by the region build
   * @param elidedSlotCount number of entries with a non-zero marker
   * @param candidateSlotCount number of entries whose kind is a fused primitive
   * @return the exact number of bytes {@link #encodeValueSection} will write
   */
  int planValueSection(final int populatedCount, final int[] slotKindIds, final short[] slotBits,
      final byte[] slotValueElided, final byte[] slotDiskTypes, final short[] slotValueWidths,
      final int[] slotRegionAbsIdx, final int elidedSlotCount, final int candidateSlotCount) {
    typeExceptions.clear();
    indexExceptions.clear();
    widthExceptions.clear();
    elidedCount = elidedSlotCount;
    allCandidatesElided = elidedSlotCount == candidateSlotCount;
    final int bitmapBytes = (populatedCount + 7) >>> 3;
    if (!allCandidatesElided) {
      Arrays.fill(elidedBitmap, 0, bitmapBytes, (byte) 0);
    }
    int ordinal = 0;
    for (int i = 0; i < populatedCount; i++) {
      if (slotValueElided[i] == 0) {
        continue;
      }
      if (!allCandidatesElided) {
        elidedBitmap[i >>> 3] |= (byte) (1 << (i & 7));
      }
      final int slot = slotBits[i] & 0xFFFF;
      final int kindId = slotKindIds[i];
      final int predicted = predictIndex(slot, kindId);
      final int actualIndex = slotRegionAbsIdx[slot];
      if (predicted != actualIndex && !ASSUME_PREDICTED_FOR_TESTING) {
        indexExceptions.add(ordinal, actualIndex, (byte) 0);
      }
      deriveTypeAndWidth(slot, kindId, actualIndex);
      final byte actualType = slotDiskTypes[i];
      if (derivedType != actualType && !ASSUME_PREDICTED_FOR_TESTING) {
        typeExceptions.add(ordinal, 0, actualType);
      }
      final int actualWidth = slotValueWidths[i] & 0xFFFF;
      if (derivedWidth != actualWidth && !ASSUME_PREDICTED_FOR_TESTING) {
        widthExceptions.add(ordinal, actualWidth, (byte) 0);
      }
      ordinal++;
    }
    if (ordinal != elidedSlotCount) {
      throw new SirixIOException(
          "value elision: planned " + ordinal + " elided slots, the pre-scan counted " + elidedSlotCount);
    }
    int bytes = 1;
    if (!allCandidatesElided) {
      bytes += bitmapBytes;
    }
    bytes += exceptionListBytes(typeExceptions);
    bytes += exceptionListBytes(indexExceptions);
    bytes += exceptionListBytes(widthExceptions);
    plannedValueSectionBytes = bytes;
    return bytes;
  }

  /**
   * Plan the derived name-key-elision section: one flag byte plus an exception per slot whose
   * stripped width is not the canonical varint width of the nameKey the region holds for it.
   *
   * @param populatedCount number of populated entries
   * @param slotBits per-entry slot id
   * @param slotNameKeyElided per-entry flag; non-zero when the writer strips the slot's name key
   * @param slotNameKeyWidths per-entry stripped width
   * @return the exact number of bytes {@link #encodeNameKeySection} will write
   */
  int planNameKeySection(final int populatedCount, final short[] slotBits, final byte[] slotNameKeyElided,
      final byte[] slotNameKeyWidths) {
    nameKeyExceptions.clear();
    int ordinal = 0;
    for (int i = 0; i < populatedCount; i++) {
      if (slotNameKeyElided[i] == 0) {
        continue;
      }
      final int slot = slotBits[i] & 0xFFFF;
      final int derived = nameKeyWidthForSlot(slot);
      final byte actual = slotNameKeyWidths[i];
      if (derived != (actual & 0xFF) && !ASSUME_PREDICTED_FOR_TESTING) {
        nameKeyExceptions.add(ordinal, 0, actual);
      }
      ordinal++;
    }
    plannedNameKeySectionBytes = 1 + exceptionListBytes(nameKeyExceptions);
    return plannedNameKeySectionBytes;
  }

  private static int exceptionListBytes(final ExceptionList list) {
    if (list.count == 0) {
      return 0;
    }
    int bytes = DeltaVarIntCodec.computeSignedEncodedWidth(list.count);
    int previous = -1;
    for (int e = 0; e < list.count; e++) {
      bytes += DeltaVarIntCodec.computeSignedEncodedWidth(list.ordinals[e] - previous);
      bytes += list.intValues != null
          ? DeltaVarIntCodec.computeSignedEncodedWidth(list.intValues[e])
          : 1;
      previous = list.ordinals[e];
    }
    return bytes;
  }

  /** Number of region-index exceptions the last plan or parse holds — read by the guard witnesses. */
  int indexExceptionCount() {
    return indexExceptions.count;
  }

  /** Number of type-byte exceptions the last plan or parse holds. */
  int typeExceptionCount() {
    return typeExceptions.count;
  }

  /** Number of heap-width exceptions the last plan or parse holds. */
  int widthExceptionCount() {
    return widthExceptions.count;
  }

  /** Number of name-key width exceptions the last plan or parse holds. */
  int nameKeyExceptionCount() {
    return nameKeyExceptions.count;
  }

  /** Whether the last plan or parse said every candidate slot on the page is elided. */
  boolean allCandidatesElided() {
    return allCandidatesElided;
  }

  /** Bytes the last {@code planValueSection} sized the value-elision section at. */
  int plannedValueSectionBytes() {
    return plannedValueSectionBytes;
  }

  /** Bytes the last {@code planNameKeySection} sized the name-key-elision section at. */
  int plannedNameKeySectionBytes() {
    return plannedNameKeySectionBytes;
  }

  // ────────────────────────────────────────────────────────── writer: encoding

  /**
   * Stage the planned value-elision section at {@code pos}.
   *
   * @return the number of bytes written, which equals the planned size
   */
  int encodeValueSection(final MemorySegment staging, final long pos, final int populatedCount) {
    long cursor = pos;
    int flags = allCandidatesElided
        ? VE_FLAG_ALL_CANDIDATES
        : 0;
    if (typeExceptions.count > 0) {
      flags |= VE_FLAG_TYPE_EXCEPTIONS;
    }
    if (indexExceptions.count > 0) {
      flags |= VE_FLAG_INDEX_EXCEPTIONS;
    }
    if (widthExceptions.count > 0) {
      flags |= VE_FLAG_WIDTH_EXCEPTIONS;
    }
    staging.set(ValueLayout.JAVA_BYTE, cursor, (byte) flags);
    cursor++;
    if (!allCandidatesElided) {
      final int bitmapBytes = (populatedCount + 7) >>> 3;
      MemorySegment.copy(elidedBitmap, 0, staging, ValueLayout.JAVA_BYTE, cursor, bitmapBytes);
      cursor += bitmapBytes;
    }
    cursor = encodeExceptionList(staging, cursor, typeExceptions);
    cursor = encodeExceptionList(staging, cursor, indexExceptions);
    cursor = encodeExceptionList(staging, cursor, widthExceptions);
    final int written = (int) (cursor - pos);
    if (written != plannedValueSectionBytes) {
      throw new SirixIOException(
          "value elision: staged " + written + " bytes for a section planned at " + plannedValueSectionBytes);
    }
    return written;
  }

  /**
   * Stage the planned name-key-elision section at {@code pos}.
   *
   * @return the number of bytes written, which equals the planned size
   */
  int encodeNameKeySection(final MemorySegment staging, final long pos) {
    long cursor = pos;
    staging.set(ValueLayout.JAVA_BYTE, cursor, (byte) (nameKeyExceptions.count > 0
        ? NK_FLAG_WIDTH_EXCEPTIONS
        : 0));
    cursor++;
    cursor = encodeExceptionList(staging, cursor, nameKeyExceptions);
    final int written = (int) (cursor - pos);
    if (written != plannedNameKeySectionBytes) {
      throw new SirixIOException(
          "name-key elision: staged " + written + " bytes for a section planned at " + plannedNameKeySectionBytes);
    }
    return written;
  }

  private static long encodeExceptionList(final MemorySegment staging, final long pos, final ExceptionList list) {
    if (list.count == 0) {
      return pos;
    }
    long cursor = pos;
    cursor += DeltaVarIntCodec.writeSignedToSegment(staging, cursor, list.count);
    int previous = -1;
    for (int e = 0; e < list.count; e++) {
      cursor += DeltaVarIntCodec.writeSignedToSegment(staging, cursor, list.ordinals[e] - previous);
      previous = list.ordinals[e];
      if (list.intValues != null) {
        cursor += DeltaVarIntCodec.writeSignedToSegment(staging, cursor, list.intValues[e]);
      } else {
        staging.set(ValueLayout.JAVA_BYTE, cursor, list.byteValues[e]);
        cursor++;
      }
    }
    return cursor;
  }

  // ────────────────────────────────────────────────────────── reader: decoding

  /**
   * Parse a derived value-elision section.
   *
   * @param section the decoded META bytes
   * @param pos offset of the section's first byte
   * @param populatedCount number of populated entries
   * @param limit one past the last byte the META sections occupy
   * @return the offset one past the section's last byte
   */
  long parseValueSection(final MemorySegment section, final long pos, final int populatedCount, final long limit) {
    long cursor = pos;
    if (cursor >= limit) {
      throw new SirixIOException("value elision: the section starts past the end of the page's metadata");
    }
    final int flags = section.get(ValueLayout.JAVA_BYTE, cursor) & 0xFF;
    cursor++;
    if ((flags & ~(VE_FLAG_ALL_CANDIDATES | VE_FLAG_TYPE_EXCEPTIONS | VE_FLAG_INDEX_EXCEPTIONS
        | VE_FLAG_WIDTH_EXCEPTIONS)) != 0) {
      throw new SirixIOException("value elision: unknown section flags 0x" + Integer.toHexString(flags));
    }
    allCandidatesElided = (flags & VE_FLAG_ALL_CANDIDATES) != 0;
    typeExceptions.clear();
    indexExceptions.clear();
    widthExceptions.clear();
    if (!allCandidatesElided) {
      final int bitmapBytes = (populatedCount + 7) >>> 3;
      if (cursor + bitmapBytes > limit) {
        throw new SirixIOException("value elision: the section is too short for its " + bitmapBytes + "-byte bitmap");
      }
      MemorySegment.copy(section, ValueLayout.JAVA_BYTE, cursor, elidedBitmap, 0, bitmapBytes);
      cursor += bitmapBytes;
    }
    if ((flags & VE_FLAG_TYPE_EXCEPTIONS) != 0) {
      cursor = parseExceptionList(typeExceptions, section, cursor, limit, populatedCount);
    }
    if ((flags & VE_FLAG_INDEX_EXCEPTIONS) != 0) {
      cursor = parseExceptionList(indexExceptions, section, cursor, limit, populatedCount);
    }
    if ((flags & VE_FLAG_WIDTH_EXCEPTIONS) != 0) {
      cursor = parseExceptionList(widthExceptions, section, cursor, limit, populatedCount);
    }
    return cursor;
  }

  /**
   * Parse a derived name-key-elision section.
   *
   * @param section the decoded META bytes
   * @param pos offset of the section's first byte
   * @param populatedCount number of populated entries
   * @param limit one past the last byte the META sections occupy
   * @return the offset one past the section's last byte
   */
  long parseNameKeySection(final MemorySegment section, final long pos, final int populatedCount, final long limit) {
    long cursor = pos;
    if (cursor >= limit) {
      throw new SirixIOException("name-key elision: the section starts past the end of the page's metadata");
    }
    final int flags = section.get(ValueLayout.JAVA_BYTE, cursor) & 0xFF;
    cursor++;
    if ((flags & ~NK_FLAG_WIDTH_EXCEPTIONS) != 0) {
      throw new SirixIOException("name-key elision: unknown section flags 0x" + Integer.toHexString(flags));
    }
    nameKeyExceptions.clear();
    if ((flags & NK_FLAG_WIDTH_EXCEPTIONS) != 0) {
      cursor = parseExceptionList(nameKeyExceptions, section, cursor, limit, populatedCount);
    }
    return cursor;
  }

  private static long parseExceptionList(final ExceptionList list, final MemorySegment section, final long pos,
      final long limit, final int populatedCount) {
    long cursor = pos;
    if (cursor >= limit) {
      throw new SirixIOException("elision exception list: truncated before its count");
    }
    final int count = DeltaVarIntCodec.decodeSignedFromSegment(section, cursor);
    cursor += DeltaVarIntCodec.computeSignedEncodedWidth(count);
    if (count <= 0 || count > populatedCount) {
      throw new SirixIOException(
          "elision exception list: invalid count " + count + " for " + populatedCount + " populated slots");
    }
    list.ensureCapacity(count);
    int previous = -1;
    for (int e = 0; e < count; e++) {
      if (cursor >= limit) {
        throw new SirixIOException("elision exception list: truncated at entry " + e);
      }
      final int gap = DeltaVarIntCodec.decodeSignedFromSegment(section, cursor);
      cursor += DeltaVarIntCodec.computeSignedEncodedWidth(gap);
      if (gap <= 0) {
        throw new SirixIOException("elision exception list: entry " + e + " has a non-ascending ordinal gap " + gap);
      }
      final int ordinal = previous + gap;
      if (ordinal >= populatedCount) {
        throw new SirixIOException("elision exception list: entry " + e + " names ordinal " + ordinal
            + ", past the page's " + populatedCount + " entries");
      }
      list.ordinals[e] = ordinal;
      previous = ordinal;
      if (cursor >= limit) {
        throw new SirixIOException("elision exception list: truncated in entry " + e + "'s value");
      }
      if (list.intValues != null) {
        final int value = DeltaVarIntCodec.decodeSignedFromSegment(section, cursor);
        cursor += DeltaVarIntCodec.computeSignedEncodedWidth(value);
        list.intValues[e] = value;
      } else {
        list.byteValues[e] = section.get(ValueLayout.JAVA_BYTE, cursor);
        cursor++;
      }
    }
    if (cursor > limit) {
      throw new SirixIOException("elision exception list: ran " + (cursor - limit) + " bytes past the metadata");
    }
    list.count = count;
    return cursor;
  }

  /**
   * Reconstruct the per-elided-entry arrays the expansion and injection passes read.
   *
   * <p>
   * Walks the populated entries in ascending order, decides membership from the bitmap — or from the
   * compact-directory kind when the page flagged every candidate elided — derives each member's
   * region index, type byte and heap width, and overrides each with an exception where the page
   * carries one.
   *
   * @param headerBitmapSeg the page's 160-byte header + slot bitmap, walked to map entry to slot
   * @param populatedCount number of populated entries
   * @param compactDir per-entry packed (on-disk length, kind id)
   * @param outSlots receives the elided slots, ascending
   * @param outTypes receives each elided slot's type byte
   * @param outWidths receives each elided slot's original heap width
   * @param outAbsIdx receives each elided slot's absolute region index
   * @return the number of elided entries written into the output arrays
   */
  int deriveValueMetadata(final MemorySegment headerBitmapSeg, final int populatedCount, final int[] compactDir,
      final short[] outSlots, final byte[] outTypes, final int[] outWidths, final int[] outAbsIdx) {
    int ordinal = 0;
    int typeCursor = 0;
    int indexCursor = 0;
    int widthCursor = 0;
    int bmIdx = 0;
    long bmWord = 0L;
    for (int i = 0; i < populatedCount; i++) {
      while (bmWord == 0) {
        bmWord = PageLayout.getBitmapWord(headerBitmapSeg, bmIdx++);
        if (bmIdx > PageLayout.BITMAP_WORDS) {
          throw new SirixIOException("value elision: bitmap exhausted at entry " + i + " / " + populatedCount);
        }
      }
      final int slot = ((bmIdx - 1) << 6) | Long.numberOfTrailingZeros(bmWord);
      bmWord &= bmWord - 1;
      final int kindId = PageLayout.unpackNodeKindId(compactDir[i]);
      final boolean candidate = isCandidateKindId(kindId);
      final boolean elided = allCandidatesElided
          ? candidate
          : ((elidedBitmap[i >>> 3] >>> (i & 7)) & 1) == 1;
      if (!elided) {
        continue;
      }
      if (!candidate) {
        throw new SirixIOException(
            "value elision names slot " + slot + ", whose kind " + kindId + " has no fused-primitive payload");
      }
      int absIdx = predictIndex(slot, kindId);
      if (indexCursor < indexExceptions.count && indexExceptions.ordinals[indexCursor] == ordinal) {
        absIdx = indexExceptions.intValues[indexCursor];
        indexCursor++;
      }
      deriveTypeAndWidth(slot, kindId, absIdx);
      byte type = derivedType;
      if (typeCursor < typeExceptions.count && typeExceptions.ordinals[typeCursor] == ordinal) {
        type = typeExceptions.byteValues[typeCursor];
        typeCursor++;
      }
      int width = derivedWidth;
      if (widthCursor < widthExceptions.count && widthExceptions.ordinals[widthCursor] == ordinal) {
        width = widthExceptions.intValues[widthCursor];
        widthCursor++;
      }
      if (width <= 0 || width > PageConstants.MAX_RECORD_SIZE) {
        throw new SirixIOException("value elision: slot " + slot + " derives an impossible heap width " + width);
      }
      outSlots[ordinal] = (short) slot;
      outTypes[ordinal] = type;
      outWidths[ordinal] = width;
      outAbsIdx[ordinal] = absIdx;
      ordinal++;
    }
    if (typeCursor < typeExceptions.count || indexCursor < indexExceptions.count
        || widthCursor < widthExceptions.count) {
      throw new SirixIOException(
          "value elision: the section carries exceptions for ordinals past its " + ordinal + " elided slots");
    }
    elidedCount = ordinal;
    return ordinal;
  }

  /**
   * Reconstruct the packed per-elided-slot name-key widths.
   *
   * <p>
   * Name-key elision is all-or-nothing over the page's fused {@code OBJECT_NAMED_*} slots, so the
   * covered set is exactly those, taken from the compact directory.
   *
   * @param headerBitmapSeg the page's 160-byte header + slot bitmap, walked to map entry to slot
   * @param populatedCount number of populated entries
   * @param compactDir per-entry packed (on-disk length, kind id)
   * @param outWidths receives one width per covered slot, ascending
   * @return the number of widths written
   */
  int deriveNameKeyWidths(final MemorySegment headerBitmapSeg, final int populatedCount, final int[] compactDir,
      final byte[] outWidths) {
    int ordinal = 0;
    int cursor = 0;
    int bmIdx = 0;
    long bmWord = 0L;
    for (int i = 0; i < populatedCount; i++) {
      while (bmWord == 0) {
        bmWord = PageLayout.getBitmapWord(headerBitmapSeg, bmIdx++);
        if (bmIdx > PageLayout.BITMAP_WORDS) {
          throw new SirixIOException("name-key elision: bitmap exhausted at entry " + i + " / " + populatedCount);
        }
      }
      final int slot = ((bmIdx - 1) << 6) | Long.numberOfTrailingZeros(bmWord);
      bmWord &= bmWord - 1;
      final int kindId = PageLayout.unpackNodeKindId(compactDir[i]);
      if (!KeyValueLeafPage.isFusedObjectNamedKindId(kindId)) {
        continue;
      }
      int width = nameKeyWidthForSlot(slot);
      if (cursor < nameKeyExceptions.count && nameKeyExceptions.ordinals[cursor] == ordinal) {
        width = nameKeyExceptions.byteValues[cursor] & 0xFF;
        cursor++;
      }
      if (width <= 0 || width > 10) {
        throw new SirixIOException("name-key elision: slot " + slot + " derives an impossible width " + width);
      }
      outWidths[ordinal] = (byte) width;
      ordinal++;
    }
    if (cursor < nameKeyExceptions.count) {
      throw new SirixIOException(
          "name-key elision: the section carries exceptions for ordinals past its " + ordinal + " elided slots");
    }
    return ordinal;
  }

  /** Number of elided entries the last plan or derivation settled on. */
  int elidedCount() {
    return elidedCount;
  }

  // ─────────────────────────────────────────────────────────────────── bounds

  /**
   * Upper bound on a derived value-elision section: the flag byte, the entry bitmap, and three
   * exception lists whose entries cost a two-byte ordinal gap plus a value of at most five bytes.
   */
  static int maxValueSectionBytes(final int populatedCount) {
    return 1 + ((populatedCount + 7) >>> 3) + 3 * 2 + populatedCount * 17;
  }

  /** Upper bound on a derived name-key-elision section. */
  static int maxNameKeySectionBytes(final int populatedCount) {
    return 1 + 2 + populatedCount * 3;
  }
}
