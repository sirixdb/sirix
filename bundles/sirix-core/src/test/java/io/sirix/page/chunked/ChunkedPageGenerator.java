/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Random;

/**
 * Builds record pages to order, one axis per thing the body format reacts to.
 *
 * <p>
 * Every structural lever in the body — the hash-elision bitmap, the parentKey and pathNodeKey
 * columns, value elision, name-key elision — is <em>content-driven</em>: the writer turns each on
 * only when the page's own bytes make it pay. They cannot be switched on from the outside (their
 * kill switches are read once into static finals at class load), so the only honest way to sweep
 * the lever space is to feed the writer content that provokes each combination and then read back
 * which combination it actually chose. That is why the sweep asserts on the structural-flags byte
 * it observes rather than one it demanded, and reports the combinations no recipe could reach.
 *
 * <p>
 * Records are written through the same {@code writeNewRecord} entry points the node factory uses,
 * so their offset tables, field widths and delta encodings are the production ones. A record's node
 * key is derived from its slot, because the structural-key columns encode every value as a delta
 * against exactly that.
 */
final class ChunkedPageGenerator {

  /** Fixed page key; the node keys derived from it are what the structural columns delta against. */
  static final long PAGE_KEY = 3L;

  /**
   * Opaque raw-record marker; {@code setSlot} publishes directory kind 0, which disables templates.
   */
  private static final byte UNKNOWN_KIND_ID = 100;

  /** One value larger than the sweep's 64-byte chunk target while remaining an inline record. */
  private static final int OVERSIZED_VALUE_BYTES = 160;

  /** The direct writer reserves this conservative metadata bound around a fused string value. */
  private static final int STRING_RECORD_METADATA_BUDGET = 96;

  /** Largest fused-string value whose conservative direct-write reservation remains inline. */
  private static final int NEAR_MAX_VALUE_BYTES =
      PageLayout.MAX_COMPACT_DIR_DATA_LENGTH - STRING_RECORD_METADATA_BUDGET;

  private ChunkedPageGenerator() {}

  /** Whether the page's records have offset tables the template pool can dedup. */
  enum Body {
    TEMPLATED, DEGENERATE
  }

  /** Which records carry an all-zero hash, the only ones hash elision may drop. */
  enum Hash {
    NONE_ZERO, ALL_ZERO, ALTERNATING, FIRST_ONLY
  }

  /**
   * What the records' parentKey fields hold. {@code ALL_NULL} is the page that says why widths are
   * derived from the template and never from the decoded value: a node may legitimately hold
   * NULL_NODE_KEY in a field it does have, and a reader that read the value would not put the bytes
   * back.
   */
  enum ParentKeys {
    SEQUENTIAL, ALL_NULL, MIXED_NULL
  }

  /** How many distinct pathNodeKeys the page holds, which decides whether the dict column pays. */
  enum PathKeys {
    SINGLE, FEW, DISTINCT
  }

  /**
   * Which record kinds the page holds, hence what value elision has to work with — and whether hash
   * elision can fire at all. The fused primitives (48-51) are deliberately excluded from hash elision
   * by {@code NodeFieldLayout.hashFieldIndexForKind}, so only the profiles carrying the fused
   * structurals (52, 53) can produce a page with a zero-hash bitmap on it.
   */
  enum Values {
    NUMBERS, STRINGS, BOOLEANS, MIXED, MIXED_WITH_NULLS, STRUCTURAL, MIXED_STRUCTURAL
  }

  /** How wide and how many distinct the name keys are, which decides whether their elision pays. */
  enum Names {
    ONE, FEW, WIDE, MANY
  }

  /** Where the records sit in the slot bitmap. */
  enum Shape {
    DENSE, ALTERNATING_HOLES, HIGH_SLOT, SEEDED_RANDOM
  }

  /** How big the records are, which is what decides where chunk boundaries fall. */
  enum Sizes {
    SMALL, MIXED, ONE_OVERSIZED, NEAR_MAX
  }

  /**
   * One page to build.
   *
   * @param entryCount populated slots; {@code 0} builds an empty page
   */
  record Recipe(Body body, Hash hash, ParentKeys parentKeys, PathKeys pathKeys, Values values, Names names, Shape shape,
      Sizes sizes, int entryCount, boolean deweyIds) {

    @Override
    public String toString() {
      return body + "/" + hash + "/" + parentKeys + "/" + pathKeys + "/" + values + "/" + names + "/" + shape + "/"
          + sizes + "/n=" + entryCount + (deweyIds
              ? "/dewey"
              : "");
    }
  }

  /** Slots a shape can hold, so a recipe never asks for more entries than its shape has room for. */
  static int capacity(final Shape shape) {
    return switch (shape) {
      case DENSE, SEEDED_RANDOM -> PageLayout.SLOT_COUNT;
      case ALTERNATING_HOLES -> PageLayout.SLOT_COUNT / 2;
      case HIGH_SLOT -> 64;
    };
  }

  /** Build the page a recipe describes. Two calls with the same recipe build byte-identical pages. */
  static KeyValueLeafPage build(final Recipe recipe, final ResourceConfiguration config) {
    final KeyValueLeafPage page = new KeyValueLeafPage(PAGE_KEY, 0, IndexType.DOCUMENT, config, recipe.deweyIds(), null,
        new LinkedHashMap<>(), Allocators.getInstance().allocate(4096), null, -1);
    final int[] slots = slots(recipe);
    final int[] heapOffsets = new int[32];
    for (int i = 0; i < slots.length; i++) {
      if (recipe.body() == Body.DEGENERATE) {
        page.setSlot(degenerateRecord(recipe, i), slots[i]);
      } else {
        writeRecord(page, recipe, i, slots[i], heapOffsets);
      }
    }
    return page;
  }

  private static int[] slots(final Recipe recipe) {
    final int n = recipe.entryCount();
    final int[] slots = new int[n];
    switch (recipe.shape()) {
      case DENSE -> {
        for (int i = 0; i < n; i++) {
          slots[i] = i;
        }
      }
      case ALTERNATING_HOLES -> {
        for (int i = 0; i < n; i++) {
          slots[i] = i * 2;
        }
      }
      case HIGH_SLOT -> {
        // Packed against the top of the bitmap, so every leading bitmap word is empty and the
        // entry-to-slot walk has to skip them.
        for (int i = 0; i < n; i++) {
          slots[i] = PageLayout.SLOT_COUNT - n + i;
        }
      }
      case SEEDED_RANDOM -> {
        final boolean[] taken = new boolean[PageLayout.SLOT_COUNT];
        final Random random = new Random(20260817L + n);
        int filled = 0;
        while (filled < n) {
          final int candidate = random.nextInt(PageLayout.SLOT_COUNT);
          if (!taken[candidate]) {
            taken[candidate] = true;
            filled++;
          }
        }
        int at = 0;
        for (int slot = 0; slot < PageLayout.SLOT_COUNT; slot++) {
          if (taken[slot]) {
            slots[at++] = slot;
          }
        }
      }
    }
    return slots;
  }

  private static void writeRecord(final KeyValueLeafPage page, final Recipe recipe, final int index, final int slot,
      final int[] heapOffsets) {
    final long nodeKey = (PAGE_KEY << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
    final long parentKey = parentKey(recipe, index, nodeKey);
    final long pathNodeKey = pathNodeKey(recipe, index);
    final int nameKey = nameKey(recipe, index);
    final long hash = hash(recipe, index);
    final long rightSib = nodeKey + 1;
    final long leftSib = nodeKey - 1;
    final byte[] deweyId = recipe.deweyIds()
        ? deweyId(index)
        : null;
    final int deweyLen = deweyId != null
        ? deweyId.length
        : 0;

    final NodeKind kind = kind(recipe, index);
    final int recordBytes;
    final long absOffset;
    switch (kind) {
      case OBJECT_NAMED_NUMBER -> {
        final Number value = numberValue(recipe, index);
        absOffset = page.prepareHeapForDirectWrite(96, deweyLen);
        recordBytes = ObjectNamedNumberNode.writeNewRecord(page.getSlottedPage(), absOffset, heapOffsets, nodeKey,
            parentKey, rightSib, leftSib, nameKey, pathNodeKey, Constants.NULL_REVISION_NUMBER, 1, hash, value);
      }
      case OBJECT_NAMED_STRING -> {
        final byte[] value = stringValue(recipe, index);
        absOffset = page.prepareHeapForDirectWrite(96 + value.length, deweyLen);
        recordBytes = ObjectNamedStringNode.writeNewRecord(page.getSlottedPage(), absOffset, heapOffsets, nodeKey,
            parentKey, rightSib, leftSib, nameKey, pathNodeKey, Constants.NULL_REVISION_NUMBER, 1, hash, value, false);
      }
      case OBJECT_NAMED_BOOLEAN -> {
        absOffset = page.prepareHeapForDirectWrite(96, deweyLen);
        recordBytes =
            ObjectNamedBooleanNode.writeNewRecord(page.getSlottedPage(), absOffset, heapOffsets, nodeKey, parentKey,
                rightSib, leftSib, nameKey, pathNodeKey, Constants.NULL_REVISION_NUMBER, 1, hash, (index & 1) == 0);
      }
      case OBJECT_NAMED_OBJECT -> {
        absOffset = page.prepareHeapForDirectWrite(112, deweyLen);
        recordBytes = ObjectNamedObjectNode.writeNewRecord(page.getSlottedPage(), absOffset, heapOffsets, nodeKey,
            parentKey, rightSib, leftSib, nodeKey + 2, nodeKey + 3, nameKey, pathNodeKey,
            Constants.NULL_REVISION_NUMBER, 1, hash, 2, 4);
      }
      case OBJECT_NAMED_ARRAY -> {
        absOffset = page.prepareHeapForDirectWrite(112, deweyLen);
        recordBytes = ObjectNamedArrayNode.writeNewRecord(page.getSlottedPage(), absOffset, heapOffsets, nodeKey,
            parentKey, rightSib, leftSib, nodeKey + 2, nodeKey + 3, nameKey, pathNodeKey,
            Constants.NULL_REVISION_NUMBER, 1, hash, 2, 4);
      }
      default -> {
        absOffset = page.prepareHeapForDirectWrite(96, deweyLen);
        recordBytes = ObjectNamedNullNode.writeNewRecord(page.getSlottedPage(), absOffset, heapOffsets, nodeKey,
            parentKey, rightSib, leftSib, nameKey, pathNodeKey, Constants.NULL_REVISION_NUMBER, 1, hash);
      }
    }
    page.completeDirectWrite(kind.getId(), nodeKey, slot, recordBytes, deweyId);
  }

  private static NodeKind kind(final Recipe recipe, final int index) {
    return switch (recipe.values()) {
      case NUMBERS -> NodeKind.OBJECT_NAMED_NUMBER;
      case STRINGS -> NodeKind.OBJECT_NAMED_STRING;
      case BOOLEANS -> NodeKind.OBJECT_NAMED_BOOLEAN;
      case MIXED -> switch (index % 3) {
        case 0 -> NodeKind.OBJECT_NAMED_NUMBER;
        case 1 -> NodeKind.OBJECT_NAMED_STRING;
        default -> NodeKind.OBJECT_NAMED_BOOLEAN;
      };
      case MIXED_WITH_NULLS -> switch (index % 4) {
        case 0 -> NodeKind.OBJECT_NAMED_NUMBER;
        case 1 -> NodeKind.OBJECT_NAMED_STRING;
        case 2 -> NodeKind.OBJECT_NAMED_BOOLEAN;
        // Fused-named, hence name-key elidable, but payload-free, hence never value-elidable: the
        // mix that makes elision per-slot rather than all-or-nothing.
        default -> NodeKind.OBJECT_NAMED_NULL;
      };
      case STRUCTURAL -> (index & 1) == 0
          ? NodeKind.OBJECT_NAMED_OBJECT
          : NodeKind.OBJECT_NAMED_ARRAY;
      case MIXED_STRUCTURAL -> switch (index % 5) {
        case 0 -> NodeKind.OBJECT_NAMED_NUMBER;
        case 1 -> NodeKind.OBJECT_NAMED_STRING;
        case 2 -> NodeKind.OBJECT_NAMED_BOOLEAN;
        // A page mixing kinds whose hashes may be elided with kinds whose may not: the zero-hash
        // bitmap is entry-indexed, so the two have to interleave without shifting it.
        case 3 -> NodeKind.OBJECT_NAMED_OBJECT;
        default -> NodeKind.OBJECT_NAMED_ARRAY;
      };
    };
  }

  /**
   * Mostly integers, which the number region can hold and value elision can therefore drop, with a
   * double every seventh record in the mixed profiles — a fused-primitive slot the writer must leave
   * inline while eliding its neighbours.
   */
  private static Number numberValue(final Recipe recipe, final int index) {
    final boolean mixed = recipe.values() == Values.MIXED || recipe.values() == Values.MIXED_WITH_NULLS
        || recipe.values() == Values.MIXED_STRUCTURAL;
    if (mixed && index % 7 == 3) {
      return 1.5d + index;
    }
    return 1000 + index;
  }

  private static byte[] stringValue(final Recipe recipe, final int index) {
    final int length = switch (recipe.sizes()) {
      case SMALL -> 8;
      case MIXED -> 4 + (index % 37);
      // One record alone exceeds the sweep's small chunk target, so the planner has to give it a
      // chunk of its own.
      case ONE_OVERSIZED -> index == 0
          ? OVERSIZED_VALUE_BYTES
          : 8;
      case NEAR_MAX -> index == 0
          ? NEAR_MAX_VALUE_BYTES
          : 8;
    };
    final byte[] value = new byte[length];
    for (int i = 0; i < length; i++) {
      value[i] = (byte) ('a' + ((index + i) % 26));
    }
    return value;
  }

  private static long parentKey(final Recipe recipe, final int index, final long nodeKey) {
    return switch (recipe.parentKeys()) {
      case SEQUENTIAL -> nodeKey - 1 - (index % 4);
      case ALL_NULL -> Fixed.NULL_NODE_KEY.getStandardProperty();
      case MIXED_NULL -> index % 3 == 0
          ? Fixed.NULL_NODE_KEY.getStandardProperty()
          : nodeKey - 1 - (index % 4);
    };
  }

  private static long pathNodeKey(final Recipe recipe, final int index) {
    return switch (recipe.pathKeys()) {
      case SINGLE -> 42L;
      case FEW -> 40L + (index % 3);
      case DISTINCT -> 1000L + index;
    };
  }

  private static int nameKey(final Recipe recipe, final int index) {
    return switch (recipe.names()) {
      case ONE -> 7;
      case FEW -> 7 + (index % 3);
      // Wide enough that dropping the varint pays for the width byte that replaces it.
      case WIDE -> 100_000 + (index % 5);
      // Past the name region's 255-value ceiling on any page with more entries than that, which is
      // the writer's reason to refuse the elision outright.
      case MANY -> 100_000 + index;
    };
  }

  private static long hash(final Recipe recipe, final int index) {
    return switch (recipe.hash()) {
      case NONE_ZERO -> 0x51D0000000000000L + index;
      case ALL_ZERO -> 0L;
      case ALTERNATING -> (index & 1) == 0
          ? 0L
          : 0x51D0000000000000L + index;
      case FIRST_ONLY -> index == 0
          ? 0L
          : 0x51D0000000000000L + index;
    };
  }

  private static byte[] deweyId(final int index) {
    return ("d" + index).getBytes(StandardCharsets.UTF_8);
  }

  /** A record with no offset-table structure, which is what the degenerate body shape is made of. */
  private static byte[] degenerateRecord(final Recipe recipe, final int index) {
    final int length = switch (recipe.sizes()) {
      case SMALL -> 24;
      case MIXED -> 8 + (index % 53);
      case ONE_OVERSIZED -> index == 0
          ? OVERSIZED_VALUE_BYTES
          : 24;
      case NEAR_MAX -> index == 0
          ? PageLayout.MAX_COMPACT_DIR_DATA_LENGTH
          : 24;
    };
    final byte[] data = new byte[length];
    data[0] = UNKNOWN_KIND_ID;
    for (int i = 1; i < length; i++) {
      data[i] = (byte) ((index + (i % 11)) & 0xFF);
    }
    return data;
  }
}
