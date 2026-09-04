/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Wire coverage for sparse per-index HOT page-key high-water metadata. */
final class HOTPageKeyMetadataSerializationTest {

  private static final ResourceConfiguration RESOURCE_CONFIGURATION =
      ResourceConfiguration.newBuilder("sparse-hot-page-key-wire").build();

  @Test
  void sparseNonZeroIndexNumbersRoundTripForEveryHOTContainer() {
    final int firstIndex = 3;
    final int farIndex = 911;
    for (final Page original : hotContainerPages(firstIndex, farIndex)) {
      final Page roundTripped = roundTrip(original);
      assertEquals(2, maxHotPageKeySize(roundTripped),
          original.getClass().getSimpleName() + " fabricated or dropped a sparse allocator entry");
      assertEquals(17L, maxHotPageKey(roundTripped, firstIndex),
          original.getClass().getSimpleName() + " lost the first sparse high-water mark");
      assertEquals(29L, maxHotPageKey(roundTripped, farIndex),
          original.getClass().getSimpleName() + " lost the far sparse high-water mark");
      assertEquals(0L, maxHotPageKey(roundTripped, 0),
          original.getClass().getSimpleName() + " fabricated a positional index-zero entry");
    }
  }

  @Test
  void sparseMetadataWireIsIndependentOfHashMapInsertionOrder() {
    final Int2LongOpenHashMap ascending = new Int2LongOpenHashMap();
    ascending.put(3, 17L);
    ascending.put(911, 29L);
    final Int2LongOpenHashMap descending = new Int2LongOpenHashMap();
    descending.put(911, 29L);
    descending.put(3, 17L);

    final CASPage first = new CASPage(new ReferencesPage4(), ascending);
    final CASPage second = new CASPage(new ReferencesPage4(), descending);

    assertArrayEquals(serialize(first), serialize(second));
  }

  @Test
  void allHOTOnlyContainersUseTheSameNonMutatingPhysicalIdAllocatorContract() {
    for (final Page page : List.of(new CASPage(), new PathPage(), new ProjectionIndexPage(),
        new ValidTimeIndexPage())) {
      final PageReference placeholder = indexReference(page, 3);
      assertFalse(isIndexInitialized(page, 3));

      placeholder.setKey(4L);
      incrementMaxHotPageKey(page, 5);

      assertTrue(isIndexInitialized(page, 3));
      assertTrue(isIndexInitialized(page, 5));
      assertEquals(4, nextUnallocatedIndex(page, 3));
      assertEquals(6, nextUnallocatedIndex(page, 5));
      assertEquals(1, page.getReferencesCount(),
          page.getClass().getSimpleName() + " allocation probes materialized a structural hole");
    }
  }

  @Test
  void nameIndexesCoexistWithEveryReservedJsonAndXmlSlot() {
    assertNamePageSlotCoexistence(IndexDef.DbType.JSON, NamePage.JSON_PROJECTION_VALUE_DICTIONARY_REFERENCE_OFFSET);
    assertNamePageSlotCoexistence(IndexDef.DbType.XML, NamePage.XML_PROJECTION_VALUE_DICTIONARY_REFERENCE_OFFSET);
  }

  private static void assertNamePageSlotCoexistence(final IndexDef.DbType dbType, final int highestReservedSlot) {
    final int nameIndexSlot = IndexDefs.createNameIdxDef(0, dbType).getID();
    assertEquals(highestReservedSlot + 1, nameIndexSlot,
        "the first secondary NAME slot must immediately follow the reserved dictionary run");

    final NamePage original = new NamePage();
    for (int offset = 0; offset <= highestReservedSlot; offset++) {
      original.setOrCreateReference(offset, new PageReference().setKey(10_000L + offset));
    }
    original.setOrCreateReference(nameIndexSlot, new PageReference().setKey(20_000L + nameIndexSlot));
    assertEquals(1L, original.incrementAndGetMaxHotPageKey(nameIndexSlot));

    final NamePage roundTripped = (NamePage) roundTrip(original);
    for (int offset = 0; offset <= highestReservedSlot; offset++) {
      assertEquals(10_000L + offset, roundTripped.getOrCreateReference(offset).getKey(),
          "reserved NamePage slot " + offset + " was overwritten");
    }
    assertEquals(20_000L + nameIndexSlot, roundTripped.getOrCreateReference(nameIndexSlot).getKey());
    assertEquals(1L, roundTripped.getMaxHotPageKey(nameIndexSlot));
    assertEquals(0L, roundTripped.getMaxHotPageKey(0),
        "secondary NAME metadata must not be reconstructed positionally at dictionary slot zero");
  }

  private static List<Page> hotContainerPages(final int firstIndex, final int farIndex) {
    return List.of(
        new NamePage(new ReferencesPage4(), new Int2LongOpenHashMap(), hotPageKeys(firstIndex, farIndex),
            new Int2IntOpenHashMap(), 0),
        new CASPage(new ReferencesPage4(), hotPageKeys(firstIndex, farIndex)),
        new PathPage(new ReferencesPage4(), hotPageKeys(firstIndex, farIndex)),
        new ProjectionIndexPage(new ReferencesPage4(), hotPageKeys(firstIndex, farIndex)),
        new ValidTimeIndexPage(new ReferencesPage4(), hotPageKeys(firstIndex, farIndex)));
  }

  private static Int2LongOpenHashMap hotPageKeys(final int firstIndex, final int farIndex) {
    final Int2LongOpenHashMap keys = new Int2LongOpenHashMap();
    keys.put(farIndex, 29L);
    keys.put(firstIndex, 17L);
    return keys;
  }

  private static byte[] serialize(final Page page) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    try {
      new PagePersister().serializePage(RESOURCE_CONFIGURATION, sink, page, SerializationType.DATA);
      return sink.toByteArray();
    } catch (final IOException e) {
      throw new AssertionError("Unable to serialize " + page.getClass().getSimpleName(), e);
    }
  }

  private static Page roundTrip(final Page page) {
    try {
      return new PagePersister().deserializePage(RESOURCE_CONFIGURATION, Bytes.wrapForRead(serialize(page)),
          SerializationType.DATA);
    } catch (final IOException e) {
      throw new AssertionError("Unable to deserialize " + page.getClass().getSimpleName(), e);
    }
  }

  private static long maxHotPageKey(final Page page, final int index) {
    if (page instanceof NamePage namePage) {
      return namePage.getMaxHotPageKey(index);
    }
    if (page instanceof CASPage casPage) {
      return casPage.getMaxHotPageKey(index);
    }
    if (page instanceof PathPage pathPage) {
      return pathPage.getMaxHotPageKey(index);
    }
    if (page instanceof ProjectionIndexPage projectionPage) {
      return projectionPage.getMaxHotPageKey(index);
    }
    if (page instanceof ValidTimeIndexPage validTimePage) {
      return validTimePage.getMaxHotPageKey(index);
    }
    throw new IllegalArgumentException("Not a HOT index container: " + page.getClass().getName());
  }

  private static int maxHotPageKeySize(final Page page) {
    if (page instanceof NamePage namePage) {
      return namePage.getMaxHotPageKeySize();
    }
    if (page instanceof CASPage casPage) {
      return casPage.getMaxHotPageKeySize();
    }
    if (page instanceof PathPage pathPage) {
      return pathPage.getMaxHotPageKeySize();
    }
    if (page instanceof ProjectionIndexPage projectionPage) {
      return projectionPage.getMaxHotPageKeySize();
    }
    if (page instanceof ValidTimeIndexPage validTimePage) {
      return validTimePage.getMaxHotPageKeySize();
    }
    throw new IllegalArgumentException("Not a HOT index container: " + page.getClass().getName());
  }

  private static PageReference indexReference(final Page page, final int index) {
    if (page instanceof CASPage casPage) {
      return casPage.getIndirectPageReference(index);
    }
    if (page instanceof PathPage pathPage) {
      return pathPage.getIndirectPageReference(index);
    }
    if (page instanceof ProjectionIndexPage projectionPage) {
      return projectionPage.getIndirectPageReference(index);
    }
    if (page instanceof ValidTimeIndexPage validTimePage) {
      return validTimePage.getIndirectPageReference(index);
    }
    throw new IllegalArgumentException("Not a HOT-only index container: " + page.getClass().getName());
  }

  private static void incrementMaxHotPageKey(final Page page, final int index) {
    if (page instanceof CASPage casPage) {
      casPage.incrementAndGetMaxHotPageKey(index);
    } else if (page instanceof PathPage pathPage) {
      pathPage.incrementAndGetMaxHotPageKey(index);
    } else if (page instanceof ProjectionIndexPage projectionPage) {
      projectionPage.incrementAndGetMaxHotPageKey(index);
    } else if (page instanceof ValidTimeIndexPage validTimePage) {
      validTimePage.incrementAndGetMaxHotPageKey(index);
    } else {
      throw new IllegalArgumentException("Not a HOT-only index container: " + page.getClass().getName());
    }
  }

  private static boolean isIndexInitialized(final Page page, final int index) {
    if (page instanceof CASPage casPage) {
      return casPage.isIndexInitialized(index);
    }
    if (page instanceof PathPage pathPage) {
      return pathPage.isIndexInitialized(index);
    }
    if (page instanceof ProjectionIndexPage projectionPage) {
      return projectionPage.isIndexInitialized(index);
    }
    if (page instanceof ValidTimeIndexPage validTimePage) {
      return validTimePage.isIndexInitialized(index);
    }
    throw new IllegalArgumentException("Not a HOT-only index container: " + page.getClass().getName());
  }

  private static int nextUnallocatedIndex(final Page page, final int fromInclusive) {
    if (page instanceof CASPage casPage) {
      return casPage.nextUnallocatedIndex(fromInclusive);
    }
    if (page instanceof PathPage pathPage) {
      return pathPage.nextUnallocatedIndex(fromInclusive);
    }
    if (page instanceof ProjectionIndexPage projectionPage) {
      return projectionPage.nextUnallocatedIndex(fromInclusive);
    }
    if (page instanceof ValidTimeIndexPage validTimePage) {
      return validTimePage.nextUnallocatedIndex(fromInclusive);
    }
    throw new IllegalArgumentException("Not a HOT-only index container: " + page.getClass().getName());
  }
}
