/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.page;

import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Structural tests for {@link ValidTimeIndexPage} plus the backward-compatibility guarantee for the
 * {@link RevisionRootPage} reference-count growth from 10 to 11 (adding the VALIDTIME slot).
 *
 * @author Johannes Lichtenberger
 */
final class ValidTimeIndexPageTest {

  @Test
  void defaultCtor_emptyCounters() {
    final ValidTimeIndexPage page = new ValidTimeIndexPage();
    assertEquals(0, page.getMaxNodeKeySize());
    assertEquals(0, page.getMaxHotPageKeySize());
    assertEquals(0, page.getCurrentMaxLevelOfIndirectPagesSize());
  }

  @Test
  void maxNodeKey_incrementPerIndex() {
    final ValidTimeIndexPage page = new ValidTimeIndexPage();
    assertEquals(0L, page.getMaxNodeKey(0));
    assertEquals(1L, page.incrementAndGetMaxNodeKey(0));
    assertEquals(2L, page.incrementAndGetMaxNodeKey(0));
    assertEquals(1L, page.incrementAndGetMaxNodeKey(1));
    assertEquals(2L, page.getMaxNodeKey(0));
    assertEquals(1L, page.getMaxNodeKey(1));
    assertEquals(2, page.getMaxNodeKeySize());
  }

  @Test
  void maxHotPageKey_independentFromNodeKey() {
    final ValidTimeIndexPage page = new ValidTimeIndexPage();
    assertEquals(1L, page.incrementAndGetMaxHotPageKey(0));
    assertEquals(2L, page.incrementAndGetMaxHotPageKey(0));
    assertEquals(2L, page.getMaxHotPageKey(0));
    assertEquals(0L, page.getMaxNodeKey(0), "node- and hot-page counters must not alias");
  }

  @Test
  void deserializationCtor_preservesState() {
    final Int2LongOpenHashMap maxNodeKeys = new Int2LongOpenHashMap();
    maxNodeKeys.put(0, 42L);
    final Int2LongOpenHashMap maxHotPageKeys = new Int2LongOpenHashMap();
    maxHotPageKeys.put(0, 7L);
    final Int2IntOpenHashMap maxLevels = new Int2IntOpenHashMap();
    maxLevels.put(0, 3);

    final ValidTimeIndexPage page =
        new ValidTimeIndexPage(new ReferencesPage4(), maxNodeKeys, maxHotPageKeys, maxLevels);

    assertEquals(42L, page.getMaxNodeKey(0));
    assertEquals(7L, page.getMaxHotPageKey(0));
    assertEquals(3, page.getCurrentMaxLevelOfIndirectPages(0));
  }

  @Test
  void pageKind_registeredAtByte17() {
    assertEquals((byte) 17, PageKind.VALIDTIMEPAGE.getID());
    assertNotNull(PageKind.valueOf("VALIDTIMEPAGE"));
    // The kind must resolve back from its id and its page class (the dispatcher relies on both).
    assertEquals(PageKind.VALIDTIMEPAGE, PageKind.getKind((byte) 17));
    assertEquals(PageKind.VALIDTIMEPAGE, PageKind.getKind(ValidTimeIndexPage.class));
  }

  @Test
  void getIndirectPageReference_returnsNonNull() {
    final ValidTimeIndexPage page = new ValidTimeIndexPage();
    assertNotNull(page.getIndirectPageReference(0));
  }

  @Test
  void freshRevisionRoot_seedsValidTimeIndexPage() {
    final RevisionRootPage root = new RevisionRootPage();
    final PageReference ref = root.getValidTimeIndexPageReference();
    assertNotNull(ref);
    assertNotNull(ref.getPage(), "fresh revision must seed a ValidTimeIndexPage at offset 10");
    assertEquals(ValidTimeIndexPage.class, ref.getPage().getClass(),
        "valid-time reference must hold a ValidTimeIndexPage, not some other container");
  }

  @Test
  void revisionRoot_referenceCountConstantIsEleven_andValidTimeIsLastSlot() {
    assertEquals(11, RevisionRootPage.REVISION_ROOT_PAGE_REFERENCE_COUNT);
    final RevisionRootPage root = new RevisionRootPage();
    // The pre-existing sibling references plus the new VALIDTIME slot are all materialized on a
    // fresh revision root, and each typed getter returns a non-null reference.
    assertNotNull(root.getNamePageReference());
    assertNotNull(root.getCASPageReference());
    assertNotNull(root.getPathPageReference());
    assertNotNull(root.getDeweyIdPageReference());
    assertNotNull(root.getVectorPageReference());
    assertNotNull(root.getProjectionIndexPageReference());
    assertNotNull(root.getValidTimeIndexPageReference());
  }

  /**
   * Backward-compatibility: the {@link RevisionRootPage} delegate is a
   * {@link BitmapReferencesPage} whose on-disk form stores a presence BITMAP, and whose
   * deserializer ({@link io.sirix.page.SerializationType#deserializeBitmapReferencesPage}) reads
   * EXACTLY {@code bitmap.cardinality()} references — it IGNORES the {@code referenceCount}
   * argument. Therefore:
   *
   * <ul>
   *   <li>a NEW revision root (11 set bits) round-trips to 11 references, and</li>
   *   <li>an OLD revision root written before the VALIDTIME slot existed (only offsets 0..9 set, 10
   *       references) round-trips to 10 references WITHOUT error, even though the deserialize ctor is
   *       now called with {@code referenceCount = 11}.</li>
   * </ul>
   *
   * After deserializing the old form, the missing VALIDTIME slot (offset 10) is materialized lazily
   * via {@link BitmapReferencesPage#getOrCreateReference(int)} (which is exactly what
   * {@code NodeStorageEngineReader.getValidTimeIndexPage} relies on), so the new code reads old
   * databases transparently.
   */
  @Test
  void backwardCompat_tenSlotAndElevenSlotBothDeserialize() {
    // --- NEW format: 11 occupied offsets (0..10). ---
    final BitmapReferencesPage elevenSlot = new BitmapReferencesPage(11);
    for (int offset = 0; offset <= 10; offset++) {
      elevenSlot.getOrCreateReference(offset).setKey(1000L + offset);
    }
    final BitmapReferencesPage elevenRoundTripped = roundTrip(elevenSlot, 11);
    assertEquals(11, elevenRoundTripped.getBitmap().cardinality(), "11-slot must round-trip to 11 refs");
    for (int offset = 0; offset <= 10; offset++) {
      assertTrue(elevenRoundTripped.getBitmap().get(offset), "offset " + offset + " must be present");
      assertEquals(1000L + offset, elevenRoundTripped.getOrCreateReference(offset).getKey());
    }

    // --- OLD format: only offsets 0..9 occupied (no VALIDTIME slot), 10 references. ---
    final BitmapReferencesPage tenSlot = new BitmapReferencesPage(11);
    for (int offset = 0; offset <= 9; offset++) {
      tenSlot.getOrCreateReference(offset).setKey(2000L + offset);
    }
    assertFalse(tenSlot.getBitmap().get(10), "simulated old page must NOT have the VALIDTIME slot set");

    // Deserialize with referenceCount = 11 (the value the NEW code passes), exactly like
    // PageKind.REVISIONROOTPAGE now does. The cardinality-driven reader must yield 10 refs.
    final BitmapReferencesPage tenRoundTripped = roundTrip(tenSlot, 11);
    assertEquals(10, tenRoundTripped.getBitmap().cardinality(),
        "old 10-slot page must round-trip to 10 refs even when deserialized with referenceCount=11");
    for (int offset = 0; offset <= 9; offset++) {
      assertEquals(2000L + offset, tenRoundTripped.getOrCreateReference(offset).getKey(),
          "old reference at offset " + offset + " must survive");
    }
    assertFalse(tenRoundTripped.getBitmap().get(10),
        "the VALIDTIME slot must still be absent right after deserializing an old page");

    // Lazy materialization of the VALIDTIME slot on first access (the production path through
    // getValidTimeIndexPage). No exception; a fresh empty reference appears.
    final PageReference lazyVt = tenRoundTripped.getOrCreateReference(10);
    assertNotNull(lazyVt, "VALIDTIME slot must materialize lazily on an old page");
    assertTrue(tenRoundTripped.getBitmap().get(10), "VALIDTIME slot is now present after lazy access");
    assertEquals(11, tenRoundTripped.getBitmap().cardinality());
    // The other 10 references are untouched by the lazy add.
    for (int offset = 0; offset <= 9; offset++) {
      assertEquals(2000L + offset, tenRoundTripped.getOrCreateReference(offset).getKey());
    }
  }

  /** Serialize a {@link BitmapReferencesPage} delegate and read it back via the production ctor. */
  private static BitmapReferencesPage roundTrip(final BitmapReferencesPage page, final int referenceCountForRead) {
    final BytesOut<?> out = Bytes.elasticOffHeapByteBuffer();
    SerializationType.DATA.serializeBitmapReferencesPage(out, page.getReferences(), page.getBitmap());
    final BytesIn<?> in = Bytes.wrapForRead(out.toByteArray());
    return new BitmapReferencesPage(referenceCountForRead, in, SerializationType.DATA);
  }
}
