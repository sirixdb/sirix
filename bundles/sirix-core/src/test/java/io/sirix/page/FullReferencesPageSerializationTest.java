package io.sirix.page;

import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips a full references page through {@link SerializationType#serializeFullReferencesPage}
 * and {@link SerializationType#deserializeFullReferencesPage}. Guards the wire format after the key
 * was written/read exactly once (previously the reference key was serialized twice — once
 * explicitly and once inside {@code writePageFragments} — and read twice on the way back).
 */
public final class FullReferencesPageSerializationTest {

  private static final long PAGE_HASH = 0x0102_0304_0506_0708L;

  @Test
  public void fullReferencesPageRoundTripsKeysFragmentsAndHashes() {
    final PageReference[] refs = new PageReference[Constants.INP_REFERENCE_COUNT];

    // A reference with a key, two page fragments and an 8-byte hash.
    final PageReference first = new PageReference();
    first.setKey(42L);
    first.setPageFragments(List.of(new PageFragmentKeyImpl(1, 200L, 0L, 0L), new PageFragmentKeyImpl(2, 763L, 0L, 0L)));
    first.setHash(PAGE_HASH);
    refs[0] = first;

    // A reference in the middle with only a key (no fragments, no hash).
    final PageReference middle = new PageReference();
    middle.setKey(9_999_999L);
    refs[500] = middle;

    // The last slot, to exercise the bitset's high bit.
    final PageReference last = new PageReference();
    last.setKey(123_456L);
    refs[Constants.INP_REFERENCE_COUNT - 1] = last;

    final BytesOut<?> out = Bytes.elasticOffHeapByteBuffer();
    SerializationType.DATA.serializeFullReferencesPage(out, refs);

    final PageReference[] back =
        SerializationType.DATA.deserializeFullReferencesPage(Bytes.wrapForRead(out.toByteArray()));

    // Exactly the three populated offsets survive; everything else is null.
    for (int i = 0; i < Constants.INP_REFERENCE_COUNT; i++) {
      if (i == 0 || i == 500 || i == Constants.INP_REFERENCE_COUNT - 1) {
        assertNotNull(back[i], "reference at offset " + i + " must round-trip");
      } else {
        assertNull(back[i], "offset " + i + " must stay empty");
      }
    }

    assertEquals(42L, back[0].getKey());
    assertTrue(back[0].hasHash());
    assertEquals(PAGE_HASH, back[0].getHashAsLong());
    final List<PageFragmentKey> fragments = back[0].getPageFragments();
    assertEquals(2, fragments.size());
    assertEquals(1, fragments.get(0).revision());
    assertEquals(200L, fragments.get(0).key());
    assertEquals(2, fragments.get(1).revision());
    assertEquals(763L, fragments.get(1).key());

    assertEquals(9_999_999L, back[500].getKey());
    assertTrue(back[500].getPageFragments().isEmpty());
    assertFalse(back[500].hasHash());

    assertEquals(123_456L, back[Constants.INP_REFERENCE_COUNT - 1].getKey());
  }

  @Test
  public void checksumKeepsCanonicalBigEndianEightByteWire() {
    final PageReference reference = new PageReference().setKey(42L);
    reference.setHash(PAGE_HASH);
    final BitSet bitmap = new BitSet();
    bitmap.set(0);

    final BytesOut<?> out = Bytes.elasticOffHeapByteBuffer();
    SerializationType.DATA.serializeBitmapReferencesPage(out, List.of(reference), bitmap);
    final byte[] wire = out.toByteArray();
    final byte[] canonicalHash = {1, 2, 3, 4, 5, 6, 7, 8};
    final int hashOffset = wire.length - canonicalHash.length;
    for (int index = 0; index < canonicalHash.length; index++) {
      assertEquals(canonicalHash[index], wire[hashOffset + index], "hash byte " + index);
    }

    final DeserializedBitmapReferencesPageTuple roundTrip =
        SerializationType.DATA.deserializeBitmapReferencesPage(Constants.INP_REFERENCE_COUNT, Bytes.wrapForRead(wire));
    assertTrue(roundTrip.getReferences().get(0).hasHash());
    assertEquals(PAGE_HASH, roundTrip.getReferences().get(0).getHashAsLong());
  }

  @Test
  public void sparseBitmapOffsetsSurviveFullConversionCopyAndSerialization() {
    final int firstOffset = 17;
    final int farOffset = Constants.INP_REFERENCE_COUNT - 3;
    final BitmapReferencesPage bitmap = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
    bitmap.getOrCreateReference(firstOffset).setKey(4_096L);
    bitmap.getOrCreateReference(farOffset).setKey(8_192L);

    final FullReferencesPage converted = new FullReferencesPage(bitmap);
    final FullReferencesPage copied = new FullReferencesPage(converted);
    assertEquals(4_096L, copied.referenceAt(firstOffset).getKey());
    assertEquals(8_192L, copied.referenceAt(farOffset).getKey());
    assertNull(copied.referenceAt(0), "copy-on-write must preserve unallocated physical slots");

    final BytesOut<?> out = Bytes.elasticOffHeapByteBuffer();
    copied.serializeReferences(out, SerializationType.DATA);
    final PageReference[] back =
        SerializationType.DATA.deserializeFullReferencesPage(Bytes.wrapForRead(out.toByteArray()));

    int durableReferenceCount = 0;
    for (int offset = 0; offset < back.length; offset++) {
      if (back[offset] != null && back[offset].getKey() != Constants.NULL_ID_LONG) {
        durableReferenceCount++;
        assertTrue(offset == firstOffset || offset == farOffset, "unexpected durable reference at offset " + offset);
      }
    }
    assertEquals(2, durableReferenceCount);
    assertEquals(4_096L, back[firstOffset].getKey());
    assertEquals(8_192L, back[farOffset].getKey());
  }
}
