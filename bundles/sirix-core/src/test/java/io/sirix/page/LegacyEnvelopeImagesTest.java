package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.UnsupportedStoreFeatureException;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.page.interfaces.Page;
import org.junit.Test;

import java.io.InputStream;
import java.time.Instant;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Every kind's body is an immutable base-writer image, independent of today's encoder. */
public final class LegacyEnvelopeImagesTest {
  @Test
  public void everyBaseKindSkipsOptionalExtensionsAndRefusesRequiredOnes() throws Exception {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("fixture").saltRecordPages(false).build();
    final PagePersister persister = new PagePersister();
    final int[] kinds = {1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19};
    for (final int kind : kinds) {
      final byte[] original;
      try (final InputStream input = getClass().getResourceAsStream("/format/readers-base-pages/" + kind + ".bin")) {
        assertNotNull("Missing base image for kind " + kind, input);
        original = input.readAllBytes();
      }
      try (final Page old = persister.deserializePage(config, Bytes.wrapForRead(original), SerializationType.DATA)) {
        assertNotNull(old);
      }
      final byte[] extended = new byte[original.length + 3];
      System.arraycopy(original, 0, extended, 0, 3);
      extended[2] |= (byte) 0x80;
      extended[3] = 2;
      extended[4] = 0x3e; // unassigned optional shared tag
      extended[5] = 0;
      System.arraycopy(original, 3, extended, 6, original.length - 3);
      try (
          final Page decoded = persister.deserializePage(config, Bytes.wrapForRead(extended), SerializationType.DATA)) {
        if (decoded instanceof RevisionRootPage root) {
          root.setCommitTimestamp(Instant.ofEpochMilli(root.getRevisionTimestamp()));
        }
        final BytesOut<?> rewritten = Bytes.elasticHeapByteBuffer();
        persister.serializePage(config, rewritten, decoded, SerializationType.DATA);
        assertArrayEquals("Unknown entry survived rewrite of kind " + kind, original, rewritten.toByteArray());
      }
      if (kind == 12) {
        assertNull(persister
                            .deserializeHOTProjectionEntry(config, Bytes.wrapForRead(extended), SerializationType.DATA,
                                new byte[] {1}, 0, false)
                            .entry());
        final var retained = persister.deserializeHOTProjectionEntry(config, Bytes.wrapForRead(extended),
            SerializationType.DATA, new byte[] {1}, 0, true);
        assertNull(retained.entry());
        assertNotNull(retained.image());
        try (final Page fragment =
            persister.deserializeHOTLeafFragment(config, Bytes.wrapForRead(extended), SerializationType.DATA)) {
          assertNotNull(fragment);
        }
      }
      extended[4] = (byte) 0xbe;
      final Class<? extends RuntimeException> unsupportedType = kind == PageKind.UBERPAGE.getID()
          ? UnsupportedStoreFeatureException.class
          : IllegalStateException.class;
      final String requiredMessage = kind == PageKind.UBERPAGE.getID()
          ? "required uber extension 0xbe"
          : "required page extension 190";
      assertTrue(assertThrows(unsupportedType, () -> persister.deserializePage(config, Bytes.wrapForRead(extended),
          SerializationType.DATA)).getMessage().contains(requiredMessage));
    }
  }
}
