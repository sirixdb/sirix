package io.sirix.page;

import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.page.delegates.BitmapReferencesPage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public final class StrictReferenceFormatTest {
  @Test
  public void everyUnknownHashFormRefusesBeforeHashPayload() {
    // bitmap length=1, slot=0, no fragments, key=0, then hash-form.
    final byte[] bytes = new byte[21];
    bytes[0] = 1;
    bytes[2] = 1;
    for (int form = 2; form < 256; form++) {
      bytes[12] = (byte) form;
      final BytesIn<?> source = Bytes.wrapForRead(bytes);
      final IllegalStateException error = assertThrows(IllegalStateException.class,
          () -> SerializationType.DATA.deserializeBitmapReferencesPage(13, source));
      assertTrue(error.getMessage(), error.getMessage().contains("hash form " + form));
      assertEquals(13, source.position());
    }
    bytes[12] = 1;
    bytes[13] = 0x12;
    bytes[20] = 0x34;
    assertEquals(0x1200000000000034L,
        SerializationType.DATA.deserializeBitmapReferencesPage(13, Bytes.wrapForRead(bytes))
                              .getReferences()
                              .getFirst()
                              .getHashAsLong());
    bytes[12] = 0;
    final BytesIn<?> source = Bytes.wrapForRead(bytes);
    assertTrue(
        !SerializationType.DATA.deserializeBitmapReferencesPage(13, source).getReferences().getFirst().hasHash());
    assertEquals(13, source.position());
  }

  @Test
  public void revisionRootBitmapRejectsFirstUnknownSlot() {
    final byte[] bytes = new byte[14];
    bytes[0] = 2;
    bytes[3] = 0x20; // slot 13, beyond the 13 known slots
    final IllegalStateException error = assertThrows(IllegalStateException.class,
        () -> new BitmapReferencesPage(13, Bytes.wrapForRead(bytes), SerializationType.DATA));
    assertTrue(error.getMessage(), error.getMessage().contains("reference slot 13"));
    bytes[3] = 0x10; // slot 12 is valid
    assertEquals(1,
        new BitmapReferencesPage(13, Bytes.wrapForRead(bytes), SerializationType.DATA).getReferencesCount());
  }
}
