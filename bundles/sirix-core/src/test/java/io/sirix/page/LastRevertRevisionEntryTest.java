package io.sirix.page;

import io.sirix.node.Bytes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * FMT-209 wire table: the revision root's last-revert revision is one optional kind-local
 * page-extension entry under FMT-203 framing — tag {@code 0x41}, stop-bit length {@code 4}, then
 * the revision as a little-endian u32 — omitted entirely while the resource never reverted.
 */
final class LastRevertRevisionEntryTest {

  /** Envelope offsets of a revision-root image: kind, version, flags, block length, entries. */
  private static final int FLAGS = 2;
  private static final int BLOCK_LENGTH = 3;
  private static final int TAG = 4;
  private static final int ENTRY_LENGTH = 5;
  private static final int PAYLOAD = 6;

  @Test
  @DisplayName("FMT-209: a never-reverted revision root keeps the unextended image")
  void neverRevertedRootHasNoExtensionBlock() throws IOException {
    final RevisionRootPage root = root(5, 0);
    final byte[] image = encode(root);
    assertEquals(0, image[FLAGS], "flags byte must stay zero without the entry");
    final byte[] reverted = encode(root(5, 3));
    // The entry adds exactly the block length, the tag, the entry length and four payload bytes.
    assertEquals(image.length + 7, reverted.length);
    assertArrayEquals(Arrays.copyOfRange(image, FLAGS + 1, image.length),
        Arrays.copyOfRange(reverted, PAYLOAD + 4, reverted.length), "the body after the envelope is unchanged");
    assertEquals(0, decode(image).getLastRevertRevision(), "absence reads as 0");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 255, 256, 65_536, 0x01020304, Integer.MAX_VALUE})
  @DisplayName("FMT-209: the entry is tag 0x41, length 4 and a little-endian u32, and round-trips")
  void entryEncodingMatchesTheWireTable(final int lastRevert) throws IOException {
    final byte[] image = encode(root(Integer.MAX_VALUE, lastRevert));
    assertEquals((byte) 0x80, image[FLAGS], "extension block flag");
    assertEquals(6, image[BLOCK_LENGTH], "block holds one entry: tag, length, four payload bytes");
    assertEquals(0x41, image[TAG] & 0xFF, "kind-local optional tag: 0x40 set, 0x80 clear");
    assertEquals(4, image[ENTRY_LENGTH]);
    assertEquals(lastRevert, (image[PAYLOAD] & 0xFF) | (image[PAYLOAD + 1] & 0xFF) << 8
        | (image[PAYLOAD + 2] & 0xFF) << 16 | (image[PAYLOAD + 3] & 0xFF) << 24, "little-endian payload");
    assertEquals(lastRevert, decode(image).getLastRevertRevision());
  }

  @Test
  @DisplayName("FMT-209: the clone constructor carries the last-revert revision forward unchanged")
  void cloneCarriesTheEntryForward() {
    final RevisionRootPage reverted = root(4, 4);
    final RevisionRootPage next = new RevisionRootPage(reverted, 5);
    assertEquals(4, next.getLastRevertRevision());
    next.markRevertPublished();
    assertEquals(5, next.getLastRevertRevision(), "a later revert-publishing commit records its own revision");
    assertEquals(4, reverted.getLastRevertRevision(), "the committed root is not mutated");
  }

  @Test
  @DisplayName("FMT-203: a malformed last-revert entry is refused")
  void malformedEntriesAreRefused() throws IOException {
    final byte[] valid = encode(root(7, 3));
    assertEquals(3, decode(valid).getLastRevertRevision());

    final byte[] zero = valid.clone();
    zero[PAYLOAD] = 0;
    assertThrows(RuntimeException.class, () -> decode(zero), "an entry is never written for 0");

    final byte[] negative = valid.clone();
    negative[PAYLOAD + 3] = (byte) 0x80;
    assertThrows(RuntimeException.class, () -> decode(negative), "beyond the revision-number range");

    final byte[] afterRevision = valid.clone();
    afterRevision[PAYLOAD] = 8;
    assertThrows(RuntimeException.class, () -> decode(afterRevision), "a revert cannot lie after its own root");

    final byte[] requiredTag = valid.clone();
    requiredTag[TAG] = (byte) 0xC1;
    assertThrows(RuntimeException.class, () -> decode(requiredTag), "an unknown required entry");

    final byte[] unknownFlag = valid.clone();
    unknownFlag[FLAGS] = (byte) 0x81;
    assertThrows(RuntimeException.class, () -> decode(unknownFlag), "an unknown flag bit");

    for (final int length : new int[] {0, 3, 5}) {
      final byte[] wrongLength = withEntryLength(valid, length);
      assertThrows(RuntimeException.class, () -> decode(wrongLength), "entry length " + length);
    }

    for (int cut = TAG; cut < PAYLOAD + 4; cut++) {
      final byte[] truncated = Arrays.copyOf(valid, cut);
      assertThrows(RuntimeException.class, () -> decode(truncated), "truncated at " + cut);
    }
  }

  @Test
  @DisplayName("FMT-203: a duplicated or misordered entry is refused, an unknown optional entry is skipped")
  void duplicatedMisorderedAndUnknownEntries() throws IOException {
    final byte[] valid = encode(root(7, 3));
    final byte[] entry = Arrays.copyOfRange(valid, TAG, PAYLOAD + 4);

    assertThrows(RuntimeException.class, () -> decode(withBlock(valid, concat(entry, entry))), "duplicated entry");

    final byte[] laterOptional = {0x42, 1, 0x7F};
    assertEquals(3, decode(withBlock(valid, concat(entry, laterOptional))).getLastRevertRevision(),
        "an unknown optional kind-local entry after it is skipped");
    assertThrows(RuntimeException.class, () -> decode(withBlock(valid, concat(laterOptional, entry))),
        "entries out of ascending tag order");
    assertEquals(0, decode(withBlock(valid, laterOptional)).getLastRevertRevision(),
        "an unknown optional entry alone carries no last-revert revision");
  }

  /** A fixed past commit time, so two images differ only in what the test varies. */
  private static final Instant COMMITTED = Instant.parse("2026-01-02T03:04:05.123456Z");

  private static RevisionRootPage root(final int revision, final int lastRevert) {
    final RevisionRootPage root;
    if (lastRevert != 0) {
      final RevisionRootPage atRevert = new RevisionRootPage(new RevisionRootPage(), lastRevert);
      atRevert.markRevertPublished();
      root = new RevisionRootPage(atRevert, revision);
    } else {
      root = new RevisionRootPage(new RevisionRootPage(), revision);
    }
    root.setCommitTimestamp(COMMITTED);
    return root;
  }

  /** Replaces the entry block of {@code image} with {@code entries}. */
  private static byte[] withBlock(final byte[] image, final byte[] entries) {
    final int bodyStart = PAYLOAD + 4;
    final byte[] result = new byte[TAG + entries.length + image.length - bodyStart];
    System.arraycopy(image, 0, result, 0, TAG);
    result[BLOCK_LENGTH] = (byte) entries.length;
    System.arraycopy(entries, 0, result, TAG, entries.length);
    System.arraycopy(image, bodyStart, result, TAG + entries.length, image.length - bodyStart);
    return result;
  }

  private static byte[] withEntryLength(final byte[] image, final int length) {
    final byte[] entry = new byte[2 + length];
    entry[0] = 0x41;
    entry[1] = (byte) length;
    System.arraycopy(image, PAYLOAD, entry, 2, Math.min(length, 4));
    return withBlock(image, entry);
  }

  private static byte[] concat(final byte[] first, final byte[] second) {
    final byte[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  private static byte[] encode(final RevisionRootPage page) throws IOException {
    try (final var sink = Bytes.elasticHeapByteBuffer()) {
      new PagePersister().serializePage(null, sink, page, SerializationType.DATA);
      return sink.toByteArray();
    }
  }

  private static RevisionRootPage decode(final byte[] bytes) throws IOException {
    return (RevisionRootPage) new PagePersister().deserializePage(null, Bytes.wrapForRead(bytes),
        SerializationType.DATA);
  }
}
