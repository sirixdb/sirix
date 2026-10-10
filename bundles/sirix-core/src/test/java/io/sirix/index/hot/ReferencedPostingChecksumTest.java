package io.sirix.index.hot;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ReferencedPostingChecksumTest {
  private static final String RESOURCE = "postings";

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void coldReopenDetectsSameLengthPayloadCorruption(final VersioningType versioningType) throws IOException {
    assertColdIntegrity(versioningType, IndexType.CAS, CASKeySerializer.INSTANCE,
        new CASValue(new Str("hot"), Type.STR, 1));
    assertColdIntegrity(versioningType, IndexType.VALIDTIME, ValidTimeKeySerializer.INSTANCE,
        new ValidTimeKey(ValidTimeKey.STORE_LOWER, 1234, 5678));
  }

  private <K extends Comparable<? super K>> void assertColdIntegrity(final VersioningType versioningType,
      final IndexType indexType, final HOTKeySerializer<K> serializer, final K key) throws IOException {
    final Path databasePath = directory.resolve(indexType.name());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final NodeReferences expected = new NodeReferences();
    for (int i = 0; i < 32; i++) {
      expected.addNodeKey(2L * i);
    }
    final byte[] payload = NodeReferencesSerializer.serialize(expected);
    assertEquals(258, payload.length);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .byteHandlerPipeline(new ByteHandlerPipeline())
                                                   .verifyChecksumsOnRead(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTBulkIndexLoader<K> loader = writer(trx, serializer, indexType).createBulkLoader();
          for (int i = 0; i < 32; i++) {
            loader.add(key, 2L * i);
          }
          loader.flush();
          trx.commit();
        }
        for (int change = 0; change < PostingDeltas.FOLD_BOUND; change++) {
          try (JsonNodeTrx trx = session.beginNodeTrx()) {
            final HOTIndexWriter<K> writer = writer(trx, serializer, indexType);
            if ((change & 1) == 0) {
              assertTrue(writer.remove(key, 0));
            } else {
              writer.indexNodeKey(key, 0);
            }
            trx.commit();
          }
        }
      }
    }
    Databases.clearGlobalCaches();
    final ResourceConfiguration config;
    final long sidePageOffset;
    final byte[] keyBuffer = new byte[serializer.maxSerializedLength(key) + HOTKeySerializer.CHUNK_IDX_BYTES];
    final byte[] composite = Arrays.copyOf(keyBuffer, serializer.serializeWithChunkIdx(key, 0, keyBuffer, 0));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      config = session.getResourceConfig();
      assertReaderPaths(session, serializer, indexType, key, expected.toSortedArray(), false);
      assertWriterPaths(session, serializer, indexType, key, composite, expected.toSortedArray(), false);
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx();
          HOTTrieReader trie = new HOTTrieReader(trx.getStorageEngineReader())) {
        final HOTIndexReader<K> reader = HOTIndexReader.create(trx.getStorageEngineReader(), serializer, indexType, 0);
        final HOTLeafPage leaf = trie.navigateToLeaf(requireNonNull(reader.getRootReference()), composite);
        assertNotNull(leaf);
        final int slot = leaf.findEntry(composite);
        assertTrue(slot >= 0);
        final long value = leaf.valueRef(slot);
        assertTrue(NodeReferencesSerializer.isReferenced(leaf, value));
        assertEquals(258, NodeReferencesSerializer.referencedPayloadLength(leaf, value));
        final PageReference reference = leaf.getPageReference(NodeReferencesSerializer.referencedKey(leaf, value));
        assertNotNull(reference);
        assertFalse(reference.hasHash(), "cold side references persist only an offset");
        sidePageOffset = reference.getKey();
        assertArrayEquals(payload,
            requireNonNull(trx.getStorageEngineReader().readSideOverflowPage(reference)).getDataBytes());
      }
    }
    Databases.clearGlobalCaches();
    corruptLastPosting(config, sidePageOffset, payload);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertReaderPaths(session, serializer, indexType, key, expected.toSortedArray(), true);
      assertWriterPaths(session, serializer, indexType, key, composite, expected.toSortedArray(), true);
    }
    Databases.clearGlobalCaches();
  }

  private static void corruptLastPosting(final ResourceConfiguration config, final long offset, final byte[] payload)
      throws IOException {
    final byte[] corrupted = payload.clone();
    corrupted[corrupted.length - 1] ^= 1;
    final long[] postings = NodeReferencesSerializer.deserializeChunk(corrupted).toSortedArray();
    assertEquals(63, postings[postings.length - 1]);
    final byte[] wire = serialize(config, payload);
    final byte[] corruptedWire = serialize(config, corrupted);
    assertEquals(wire.length, corruptedWire.length, "the corrupted side page must retain its serialized length");
    if (wire[2] == 0) {
      int differences = 0;
      for (int i = 0; i < wire.length; i++) {
        differences += Integer.bitCount((wire[i] ^ corruptedWire[i]) & 0xFF);
      }
      assertEquals(1, differences, "uncompressed corruption must change exactly one persisted bit");
    }
    final Path file =
        config.resourcePath.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve("sirix.data");
    try (RandomAccessFile data = new RandomAccessFile(file.toFile(), "rw")) {
      data.seek(offset);
      final byte[] lengthBytes = new byte[Integer.BYTES];
      data.readFully(lengthBytes);
      assertEquals(wire.length, ByteBuffer.wrap(lengthBytes).order(ByteOrder.nativeOrder()).getInt());
      final byte[] stored = new byte[wire.length];
      data.readFully(stored);
      assertArrayEquals(wire, stored);
      data.seek(offset + Integer.BYTES);
      data.write(corruptedWire);
    }
  }

  private static byte[] serialize(final ResourceConfiguration config, final byte[] payload) throws IOException {
    try (BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer()) {
      new PagePersister().serializePage(config, sink, new OverflowPage(payload), SerializationType.DATA);
      return sink.toByteArray();
    }
  }

  private static <K extends Comparable<? super K>> void assertReaderPaths(final JsonResourceSession session,
      final HOTKeySerializer<K> serializer, final IndexType indexType, final K key, final long[] expected,
      final boolean corrupted) {
    try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
      final HOTIndexReader<K> reader = HOTIndexReader.create(trx.getStorageEngineReader(), serializer, indexType, 0);
      if (corrupted) {
        assertThrows(SirixCorruptionException.class, () -> reader.get(key, SearchMode.EQUAL));
      } else {
        assertArrayEquals(expected, requireNonNull(reader.get(key, SearchMode.EQUAL)).toSortedArray());
      }
      final List<Supplier<Iterator<Map.Entry<K, NodeReferences>>>> scans = List.of(reader::iterator,
          () -> reader.iteratorFrom(key, true), () -> reader.iteratorTo(key, true), () -> reader.range(key, key));
      for (final Supplier<Iterator<Map.Entry<K, NodeReferences>>> scan : scans) {
        if (corrupted) {
          assertThrows(SirixCorruptionException.class, () -> scan.get().next());
        } else {
          final Iterator<Map.Entry<K, NodeReferences>> entries = scan.get();
          assertArrayEquals(expected, entries.next().getValue().toSortedArray());
          assertFalse(entries.hasNext());
        }
      }
    }
  }

  private static <K extends Comparable<? super K>> void assertWriterPaths(final JsonResourceSession session,
      final HOTKeySerializer<K> serializer, final IndexType indexType, final K key, final byte[] composite,
      final long[] expected, final boolean corrupted) {
    for (int operation = 0; operation < 6; operation++) {
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        final HOTIndexWriter<K> writer = writer(trx, serializer, indexType);
        final PlainWriter plain = new PlainWriter(trx.getStorageEngineWriter(), indexType, composite);
        final int action = operation;
        final Runnable read = () -> {
          switch (action) {
            case 0 -> writer.get(key, SearchMode.EQUAL);
            case 1 -> writer.indexNodeKey(key, 64);
            case 2 -> assertTrue(writer.remove(key, 0));
            case 3 -> plain.lookup();
            case 4 -> plain.merge();
            case 5 -> assertTrue(plain.remove());
            default -> throw new AssertionError(action);
          }
        };
        if (corrupted) {
          assertThrows(SirixCorruptionException.class, read::run);
        } else {
          read.run();
          final long[] result = action == 1 || action == 4
              ? Arrays.copyOf(expected, expected.length + 1)
              : action == 2 || action == 5
                  ? Arrays.copyOfRange(expected, 1, expected.length)
                  : expected;
          if (action == 1 || action == 4) {
            result[result.length - 1] = 64;
          }
          assertArrayEquals(result, requireNonNull(writer.get(key, SearchMode.EQUAL)).toSortedArray());
        }
        trx.rollback();
      }
    }
  }

  private static <K extends Comparable<? super K>> HOTIndexWriter<K> writer(final JsonNodeTrx trx,
      final HOTKeySerializer<K> serializer, final IndexType indexType) {
    return HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, indexType, 0);
  }

  private static final class PlainWriter extends AbstractHOTIndexWriter<byte[]> {
    private byte[] buffer;

    private PlainWriter(final StorageEngineWriter storage, final IndexType indexType, final byte[] composite) {
      super(storage, indexType, 0);
      buffer = composite;
      if (indexType == IndexType.CAS) {
        initializeCASIndex();
      } else {
        initializeValidTimeIndex();
      }
    }

    private void lookup() {
      final HOTLeafPage leaf = acquireLeafForRead(buffer);
      assertNotNull(leaf);
      try {
        assertNotNull(getFromLeaf(leaf, buffer));
      } finally {
        releaseLeafReadGuard(leaf, null);
      }
    }

    private void merge() {
      final NodeReferences addition = new NodeReferences();
      addition.addNodeKey(64);
      final byte[] payload = NodeReferencesSerializer.serialize(addition);
      doIndex(buffer, buffer.length, payload, payload.length);
    }

    private boolean remove() {
      return doRemovePostingBit(buffer, buffer.length, 0);
    }

    @Override
    protected byte[] getKeyBuffer() {
      return buffer;
    }

    @Override
    protected void setKeyBuffer(final byte[] replacement) {
      buffer = replacement;
    }

    @Override
    protected int serializeKey(final byte[] key, final byte[] target, final int offset) {
      System.arraycopy(key, 0, target, offset, key.length);
      return key.length;
    }
  }
}
