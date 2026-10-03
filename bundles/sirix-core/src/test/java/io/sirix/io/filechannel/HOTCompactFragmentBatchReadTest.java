package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.PageHasher;
import io.sirix.io.Reader;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.BytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Same durable bytes, reads and merged images; only the decoded fragment representation changes.
 */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class HOTCompactFragmentBatchReadTest {
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("compact-batch")
                           .versioningApproach(VersioningType.FULL)
                           .byteHandlerPipeline(new ByteHandlerPipeline())
                           .verifyChecksumsOnRead(true)
                           .build();
  private static final String BORROW_INPUT = "sirix.filechannel.borrowBatchInput";

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rawReadCopyPreservesCompletenessAndOutlivesTheCachedSource(final boolean completeDump) throws IOException {
    final HOTLeafPage source = new HOTLeafPage(1, 7, IndexType.PROJECTION);
    HOTLeafPage raw = null;
    HOTLeafPage copy = null;
    try {
      source.put(new byte[] {7, 1}, new byte[] {31});
      source.put(new byte[] {7, 2}, new byte[0]);
      final PageReference side = new PageReference().setKey(777);
      side.setHash(0L);
      source.setPageReference(42, side);
      source.setCompleteDump(completeDump);
      final byte[] wire = serialize(source);
      raw = assertInstanceOf(HOTLeafPage.class, new PagePersister().deserializeHOTLeafFragment(CONFIG,
          new MemorySegmentBytesIn(MemorySegment.ofArray(wire)), SerializationType.DATA));
      assertFalse(raw.slots().isNative());
      copy = raw.copyForRead();
      assertTrue(copy.slots().isNative());
      assertEquals(completeDump, copy.isCompleteDump());
      assertArrayEquals(serialize(raw), serialize(copy));
      raw.close();
      assertArrayEquals(new byte[] {31}, copy.copyStoredValue(copy.findEntry(new byte[] {7, 1})));
      assertEquals(0, copy.copyStoredValue(copy.findEntry(new byte[] {7, 2})).length);
      assertEquals(777, copy.getPageReference(42).getKey());
      assertFalse(copy.getPageReference(42).hasHash(), "the durable side-reference format contains offsets only");
      assertArrayEquals(wire, serialize(source));
    } finally {
      if (copy != null)
        copy.close();
      if (raw != null)
        raw.close();
      source.close();
    }
  }

  @Test
  void compactPointSearchMatchesNativeOrderingAndItsWritableCopy() throws IOException {
    final HOTLeafPage source = new HOTLeafPage(1, 1, IndexType.PROJECTION);
    HOTLeafPage compact = null;
    try {
      for (int i = 0; i < 257; i++) {
        final byte[] key = {42, (byte) (i >>> 7), (byte) (i << 1)};
        final byte[] value = i % 7 == 0
            ? new byte[0]
            : new byte[] {(byte) i, (byte) (i >>> 8)};
        assertTrue(source.put(key, value));
      }
      source.setCompleteDump(true);
      final byte[] wire = serialize(source);
      compact = assertInstanceOf(HOTLeafPage.class, new PagePersister().deserializeHOTLeafFragment(CONFIG,
          new MemorySegmentBytesIn(MemorySegment.ofArray(wire)), SerializationType.DATA));
      assertFalse(compact.slots().isNative());
      for (int i = 0; i < 516; i++) {
        final byte[] key = {42, (byte) (i >>> 8), (byte) i};
        final int expected = source.findEntry(key);
        assertEquals(expected, compact.findEntry(key));
        if (expected >= 0) {
          assertArrayEquals(source.copyStoredValue(expected), compact.copyStoredValue(expected));
        }
      }
      assertEquals(source.findEntry(new byte[] {41}), compact.findEntry(new byte[] {41}));
      assertEquals(source.findEntry(new byte[] {43}), compact.findEntry(new byte[] {43}));
      assertEquals(source.findEntry(new byte[] {42}), compact.findEntry(new byte[] {42}));
      final byte[] addedKey = {42, 3, 7};
      try (HOTLeafPage copy = compact.copyForRead()) {
        assertTrue(copy.put(addedKey, new byte[] {99}));
        assertTrue(copy.slots().isNative(), "a copy owns independent writable capacity");
        assertArrayEquals(new byte[] {99}, copy.copyStoredValue(copy.findEntry(addedKey)));
      }
      assertTrue(compact.findEntry(addedKey) < 0);
      assertArrayEquals(wire, serialize(compact));
      assertTrue(source.findEntry(addedKey) < 0);
      assertArrayEquals(wire, serialize(source), "the committed source bytes remain untouched");
    } finally {
      if (compact != null)
        compact.close();
      source.close();
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void reconstructionIsByteIdenticalWithTheSameReads(final VersioningType versioning) throws IOException {
    final int count = switch (versioning) {
      case FULL -> 1;
      case DIFFERENTIAL -> 2;
      case INCREMENTAL, SLIDING_SNAPSHOT -> 32;
    };
    final Fixture fixture = fixture(count, 4096);
    final List<String> reads = new ArrayList<>();
    final String previous = System.getProperty(BORROW_INPUT);
    System.setProperty(BORROW_INPUT, "true");
    try (FileChannelReader reader = reader(fixture.bytes(), new ByteHandlerPipeline(), new PagePersister(), reads)) {
      final Page[] scalar = new Page[count];
      for (int i = 0; i < count; i++) {
        scalar[i] = reader.read(fixture.references()[i], CONFIG);
      }
      reads.clear();
      final Page[] ordinary = reader.read(fixture.references(), CONFIG);
      final List<String> ordinaryReads = List.copyOf(reads);
      reads.clear();
      final Page[] compact = reader.readHOTLeafFragments(fixture.references(), CONFIG);
      try {
        assertEquals(ordinaryReads, reads,
            "identical positional calls and byte extents, including all 31 older fragments");
        final List<HOTLeafPage> expected = new ArrayList<>(count);
        final List<HOTLeafPage> actual = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
          final HOTLeafPage oldPage = assertInstanceOf(HOTLeafPage.class, scalar[i]);
          final HOTLeafPage newPage = assertInstanceOf(HOTLeafPage.class, compact[i]);
          assertTrue(oldPage.slots().isNative());
          assertFalse(newPage.slots().isNative(), "batch decode must avoid writable allocator frames");
          assertEquals(newPage.getUsedSlotsSize(), newPage.slots().byteSize());
          assertArrayEquals(serialize(oldPage), serialize(ordinary[i]), "ordinary batch versus scalar fragment " + i);
          assertArrayEquals(serialize(oldPage), serialize(newPage), "fragment " + i);
          expected.add(oldPage);
          actual.add(newPage);
        }
        // The actual chain loader retains its ordinary head. Compact only the older members.
        compact[0].close();
        compact[0] = reader.read(fixture.references()[0], CONFIG);
        actual.set(0, (HOTLeafPage) compact[0]);
        final HOTLeafPage oldMerged = versioning.combineHOTLeafPages(expected, count, null);
        final HOTLeafPage newMerged = versioning.combineHOTLeafPages(actual, count, null);
        try {
          assertArrayEquals(serialize(oldMerged), serialize(newMerged), "full reconstructed page image");
          assertArrayEquals(new byte[0], newMerged.copyStoredValue(newMerged.findEntry(key(0))),
              "newest tombstone wins");
          assertEquals(777, newMerged.getPageReference(42).getKey());
        } finally {
          if (oldMerged != expected.getFirst())
            oldMerged.close();
          if (newMerged != actual.getFirst())
            newMerged.close();
        }
        // Repeated span/final-body buffer reuse must not mutate any returned fragment.
        for (int reuse = 0; reuse < 4; reuse++)
          close(reader.read(fixture.references(), CONFIG));
        Arrays.fill(fixture.bytes(), (byte) 0xA5);
        for (int i = 0; i < count; i++)
          assertArrayEquals(serialize(ordinary[i]), serialize(compact[i]));
      } finally {
        close(scalar);
        close(ordinary);
        close(compact);
      }
    } finally {
      restoreProperty(previous);
    }
  }

  @ParameterizedTest
  @CsvSource({"4096,true,true", "4096,false,true", "4096,true,false", "131072,true,true"})
  void fallbacksPreserveAlignmentAndReadExtents(final int stride, final boolean borrow, final boolean segments)
      throws IOException {
    final String previous = System.getProperty(BORROW_INPUT);
    System.setProperty(BORROW_INPUT, Boolean.toString(borrow));
    final Fixture fixture = fixture(3, stride);
    final PageReference[] references = {fixture.references()[1], null, new PageReference(), fixture.references()[0],
        fixture.references()[2], fixture.references()[1]};
    final ByteHandler handler;
    if (segments) {
      handler = new ByteHandlerPipeline();
    } else {
      handler = mock(ByteHandler.class);
      when(handler.deserialize(any(InputStream.class))).thenAnswer(invocation -> invocation.getArgument(0));
    }
    final List<String> reads = new ArrayList<>();
    try (FileChannelReader reader = reader(fixture.bytes(), handler, new PagePersister(), reads)) {
      final Page[] ordinary = reader.read(references, CONFIG);
      final List<String> expectedReads = List.copyOf(reads);
      reads.clear();
      final Page[] compact = reader.readHOTLeafFragments(references, CONFIG);
      try {
        assertEquals(expectedReads, reads);
        for (int i = 0; i < references.length; i++) {
          if (ordinary[i] == null) {
            assertNull(compact[i]);
            continue;
          }
          assertArrayEquals(serialize(ordinary[i]), serialize(compact[i]));
          // Residency, not just bytes: a compact decode copies its own array and borrows nothing of
          // the span, so input borrowing must not decide it. Only segment support may.
          final HOTLeafPage decoded = assertInstanceOf(HOTLeafPage.class, compact[i]);
          assertEquals(!segments, decoded.slots().isNative(), "member " + i
              + " residency must follow segment support alone (borrow=" + borrow + ", segments=" + segments + ')');
        }
        reads.clear();
        assertEquals(0, reader.readHOTLeafFragments(new PageReference[0], CONFIG).length);
        assertArrayEquals(new Page[2],
            reader.readHOTLeafFragments(new PageReference[] {null, new PageReference()}, CONFIG));
        assertTrue(reads.isEmpty());
      } finally {
        close(ordinary);
        close(compact);
      }
    } finally {
      restoreProperty(previous);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 15, 31})
  void failedBatchClosesEveryDecodedImage(final int failingMember) throws IOException {
    final Fixture fixture = fixture(32, 4096);
    // Physical order is oldest first, whereas the input is newest first.
    final int end = failingMember == 31
        ? fixture.bytes().length
        : 64 + (failingMember + 1) * 4096;
    final int offset = 64 + failingMember * 4096;
    final int length = ByteBuffer.wrap(fixture.bytes()).order(ByteOrder.LITTLE_ENDIAN).getInt(offset);
    assertTrue(offset + Integer.BYTES + length <= end);
    fixture.bytes()[offset + Integer.BYTES + length - 1] ^= 1;
    final List<Page> decoded = new ArrayList<>();
    final PagePersister persister = spy(new PagePersister());
    doAnswer(invocation -> {
      final Page page = (Page) invocation.callRealMethod();
      decoded.add(page);
      return page;
    }).when(persister)
      .deserializeHOTLeafFragment(any(ResourceConfiguration.class), any(BytesIn.class), any(SerializationType.class));
    final String previous = System.getProperty(BORROW_INPUT);
    System.setProperty(BORROW_INPUT, "true");
    try (FileChannelReader reader = reader(fixture.bytes(), new ByteHandlerPipeline(), persister, new ArrayList<>())) {
      assertThrows(SirixCorruptionException.class, () -> reader.readHOTLeafFragments(fixture.references(), CONFIG));
      assertEquals(failingMember, decoded.size());
      for (final Page page : decoded)
        assertTrue(((HOTLeafPage) page).isClosed());
    } finally {
      restoreProperty(previous);
    }
  }

  @Test
  void defaultBatchRetainsSharedBackendOwnership() {
    final Reader reader = mock(Reader.class, CALLS_REAL_METHODS);
    final PageReference[] references = {new PageReference().setKey(3)};
    try (HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PROJECTION)) {
      final Page[] shared = {page};
      doReturn(true).when(reader).returnsSharedPages();
      doReturn(shared).when(reader).read(references, CONFIG);
      assertSame(shared, reader.readHOTLeafFragments(references, CONFIG));
      assertFalse(page.isClosed());
      final SirixIOException failure = new SirixIOException("batch rejected");
      doAnswer(_ -> {
        throw failure;
      }).when(reader).read(references, CONFIG);
      assertSame(failure, assertThrows(SirixIOException.class, () -> reader.readHOTLeafFragments(references, CONFIG)));
      assertFalse(page.isClosed());
    }
  }

  @Test
  void nonHotMembersKeepTheirOrdinaryDecoder() throws IOException {
    final byte[] bytes = frame(new OverflowPage(new byte[] {7, 8, 9}));
    try (FileChannelReader reader = reader(bytes, new ByteHandlerPipeline(), new PagePersister(), new ArrayList<>())) {
      final Page[] pages = reader.readHOTLeafFragments(new PageReference[] {new PageReference().setKey(0)}, CONFIG);
      try {
        assertArrayEquals(new byte[] {7, 8, 9}, assertInstanceOf(OverflowPage.class, pages[0]).getDataBytes());
      } finally {
        close(pages);
      }
    }
  }

  private record Fixture(byte[] bytes, PageReference[] references) {
  }

  private static Fixture fixture(final int count, final int stride) throws IOException {
    final byte[][] frames = new byte[count][];
    final PageReference[] references = new PageReference[count];
    for (int i = 0; i < count; i++) {
      try (HOTLeafPage page = new HOTLeafPage(123, count - i, IndexType.PROJECTION)) {
        if (i == count - 1) {
          for (int row = 0; row < 128; row++)
            assertTrue(page.put(key(row), new byte[] {(byte) row}));
          page.setCompleteDump(true);
        } else if (i % 7 != 6) {
          assertTrue(page.put(key(i + 1), new byte[] {(byte) i, 11, 13}));
        }
        if (i == 0) {
          if (count == 1) {
            // FULL already contains the live key. Use the native delete operation, not insert-only put.
            assertArrayEquals(new byte[] {0}, page.getValue(page.findEntry(key(0))));
            assertTrue(page.deleteAt(page.findEntry(key(0))));
          } else {
            // A delta records the same deletion as a newly inserted tombstone for the older live key.
            assertTrue(page.findEntry(key(0)) < 0);
            assertTrue(page.put(key(0), new byte[0]));
          }
          // Assert logical absence before serialization/decoding; the tombstone key stays stored
          // because physically removing it would permit an older live value to be resurrected.
          assertNull(page.getValue(page.findEntry(key(0))), "fixture must contain no live key 0 before decoding");
          assertArrayEquals(new byte[0], page.copyStoredValue(page.findEntry(key(0))));
        }
        page.setPageReference(42, new PageReference().setKey(777));
        frames[i] = frame(page);
        assertTrue(frames[i].length < stride);
        references[i] = new PageReference().setKey(64L + (long) (count - 1 - i) * stride);
        references[i].setHash(PageHasher.computeLong(Arrays.copyOfRange(frames[i], Integer.BYTES, frames[i].length),
            CONFIG.hashAlgorithm));
      }
    }
    final byte[] bytes = new byte[Math.toIntExact(references[0].getKey()) + frames[0].length];
    for (int i = 0; i < count; i++)
      System.arraycopy(frames[i], 0, bytes, Math.toIntExact(references[i].getKey()), frames[i].length);
    return new Fixture(bytes, references);
  }

  private static byte[] key(final int row) {
    return new byte[] {12, 13, 14, 15, (byte) (row >>> 8), (byte) row};
  }

  private static byte[] serialize(final Page page) throws IOException {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(128 * 1024)) {
      new PagePersister().serializePage(CONFIG, out, page, SerializationType.DATA);
      return out.toByteArray();
    }
  }

  private static byte[] frame(final Page page) throws IOException {
    final byte[] bytes = serialize(page);
    return ByteBuffer.allocate(bytes.length + Integer.BYTES)
                     .order(ByteOrder.LITTLE_ENDIAN)
                     .putInt(bytes.length)
                     .put(bytes)
                     .array();
  }

  private static FileChannelReader reader(final byte[] bytes, final ByteHandler handler, final PagePersister persister,
      final List<String> reads) throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    when(channel.size()).thenReturn((long) bytes.length);
    when(channel.read(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      final ByteBuffer target = invocation.getArgument(0);
      final int from = Math.toIntExact(invocation.<Long>getArgument(1));
      reads.add(from + ":" + target.remaining());
      if (from >= bytes.length)
        return -1;
      final int length = Math.min(target.remaining(), bytes.length - from);
      target.put(bytes, from, length);
      return length;
    });
    return new FileChannelReader(channel, mock(FileChannel.class), handler, SerializationType.DATA, persister,
        Caffeine.newBuilder().build());
  }

  private static void close(final Page[] pages) {
    for (final Page page : pages)
      if (page != null)
        page.close();
  }

  private static void restoreProperty(final String previous) {
    if (previous == null)
      System.clearProperty(BORROW_INPUT);
    else
      System.setProperty(BORROW_INPUT, previous);
  }
}
