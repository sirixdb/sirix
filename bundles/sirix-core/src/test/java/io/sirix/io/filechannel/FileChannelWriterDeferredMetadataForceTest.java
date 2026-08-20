package io.sirix.io.filechannel;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixIOException;
import io.sirix.io.IOStorage;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.RevisionRecordDurability;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class FileChannelWriterDeferredMetadataForceTest {

  private enum ChannelRole {
    DATA,
    REVISIONS,
    BEACON
  }

  private enum Operation {
    WRITE,
    FORCE
  }

  private record Event(ChannelRole channel, Operation operation, long offset, int length, boolean metadata) {
  }

  private record Fixture(FileChannelWriter writer, List<Event> events,
      AtomicBoolean failNextDataMetadataForce, AtomicBoolean failNextRevisionsMetadataForce,
      FileChannel data, FileChannel revisions, FileChannel beacon,
      Path revisionsFilePath) {
  }

  @Test
  void preallocationGrowthDefersAndCoalescesItsMetadataForce(@TempDir final Path tempDir) throws Exception {
    final Fixture fixture = fixture(tempDir);

    flushUncommittedTail(fixture.writer(), new byte[] {3, 1, 4});
    assertEquals(List.of(), dataForces(fixture.events()),
        "an unreachable async page tail must not fsync from the rotation path");

    fixture.writer().forceAll();
    assertEquals(List.of(true), dataForces(fixture.events()),
        "the first explicit durability barrier must absorb the pending allocation metadata");

    fixture.writer().forceAll();
    assertEquals(List.of(true, false), dataForces(fixture.events()),
        "after a successful metadata force, a stable-size barrier must return to fdatasync");
  }

  @Test
  void hftAllocationCountersAreWriterLocalAndCountOnlyRealGrowth(@TempDir final Path tempDir) throws Exception {
    assumeTrue(Boolean.getBoolean("sirix.hft.telemetry"),
        "run with -Dsirix.hft.telemetry=true to exercise static-final HFT instrumentation");
    final Fixture first = fixture(tempDir.resolve("first"));
    final Fixture second = fixture(tempDir.resolve("second"));

    flushUncommittedTail(first.writer(), new byte[] {1, 2, 3});

    assertEquals(1L, first.writer().hftDataAllocationGrowCount());
    assertTrue(first.writer().hftDataAllocationGrowBytes() > 0L);
    assertTrue(first.writer().hftDataAllocationGrowNanos() >= 0L);
    assertEquals(0L, second.writer().hftDataAllocationGrowCount(),
        "one writer's growth must not leak into another epoch's attribution baseline");

    // The second tiny tail remains inside the already allocated headroom.
    flushUncommittedTail(first.writer(), new byte[] {4, 5, 6});
    assertEquals(1L, first.writer().hftDataAllocationGrowCount());

    flushUncommittedTail(second.writer(), new byte[] {7, 8, 9});
    assertEquals(1L, second.writer().hftDataAllocationGrowCount());
  }

  @Test
  void failedMetadataForceKeepsTheUpgradeArmedForRetry(@TempDir final Path tempDir) throws Exception {
    final Fixture fixture = fixture(tempDir);
    flushUncommittedTail(fixture.writer(), new byte[] {2, 7, 1, 8});
    fixture.failNextDataMetadataForce().set(true);

    assertThrows(SirixIOException.class, fixture.writer()::forceAll);
    fixture.writer().forceAll();
    fixture.writer().forceAll();

    assertEquals(List.of(true, true, false), dataForces(fixture.events()),
        "a failed fsync must not let the retry downgrade to fdatasync");
  }

  @Test
  void abortedWriterForcesPendingAllocationBeforeCloseReturns(@TempDir final Path tempDir) throws Exception {
    final Fixture fixture = fixture(tempDir);
    flushUncommittedTail(fixture.writer(), new byte[] {1, 6, 1, 8});

    fixture.writer().close();

    assertEquals(List.of(true), dataForces(fixture.events()),
        "close must make a partially used preallocation durable before a pooled channel is reusable");
  }

  @Test
  void failedCloseForceIsInheritedByTheNextPooledWriter(@TempDir final Path tempDir) throws Exception {
    final AtomicInteger releases = new AtomicInteger();
    final FileChannelWriter.DataAllocationDurability sharedDurability =
        new FileChannelWriter.DataAllocationDurability();
    final Fixture first = fixture(tempDir, sharedDurability, releases::incrementAndGet);
    flushUncommittedTail(first.writer(), new byte[] {1, 4, 1, 4});
    first.failNextDataMetadataForce().set(true);

    final SirixIOException primary = assertThrows(SirixIOException.class, first.writer()::close);
    assertEquals("injected metadata-force failure", primary.getCause().getMessage());
    assertEquals(1, releases.get(), "a failed force must not prevent the pooled borrow from being returned");

    final FileChannelWriter successor = newWriter(first.data(), first.revisions(), first.beacon(),
        first.revisionsFilePath(), sharedDurability, () -> { });
    successor.forceAll();
    successor.forceAll();

    assertEquals(List.of(true, true, false), dataForces(first.events()),
        "the successor must inherit the failed fsync, then return to fdatasync only after success");
  }

  @Test
  void partialAllocationWriteFailureLeavesMetadataDirtyAcrossPooledHandoff(@TempDir final Path tempDir)
      throws Exception {
    final AtomicInteger releases = new AtomicInteger();
    final FileChannelWriter.DataAllocationDurability sharedDurability =
        new FileChannelWriter.DataAllocationDurability();
    final Fixture first = fixture(tempDir, sharedDurability, releases::incrementAndGet);
    final AtomicInteger allocationWriteCalls = new AtomicInteger();
    when(first.data().write(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      final ByteBuffer source = invocation.getArgument(0);
      final long offset = invocation.getArgument(1);
      final int call = allocationWriteCalls.getAndIncrement();
      if (call == 0) {
        final int partial = Math.min(1_024, source.remaining() - 1);
        source.position(source.position() + partial);
        first.events().add(new Event(ChannelRole.DATA, Operation.WRITE, offset, partial, false));
        return partial;
      }
      if (call == 1) {
        throw new IOException("injected partial allocation failure");
      }
      final int length = source.remaining();
      source.position(source.limit());
      first.events().add(new Event(ChannelRole.DATA, Operation.WRITE, offset, length, false));
      return length;
    });

    final SirixIOException allocationFailure = assertThrows(SirixIOException.class,
        () -> flushUncommittedTail(first.writer(), new byte[] {2, 7, 1, 8}));
    assertEquals("injected partial allocation failure", allocationFailure.getCause().getMessage());

    // Close must still surrender the pooled borrow. Inject another failure so only the shared
    // marker, not a successful close-time force, can carry the obligation to the successor.
    first.failNextDataMetadataForce().set(true);
    assertThrows(SirixIOException.class, first.writer()::close);
    assertEquals(1, releases.get());

    final FileChannelWriter successor = newWriter(first.data(), first.revisions(), first.beacon(),
        first.revisionsFilePath(), sharedDurability, () -> { });
    successor.forceAll();
    successor.forceAll();
    assertEquals(List.of(true, true, false), dataForces(first.events()),
        "a partial zero-fill must arm metadata durability before its first file write");
  }

  @Test
  void revisionsGrowthForcesImmediatelyAndPublishesItsFrontierOnlyAfterSuccess(@TempDir final Path tempDir)
      throws Exception {
    final Fixture fixture = fixture(tempDir);
    fixture.failNextRevisionsMetadataForce().set(true);
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("revisions-growth-force")
        .byteHandlerPipeline(new ByteHandlerPipeline())
        .build();
    config.resourcePath = tempDir;

    try (MemorySegmentBytesOut appendBuffer = new MemorySegmentBytesOut(4_096)) {
      assertThrows(SirixIOException.class,
          () -> fixture.writer().write(config, new PageReference(), new RevisionRootPage(), appendBuffer));
      fixture.writer().write(config, new PageReference(), new RevisionRootPage(), appendBuffer);
    }

    assertEquals(List.of(true, true), revisionsForces(fixture.events()),
        "a failed revisions fsync must retry the same grow instead of publishing its frontier");
    assertEquals(List.of(
        new Event(ChannelRole.REVISIONS, Operation.WRITE, 0L, 64 * 1_024, false),
        new Event(ChannelRole.REVISIONS, Operation.FORCE, -1L, 0, true),
        new Event(ChannelRole.REVISIONS, Operation.WRITE, 0L, 64 * 1_024, false),
        new Event(ChannelRole.REVISIONS, Operation.FORCE, -1L, 0, true),
        new Event(ChannelRole.REVISIONS, Operation.WRITE, IOStorage.revisionsFileOffset(0),
            IOStorage.REVISIONS_FILE_RECORD_SIZE, false)),
        fixture.events().stream().filter(event -> event.channel() == ChannelRole.REVISIONS).toList(),
        "revisions allocation must force immediately, retry after failure, then publish its record");
  }

  @Test
  void commitMetadataBarrierFollowsThePageTailAndPrecedesBothBeacons(@TempDir final Path tempDir)
      throws Exception {
    final Fixture fixture = fixture(tempDir);
    final byte[] pageTail = {9, 7, 9};
    flushUncommittedTail(fixture.writer(), pageTail);

    final ResourceConfiguration config = ResourceConfiguration.newBuilder("deferred-metadata-force")
        .byteHandlerPipeline(new ByteHandlerPipeline())
        .build();
    config.resourcePath = tempDir;
    try (MemorySegmentBytesOut beaconBuffer = new MemorySegmentBytesOut(2 * IOStorage.BEACON_SLOT_BYTES)) {
      fixture.writer().writeUberPageReference(config, new PageReference(), new UberPage(), beaconBuffer);
    }

    final int allocation = indexOf(fixture.events(), 0,
        event -> event.channel() == ChannelRole.DATA && event.operation() == Operation.WRITE
            && event.offset() == IOStorage.DATA_REGION_START && event.length() > pageTail.length);
    final int tail = indexOf(fixture.events(), allocation + 1,
        event -> event.channel() == ChannelRole.DATA && event.operation() == Operation.WRITE
            && event.offset() == IOStorage.DATA_REGION_START && event.length() == pageTail.length);
    final int barrier = indexOf(fixture.events(), tail + 1,
        event -> event.channel() == ChannelRole.DATA && event.operation() == Operation.FORCE && event.metadata());
    final List<Integer> beaconWriteIndexes = new ArrayList<>();
    for (int index = 0; index < fixture.events().size(); index++) {
      final Event event = fixture.events().get(index);
      if (event.operation() == Operation.WRITE
          && (event.offset() == IOStorage.PRIMARY_BEACON_OFFSET
              || event.offset() == IOStorage.SECONDARY_BEACON_OFFSET)) {
        beaconWriteIndexes.add(index);
      }
    }

    assertTrue(allocation >= 0, "the fixture must exercise a real adaptive preallocation grow");
    assertTrue(tail > allocation, "the page tail must overwrite the start of the zero-filled allocation");
    assertTrue(barrier > tail,
        "the allocation fsync must be coalesced after the complete page tail, not run inside growth");
    assertEquals(2, beaconWriteIndexes.size(), "the commit must publish exactly two uber-page beacon writes");
    assertEquals(1L, beaconWriteIndexes.stream()
        .map(fixture.events()::get)
        .filter(event -> event.offset() == IOStorage.PRIMARY_BEACON_OFFSET)
        .count());
    assertEquals(1L, beaconWriteIndexes.stream()
        .map(fixture.events()::get)
        .filter(event -> event.offset() == IOStorage.SECONDARY_BEACON_OFFSET)
        .count());
    assertTrue(beaconWriteIndexes.stream().allMatch(index -> index > barrier),
        "both durable uber-page beacons must follow allocation + page-tail durability");
  }

  private static Fixture fixture(final Path tempDir) throws IOException {
    return fixture(tempDir, new FileChannelWriter.DataAllocationDurability(), null);
  }

  private static Fixture fixture(final Path tempDir,
      final FileChannelWriter.DataAllocationDurability dataAllocationDurability,
      final Runnable releaseAction) throws IOException {
    final Path revisionsFilePath = tempDir.resolve("sirix.revisions");
    RevisionRecordDurability.invalidateFor(revisionsFilePath);
    final List<Event> events = new ArrayList<>();
    final AtomicBoolean failNextDataMetadataForce = new AtomicBoolean();
    final AtomicBoolean failNextRevisionsMetadataForce = new AtomicBoolean();
    final FileChannel data = mock(FileChannel.class);
    final FileChannel revisions = mock(FileChannel.class);
    final FileChannel beacon = mock(FileChannel.class);

    when(data.size()).thenReturn(0L);
    when(revisions.size()).thenReturn(0L);
    recordWrites(data, ChannelRole.DATA, events);
    recordWrites(revisions, ChannelRole.REVISIONS, events);
    recordWrites(beacon, ChannelRole.BEACON, events);
    recordForces(data, ChannelRole.DATA, events, failNextDataMetadataForce);
    recordForces(revisions, ChannelRole.REVISIONS, events, failNextRevisionsMetadataForce);
    recordForces(beacon, ChannelRole.BEACON, events, new AtomicBoolean());

    final FileChannelWriter writer = newWriter(data, revisions, beacon, revisionsFilePath,
        dataAllocationDurability, releaseAction);
    return new Fixture(writer, events, failNextDataMetadataForce, failNextRevisionsMetadataForce,
        data, revisions, beacon, revisionsFilePath);
  }

  private static FileChannelWriter newWriter(final FileChannel data, final FileChannel revisions,
      final FileChannel beacon, final Path revisionsFilePath,
      final FileChannelWriter.DataAllocationDurability dataAllocationDurability,
      final Runnable releaseAction) throws IOException {
    final FileChannelReader reader = mock(FileChannelReader.class);
    when(reader.beaconRevisionOrMinusOne(anyLong())).thenReturn(-1);
    when(reader.readBeaconSlot(anyLong()))
        .thenAnswer(invocation -> ByteBuffer.allocate(IOStorage.BEACON_SLOT_BYTES));
    return new FileChannelWriter(data, revisions, beacon, SerializationType.DATA,
        new PagePersister(), Caffeine.newBuilder().buildAsync(), new RevisionIndexHolder(), reader,
        true, true, revisionsFilePath, 0L, 0L, dataAllocationDurability, releaseAction);
  }

  private static void flushUncommittedTail(final FileChannelWriter writer, final byte[] bytes) {
    try (MemorySegmentBytesOut appendBuffer = new MemorySegmentBytesOut(32)) {
      appendBuffer.write(bytes);
      writer.flushBufferedWrites(appendBuffer);
    }
  }

  private static void recordWrites(final FileChannel channel, final ChannelRole role,
      final List<Event> events) throws IOException {
    when(channel.write(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      final ByteBuffer source = invocation.getArgument(0);
      final long offset = invocation.getArgument(1);
      final int length = source.remaining();
      source.position(source.limit());
      events.add(new Event(role, Operation.WRITE, offset, length, false));
      return length;
    });
  }

  private static void recordForces(final FileChannel channel, final ChannelRole role,
      final List<Event> events, final AtomicBoolean failNextMetadataForce) throws IOException {
    doAnswer(invocation -> {
      final boolean metadata = invocation.getArgument(0);
      events.add(new Event(role, Operation.FORCE, -1L, 0, metadata));
      if (metadata && failNextMetadataForce.compareAndSet(true, false)) {
        throw new IOException("injected metadata-force failure");
      }
      return null;
    }).when(channel).force(anyBoolean());
  }

  private static List<Boolean> dataForces(final List<Event> events) {
    return events.stream()
        .filter(event -> event.channel() == ChannelRole.DATA && event.operation() == Operation.FORCE)
        .map(Event::metadata)
        .toList();
  }

  private static List<Boolean> revisionsForces(final List<Event> events) {
    return events.stream()
        .filter(event -> event.channel() == ChannelRole.REVISIONS && event.operation() == Operation.FORCE)
        .map(Event::metadata)
        .toList();
  }

  private static int indexOf(final List<Event> events, final int from,
      final java.util.function.Predicate<Event> predicate) {
    for (int index = Math.max(0, from); index < events.size(); index++) {
      if (predicate.test(events.get(index))) {
        return index;
      }
    }
    return -1;
  }
}
