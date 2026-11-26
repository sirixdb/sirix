package io.sirix.budget;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.io.IOStorage;
import io.sirix.io.ResourceFormatCompatibility;
import io.sirix.io.StorageType;
import io.sirix.budget.WorkCapture.ThrowingAction;
import io.sirix.io.filechannel.FileChannelReader;
import io.sirix.page.SerializationType;
import io.sirix.page.PagePersister;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.nio.file.StandardOpenOption;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mockStatic;

@Isolated
final class FormatDiscoveryWorkBudgetTest {
  private static final int WINDOW = 16;
  private static final int INSPECTIONS = WINDOW + 1;

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void coldOpenDiscoveryStaysBoundedAsHistoryGrows(final StorageType backend) throws Exception {
    for (final int revisions : new int[] {20, 80}) {
      final Path database = createStore(backend, revisions);
      final Path resource = database.resolve("resources/fixture");
      final long byteBudget = byteBudget(resource, revisions);
      for (final boolean repeatDiscovery : new boolean[] {false, true}) {
        Databases.clearGlobalCaches();
        try (final DiscoveryReads reads = new DiscoveryReads(resource, repeatDiscovery)) {
          final WorkReport work = reads.capture.run(() -> {
            try (final var opened = Databases.openJsonDatabase(database);
                final var session = opened.beginResourceSession("fixture")) {
              assertEquals(revisions, session.getMostRecentRevisionNumber());
              try (final var reader = session.beginNodeReadOnlyTrx()) {
                assertTrue(reader.moveToFirstChild());
                assertEquals("preserved", reader.getValue());
              }
            }
          });
          work.assertExactly(reads.gatesCounter, 1, "one cold resource open must establish one compatibility proof");
          if (repeatDiscovery) {
            assertThrows(AssertionError.class, () -> work.assertAtMost(reads.bytesCounter, byteBudget,
                "repeating real discovery must exceed its byte budget"));
            assertThrows(AssertionError.class, () -> work.assertAtMost(reads.recordsCounter, INSPECTIONS,
                "repeating real discovery must exceed its record budget"));
            assertThrows(AssertionError.class, () -> work.assertAtMost(reads.envelopesCounter, INSPECTIONS,
                "repeating real discovery must exceed its envelope budget"));
          } else {
            work.assertBetween(reads.bytesCounter, 1, byteBudget,
                "FMT-401 discovery may read only both identities, one beacon, the latest root and 16 root records");
            work.assertBetween(reads.recordsCounter, 1, INSPECTIONS,
                "normal discovery must not grow with history beyond its latest record and 16-entry window");
            work.assertBetween(reads.envelopesCounter, 1, INSPECTIONS,
                "normal discovery must not inspect more than the latest envelope and its 16-entry window");
          }
        }
      }
    }
  }

  /** CONV-301: a complete window has one directory I/O, independently of envelope byte counts. */
  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void completeWindowNeedsOneDirectoryRead(final StorageType backend) throws Exception {
    final Path database = createStore(backend, 20);
    final Path data = database.resolve("resources/fixture/data");
    Databases.clearGlobalCaches();
    try (final FileChannel pages = FileChannel.open(data.resolve(IOStorage.FILENAME), StandardOpenOption.READ);
        final FileChannel records =
            FileChannel.open(data.resolve(IOStorage.REVISIONS_FILENAME), StandardOpenOption.READ)) {
      final long[] reads = {0};
      final WorkCounter calls = WorkCounter.alwaysOn("format.directoryReadCalls",
          "positional reads of the complete 16-slot discovery window", () -> reads[0]);
      final WorkCapture capture = WorkCapture.of(calls);
      final FileChannel observed = mock(FileChannel.class, delegatesTo(records));
      doAnswer(call -> {
        reads[0]++;
        return records.read(call.getArgument(0), call.getArgument(1));
      }).when(observed).read(any(ByteBuffer.class), anyLong());
      try (final FileChannelReader reader = new FileChannelReader(pages, observed, new ByteHandlerPipeline(),
          SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build())) {
        final ThrowingAction discover = () -> reader.requireCompatibleRevisionRoots(20);
        capture.run(discover).assertExactly(calls, 1, "a complete window must not fall back to per-record reads");
        Databases.clearGlobalCaches();
        final WorkReport repeated = capture.run(() -> {
          discover.run();
          discover.run();
        });
        assertThrows(AssertionError.class,
            () -> repeated.assertAtMost(calls, 1, "two real window validations must exceed the single-read bound"));
      }
    }
  }

  private Path createStore(final StorageType backend, final int revisions) {
    // Reopen canonicalizes this spelling, as with macOS temporary-directory aliases.
    final Path database = directory.resolve(".").resolve(backend + "-" + revisions);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(database)));
    try (final var opened = Databases.openJsonDatabase(database)) {
      assertTrue(opened.createResource(ResourceConfiguration.newBuilder("fixture")
                                                            .storageType(backend)
                                                            .byteHandlerPipeline(new ByteHandlerPipeline())
                                                            .hashKind(HashType.NONE)
                                                            .buildPathSummary(false)
                                                            .build()));
      try (final var session = opened.beginResourceSession("fixture"); final var writer = session.beginNodeTrx()) {
        writer.insertStringValueAsFirstChild("preserved");
        for (int revision = 0; revision < revisions; revision++) {
          writer.commit();
        }
      }
    }
    return database;
  }

  private static long byteBudget(final Path resource, final int revision) throws Exception {
    final Path data = resource.resolve("data");
    final ByteBuffer records =
        ByteBuffer.wrap(Files.readAllBytes(data.resolve(IOStorage.REVISIONS_FILENAME))).order(ByteOrder.LITTLE_ENDIAN);
    final ByteBuffer pages =
        ByteBuffer.wrap(Files.readAllBytes(data.resolve(IOStorage.FILENAME))).order(ByteOrder.LITTLE_ENDIAN);
    long bytes = 2 * 64 + 4096 + INSPECTIONS * 32;
    for (int inspected = revision - WINDOW + 1; inspected <= revision; inspected++) {
      bytes += framedRootBytes(records, pages, inspected);
    }
    return bytes + framedRootBytes(records, pages, revision);
  }

  private static int framedRootBytes(final ByteBuffer records, final ByteBuffer pages, final int revision) {
    final int offset = Math.toIntExact(records.getLong(4096 + revision * 32));
    final int payload = pages.getInt(offset);
    assertTrue(payload > 0 && payload <= 512, "the scalar fixture must retain small revision roots");
    return Integer.BYTES + payload;
  }

  private static final class DiscoveryReads implements AutoCloseable {
    private long bytes;
    private long recordBytes;
    private long envelopes;
    private long gates;
    private boolean discovering;
    private final WorkCounter bytesCounter = WorkCounter.alwaysOn("format.bytesRead",
        "actual file bytes returned during compatibility discovery", () -> bytes);
    private final WorkCounter recordsCounter = WorkCounter.alwaysOn("format.recordsRead",
        "32-byte revision record slots returned during compatibility discovery", () -> recordBytes / 32);
    private final WorkCounter envelopesCounter = WorkCounter.alwaysOn("format.envelopesInspected",
        "framed page-length headers read during compatibility discovery", () -> envelopes);
    private final WorkCounter gatesCounter =
        WorkCounter.alwaysOn("format.proofs", "normal-open calls to the shared compatibility gate", () -> gates);
    private final WorkCapture capture = WorkCapture.of(bytesCounter, recordsCounter, envelopesCounter, gatesCounter);
    private final MockedStatic<FileChannel> channels;
    private final MockedStatic<ResourceFormatCompatibility> compatibility;

    private DiscoveryReads(final Path resource, final boolean repeatDiscovery) {
      final Path data = resource.resolve("data");
      channels = mockStatic(FileChannel.class, invocation -> {
        final Object result = invocation.callRealMethod();
        if (discovering && invocation.getRawArguments().length == 2 && invocation.getArgument(0) instanceof Path path
            && Files.isSameFile(path.getParent(), data) && result instanceof FileChannel channel) {
          return observe(channel, path.getFileName().toString().equals(IOStorage.REVISIONS_FILENAME));
        }
        return result;
      });
      compatibility = mockStatic(ResourceFormatCompatibility.class, invocation -> {
        if (!invocation.getMethod().getName().equals("verify") || invocation.getRawArguments().length != 1) {
          return invocation.callRealMethod();
        }
        gates++;
        discovering = true;
        try {
          final Object proof = invocation.callRealMethod();
          if (repeatDiscovery) {
            invocation.callRealMethod();
          }
          return proof;
        } finally {
          discovering = false;
        }
      });
    }

    private FileChannel observe(final FileChannel channel, final boolean revisions) {
      final Answer<Object> delegate = delegatesTo(channel);
      return mock(FileChannel.class, invocation -> {
        if (!invocation.getMethod().getName().equals("read")) {
          return delegate.answer(invocation);
        }
        final long offset =
            invocation.getRawArguments().length == 2 && invocation.getArgument(1) instanceof Long position
                ? position
                : channel.position();
        final int requested = invocation.getArgument(0) instanceof ByteBuffer buffer
            ? buffer.remaining()
            : 0;
        final Object result = delegate.answer(invocation);
        final long read = ((Number) result).longValue();
        if (discovering && read > 0) {
          bytes += read;
          if (revisions && offset >= 4096) {
            recordBytes += read;
          } else if (!revisions && offset >= IOStorage.DATA_REGION_START && requested == Integer.BYTES) {
            envelopes++;
          }
        }
        return result;
      });
    }

    @Override
    public void close() {
      compatibility.close();
      channels.close();
    }
  }
}
