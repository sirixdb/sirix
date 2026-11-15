/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.cache.PageContainer;
import io.sirix.index.IndexType;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.json.StringNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageConstants;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.Constants;
import io.sirix.settings.StringCompressionType;
import io.sirix.settings.VersioningType;
import io.sirix.utils.FSSTCompressor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The inline-or-carrier contract for FSST-compressed string values across storage epochs.
 *
 * <p>
 * <b>Specification.</b> Whether a string value is stored inline or in an overflow carrier depends
 * only on its encoded size and the page budget. A transaction that rotates storage epochs (bounded
 * intent log, explicit spilling epochs, or the async-flush import mode) makes the same decision for
 * every record as a transaction that never rotates, under every hash kind:
 * </p>
 * <ul>
 * <li>a pending string record is encoded against its page's table before any route judges its size
 * (an epoch rotation is such a route, exactly like the commit);</li>
 * <li>a rotation never changes a page's table binding: re-reading a page the transaction flushed
 * rebuilds the page the first copy-on-write built, binding included;</li>
 * <li>hence a value that fits inline once encoded is never left in a carrier, and the committed
 * values, carrier set and (for spilling epochs) persisted page frames equal the plain twin's.</li>
 * </ul>
 *
 * <p>
 * Error case covered by construction: a page bound to a table only by id is resolved before its
 * pending strings are materialized ({@code KeyValueLeafPage#materializePendingRecords} refuses the
 * unresolved state).
 * </p>
 */
final class StorageEpochFsstInlineDecisionTest {
  private static final User USER = new User("inline-decision", new UUID(0L, 3L));
  /** Two node keys per record (object and fused string); 1,100 records overfill page 0. */
  private static final int SMALL_RECORDS = 1100;
  private static final int PROPERTY_RECORDS = 9000;
  private static final long[] SEEDS = {0x5EED_0001L, 0x5EED_0002L};
  private static final String[] VOCABULARY = {"https://", "sirix.io/", "records/", "common-prefix/", "publication/",
      "revision/", "temporal/", "bitemporal/", "document/", "valid-time/", "transaction-time/", "snapshot/"};
  private static final String ALPHANUMERIC = "abcdefghijklmnopqrstuvwxyz0123456789";

  @TempDir
  Path directory;

  @AfterEach
  void restoreBound() {
    StorageEpochTestSupport.setPlainTransactionBound(0);
    Databases.clearGlobalCaches();
  }

  /**
   * Regression for the rotation that unbound a full page. A page whose newest committed fragment
   * holds every slot is read as that one fragment and keeps its table; once a spilling epoch flushed
   * it, the owned re-read combined the whole committed chain, decompressed every string, and left the
   * page unbound — so the next oversized edit stayed a raw carrier through the commit. Failed under
   * {@code NONE} on the original identity test; the hash kind only decides which pages a rotation
   * catches mid-edit, so every kind must hold.
   */
  @ParameterizedTest
  @EnumSource(value = HashType.class, names = {"NONE", "ROLLING", "POSTORDER"})
  void aRotatedFullPageKeepsItsTableAndItsEditsInline(final HashType hash) throws IOException {
    final long replaysBefore = VersioningType.ownedReplaysKeepingFsstBinding();
    final Path plainPath = directory.resolve("plain");
    final Path rotatedPath = directory.resolve("rotated");
    final Decisions plain = editFullPage(plainPath, hash, false);
    final Decisions rotated = editFullPage(rotatedPath, hash, true);
    assertEquals(new TreeSet<Long>(), plain.carriers(), "the plain twin keeps every encoded value inline");
    assertEquals(plain.carriers(), rotated.carriers(), "rotation must not change any inline-or-carrier decision");
    assertEquals(plain.values(), rotated.values());
    assertEquals(plain.tableIds(), rotated.tableIds(), "rotation must not change any page's table binding");
    // Asserted last so that, against an engine without the fix, the failure names the decision.
    assertTrue(VersioningType.ownedReplaysKeepingFsstBinding() > replaysBefore,
        "the flushed page must have been re-read through the owned-fragment replay");
  }

  /**
   * The replay rebuilds a page created in this transaction onto fresh, unbound pages. They must adopt
   * the owned fragment's binding: its strings were encoded at insert against the transaction's table,
   * and a page without that binding could neither decode them nor keep later edits inline.
   */
  @ParameterizedTest
  @EnumSource(value = HashType.class, names = {"NONE", "ROLLING"})
  void aPageCreatedInTheTransactionKeepsItsBindingAcrossARotation(final HashType hash) throws IOException {
    final long replaysBefore = VersioningType.ownedReplaysKeepingFsstBinding();
    final Decisions plain = extendAndEdit(directory.resolve("plain"), hash, false);
    final Decisions rotated = extendAndEdit(directory.resolve("rotated"), hash, true);
    assertEquals(plain.values(), rotated.values());
    assertEquals(new TreeSet<Long>(), plain.carriers(), "the plain twin keeps every encoded value inline");
    assertEquals(plain.carriers(), rotated.carriers(), "rotation must not change any inline-or-carrier decision");
    assertEquals(plain.tableIds(), rotated.tableIds(), "rotation must not change any page's table binding");
    assertTrue(VersioningType.ownedReplaysKeepingFsstBinding() > replaysBefore,
        "the new page must have been re-read through the owned-fragment replay");
  }

  /**
   * The rebuilt pages are the ones the first copy-on-write built: the full newest committed fragment
   * alone, so its encoded strings stay encoded and no older fragment is decompressed into read-side
   * carriers.
   */
  @Test
  void theReplayRebuildsTheFullPageTheFirstCopyOnWriteRead() throws IOException {
    final Path path = directory.resolve("replay");
    createResource(path, HashType.NONE, StorageType.FILE_CHANNEL);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER);
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      final List<Long> keys = new ArrayList<>(SMALL_RECORDS);
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        insertRecords(writer, SMALL_RECORDS, keys, StorageEpochFsstInlineDecisionTest::oversizedCompressible);
        writer.commit(null, Instant.parse("2019-01-01T00:00:00Z"));
      }
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        assertTrue(writer.moveTo(keys.get(1)) && writer.moveToFirstChild());
        writer.setStringValue(oversizedCompressible(1, 1));
        writer.getStorageEngineWriter().asyncFlushStorageEpoch();
        writer.getStorageEngineWriter().awaitPendingAsyncFlush();
        assertTrue(writer.moveTo(keys.get(2)) && writer.moveToFirstChild());
        writer.setStringValue(oversizedCompressible(2, 1));

        assertTrue(writer.moveTo(keys.get(250)) && writer.moveToFirstChild());
        final long untouched = writer.getNodeKey();
        final int slot = StorageEngineReader.recordPageOffset(untouched);
        final PageContainer container = documentPage((NodeStorageEngineWriter) writer.getStorageEngineWriter(), 0L);
        final KeyValueLeafPage complete = (KeyValueLeafPage) container.getComplete();
        final KeyValueLeafPage modifying = (KeyValueLeafPage) container.getModified();
        assertNotEquals(KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID, complete.getFsstSymbolTableId(),
            "the rebuilt page keeps its table");
        assertEquals(complete.getFsstSymbolTableId(), modifying.getFsstSymbolTableId());
        assertTrue(complete.isFusedObjectNamedStringValueCompressed(slot),
            "an untouched encoded string stays encoded on the rebuilt page");
        assertNull(complete.getPageReference(untouched), "no older fragment was decompressed into a carrier");
        writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
      }
    }
  }

  /**
   * A leaf bound to its table only by id gets its symbols before an epoch materializes it; otherwise
   * its pending strings could only be judged raw.
   */
  @Test
  void materializationResolvesATableBoundOnlyById() throws IOException {
    final long encodedBefore = BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization();
    final Path path = directory.resolve("id-only");
    createResource(path, HashType.NONE, StorageType.FILE_CHANNEL);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER);
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        insertRecords(writer, SMALL_RECORDS, new ArrayList<>(SMALL_RECORDS),
            StorageEpochFsstInlineDecisionTest::oversizedCompressible);
        writer.commit(null, Instant.parse("2019-01-01T00:00:00Z"));
      }
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        final NodeStorageEngineWriter engine = (NodeStorageEngineWriter) writer.getStorageEngineWriter();
        final long tableId =
            engine.getNamePage(engine.getActualRevisionRootPage()).getLatestFsstSymbolTableId(DatabaseType.JSON);
        final ResourceConfiguration config = session.getResourceConfig();
        final long pageKey = 4000L;
        final KeyValueLeafPage page =
            new KeyValueLeafPage(pageKey, IndexType.DOCUMENT, config, writer.getRevisionNumber(), null, null, false);
        try {
          page.setFsstSymbolTableId(tableId);
          final long nodeKey = (pageKey << Constants.NDP_NODE_COUNT_EXPONENT) + 5;
          page.setRecord(new StringNode(nodeKey, 0L, 0, 0, 0L, 0L, 0L,
              oversizedCompressible(5, 1).getBytes(StandardCharsets.UTF_8), config.nodeHashFunction, (byte[]) null,
              false, null));
          engine.stageOverflowCarriersOfLiveLeaf(page);
          assertNotNull(page.getFsstSymbolTable(), "the table bound by id must have been resolved");
          assertTrue(page.getReferencesMap().isEmpty(), "the encoded value fits inline");
        } finally {
          page.close();
        }
      }
    }
    assertTrue(BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization() > encodedBefore);
  }

  /**
   * Regression for the epoch materialization that judged raw sizes. The async-flush import mode
   * materializes a live leaf's pending records before it stages their carriers into the data file;
   * without the encode, a value that fits once FSST-encoded became a raw carrier with a durable key,
   * which the commit never revisits on a page already bound to its table.
   */
  @ParameterizedTest
  @EnumSource(value = HashType.class, names = {"NONE", "ROLLING"})
  void anAsyncFlushEncodesPendingStringsBeforeJudgingTheirSize(final HashType hash) throws IOException {
    final long encodedBefore = BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization();
    final Path path = directory.resolve("async");
    createResource(path, hash, StorageType.FILE_CHANNEL);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER);
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      final List<Long> keys = new ArrayList<>(SMALL_RECORDS);
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        insertRecords(writer, SMALL_RECORDS, keys, StorageEpochFsstInlineDecisionTest::oversizedCompressible);
        writer.commit(null, Instant.parse("2019-01-01T00:00:00Z"));
      }
      try (final JsonNodeTrx writer = session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        for (int i = 600; i < 640; i += 7) {
          assertTrue(writer.moveTo(keys.get(i)) && writer.moveToFirstChild());
          writer.setStringValue(oversizedCompressible(i, 1));
        }
        writer.getStorageEngineWriter().asyncFlush();
        writer.getStorageEngineWriter().awaitPendingAsyncFlush();
        writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
      }
    }
    final Decisions decisions = decisions(path);
    assertEquals(new TreeSet<Long>(), decisions.carriers(), "values that fit once encoded stay inline");
    for (int i = 600; i < 640; i += 7) {
      assertEquals(oversizedCompressible(i, 1), decisions.values().get(i));
    }
    // Asserted last so that, against an engine without the fix, the failure names the decision.
    assertTrue(BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization() > encodedBefore,
        "the flush must have materialized pending string records");
  }

  static Stream<Arguments> trials() {
    final List<Arguments> trials = new ArrayList<>();
    for (final HashType hash : new HashType[] {HashType.NONE, HashType.ROLLING}) {
      for (final Rotation rotation : Rotation.values()) {
        // The import mode freezes incremental hashing at its first epoch by design, so stored hashes
        // (and with them record sizes) legitimately differ from a plain transaction under any kind
        // but NONE.
        if (rotation == Rotation.ASYNC_FLUSH_MODE && hash != HashType.NONE) {
          continue;
        }
        for (int s = 0; s < SEEDS.length; s++) {
          // The import mode samples its first table from different pages than a plain commit (its
          // flushes take pages out of the commit's sample), so a shared first transaction would hand the
          // arms different, equally valid tables. Its arms share a plain first transaction instead.
          final boolean sameTransaction = rotation != Rotation.ASYNC_FLUSH_MODE && (s & 1) == 0;
          trials.add(Arguments.of(hash, rotation, SEEDS[s], sameTransaction));
        }
      }
    }
    return trials.stream();
  }

  /**
   * Differential property: random edits whose encoded sizes straddle the inline ceiling, against a
   * reused symbol table, applied by a plain transaction and by a rotating one. Fixed seeds keep the
   * run deterministic (and mutation-testable); {@code -Dsirix.test.inlineDecision.seed=<n>} adds one
   * exploratory seed.
   */
  @ParameterizedTest(name = "{0} {1} seed={2} sameTransaction={3}")
  @MethodSource("trials")
  void plainAndRotatingTransactionsMakeIdenticalInlineDecisions(final HashType hash, final Rotation rotation,
      final long seed, final boolean sameTransaction) throws IOException {
    runTrial(directory.resolve("fixed"), hash, rotation, seed, sameTransaction);
    final String exploratory = System.getProperty("sirix.test.inlineDecision.seed");
    if (exploratory != null && seed == SEEDS[0]) {
      runTrial(directory.resolve("exploratory"), hash, rotation, Long.parseLong(exploratory), sameTransaction);
    }
  }

  private void runTrial(final Path base, final HashType hash, final Rotation rotation, final long seed,
      final boolean sameTransaction) throws IOException {
    final Trial plain = runArm(base.resolve("plain"), hash, rotation, false, seed, sameTransaction);
    final Trial rotated = runArm(base.resolve("rotated"), hash, rotation, true, seed, sameTransaction);
    final String context = hash + " " + rotation + " seed=" + seed + " sameTransaction=" + sameTransaction;
    assertTrue(rotated.rotations() >= 1, "the rotating arm must rotate at least once: " + context);
    assertEquals(0, plain.rotations(), context);
    assertArrayEquals(plain.table(), rotated.table(), "both arms must edit against the same symbol table: " + context);
    assertTrue(plain.encodedOnlyFits() > 0,
        "the edits must include values that fit inline only once encoded: " + context);
    assertTrue(plain.neverFits() > 0, "the edits must include values that need a carrier: " + context);
    assertEquals(plain.decisions().values(), rotated.decisions().values(), "committed values: " + context);
    assertEquals(plain.decisions().carriers(), rotated.decisions().carriers(),
        "inline-or-carrier decisions: " + context);
    assertEquals(plain.decisions().tableIds(), rotated.decisions().tableIds(), "page table bindings: " + context);
    if (rotation != Rotation.ASYNC_FLUSH_MODE) {
      // Spilling epochs are byte-invisible: the data file holds the plain twin's frames.
      assertEquals(plain.frames().size(), rotated.frames().size(), "page frames: " + context);
      for (int i = 0; i < plain.frames().size(); i++) {
        assertArrayEquals(plain.frames().get(i), rotated.frames().get(i), "page frame " + i + ": " + context);
      }
    }
  }

  /** How the rotating arm rotates. */
  enum Rotation {
    /** Plain transaction, bounded intent log only: epochs rotate when the log fills. */
    BOUNDED_LOG,
    /** Bounded log plus explicit spilling epochs at random edits. */
    BOUNDED_LOG_AND_ASYNC_FLUSH,
    /** The async-flush import mode, flushing (without spilling) at random edits. */
    ASYNC_FLUSH_MODE
  }

  private record Trial(byte[] table, int rotations, int encodedOnlyFits, int neverFits, Decisions decisions,
      List<byte[]> frames) {
  }

  private Trial runArm(final Path path, final HashType hash, final Rotation rotation, final boolean rotating,
      final long seed, final boolean sameTransaction) throws IOException {
    createResource(path, hash, StorageType.FILE_CHANNEL);
    final boolean bounded = rotating && rotation != Rotation.ASYNC_FLUSH_MODE;
    final boolean importMode = rotating && rotation == Rotation.ASYNC_FLUSH_MODE;
    final boolean explicitFlush = rotating && rotation != Rotation.BOUNDED_LOG;
    StorageEpochTestSupport.setPlainTransactionBound(bounded
        ? StorageEpochTestSupport.IMPORT_BOUND
        : Integer.MAX_VALUE);
    final Random random = new Random(seed);
    // The flush schedule draws from its own stream: only the rotating arm consumes it, and the values
    // both arms write must stay identical.
    final Random schedule = new Random(~seed);
    byte[] table;
    int rotations;
    int encodedOnlyFits = 0;
    int neverFits = 0;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER);
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      final List<Long> keys = new ArrayList<>(PROPERTY_RECORDS);
      JsonNodeTrx writer = beginWriter(session, sameTransaction && importMode);
      try {
        insertRecords(writer, PROPERTY_RECORDS, keys, StorageEpochFsstInlineDecisionTest::phrased);
        writer.commit(null, Instant.parse("2019-01-01T00:00:00Z"));
        if (!sameTransaction) {
          writer.close();
          writer = beginWriter(session, importMode);
        }
        table = latestTable(session);
        final byte[][] symbols = FSSTCompressor.parsedFor(table);
        final int[] edited = new int[48];
        for (int i = 0; i < edited.length; i++) {
          edited[i] = random.nextInt(PROPERTY_RECORDS);
        }
        int untilFlush = 3 + schedule.nextInt(10);
        for (int pass = 0; pass < 3; pass++) {
          for (final int record : edited) {
            final byte[] value = boundaryValue(random, symbols);
            final int encoded = encodedLength(value, symbols);
            if (value.length > PageConstants.MAX_RECORD_SIZE && encoded < PageConstants.MAX_RECORD_SIZE - 96) {
              encodedOnlyFits++;
            } else if (encoded > PageConstants.MAX_RECORD_SIZE) {
              neverFits++;
            }
            assertTrue(writer.moveTo(keys.get(record)) && writer.moveToFirstChild());
            writer.setStringValue(new String(value, StandardCharsets.US_ASCII));
            if (explicitFlush && --untilFlush == 0) {
              untilFlush = 3 + schedule.nextInt(10);
              if (importMode) {
                writer.getStorageEngineWriter().asyncFlush();
              } else {
                writer.getStorageEngineWriter().asyncFlushStorageEpoch();
              }
              writer.getStorageEngineWriter().awaitPendingAsyncFlush();
            }
          }
        }
        rotations = StorageEpochTestSupport.rotatedEpochs(writer);
        writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
      } finally {
        writer.close();
      }
    }
    Databases.clearGlobalCaches();
    final Decisions decisions = decisions(path);
    return new Trial(table, rotations, encodedOnlyFits, neverFits, decisions, frames(path));
  }

  private static JsonNodeTrx beginWriter(final JsonResourceSession session, final boolean importMode) {
    return importMode
        ? session.beginNodeTrx(64, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)
        : session.beginNodeTrx();
  }

  private Decisions editFullPage(final Path path, final HashType hash, final boolean rotate) throws IOException {
    createResource(path, hash, StorageType.FILE_CHANNEL);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER);
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      final List<Long> keys = new ArrayList<>(SMALL_RECORDS);
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        insertRecords(writer, SMALL_RECORDS, keys, StorageEpochFsstInlineDecisionTest::oversizedCompressible);
        writer.commit(null, Instant.parse("2019-01-01T00:00:00Z"));
      }
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        // Records 0..510 live on page 0, which revision 1 wrote with every slot populated.
        for (int pass = 1; pass <= 3; pass++) {
          for (int i = pass; i < 500; i += 97) {
            assertTrue(writer.moveTo(keys.get(i)) && writer.moveToFirstChild());
            writer.setStringValue(oversizedCompressible(i, pass));
          }
          if (rotate) {
            writer.getStorageEngineWriter().asyncFlushStorageEpoch();
            writer.getStorageEngineWriter().awaitPendingAsyncFlush();
          }
        }
        writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
      }
    }
    Databases.clearGlobalCaches();
    return decisions(path);
  }

  private Decisions extendAndEdit(final Path path, final HashType hash, final boolean rotate) throws IOException {
    createResource(path, hash, StorageType.FILE_CHANNEL);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER);
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      final List<Long> keys = new ArrayList<>(2 * SMALL_RECORDS);
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        insertRecords(writer, SMALL_RECORDS, keys, StorageEpochFsstInlineDecisionTest::oversizedCompressible);
        writer.commit(null, Instant.parse("2019-01-01T00:00:00Z"));
      }
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        // Appended records land on pages this transaction creates; their values are encoded at insert.
        assertTrue(writer.moveToDocumentRoot() && writer.moveToFirstChild());
        final long array = writer.getNodeKey();
        for (int i = SMALL_RECORDS; i < 2 * SMALL_RECORDS; i++) {
          assertTrue(writer.moveTo(array));
          writer.insertSubtreeAsLastChild(
              JsonShredder.createStringReader("{\"v\":\"" + oversizedCompressible(i, 0) + "\"}"),
              JsonNodeTrx.Commit.NO);
          assertTrue(writer.moveTo(array) && writer.moveToLastChild());
          keys.add(writer.getNodeKey());
        }
        if (rotate) {
          writer.getStorageEngineWriter().asyncFlushStorageEpoch();
          writer.getStorageEngineWriter().awaitPendingAsyncFlush();
        }
        boolean inspected = false;
        for (int i = SMALL_RECORDS + 600; i < 2 * SMALL_RECORDS; i += 61) {
          assertTrue(writer.moveTo(keys.get(i)) && writer.moveToFirstChild());
          writer.setStringValue(oversizedCompressible(i, 1));
          if (rotate && !inspected) {
            // The edit re-read a page this transaction created and flushed: both rebuilt pages hold the
            // owned fragment's encoded strings, so both must name its table.
            inspected = true;
            final PageContainer container = documentPage((NodeStorageEngineWriter) writer.getStorageEngineWriter(),
                writer.getNodeKey() >> Constants.NDP_NODE_COUNT_EXPONENT);
            final KeyValueLeafPage complete = (KeyValueLeafPage) container.getComplete();
            final KeyValueLeafPage modifying = (KeyValueLeafPage) container.getModified();
            assertNotEquals(KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID, complete.getFsstSymbolTableId(),
                "the rebuilt complete page adopts the owned binding");
            assertNotNull(complete.getFsstSymbolTable());
            assertEquals(complete.getFsstSymbolTableId(), modifying.getFsstSymbolTableId());
            assertNotNull(modifying.getFsstSymbolTable());
          }
        }
        writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
      }
    }
    Databases.clearGlobalCaches();
    return decisions(path);
  }

  private static PageContainer documentPage(final NodeStorageEngineWriter engine, final long pageKey) {
    for (final PageContainer container : engine.getLog().getList()) {
      if (container.getComplete() instanceof final KeyValueLeafPage page && page.getIndexType() == IndexType.DOCUMENT
          && page.getPageKey() == pageKey) {
        return container;
      }
    }
    throw new AssertionError("document page " + pageKey + " is not in the live intent log");
  }

  private static void createResource(final Path path, final HashType hash, final StorageType storage) {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(storage)
                                                              .stringCompressionType(StringCompressionType.FSST)
                                                              .hashKind(hash)
                                                              .byteHandlerPipeline(new ByteHandlerPipeline())
                                                              .customCommitTimestamps(true)
                                                              .build()));
    }
    Databases.clearGlobalCaches();
  }

  /** Value generator for one record: {@code value(record, pass)}. */
  @FunctionalInterface
  private interface Values {
    String value(int record, int pass);
  }

  private static void insertRecords(final JsonNodeTrx writer, final int records, final List<Long> keys,
      final Values values) {
    writer.insertArrayAsFirstChild();
    final long array = writer.getNodeKey();
    for (int i = 0; i < records; i++) {
      assertTrue(writer.moveTo(array));
      writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"v\":\"" + values.value(i, 0) + "\"}"),
          JsonNodeTrx.Commit.NO);
      assertTrue(writer.moveTo(array) && writer.moveToLastChild());
      keys.add(writer.getNodeKey());
    }
  }

  /** Over the inline ceiling raw, well under it once encoded against a table trained on it. */
  private static String oversizedCompressible(final int record, final int pass) {
    return "https://sirix.io/records/common-prefix/".repeat(40) + record + "/publication/" + pass;
  }

  /** Short, vocabulary-heavy values that train a table the edits can reuse. */
  private static String phrased(final int record, final int pass) {
    final StringBuilder builder = new StringBuilder(256);
    for (int i = 0; i < 6 + (record % 5); i++) {
      builder.append(VOCABULARY[(record * 7 + i * 3) % VOCABULARY.length]);
    }
    return builder.append(record).append('/').append(pass).toString();
  }

  /**
   * A value whose encoded length lands near the inline ceiling. The vocabulary share decides how far
   * apart the raw and encoded sizes are, so the same target yields values that fit only once encoded,
   * values that never fit, and values that fit raw.
   */
  private static byte[] boundaryValue(final Random random, final byte[][] symbols) {
    final int target = PageConstants.MAX_RECORD_SIZE - 192 + random.nextInt(288);
    final double vocabularyShare = new double[] {0.0, 0.4, 0.8, 0.95, 1.0}[random.nextInt(5)];
    final StringBuilder builder = new StringBuilder(4096);
    while (true) {
      for (int i = 0; i < 8; i++) {
        if (random.nextDouble() < vocabularyShare) {
          builder.append(VOCABULARY[random.nextInt(VOCABULARY.length)]);
        } else {
          builder.append(ALPHANUMERIC.charAt(random.nextInt(ALPHANUMERIC.length())));
        }
      }
      final byte[] value = builder.toString().getBytes(StandardCharsets.US_ASCII);
      if (encodedLength(value, symbols) >= target) {
        return value;
      }
    }
  }

  private static int encodedLength(final byte[] value, final byte[][] symbols) {
    final byte[] encoded = FSSTCompressor.encodeOrNull(value, 0, value.length, symbols);
    return encoded == null
        ? value.length
        : encoded.length;
  }

  private static byte[] latestTable(final JsonResourceSession session) {
    try (final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader engine = reader.getStorageEngineReader();
      final NamePage names = engine.getNamePage(engine.getActualRevisionRootPage());
      final long id = names.getLatestFsstSymbolTableId(DatabaseType.JSON);
      assertTrue(id > 0, "the first commit must store a symbol table");
      return names.getFsstSymbolTable(id, DatabaseType.JSON, engine);
    }
  }

  /**
   * The committed revision's state: every record's value, and for each document page's newest
   * persisted fragment the node keys it holds in overflow carriers and its table binding. The newest
   * fragment is where this revision's decisions were written; a combined read view is no oracle,
   * since reconstructing a page decompresses older fragments' strings and may give them read-side
   * carriers.
   */
  private record Decisions(List<String> values, TreeSet<Long> carriers, List<Long> tableIds) {
  }

  private static Decisions decisions(final Path path) throws IOException {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER);
        final JsonResourceSession session = database.beginResourceSession("resource");
        final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      final List<String> values = new ArrayList<>();
      assertTrue(reader.moveToDocumentRoot() && reader.moveToFirstChild() && reader.moveToFirstChild());
      do {
        final long key = reader.getNodeKey();
        assertTrue(reader.moveToFirstChild());
        values.add(reader.getValue());
        assertTrue(reader.moveTo(key));
      } while (reader.moveToRightSibling());
      final StorageEngineReader engine = reader.getStorageEngineReader();
      final TreeSet<Long> carriers = new TreeSet<>();
      final List<Long> tableIds = new ArrayList<>();
      final long lastPage = engine.pageKey(reader.getMaxNodeKey(), IndexType.DOCUMENT);
      for (long pageKey = 0; pageKey <= lastPage; pageKey++) {
        final long newestFragment =
            engine.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pageKey, 0, reader.getRevisionNumber()))
                  .reference()
                  .getKey();
        final Page fragment = ((NodeStorageEngineReader) engine).getReader()
                                                                .read(new PageReference().setKey(newestFragment),
                                                                    session.getResourceConfig());
        try {
          final KeyValueLeafPage leaf = (KeyValueLeafPage) fragment;
          carriers.addAll(leaf.getReferencesMap().keySet());
          tableIds.add(leaf.getFsstSymbolTableId());
        } finally {
          fragment.close();
        }
      }
      return new Decisions(values, carriers, tableIds);
    }
  }

  /** Every page frame of the data file up to the committed frontier, in file order. */
  private static List<byte[]> frames(final Path path) throws IOException {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER);
        final JsonResourceSession session = database.beginResourceSession("resource");
        final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      final Path dataPath =
          session.getResourceConfig().resourcePath.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                                  .resolve("sirix.data");
      final long rootOffset =
          ((NodeStorageEngineReader) reader.getStorageEngineReader()).getReader()
                                                                     .getRevisionFileData(reader.getRevisionNumber())
                                                                     .offset();
      final List<byte[]> frames = new ArrayList<>();
      try (final FileChannel file = FileChannel.open(dataPath)) {
        final long frontier = rootOffset + Integer.BYTES + readInt(file, rootOffset);
        long offset = IOStorage.DATA_REGION_START;
        while (offset < frontier) {
          final int size = readInt(file, offset);
          final ByteBuffer payload = ByteBuffer.allocate(size);
          while (payload.hasRemaining()) {
            assertTrue(file.read(payload, offset + Integer.BYTES + payload.position()) > 0);
          }
          frames.add(payload.array());
          offset = (offset + Integer.BYTES + size + 7) & ~7L;
        }
      }
      return frames;
    }
  }

  private static int readInt(final FileChannel file, final long offset) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    while (buffer.hasRemaining()) {
      assertTrue(file.read(buffer, offset + buffer.position()) > 0);
    }
    return buffer.flip().getInt();
  }
}
