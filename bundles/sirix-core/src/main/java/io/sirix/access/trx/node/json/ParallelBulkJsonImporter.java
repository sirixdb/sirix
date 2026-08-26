/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.path.summary.PathSummaryWriter;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.settings.Fixed;
import io.brackit.query.atomic.QNm;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/**
 * GENERAL parallel bulk import of a JSON document whose top level is a large array — the shape
 * every bulk corpus has (NDJSON rides a trivial adapter) and the only shape that parallelizes:
 * members are independent subtrees under one array, so chunks of members can be BUILT off-thread
 * while a single coordinator owns everything shared.
 *
 * <p>
 * Pipeline (see the campaign plan for the full invariant audit):
 * <ol>
 * <li><b>Slice</b>: a lightweight structural scanner cuts the stream into member-aligned char
 * chunks (strings/escapes tracked; no tokenization).</li>
 * <li><b>Count + resolve (coordinator)</b>: the fused pass produces exact node counts, member
 * boundaries, names (the only decoded text) and path resolution in the SAME byte sweep that
 * slices — reference deltas flush per chunk, before any rotation can flush summary pages
 * cold.</li>
 * <li><b>Reserve</b>: the chunk's exact contiguous key range via
 * {@link RevisionRootPage#reserveKeyRangeInDocumentIndex(long)} — dense keys, so the result is
 * record-identical to a sequential load.</li>
 * <li><b>Build (worker)</b>: the same assembler core with a {@link WorkerPageBuilder} emits
 * FINAL record bytes into standalone pages; pre-resolved names/PCRs; range/slot verification
 * always on.</li>
 * <li><b>Stitch + adopt (coordinator)</b>: pages sharing a page key with already-live territory
 * (page 0's prologue records, the previous chunk's held tail) are merged record-by-record through
 * the CoW-checked blit seam; whole pages are adopted via
 * {@link StorageEngineWriter#adoptDocumentLeafPage}; the chunk's TAIL partial page is HELD out of
 * the intent log until its successor's head is merged into it (the flusher-race amendment).</li>
 * </ol>
 *
 * <p>
 * v1 scope: fresh resource, same refusals as {@link BulkJsonTreeAssembler}; index notifications
 * refused (no primitive indexes during import); the caller commits. This entry point is
 * single-builder (the M2 pipeline); the M3 executor fan-out layers on top of the same chunk
 * protocol.
 */
public final class ParallelBulkJsonImporter {

  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();
  private static final QNm ARRAY_PATH_QNM = new QNm("__array__");
  private static final int DEFAULT_CHUNK_CHAR_BUDGET =
      Integer.getInteger("sirix.parallelImport.chunkBytes", 4 << 20);
  private static final int ADOPT_BURST_PAGES = 16;

  private final JsonNodeTrxImpl wtx;
  private final StorageEngineWriter storageEngineWriter;
  private final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter;
  private final boolean buildPathSummary;
  private final WtxBulkRecordSink wtxSink;
  private final RevisionRootPage revisionRootPage;
  private final ResourceConfiguration resourceConfig;
  private final int chunkCharBudget;

  /** Interned dictionary keys by the feeder's STABLE dense name id, in first-occurrence order. */
  private final ArrayList<String> nameById = new ArrayList<>(64);
  private final it.unimi.dsi.fastutil.ints.IntArrayList nameKeyById = new it.unimi.dsi.fastutil.ints.IntArrayList(64);

  /** The coordinator's growing (parentPCR, name) → PCR view, snapshot per chunk for the builder. */
  private Long2ObjectOpenHashMap<Object2LongOpenHashMap<String>> masterPcrMemo = new Long2ObjectOpenHashMap<>();

  private KeyValueLeafPage heldTailPage;
  private long heldTailPageKey = -1;

  /** Recycled decode scratch: chars ≤ bytes for UTF-8, so one budget-sized array fits a chunk. */
  private final ArrayBlockingQueue<char[]> charScratchPool = new ArrayBlockingQueue<>(16);

  private char[] acquireCharScratch(final int neededChars) {
    final char[] pooled = charScratchPool.poll();
    if (pooled != null && pooled.length >= neededChars) {
      return pooled;
    }
    return new char[Math.max(neededChars, chunkCharBudget + (chunkCharBudget >> 2))];
  }

  private ParallelBulkJsonImporter(final JsonNodeTrxImpl wtx, final int chunkCharBudget) {
    this.wtx = wtx;
    this.storageEngineWriter = wtx.getStorageEngineWriter();
    this.pathSummaryWriter = wtx.bulkPathSummaryWriter();
    this.buildPathSummary = wtx.bulkBuildPathSummary();
    this.wtxSink = new WtxBulkRecordSink(wtx);
    this.revisionRootPage = storageEngineWriter.getActualRevisionRootPage();
    this.resourceConfig = wtx.getResourceSession().getResourceConfig();
    this.chunkCharBudget = chunkCharBudget;
  }

  /** As {@link #assemble(JsonNodeTrx, Reader, int)} with the default chunk budget. */
  public static void assemble(final JsonNodeTrx wtx, final Reader input) {
    assemble(wtx, input, DEFAULT_CHUNK_CHAR_BUDGET, defaultParallelism());
  }

  /** Byte-stream entry — the primary path: the coordinator never decodes value bytes. */
  public static void assemble(final JsonNodeTrx wtx, final InputStream input) {
    assembleBytes(wtx, input, DEFAULT_CHUNK_CHAR_BUDGET, defaultParallelism());
  }

  /** Builder threads: builds are cheap relative to the flush pipeline's parallel serialization,
   * so the default leaves MOST cores to the snapshot flush pool — measured: an oversized build
   * pool starves serialization (serializeJoinWait dominated the flush worker at builders=16). */
  private static int defaultParallelism() {
    final int configured = Integer.getInteger("sirix.parallelImport.builders", -1);
    if (configured > 0) {
      return configured;
    }
    return Math.max(2, Runtime.getRuntime().availableProcessors() / 4);
  }

  /**
   * Imports ONE top-level JSON value from {@code input} into the fresh resource behind
   * {@code wtx}, building member chunks through the parallel page pipeline when the top level is
   * an array and falling back to the sequential assembler otherwise. The caller commits.
   *
   * @param wtx a write transaction on a FRESH resource (standard implementation)
   * @param input the character source
   * @param chunkCharBudget approximate chars per build chunk (test seam; small values force many
   *        chunks and page-sharing boundaries)
   */
  public static void assemble(final JsonNodeTrx wtx, final Reader input, final int chunkCharBudget) {
    assemble(wtx, input, chunkCharBudget, defaultParallelism());
  }

  /**
   * As {@link #assemble(JsonNodeTrx, Reader, int)} with explicit builder parallelism; {@code 1}
   * builds inline on the coordinator.
   */
  public static void assemble(final JsonNodeTrx wtx, final Reader input, final int chunkCharBudget,
      final int parallelism) {
    assembleBytes(wtx, new ReaderUtf8InputStream(input), chunkCharBudget, parallelism);
  }

  /**
   * The pipeline proper, over raw UTF-8 bytes: the coordinator slices and scans BYTES (structural
   * JSON is ASCII; multi-byte sequences never contain ASCII bytes), and each build worker decodes
   * only its own chunk — the corpus-wide decode runs in parallel instead of on the spine.
   */
  public static void assembleBytes(final JsonNodeTrx wtx, final InputStream input, final int chunkCharBudget,
      final int parallelism) {
    if (!(wtx instanceof final JsonNodeTrxImpl impl)) {
      throw new IllegalArgumentException(
          "parallel bulk import requires the standard JsonNodeTrx implementation, got " + wtx.getClass().getName());
    }
    refuseUnsupportedShape(impl);
    try {
      final PushbackInputStream stream = new PushbackInputStream(input, 1);
      if (!nextIsArray(stream)) {
        // Not a top-level array: nothing to parallelize — the sequential assembler is the
        // general path for every other shape.
        BulkJsonTreeAssembler.assemble(wtx, new InputStreamReader(stream, StandardCharsets.UTF_8));
        return;
      }
      new ParallelBulkJsonImporter(impl, chunkCharBudget).run(stream, Math.max(1, parallelism));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The assembler's refusals plus the importer's own: no primitive indexes during import. */
  private static void refuseUnsupportedShape(final JsonNodeTrxImpl wtx) {
    final ResourceConfiguration config = wtx.getResourceSession().getResourceConfig();
    if (config.hashType != HashType.NONE) {
      throw new IllegalStateException("parallel bulk import supports hashType=NONE only, got " + config.hashType);
    }
    if (config.areDeweyIDsStored) {
      throw new IllegalStateException("parallel bulk import does not support DeweyIDs");
    }
    if (config.storeNodeHistory()) {
      throw new IllegalStateException("parallel bulk import does not support node history");
    }
    final PathSummaryWriter<JsonNodeReadOnlyTrx> summary = wtx.bulkPathSummaryWriter();
    if (summary != null && summary.isPathStatisticsEnabled()) {
      throw new IllegalStateException("parallel bulk import does not support path statistics");
    }
    if (wtx.bulkHasPrimitiveIndexes()) {
      throw new IllegalStateException(
          "parallel bulk import does not support primitive-index maintenance during the load");
    }
    if (!wtx.moveToDocumentRoot() || wtx.hasFirstChild()) {
      throw new IllegalStateException("parallel bulk import requires a FRESH resource (empty document root)");
    }
  }

  /** Peeks past leading whitespace; pushes the decisive byte back either way. */
  private static boolean nextIsArray(final PushbackInputStream stream) throws IOException {
    while (true) {
      final int c = stream.read();
      if (c < 0) {
        return false;
      }
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
        continue;
      }
      stream.unread(c);
      return c == '[';
    }
  }

  // ==== the pipeline ===========================================================================

  /** One dispatched chunk build in flight; adoption strictly in chunk order (tail-hold chain). */
  private record PendingBuild(Future<List<KeyValueLeafPage>> pages, WorkerPageBuilder builder, long lastKey,
      long chunkLastMemberKey, long chunkMembers) {
  }

  private void run(final InputStream input, final int parallelism) throws IOException {

    // Prologue: the root array through the ordinary transaction path — page 0 enters the intent
    // log here and is stitched via the CoW-checked blit seam, never held.
    final long rootArrayPcr = buildPathSummary
        ? pathSummaryWriter.getPathNodeKey(0, ARRAY_PATH_QNM, NodeKind.ARRAY)
        : 0;
    final long rootArrayKey =
        wtxSink.createArrayNode(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), NULL_KEY, rootArrayPcr);
    rememberRootArrayPcr(rootArrayPcr);

    final FusedSliceAndScan fused = new FusedSliceAndScan(input, chunkCharBudget);
    fused.consumeArrayOpen();
    fused.rootStep().pcr = rootArrayPcr;

    // The FEEDER: scans + slices ahead on its own thread, fully writer-free — all shared-state
    // resolution (paths, names, reservation) happens HERE on the coordinator at chunk handoff.
    final BlockingQueue<Object> handoff = new ArrayBlockingQueue<>(4);
    final Thread feeder = new Thread(() -> {
      try {
        FusedSliceAndScan.Chunk next;
        while ((next = fused.nextChunk()) != null) {
          handoff.put(next);
        }
        handoff.put(FEEDER_DONE);
      } catch (final Throwable t) {
        try {
          handoff.put(t);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }, "sirix-bulk-import-feeder");
    feeder.setDaemon(true);
    feeder.start();

    final ExecutorService buildPool = parallelism > 1
        ? Executors.newFixedThreadPool(parallelism)
        : null;
    final ArrayDeque<PendingBuild> inFlight = new ArrayDeque<>(parallelism + 2);
    final int maxInFlight = parallelism + 2;

    long totalMembers = 0;
    long lastMemberKey = NULL_KEY;
    long expectedNextReservation = rootArrayKey + 1;

    try {
    while (true) {
      final Object taken;
      try {
        taken = handoff.take();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted awaiting the feeder", e);
      }
      if (taken == FEEDER_DONE) {
        break;
      }
      if (taken instanceof Throwable failure) {
        if (failure instanceof RuntimeException runtime) {
          throw runtime;
        }
        if (failure instanceof IOException io) {
          throw io;
        }
        throw new IllegalStateException("feeder failed", failure);
      }
      final FusedSliceAndScan.Chunk chunk = (FusedSliceAndScan.Chunk) taken;

      final long chunkNodes = chunk.nodes();
      final long chunkMembers = chunk.members();
      if (chunkNodes == 0) {
        continue;
      }
      // --- Resolve this chunk's metadata on the coordinator, in document order ------------------
      final Object2IntOpenHashMap<String> chunkNames = resolveChunkMetadata(chunk);

      // --- Reserve the exact dense range ---------------------------------------------------
      final long firstKey = revisionRootPage.reserveKeyRangeInDocumentIndex(chunkNodes);
      if (firstKey != expectedNextReservation) {
        throw new IllegalStateException(
            "reserved range starts at " + firstKey + " but the pipeline expected " + expectedNextReservation);
      }
      final long lastKey = firstKey + chunkNodes - 1;
      final long chunkLastMemberKey = firstKey + chunkNodes - chunk.lastMemberNodes();
      final long trailingSiblingKey = chunk.isFinal()
          ? NULL_KEY
          : lastKey + 1;

      // --- Stage B: build — off-thread when a pool exists, inline otherwise ---------------------
      final WorkerPageBuilder builder =
          new WorkerPageBuilder(resourceConfig, storageEngineWriter.getRevisionNumber(),
              resourceConfig.nodeHashFunction, wtx.bulkStoreChildCount(), chunkNames, firstKey, lastKey);
      final byte[] chunkBytes = chunk.bytes();
      final int chunkByteLength = chunk.length();
      final long buildFirstKey = firstKey;
      final long buildLastMemberBoundary = lastMemberKey;
      final long buildTrailing = trailingSiblingKey;
      final Long2ObjectOpenHashMap<Object2LongOpenHashMap<String>> memoSnapshot = copyMemo(masterPcrMemo);

      if (buildPool == null) {
        stitchAndAdopt(buildChunk(fused, builder, chunkBytes, chunkByteLength, buildFirstKey, lastKey, rootArrayKey,
            rootArrayPcr, buildLastMemberBoundary, buildTrailing, memoSnapshot), lastKey);
      } else {
        final long submittedLastKey = lastKey;
        inFlight.addLast(new PendingBuild(buildPool.submit(() -> {
          // The chunk's own UTF-8 decode happens HERE, on the pool thread — the whole corpus
          // decode parallelizes across builders, into POOLED scratch (no per-chunk large arrays).
          return buildChunk(fused, builder, chunkBytes, chunkByteLength, buildFirstKey, submittedLastKey, rootArrayKey,
              rootArrayPcr, buildLastMemberBoundary, buildTrailing, memoSnapshot);
        }), builder, lastKey, chunkLastMemberKey, chunkMembers));
        // Admission control: never more than P+2 chunks of frames in flight; adoption is strictly
        // in chunk order, which the tail-hold stitch chain requires anyway.
        while (inFlight.size() >= maxInFlight) {
          adoptNext(inFlight);
        }
      }

      totalMembers += chunkMembers;
      lastMemberKey = chunkLastMemberKey;
      expectedNextReservation = lastKey + 1;
    }

    while (!inFlight.isEmpty()) {
      adoptNext(inFlight);
    }
    } finally {
      if (buildPool != null) {
        buildPool.shutdownNow();
      }
    }

    if (heldTailPage != null) {
      adoptBurst(new KeyValueLeafPage[] { heldTailPage }, 1);
      heldTailPage = null;
      heldTailPageKey = -1;
    }

    // Root array + document fixups, once, with GLOBAL counts.
    if (totalMembers > 0) {
      wtxSink.fixupContainer(rootArrayKey, rootArrayKey + 1, lastMemberKey, totalMembers, NULL_KEY);
    }
    wtxSink.fixupDocument(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), rootArrayKey, rootArrayKey, 1);

    if (revisionRootPage.getMaxNodeKeyInDocumentIndex() != expectedNextReservation - 1) {
      throw new IllegalStateException("document max node key " + revisionRootPage.getMaxNodeKeyInDocumentIndex()
          + " does not match the last reserved key " + (expectedNextReservation - 1));
    }
  }

  /** Runs one chunk build on the calling thread: pooled decode, run, verify, release buffers. */
  private List<KeyValueLeafPage> buildChunk(final FusedSliceAndScan fused, final WorkerPageBuilder builder,
      final byte[] chunkBytes, final int chunkByteLength, final long firstKey, final long lastKey,
      final long rootArrayKey, final long rootArrayPcr, final long leftBoundaryKey, final long trailingSiblingKey,
      final Long2ObjectOpenHashMap<Object2LongOpenHashMap<String>> memoSnapshot) throws IOException {
    final char[] scratch = acquireCharScratch(chunkByteLength);
    try {
      // Decode straight into the pooled scratch — no String detour, no second copy.
      final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                                                           .onMalformedInput(CodingErrorAction.REPLACE)
                                                           .onUnmappableCharacter(CodingErrorAction.REPLACE);
      final CharBuffer out = CharBuffer.wrap(scratch);
      decoder.decode(ByteBuffer.wrap(chunkBytes, 0, chunkByteLength), out, true);
      final int chars = out.position();
      fused.releaseChunkBuffer(chunkBytes);
      final BulkJsonTreeAssembler building = new BulkJsonTreeAssembler(builder, null, buildPathSummary,
          new BulkJsonScanner(new CharArrayReader(scratch, 0, chars)), firstKey, true, rootArrayKey, rootArrayPcr,
          leftBoundaryKey, trailingSiblingKey);
      building.prefillPcrMemo(memoSnapshot);
      building.run();
      return builder.finish(lastKey);
    } finally {
      charScratchPool.offer(scratch);
    }
  }

  private void adoptNext(final ArrayDeque<PendingBuild> inFlight) {
    final PendingBuild next = inFlight.pollFirst();
    try {
      stitchAndAdopt(next.pages().get(), next.lastKey());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while awaiting a chunk build", e);
    } catch (final java.util.concurrent.ExecutionException e) {
      throw new IllegalStateException("chunk build failed", e.getCause());
    }
  }

  // ==== name interning + counting ==============================================================

  /** Marker object closing the feeder handoff. */
  private static final Object FEEDER_DONE = new Object();

  /**
   * Resolves a chunk's deferred metadata IN DOCUMENT ORDER on the coordinator — the sequential
   * path's exact dictionary/summary call sequence, batched: new paths resolve through the real
   * summary writer (each miss counts its first occurrence), new names intern via createNameKey,
   * repeat occurrences land as reference/count deltas, and the master memo gains every new
   * (parentPCR, name) → PCR pair for the build worker's snapshot.
   *
   * @return the chunk's name → dictionary-key table for the build worker
   */
  private Object2IntOpenHashMap<String> resolveChunkMetadata(final FusedSliceAndScan.Chunk chunk) {
    // Names first-occurring in this chunk, in document order (interleaved order vs paths is
    // preserved by the sequential path only per-record; the dictionary probe sequences that
    // matter are name-vs-name and path-vs-path order, both exact here).
    for (final String name : chunk.newNames()) {
      nameById.add(name);
      nameKeyById.add(storageEngineWriter.createNameKey(name, NodeKind.OBJECT_NAMED_OBJECT));
    }
    // New path steps, in document order; resolution counts each step's first occurrence.
    for (final FusedSliceAndScan.PathStep step : chunk.newSteps()) {
      if (!buildPathSummary) {
        step.pcr = 0;
        continue;
      }
      final long parentPcr = step.parent.pcr;
      if (parentPcr < 0) {
        throw new IllegalStateException("path step resolved before its parent — feeder order broken");
      }
      step.pcr = step.name == null
          ? pathSummaryWriter.getPathNodeKey(parentPcr, ARRAY_PATH_QNM, NodeKind.ARRAY)
          : pathSummaryWriter.getPathNodeKey(parentPcr, new QNm(step.name), NodeKind.OBJECT_NAMED_OBJECT);
      recordInMasterMemo(parentPcr, step.name == null
          ? "__array__"
          : step.name, step.pcr);
    }
    // Reference-count deltas: a step created THIS chunk had its first occurrence counted by the
    // resolution above, so its delta is occurrences - 1.
    if (buildPathSummary) {
      final List<FusedSliceAndScan.PathStep> touched = chunk.touchedSteps();
      final int[] occurrences = chunk.stepOccurrences();
      for (int i = 0; i < touched.size(); i++) {
        final FusedSliceAndScan.PathStep step = touched.get(i);
        final int delta = chunk.newSteps().contains(step)
            ? occurrences[i] - 1
            : occurrences[i];
        if (delta > 0) {
          pathSummaryWriter.addReferences(step.pcr, delta);
        }
      }
    }
    // Name occurrence deltas + the worker's name table.
    final Object2IntOpenHashMap<String> table = new Object2IntOpenHashMap<>(chunk.nameTallies().size());
    table.defaultReturnValue(Integer.MIN_VALUE);
    final int newNamesStart = nameById.size() - chunk.newNames().size();
    for (final int[] tally : chunk.nameTallies()) {
      final int nameId = tally[0];
      final int count = tally[1];
      final int nameKey = nameKeyById.getInt(nameId);
      table.put(nameById.get(nameId), nameKey);
      final int delta = nameId >= newNamesStart
          ? count - 1
          : count;
      if (delta > 0) {
        storageEngineWriter.addNameCount(nameKey, delta, NodeKind.OBJECT_NAMED_OBJECT);
      }
    }
    return table;
  }

  private void recordInMasterMemo(final long parentPcr, final String name, final long pcr) {
    Object2LongOpenHashMap<String> byName = masterPcrMemo.get(parentPcr);
    if (byName == null) {
      byName = new Object2LongOpenHashMap<>();
      byName.defaultReturnValue(-1);
      masterPcrMemo.put(parentPcr, byName);
    }
    byName.put(name, pcr);
  }


  private void rememberRootArrayPcr(final long rootArrayPcr) {
    if (!buildPathSummary) {
      return;
    }
    final Object2LongOpenHashMap<String> underDocument = new Object2LongOpenHashMap<>();
    underDocument.defaultReturnValue(-1);
    underDocument.put("__array__", rootArrayPcr);
    masterPcrMemo.put(0L, underDocument);
  }

  private static Long2ObjectOpenHashMap<Object2LongOpenHashMap<String>> copyMemo(
      final Long2ObjectOpenHashMap<Object2LongOpenHashMap<String>> source) {
    final Long2ObjectOpenHashMap<Object2LongOpenHashMap<String>> copy = new Long2ObjectOpenHashMap<>(source.size());
    for (final var entry : source.long2ObjectEntrySet()) {
      final Object2LongOpenHashMap<String> byName = new Object2LongOpenHashMap<>(entry.getValue());
      byName.defaultReturnValue(-1);
      copy.put(entry.getLongKey(), byName);
    }
    return copy;
  }

  // ==== stitch + adopt =========================================================================

  private void stitchAndAdopt(final List<KeyValueLeafPage> pages, final long lastKey) {
    final KeyValueLeafPage[] burst = new KeyValueLeafPage[ADOPT_BURST_PAGES];
    int burstSize = 0;
    final boolean tailPartial = ((lastKey + 1) & 1023) != 0;

    for (int i = 0; i < pages.size(); i++) {
      final KeyValueLeafPage page = pages.get(i);
      final boolean isTail = i == pages.size() - 1;
      final long pageKey = page.getPageKey();

      if (pageKey == heldTailPageKey) {
        // Shares the previous chunk's held tail: merge into the coordinator-owned page object.
        mergeInto(heldTailPage, page);
        if (isTail && tailPartial) {
          // Whole chunk inside the held page; keep holding.
          continue;
        }
        // The held page is now complete territory-wise up to this chunk's coverage.
        if (!isTail || !tailPartial) {
          burstSize = enqueue(burst, burstSize, heldTailPage);
          heldTailPage = null;
          heldTailPageKey = -1;
        }
        continue;
      }

      if (pageKey == 0) {
        // Page 0 is TIL-live (document root + root array prologue): CoW-checked blit.
        mergeInto(storageEngineWriter.prepareDocumentLeafForBlit(0), page);
        page.retire();
        continue;
      }

      if (isTail && tailPartial) {
        heldTailPage = page;
        heldTailPageKey = pageKey;
        continue;
      }

      burstSize = enqueue(burst, burstSize, page);
    }
    flushBurst(burst, burstSize);
  }

  private int enqueue(final KeyValueLeafPage[] burst, final int burstSize, final KeyValueLeafPage page) {
    burst[burstSize] = page;
    if (burstSize + 1 == burst.length) {
      flushBurst(burst, burst.length);
      return 0;
    }
    return burstSize + 1;
  }

  private void flushBurst(final KeyValueLeafPage[] burst, final int burstSize) {
    adoptBurst(burst, burstSize);
  }

  private void adoptBurst(final KeyValueLeafPage[] burst, final int burstSize) {
    if (burstSize == 0) {
      return;
    }
    int records = 0;
    for (int i = 0; i < burstSize; i++) {
      storageEngineWriter.adoptDocumentLeafPage(burst[i]);
      records += burst[i].populatedSlots().length;
      burst[i] = null;
    }
    // Accounting after the burst lets the ordinary predicate rotate the flush epoch — the same
    // cadence machinery the sequential path drives per top-level record.
    wtx.bulkAccountRecord(records);
  }

  /** Replays every record of {@code source} into {@code target} (same page key, disjoint slots). */
  private static void mergeInto(final KeyValueLeafPage target, final KeyValueLeafPage source) {
    final long pageBase = source.getPageKey() << 10;
    for (final int slot : source.populatedSlots()) {
      final MemorySegment record = source.getSlot(slot);
      final int kindId = source.getSlotNodeKindId(slot);
      final long absOffset = target.prepareHeapForDirectWriteOrOverflow((int) record.byteSize(), 0);
      if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
        throw new IllegalStateException(
            "boundary merge overflowed page " + target.getPageKey() + " at slot " + slot);
      }
      MemorySegment.copy(record, 0, target.getSlottedPage(), absOffset, record.byteSize());
      target.completeDirectWrite((byte) kindId, pageBase | slot, slot, (int) record.byteSize(), null);
    }
    // Overflow-sized values live as heap records, not slots — carry them across as objects.
    // Direct-write records never enter records[] (write singletons skip it), so every non-null
    // entry here IS an overflow node.
    for (int slot = 0; slot < 1024; slot++) {
      final var heapRecord = source.getRecord(slot);
      if (heapRecord != null) {
        target.setRecord(heapRecord);
      }
    }
  }
}
