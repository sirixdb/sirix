/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.access.DatabaseType;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.cas.CASIndexBuilder;
import io.sirix.index.cas.CASIndexBuilderFactory;
import io.sirix.index.name.NameIndexBuilder;
import io.sirix.index.name.NameIndexBuilderFactory;
import io.sirix.index.path.PathIndexBuilder;
import io.sirix.index.path.PathIndexBuilderFactory;
import io.sirix.index.path.summary.PathSummaryWriter;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.index.projection.ProjectionChunkRowBatch;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.settings.Fixed;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.jspecify.annotations.Nullable;

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
 * boundaries, names (the only decoded text) and path resolution in the SAME byte sweep that slices
 * — reference deltas flush per chunk, before any rotation can flush summary pages cold.</li>
 * <li><b>Reserve</b>: the chunk's exact contiguous key range via
 * {@link RevisionRootPage#reserveKeyRangeInDocumentIndex(long)} — dense keys, so the result is
 * record-identical to a sequential load.</li>
 * <li><b>Build (worker)</b>: the same assembler core with a {@link WorkerPageBuilder} emits FINAL
 * record bytes into standalone pages; pre-resolved names/PCRs; range/slot verification always
 * on.</li>
 * <li><b>Stitch + adopt (coordinator)</b>: pages sharing a page key with already-live territory
 * (page 0's prologue records, the previous chunk's held tail) are merged record-by-record through
 * the CoW-checked blit seam; whole pages are adopted via
 * {@link StorageEngineWriter#adoptDocumentLeafPage}; the chunk's TAIL partial page is HELD out of
 * the intent log until its successor's head is merged into it (the flusher-race amendment).</li>
 * </ol>
 *
 * <p>
 * Scope: fresh resource, same refusals as {@link BulkJsonTreeAssembler}. PATH, CAS and NAME index
 * definitions are maintained BY the load: workers collect each family's tuples from the primitives
 * they hold, the coordinator drains them into the families' ordinary builders, and one flush per
 * family materialises each trie before the caller's commit. A PROJECTION index armed for this
 * transaction is likewise maintained in the same single pass, fed by the coordinator's record
 * attribution over the workers' row batches. Only valid-time interval maintenance is refused — it
 * is resolved by a configured-path visitor over whole records, with no chunk-local equivalent yet.
 * The caller commits, and that final commit is what closes the builds. This entry point is
 * single-builder (the M2 pipeline); the M3 executor fan-out layers on top of the same chunk
 * protocol.
 */
public final class ParallelBulkJsonImporter {

  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();
  private static final QNm ARRAY_PATH_QNM = new QNm("__array__");
  private static final int DEFAULT_CHUNK_CHAR_BUDGET = Integer.getInteger("sirix.parallelImport.chunkBytes", 4 << 20);
  private static final int ADOPT_BURST_PAGES = 16;
  private static final ProjectionBulkLoad[] NO_PROJECTION_LOADS = new ProjectionBulkLoad[0];

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
  private final IntArrayList nameKeyById = new IntArrayList(64);

  /** The coordinator's growing (parentPCR, name) → PCR view, snapshot per chunk for the builder. */
  private Long2ObjectOpenHashMap<Object2LongOpenHashMap<String>> masterPcrMemo = new Long2ObjectOpenHashMap<>();

  private KeyValueLeafPage heldTailPage;
  private long heldTailPageKey = -1;

  /**
   * Load-time projection builds this import feeds, empty when none is armed. Non-empty switches on
   * the feeder's per-member node counts and the record attribution below; empty leaves every code
   * path exactly as it was.
   */
  private final ProjectionBulkLoad[] projectionLoads;

  /**
   * Record roots and their last node keys, in document order, awaiting their row feed. A record's row
   * is handed to the builds only once its WHOLE subtree has entered the intent log — the same
   * watermark discipline the old key-attribution queue drained against — so the feed's document order
   * and the storage epoch a row's dictionary interns land in stay exactly where they were. Only the
   * row's SOURCE changed: the worker's batch, extracted during the build, instead of a coordinator
   * re-read through the transaction.
   */
  private final @Nullable LongArrayList pendingRecordRoots;
  private final @Nullable LongArrayList pendingRecordEnds;

  /** Index of the oldest record in the two queues above whose row has not been fed yet. */
  private int pendingRecordHead;

  /**
   * Worker-extracted row batches, in chunk order — one array (one batch per armed load) per chunk.
   */
  private final @Nullable ArrayDeque<ProjectionChunkRowBatch[]> pendingFeedBatches;

  /** Next unfed row of the head entry of {@link #pendingFeedBatches}. */
  private int feedRowInHeadBatch;

  /** The record-set container (the root array) every fed record is a child of. */
  private long feedRecordSetKey = -1;

  // ==== PATH/CAS/NAME maintenance riding the load ==============================================

  private final IndexDef[] pathIndexDefs;
  private final IndexDef[] casIndexDefs;
  private final IndexDef[] nameIndexDefs;
  private final PathIndexBuilder[] pathIndexBuilders;
  private final CASIndexBuilder[] casIndexBuilders;
  private final NameIndexBuilder[] nameIndexBuilders;
  private final boolean indexTuplesActive;
  private final boolean pathIndexesEverything;
  private final boolean casIndexesEverything;
  private final boolean collectAllNames;
  private final Set<String> includedNameStrings;
  private final IntOpenHashSet includedNameKeys;
  private final @Nullable Int2ObjectOpenHashMap<QNm> qnmByNameKey;
  private final @Nullable Long2LongOpenHashMap mirrorParentPcrMemo;

  private static final Str STR_TRUE = new Str("true");
  private static final Str STR_FALSE = new Str("false");

  /** Highest node key that is live in the intent log, i.e. readable back through the wtx. */
  private long adoptedWatermark = -1;

  /**
   * Last key of the chunk being adopted. A page is adopted whole, but the chunk that produced it may
   * stop part-way through the page's slot range (page 0's prologue chunk does), so the watermark a
   * page can justify is capped by what the chunk actually built.
   */
  private long currentChunkLastKey = -1;

  /** Recycled decode scratch: chars ≤ bytes for UTF-8, so one budget-sized array fits a chunk. */
  private final ArrayBlockingQueue<char[]> charScratchPool = new ArrayBlockingQueue<>(16);

  private char[] acquireCharScratch(final int neededChars) {
    final char[] pooled = charScratchPool.poll();
    if (pooled != null && pooled.length >= neededChars) {
      return pooled;
    }
    return new char[Math.max(neededChars, chunkCharBudget + (chunkCharBudget >> 2))];
  }

  private ParallelBulkJsonImporter(final JsonNodeTrxImpl wtx, final int chunkCharBudget,
      final ProjectionBulkLoad[] projectionLoads) {
    this.wtx = wtx;
    this.storageEngineWriter = wtx.getStorageEngineWriter();
    this.pathSummaryWriter = wtx.bulkPathSummaryWriter();
    this.buildPathSummary = wtx.bulkBuildPathSummary();
    // The coordinator attributes records to the armed builds itself; the spine's own handful of
    // nodes must not additionally arrive as notifications, or the same record set would be
    // announced twice through two different mechanisms.
    this.wtxSink = new WtxBulkRecordSink(wtx, false);
    this.revisionRootPage = storageEngineWriter.getActualRevisionRootPage();
    this.resourceConfig = wtx.getResourceSession().getResourceConfig();
    this.chunkCharBudget = chunkCharBudget;
    this.projectionLoads = projectionLoads;
    this.pendingRecordRoots = projectionLoads.length == 0
        ? null
        : new LongArrayList(1024);
    this.pendingRecordEnds = projectionLoads.length == 0
        ? null
        : new LongArrayList(1024);
    this.pendingFeedBatches = projectionLoads.length == 0
        ? null
        : new ArrayDeque<>(8);

    // PATH/CAS/NAME maintenance riding the load: the catalogued definitions get their ordinary
    // builders, whose empty-tree bulk-loader arms this import feeds through the workers' tuple
    // batches. The refusal above already rejected the one family this cannot serve (valid-time).
    final Set<IndexDef> pathDefs = wtx.bulkIndexDefsOfType(IndexType.PATH);
    final Set<IndexDef> casDefs = wtx.bulkIndexDefsOfType(IndexType.CAS);
    final Set<IndexDef> nameDefs = wtx.bulkIndexDefsOfType(IndexType.NAME);
    this.pathIndexDefs = pathDefs.toArray(IndexDef[]::new);
    this.casIndexDefs = casDefs.toArray(IndexDef[]::new);
    this.nameIndexDefs = nameDefs.toArray(IndexDef[]::new);
    this.pathIndexBuilders = createPathIndexBuilders();
    this.casIndexBuilders = createCasIndexBuilders();
    this.nameIndexBuilders = createNameIndexBuilders();
    this.indexTuplesActive =
        pathIndexBuilders.length > 0 || casIndexBuilders.length > 0 || nameIndexBuilders.length > 0;

    // NAME-family worker pre-filter: dictionary keys of the names any definition INCLUDES,
    // maintained as names intern (a name first occurring in chunk N is interned before chunk N
    // dispatches, so per-chunk snapshots are exact). A definition with an empty include set — or
    // an include the JSON name space cannot express — indexes every name, so the pre-filter
    // degrades to collect-all and the builders' own include/exclude filter decides at drain.
    final NamePreFilter namePreFilter = buildNamePreFilter();
    this.collectAllNames = namePreFilter.collectAll();
    this.includedNameStrings = namePreFilter.included();
    this.includedNameKeys = new IntOpenHashSet(Math.max(4, namePreFilter.included().size() * 2));
    this.qnmByNameKey = nameIndexBuilders.length > 0
        ? new Int2ObjectOpenHashMap<>(64)
        : null;
    this.pathIndexesEverything = anyDefinitionIndexesEveryPath(pathIndexDefs);
    this.casIndexesEverything = anyDefinitionIndexesEveryPath(casIndexDefs);
    this.mirrorParentPcrMemo = pathIndexBuilders.length > 0
        ? new Long2LongOpenHashMap(16)
        : null;
    if (mirrorParentPcrMemo != null) {
      mirrorParentPcrMemo.defaultReturnValue(Long.MIN_VALUE);
    }
  }

  /** The NAME-family worker pre-filter: the included local names, or "collect every name". */
  private record NamePreFilter(boolean collectAll, Set<String> included) {
  }

  /** The top-level array's path class and node key, as minted by the coordinator's prologue. */
  private record RootArray(long pcr, long key) {
  }

  /**
   * Prologue: create the root array through the ordinary transaction path, so page 0 enters the
   * intent log here and is stitched via the CoW-checked blit seam rather than held. Everything up to
   * this point is therefore already readable back.
   */
  private RootArray openRootArray() {
    final long rootArrayPcr = buildPathSummary
        ? pathSummaryWriter.getPathNodeKey(0, ARRAY_PATH_QNM, NodeKind.ARRAY)
        : 0;
    final long rootArrayKey =
        wtxSink.createArrayNode(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), NULL_KEY, rootArrayPcr);
    rememberRootArrayPcr(rootArrayPcr);
    adoptedWatermark = rootArrayKey;
    currentChunkLastKey = rootArrayKey;
    feedRecordSetKey = rootArrayKey;
    for (final ProjectionBulkLoad load : projectionLoads) {
      load.noteArrayRootInstance(rootArrayKey, wtx);
    }
    // The root array is created by the coordinator's own sink, so no worker batch ever carries it —
    // but the sequential leg's PATH listener indexes every ARRAY node, this one included.
    for (final PathIndexBuilder pathIndexBuilder : pathIndexBuilders) {
      pathIndexBuilder.add(rootArrayPcr, rootArrayKey);
    }
    return new RootArray(rootArrayPcr, rootArrayKey);
  }

  private PathIndexBuilder[] createPathIndexBuilders() {
    final PathIndexBuilder[] builders = new PathIndexBuilder[pathIndexDefs.length];
    final PathIndexBuilderFactory factory = new PathIndexBuilderFactory(DatabaseType.JSON);
    for (int i = 0; i < pathIndexDefs.length; i++) {
      builders[i] = factory.create(storageEngineWriter, wtx.getPathSummary(), pathIndexDefs[i]);
    }
    return builders;
  }

  private CASIndexBuilder[] createCasIndexBuilders() {
    final CASIndexBuilder[] builders = new CASIndexBuilder[casIndexDefs.length];
    final CASIndexBuilderFactory factory = new CASIndexBuilderFactory(DatabaseType.JSON);
    for (int i = 0; i < casIndexDefs.length; i++) {
      builders[i] = factory.create(storageEngineWriter, wtx.getPathSummary(), casIndexDefs[i]);
    }
    return builders;
  }

  private NameIndexBuilder[] createNameIndexBuilders() {
    final NameIndexBuilder[] builders = new NameIndexBuilder[nameIndexDefs.length];
    final NameIndexBuilderFactory factory = new NameIndexBuilderFactory(DatabaseType.JSON);
    for (int i = 0; i < nameIndexDefs.length; i++) {
      builders[i] = factory.create(storageEngineWriter, nameIndexDefs[i]);
    }
    return builders;
  }

  /**
   * A definition with an empty include set — or an include the JSON name space cannot express (a
   * prefix or namespace) — indexes every name, so the pre-filter degrades to collect-all and the
   * builders' own include/exclude filter decides at drain.
   */
  private NamePreFilter buildNamePreFilter() {
    final Set<String> included = new HashSet<>();
    for (final IndexDef nameDef : nameIndexDefs) {
      if (nameDef.getIncluded().isEmpty()) {
        return new NamePreFilter(true, included);
      }
      for (final QNm name : nameDef.getIncluded()) {
        if (isQualifiedBeyondLocalName(name)) {
          return new NamePreFilter(true, included);
        }
        included.add(name.getLocalName());
      }
    }
    return new NamePreFilter(false, included);
  }

  /** Whether a name carries a prefix or namespace, which the JSON name space cannot express. */
  private static boolean isQualifiedBeyondLocalName(final QNm name) {
    return (name.getPrefix() != null && !name.getPrefix().isEmpty())
        || (name.getNamespaceURI() != null && !name.getNamespaceURI().isEmpty());
  }

  /**
   * Whether any definition of the family indexes EVERY path — then the worker collects every eligible
   * node and each builder's own filter decides at drain.
   */
  private static boolean anyDefinitionIndexesEveryPath(final IndexDef[] defs) {
    for (final IndexDef def : defs) {
      if (def.getPaths().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /** As {@link #assemble(JsonNodeTrx, Reader, int)} with the default chunk budget. */
  public static void assemble(final JsonNodeTrx wtx, final Reader input) {
    assemble(wtx, input, DEFAULT_CHUNK_CHAR_BUDGET, defaultParallelism());
  }

  /** Byte-stream entry — the primary path: the coordinator never decodes value bytes. */
  public static void assemble(final JsonNodeTrx wtx, final InputStream input) {
    assembleBytes(wtx, input, DEFAULT_CHUNK_CHAR_BUDGET, defaultParallelism());
  }

  /**
   * Builder threads: builds are cheap relative to the flush pipeline's parallel serialization, so the
   * default leaves MOST cores to the snapshot flush pool — measured: an oversized build pool starves
   * serialization (serializeJoinWait dominated the flush worker at builders=16).
   */
  private static int defaultParallelism() {
    final int configured = Integer.getInteger("sirix.parallelImport.builders", -1);
    if (configured > 0) {
      return configured;
    }
    return Math.max(2, Runtime.getRuntime().availableProcessors() / 4);
  }

  /**
   * Imports ONE top-level JSON value from {@code input} into the fresh resource behind {@code wtx},
   * building member chunks through the parallel page pipeline when the top level is an array and
   * falling back to the sequential assembler otherwise. The caller commits.
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
    final ProjectionBulkLoad[] projectionLoads = refuseUnsupportedShape(impl);
    try {
      final PushbackInputStream stream = new PushbackInputStream(input, 1);
      if (!nextIsArray(stream)) {
        // Not a top-level array: nothing to parallelize — the sequential assembler is the
        // general path for every other shape. Its own record sink notifies the projection
        // listener, which feeds any armed load exactly as it does for a sequential import.
        BulkJsonTreeAssembler.assemble(wtx, new InputStreamReader(stream, StandardCharsets.UTF_8));
        return;
      }
      new ParallelBulkJsonImporter(impl, chunkCharBudget, projectionLoads).run(stream, Math.max(1, parallelism));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The assembler's refusals plus the importer's own.
   *
   * <p>
   * Index maintenance splits three ways here. PATH, CAS and NAME definitions are MAINTAINED by the
   * import itself: the workers collect each family's (key, nodeKey) tuples from the primitives they
   * already hold, and the coordinator drains them into the families' ordinary builders — the same
   * entries the sequential path's per-node change notifications produce. A PROJECTION index is
   * accepted when, and only when, a LOAD-TIME build is armed for it on THIS transaction
   * ({@code createProjectionIndexAtLoadStart}), because that build is fed by the coordinator's own
   * record attribution; a catalogued projection with no armed load would be silently left
   * unmaintained, so it refuses. Valid-time interval maintenance remains refused — it is resolved by
   * a configured-path visitor over whole records, which has no chunk-local equivalent yet.
   *
   * @return the armed loads this import must feed, empty when none
   */
  private static ProjectionBulkLoad[] refuseUnsupportedShape(final JsonNodeTrxImpl wtx) {
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
    // Path statistics are SUPPORTED: workers collect per-chunk per-path partials
    // (ChunkPathStatsBatch, sharing the cursor path's PathStatsAccumulator semantics) and the
    // coordinator merges them into its summary writer at chunk adoption, in document order. The
    // standard pre-commit flush applies the merged deltas.
    if (wtx.bulkHasValidTimeIndex()) {
      throw new IllegalStateException(
          "parallel bulk import does not support valid-time index maintenance during the load — load with no "
              + "valid-time index configured and build it afterwards");
    }
    if (!wtx.moveToDocumentRoot() || wtx.hasFirstChild()) {
      throw new IllegalStateException("parallel bulk import requires a FRESH resource (empty document root)");
    }
    // Resolved LAST: every prior refusal throws before any armed load is looked up, so a refused
    // import never leaves an armed load resolved-but-unfed behind.
    return resolveArmedProjectionLoads(wtx);
  }

  private static ProjectionBulkLoad[] resolveArmedProjectionLoads(final JsonNodeTrxImpl wtx) {
    final Set<IndexDef> projectionDefs = wtx.bulkProjectionIndexDefs();
    if (projectionDefs.isEmpty()) {
      return NO_PROJECTION_LOADS;
    }
    final String resourceKey = wtx.getResourceSession().getResourceConfig().getResource().toString();
    final ProjectionBulkLoad[] loads = new ProjectionBulkLoad[projectionDefs.size()];
    int count = 0;
    for (final IndexDef indexDef : projectionDefs) {
      final ProjectionBulkLoad load = ProjectionBulkLoad.active(resourceKey, indexDef.getID(), wtx);
      if (load == null) {
        throw new IllegalStateException("Projection index " + indexDef.getID()
            + " is catalogued on this resource but has no load-time build armed for this transaction. The parallel "
            + "importer feeds a projection by attributing the records IT writes, which requires the definition to be "
            + "declared before the data through createProjectionIndexAtLoadStart. Either arm it there, or load with "
            + "no declared projection and build the index afterwards with jn:create-projection-index.");
      }
      loads[count++] = load;
    }
    return loads;
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

    final RootArray rootArray = openRootArray();
    final long rootArrayPcr = rootArray.pcr();
    final long rootArrayKey = rootArray.key();

    final FusedSliceAndScan fused = new FusedSliceAndScan(input, chunkCharBudget, projectionLoads.length > 0);
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
        enqueueRecordRoots(chunk, firstKey, chunkLastMemberKey, lastKey);
        final long trailingSiblingKey = chunk.isFinal()
            ? NULL_KEY
            : lastKey + 1;

        // --- Stage B: build — off-thread when a pool exists, inline otherwise ---------------------
        // With a projection armed, the chunk carries a row batch per armed build: the worker extracts
        // the rows AS IT BUILDS, from the primitives it already holds, and the coordinator replays
        // them at adoption. The batch snapshot is taken HERE, after resolveChunkMetadata, so a field
        // whose first occurrence is in this chunk has already acquired its path class.
        final ProjectionChunkRowBatch[] chunkBatches = newChunkBatches((int) chunkMembers);
        final ChunkIndexTupleBatch chunkIndexBatch = newChunkIndexBatch();
        final ChunkPathStatsBatch chunkPathStatsBatch = newChunkPathStatsBatch();
        final WorkerPageBuilder builder = new WorkerPageBuilder(resourceConfig, storageEngineWriter.getRevisionNumber(),
            resourceConfig.nodeHashFunction, wtx.bulkStoreChildCount(), chunkNames, firstKey, lastKey, chunkBatches,
            feedRecordSetKey, chunkIndexBatch, chunkPathStatsBatch);
        if (chunkBatches != null) {
          pendingFeedBatches.addLast(chunkBatches);
        }
        final byte[] chunkBytes = chunk.bytes();
        final int chunkByteLength = chunk.length();
        final long buildFirstKey = firstKey;
        final long buildLastMemberBoundary = lastMemberKey;
        final long buildTrailing = trailingSiblingKey;
        final Long2ObjectOpenHashMap<Object2LongOpenHashMap<String>> memoSnapshot = copyMemo(masterPcrMemo);

        if (buildPool == null) {
          final List<KeyValueLeafPage> builtPages = buildChunk(fused, builder, chunkBytes, chunkByteLength,
              buildFirstKey, lastKey, rootArrayKey, rootArrayPcr, buildLastMemberBoundary, buildTrailing, memoSnapshot);
          drainIndexTuples(builder.indexTuples());
          drainPathStats(builder.pathStatsBatch());
          stitchAndAdopt(builtPages, lastKey);
        } else {
          final long submittedLastKey = lastKey;
          inFlight.addLast(new PendingBuild(buildPool.submit(() -> {
            // The chunk's own UTF-8 decode happens HERE, on the pool thread — the whole corpus
            // decode parallelizes across builders, into POOLED scratch (no per-chunk large arrays).
            return buildChunk(fused, builder, chunkBytes, chunkByteLength, buildFirstKey, submittedLastKey,
                rootArrayKey, rootArrayPcr, buildLastMemberBoundary, buildTrailing, memoSnapshot);
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
      adoptBurst(new KeyValueLeafPage[] {heldTailPage}, 1);
      heldTailPage = null;
      heldTailPageKey = -1;
    }

    // Everything built is now in the intent log, so the remaining rows — the records that were
    // still inside a held tail page — can be fed. Nothing must be left pending: the builds' own
    // end-of-load row count would come up short and refuse.
    if (projectionLoads.length > 0) {
      adoptedWatermark = expectedNextReservation - 1;
      feedReadableRows();
      if (pendingRecordCount() != 0) {
        throw new IllegalStateException(
            "the import finished with " + pendingRecordCount() + " records never handed to the projection build");
      }
    }

    // Materialise the PATH/CAS/NAME indexes: every chunk's tuples are drained, so one flush per
    // family builds each trie in a single pass, before the caller's commit persists it.
    for (final PathIndexBuilder pathIndexBuilder : pathIndexBuilders) {
      pathIndexBuilder.finish();
    }
    for (final CASIndexBuilder casIndexBuilder : casIndexBuilders) {
      casIndexBuilder.finish();
    }
    for (final NameIndexBuilder nameIndexBuilder : nameIndexBuilders) {
      nameIndexBuilder.finish();
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
      if (builder.pathStatsBatch() != null) {
        building.collectPathStatsInto(builder.pathStatsBatch());
      }
      building.run();
      return builder.finish(lastKey);
    } finally {
      charScratchPool.offer(scratch);
    }
  }

  private void adoptNext(final ArrayDeque<PendingBuild> inFlight) {
    final PendingBuild next = inFlight.pollFirst();
    try {
      final List<KeyValueLeafPage> builtPages = next.pages().get();
      drainIndexTuples(next.builder().indexTuples());
      drainPathStats(next.builder().pathStatsBatch());
      stitchAndAdopt(builtPages, next.lastKey());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while awaiting a chunk build", e);
    } catch (final ExecutionException e) {
      throw new IllegalStateException("chunk build failed", e.getCause());
    }
  }

  // ==== name interning + counting ==============================================================

  /** Marker object closing the feeder handoff. */
  private static final Object FEEDER_DONE = new Object();

  /**
   * Resolves a chunk's deferred metadata IN DOCUMENT ORDER on the coordinator — the sequential path's
   * exact dictionary/summary call sequence, batched: new paths resolve through the real summary
   * writer (each miss counts its first occurrence), new names intern via createNameKey, repeat
   * occurrences land as reference/count deltas, and the master memo gains every new (parentPCR, name)
   * → PCR pair for the build worker's snapshot.
   *
   * @return the chunk's name → dictionary-key table for the build worker
   */
  private Object2IntOpenHashMap<String> resolveChunkMetadata(final FusedSliceAndScan.Chunk chunk) {
    // Names first-occurring in this chunk, in document order (interleaved order vs paths is
    // preserved by the sequential path only per-record; the dictionary probe sequences that
    // matter are name-vs-name and path-vs-path order, both exact here).
    for (final String name : chunk.newNames()) {
      nameById.add(name);
      final int nameKey = storageEngineWriter.createNameKey(name, NodeKind.OBJECT_NAMED_OBJECT);
      nameKeyById.add(nameKey);
      noteInternedName(name, nameKey);
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
    currentChunkLastKey = lastKey;
    final KeyValueLeafPage[] burst = new KeyValueLeafPage[ADOPT_BURST_PAGES];
    int burstSize = 0;
    final boolean tailPartial = ((lastKey + 1) & 1023) != 0;

    for (int i = 0; i < pages.size(); i++) {
      final KeyValueLeafPage page = pages.get(i);
      final boolean isTail = i == pages.size() - 1;
      final long pageKey = page.getPageKey();

      if (pageKey == heldTailPageKey) {
        // Shares the previous chunk's held tail: merge into the coordinator-owned page object.
        // The worker page owns an allocator frame and nothing reads it after the merge, so it is
        // retired here exactly as the page-0 blit below retires its source.
        mergeInto(heldTailPage, page);
        page.retire();
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
        noteAdoptedPage(0);
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

  /**
   * Queue this chunk's record roots with the last node key of each, derived from the feeder's
   * per-member node counts over the chunk's contiguous reserved range.
   */
  private void enqueueRecordRoots(final FusedSliceAndScan.Chunk chunk, final long firstKey,
      final long chunkLastMemberKey, final long lastKey) {
    if (projectionLoads.length == 0) {
      return;
    }
    final long[] memberNodes = chunk.memberNodes();
    if (memberNodes == null || memberNodes.length != chunk.members()) {
      throw new IllegalStateException("the feeder handed " + (memberNodes == null
          ? "no"
          : String.valueOf(memberNodes.length)) + " member node counts for a chunk of " + chunk.members()
          + " members; a load-time projection build cannot name its records without them");
    }
    long recordKey = firstKey;
    for (final long memberNodeCount : memberNodes) {
      pendingRecordRoots.add(recordKey);
      recordKey += memberNodeCount;
      pendingRecordEnds.add(recordKey - 1);
    }
    // The two independently-derived boundaries of the same chunk must agree, or the record roots
    // this queue names are not the ones the build produced.
    if (recordKey - 1 != lastKey) {
      throw new IllegalStateException("member node counts cover keys up to " + (recordKey - 1)
          + " but the chunk's reserved range ends at " + lastKey);
    }
    if (chunk.members() > 0 && pendingRecordRoots.getLong(pendingRecordRoots.size() - 1) != chunkLastMemberKey) {
      throw new IllegalStateException("the last member root derived from node counts is "
          + pendingRecordRoots.getLong(pendingRecordRoots.size() - 1) + " but range arithmetic says "
          + chunkLastMemberKey);
    }
  }

  /** Per-chunk row batches for the armed builds, snapshot after the chunk's paths resolved. */
  private ProjectionChunkRowBatch @Nullable [] newChunkBatches(final int members) {
    if (projectionLoads.length == 0) {
      return null;
    }
    final ProjectionChunkRowBatch[] batches = new ProjectionChunkRowBatch[projectionLoads.length];
    for (int i = 0; i < projectionLoads.length; i++) {
      batches[i] = projectionLoads[i].newChunkBatch(wtx.getPathSummary(), members, feedRecordSetKey);
    }
    return batches;
  }

  /**
   * Feed every queued record whose WHOLE subtree has been adopted to the armed builds, oldest first,
   * from the worker-extracted batches. The batch row and the queue entry describe the same record
   * through two independent derivations — the worker's parent-key detection and the feeder's member
   * node counts — so their root keys are cross-checked per row before the row is appended.
   */
  private void feedReadableRows() {
    final int pending = pendingRecordRoots.size();
    int head = pendingRecordHead;
    while (head < pending && pendingRecordEnds.getLong(head) <= adoptedWatermark) {
      ProjectionChunkRowBatch[] batches = pendingFeedBatches.peekFirst();
      while (batches != null && feedRowInHeadBatch >= batches[0].rowCount()) {
        pendingFeedBatches.pollFirst();
        feedRowInHeadBatch = 0;
        batches = pendingFeedBatches.peekFirst();
      }
      if (batches == null) {
        throw new IllegalStateException(
            "record root " + pendingRecordRoots.getLong(head) + " is adopted but no chunk batch holds its row");
      }
      final long recordKey = pendingRecordRoots.getLong(head);
      final int row = feedRowInHeadBatch;
      if (batches[0].recordRootAt(row) != recordKey) {
        throw new IllegalStateException("the worker attributed batch row " + row + " to record root "
            + batches[0].recordRootAt(row) + " but the feeder's member counts derived root " + recordKey);
      }
      for (int i = 0; i < projectionLoads.length; i++) {
        projectionLoads[i].appendCoordinatorRow(storageEngineWriter, batches[i], row, recordKey, feedRecordSetKey,
            Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
      }
      feedRowInHeadBatch = row + 1;
      head++;
    }
    if (head == pendingRecordHead) {
      return;
    }
    pendingRecordHead = head;
    // Compact only once the consumed prefix dominates, so a chunk's worth of members costs one
    // amortized shift rather than one per adoption burst.
    if (pendingRecordHead > (pending >> 1)) {
      pendingRecordRoots.removeElements(0, pendingRecordHead);
      pendingRecordEnds.removeElements(0, pendingRecordHead);
      pendingRecordHead = 0;
    }
  }

  /** Records still awaiting attribution — the queue minus its already-consumed prefix. */
  private int pendingRecordCount() {
    return pendingRecordRoots.size() - pendingRecordHead;
  }

  // ==== PATH/CAS/NAME tuple flow ===============================================================

  /** Book-keeping for a freshly interned field name: its drain-time QNm and the NAME pre-filter. */
  private void noteInternedName(final String name, final int nameKey) {
    if (qnmByNameKey == null) {
      return;
    }
    qnmByNameKey.put(nameKey, new QNm(name));
    if (!collectAllNames && includedNameStrings.contains(name)) {
      includedNameKeys.add(nameKey);
    }
  }

  /**
   * A tuple batch for the next chunk, with the per-family union filters resolved against the LIVE
   * path summary and name dictionary — after this chunk's Stage-A resolution, so a path class or name
   * first occurring in this chunk is in this chunk's snapshot. The union sets are fresh (or copied)
   * objects per chunk because the workers read them off-thread while later chunks grow the
   * coordinator's originals.
   */
  private @Nullable ChunkIndexTupleBatch newChunkIndexBatch() {
    if (!indexTuplesActive) {
      return null;
    }
    final boolean pathActive = pathIndexBuilders.length > 0;
    final boolean casActive = casIndexBuilders.length > 0;
    final boolean nameActive = nameIndexBuilders.length > 0;
    LongOpenHashSet pathUnion = null;
    if (pathActive && !pathIndexesEverything) {
      pathUnion = new LongOpenHashSet();
      for (final IndexDef pathDef : pathIndexDefs) {
        pathUnion.addAll(wtx.getPathSummary().getPCRsForPaths(pathDef.getPaths()));
      }
    }
    LongOpenHashSet casUnion = null;
    if (casActive && !casIndexesEverything) {
      casUnion = new LongOpenHashSet();
      for (final IndexDef casDef : casIndexDefs) {
        casUnion.addAll(wtx.getPathSummary().getPCRsForPaths(casDef.getPaths()));
      }
    }
    IntOpenHashSet nameUnion = null;
    if (nameActive && !collectAllNames) {
      nameUnion = new IntOpenHashSet(includedNameKeys);
    }
    return new ChunkIndexTupleBatch(pathActive, pathUnion, casActive, casUnion, nameActive, nameUnion);
  }

  /**
   * A fresh per-chunk path-statistics batch, or {@code null} when the resource does not maintain
   * statistics (or builds no summary — statistics are keyed by path classes, which only exist with a
   * summary).
   */
  private @Nullable ChunkPathStatsBatch newChunkPathStatsBatch() {
    return buildPathSummary && pathSummaryWriter != null && pathSummaryWriter.isPathStatisticsEnabled()
        ? new ChunkPathStatsBatch()
        : null;
  }

  /**
   * Merge one chunk's path-statistics partials into the coordinator's summary writer. Chunks drain in
   * adoption order, which is document order — count, min, max, nullCount, the HLL sketch, the page
   * keys and the 128-bit integral sum (with its trust verdict) are order-free, but
   * {@code sumFraction} is not, so document order is what makes the merged statistics match the
   * cursor path's. The standard pre-commit {@code flushPendingStats()} then applies them through the
   * ordinary COW path.
   */
  private void drainPathStats(final @Nullable ChunkPathStatsBatch batch) {
    if (batch == null || batch.isEmpty()) {
      return;
    }
    batch.mergeInto(pathSummaryWriter);
  }

  /**
   * Drain one chunk's collected tuples into every family builder, in chunk (document) order. The
   * builders re-apply their own per-definition filters and the CAS type conversion — exactly the
   * semantics the sequential import's listeners run per notification.
   */
  private void drainIndexTuples(final @Nullable ChunkIndexTupleBatch batch) {
    if (batch == null) {
      return;
    }
    refreshBuilderPathSets();
    drainPathEntries(batch);
    drainMirrorEntries(batch);
    drainNameEntries(batch);
    drainCasEntries(batch);
  }

  /**
   * The builders cache their resolved path classes for a FROZEN summary; this feeder's summary grows
   * between drains, so a class first minted since the previous drain (or after the prologue's
   * root-array entry) would stay invisible behind the stale set.
   */
  private void refreshBuilderPathSets() {
    for (final PathIndexBuilder pathIndexBuilder : pathIndexBuilders) {
      pathIndexBuilder.refreshIndexedPaths();
    }
    for (final CASIndexBuilder casIndexBuilder : casIndexBuilders) {
      casIndexBuilder.refreshIndexedPaths();
    }
  }

  /** PATH-family tuples, in chunk (document) order. */
  private void drainPathEntries(final ChunkIndexTupleBatch batch) {
    final int pathEntries = batch.pathEntryCount();
    for (int i = 0; i < pathEntries; i++) {
      final long pcr = batch.pathPcrAt(i);
      final long nodeKey = batch.pathNodeKeyAt(i);
      for (final PathIndexBuilder pathIndexBuilder : pathIndexBuilders) {
        pathIndexBuilder.add(pcr, nodeKey);
      }
    }
  }

  /**
   * OBJECT_NAMED_ARRAY plays BOTH the ARRAY and the OBJECT_KEY structural roles: mirror each entry
   * under the parent (OBJECT_KEY-layer) path class, exactly as the sequential listener does. The
   * parent resolution is memoised per distinct array-layer class.
   */
  private void drainMirrorEntries(final ChunkIndexTupleBatch batch) {
    final int mirrorEntries = batch.mirrorCandidateCount();
    for (int i = 0; i < mirrorEntries; i++) {
      final long objectKeyLayerPcr = mirrorObjectKeyLayerPcr(batch.mirrorArrayPcrAt(i));
      if (objectKeyLayerPcr < 0) {
        continue;
      }
      final long nodeKey = batch.mirrorNodeKeyAt(i);
      for (final PathIndexBuilder pathIndexBuilder : pathIndexBuilders) {
        pathIndexBuilder.add(objectKeyLayerPcr, nodeKey);
      }
    }
  }

  /** NAME-family tuples; a name key collected but never interned is a hard inconsistency. */
  private void drainNameEntries(final ChunkIndexTupleBatch batch) {
    final int nameEntries = batch.nameEntryCount();
    for (int i = 0; i < nameEntries; i++) {
      final QNm name = qnmByNameKey.get(batch.nameKeyAt(i));
      if (name == null) {
        throw new IllegalStateException(
            "name key " + batch.nameKeyAt(i) + " was collected for the NAME index but never interned");
      }
      final long nodeKey = batch.nameNodeKeyAt(i);
      for (final NameIndexBuilder nameIndexBuilder : nameIndexBuilders) {
        nameIndexBuilder.add(name, nodeKey);
      }
    }
  }

  /**
   * CAS-family tuples. The string and number payloads live in their own dense side arenas, so each
   * kind advances its own ordinal rather than indexing by the tuple position.
   */
  private void drainCasEntries(final ChunkIndexTupleBatch batch) {
    final int casEntries = batch.casEntryCount();
    int stringOrdinal = 0;
    int numberOrdinal = 0;
    for (int i = 0; i < casEntries; i++) {
      final Str value;
      switch (batch.casKindAt(i)) {
        case ChunkIndexTupleBatch.CAS_KIND_STRING -> {
          final int offset = batch.casStringOffsetAt(stringOrdinal);
          final int length = batch.casStringLengthAt(stringOrdinal);
          stringOrdinal++;
          value = new Str(new String(batch.casStringArena(), offset, length, StandardCharsets.UTF_8));
        }
        case ChunkIndexTupleBatch.CAS_KIND_NUMBER -> {
          value = new Str(String.valueOf(batch.casNumberAt(numberOrdinal)));
          numberOrdinal++;
        }
        case ChunkIndexTupleBatch.CAS_KIND_BOOLEAN_TRUE -> value = STR_TRUE;
        case ChunkIndexTupleBatch.CAS_KIND_BOOLEAN_FALSE -> value = STR_FALSE;
        default -> throw new IllegalStateException("unknown CAS tuple kind " + batch.casKindAt(i));
      }
      final long pcr = batch.casPcrAt(i);
      final long nodeKey = batch.casNodeKeyAt(i);
      for (final CASIndexBuilder casIndexBuilder : casIndexBuilders) {
        casIndexBuilder.add(value, pcr, nodeKey);
      }
    }
  }

  /**
   * The OBJECT_KEY-layer path class above an OBJECT_NAMED_ARRAY's array-layer class, or {@code -1}
   * when the summary holds no parent for it. Memoised: distinct array-layer classes are few.
   */
  private long mirrorObjectKeyLayerPcr(final long arrayLayerPcr) {
    final long memoised = mirrorParentPcrMemo.get(arrayLayerPcr);
    if (memoised != Long.MIN_VALUE) {
      return memoised;
    }
    final var arrayPathNode = wtx.getPathSummary().getPathNodeForPathNodeKey(arrayLayerPcr);
    final long parent = arrayPathNode == null
        ? -1L
        : arrayPathNode.getParentKey();
    mirrorParentPcrMemo.put(arrayLayerPcr, parent);
    return parent;
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
      noteAdoptedPage(burst[i].getPageKey());
      burst[i] = null;
    }
    // Accounting after the burst lets the ordinary predicate rotate the flush epoch — the same
    // cadence machinery the sequential path drives per top-level record.
    if (projectionLoads.length == 0) {
      wtx.bulkAccountRecord(records);
      return;
    }
    // Feed every now-readable row to the armed builds BEFORE accounting, because accounting is
    // what rotates the flush epoch: the rotation drain closes the builds' current dictionary
    // generation, so feeding first keeps each row's interns in the epoch its record was adopted
    // in. The rows themselves come from the worker batches and read nothing back, so — unlike the
    // key-attribution design this replaced — a record straddling the held tail page no longer
    // forces the accounting (and with it the whole flush epoch) to wait for the next chunk.
    feedReadableRows();
    wtx.bulkAccountRecord(records);
  }

  /**
   * Advance the readable-key watermark for a page that just entered the intent log. A page covers its
   * whole 1024-slot range, but never past what the chunk that produced it actually built — the
   * prologue chunk stops inside page 0.
   */
  private void noteAdoptedPage(final long pageKey) {
    if (projectionLoads.length == 0) {
      return;
    }
    final long covered = Math.min(((pageKey + 1) << 10) - 1, currentChunkLastKey);
    if (covered > adoptedWatermark) {
      adoptedWatermark = covered;
    }
  }

  /** Replays every record of {@code source} into {@code target} (same page key, disjoint slots). */
  private static void mergeInto(final KeyValueLeafPage target, final KeyValueLeafPage source) {
    final long pageBase = source.getPageKey() << 10;
    for (final int slot : source.populatedSlots()) {
      final MemorySegment record = source.getSlot(slot);
      final int kindId = source.getSlotNodeKindId(slot);
      final long absOffset = target.prepareHeapForDirectWriteOrOverflow((int) record.byteSize(), 0);
      if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
        throw new IllegalStateException("boundary merge overflowed page " + target.getPageKey() + " at slot " + slot);
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
