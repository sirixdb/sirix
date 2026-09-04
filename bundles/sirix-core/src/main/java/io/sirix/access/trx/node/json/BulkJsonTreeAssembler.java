/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import org.jspecify.annotations.Nullable;
import io.sirix.index.path.summary.PathSummaryWriter;
import io.sirix.node.NodeKind;
import io.sirix.settings.Fixed;
import io.brackit.query.atomic.QNm;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.Arrays;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

/**
 * Bulk page-assembly loader, v0.1: append-only construction of a FRESH resource from a JSON token
 * stream, producing a tree STRUCTURALLY IDENTICAL to what the cursor inserts build — same node
 * keys, kinds, names, values, pointers, counts, path summary and notifications — while touching
 * each record's slot essentially once.
 *
 * <p>
 * <b>Why it is faster than the cursor path.</b> A cursor insert pays, per node: 2–3
 * {@code prepareRecordForModification} page-container resolutions (parent childCount/firstChild,
 * left sibling rightSib), cursor repositioning, a per-node epoch check, and — when notifications
 * need it — a parent walk for the path class. This assembler computes every pointer from its own
 * container stack BEFORE a record is first written, so leaves are created with FINAL pointers, and
 * each CONTAINER takes exactly ONE in-place fixup at its close
 * (firstChild/lastChild/childCount/rightSib together) instead of one per child.
 *
 * <p>
 * <b>The sink seam.</b> Record emission goes through a {@link BulkRecordSink}: the
 * transaction-backed sink is the original sequential behavior; a counting sink gives the parallel
 * importer's Stage-A its node counts through the SAME code path that later builds (so a count can
 * never disagree with a build by construction); a worker page-builder sink emits standalone pages
 * for pre-reserved key ranges. The assembler core — scanner drive, key prediction, container stack,
 * PCR memoization — is identical for all three.
 *
 * <p>
 * <b>Key prediction.</b> Node keys mint sequentially and deterministically. The assembler predicts
 * forward references — a record's right sibling — as {@code expectedNextKey} under one-token
 * lookahead, and ASSERTS the prediction against the key the sink actually minted at every creation.
 * A violated prediction is a hard {@link IllegalStateException}; a silent mis-pointer is impossible
 * by construction.
 *
 * <p>
 * <b>Path summary.</b> Resolution uses only the KEYED writer API
 * ({@link PathSummaryWriter#getPathNodeKey(long, QNm, NodeKind)} under a stack-carried context
 * PCR), never the cursor-positional variant. The context rules mirror the cursor path exactly,
 * including its as-built quirks: plain {@code OBJECT} is transparent (inherits the parent context),
 * the four stop kinds anchor their own; ALL arrays resolve one {@code __array__} step under the
 * parent context (user-ruled unification); a fused named array stores the {@code __array__}-layer
 * PCR obtained via {@link PathSummaryWriter#getArrayChildPathNodeKey(long)}. The keyed calls are
 * made once per record occurrence — the writer's reference counting must observe the same call
 * sequence the cursor path produces.
 *
 * <p>
 * <b>Scope guard.</b> Refuses up front — before writing anything — any configuration this version
 * does not faithfully reproduce: hashes, DeweyIDs, node history, or a non-empty target document.
 * Path statistics ARE reproduced: every leaf observation is recorded through the summary writer's
 * deferred machinery (sequential mode) or a per-chunk partial batch (worker mode), the same
 * accumulation semantics cursor ingestion uses. Mutating existing nodes is permanently out of
 * scope; that is the cursor's job.
 */
public final class BulkJsonTreeAssembler {

  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** Mirrors the cursor path's first-child array path step ({@code ARRAY_PATH_QNM}). */
  private static final QNm ARRAY_PATH_QNM = new QNm("__array__");

  private static final int INITIAL_DEPTH_CAPACITY = 64;

  private static final byte LEVEL_DOCUMENT = 0;
  private static final byte LEVEL_OBJECT = 1;
  private static final byte LEVEL_ARRAY = 2;

  private final BulkRecordSink sink;
  private final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter;
  private final boolean buildPathSummary;
  private final BulkJsonScanner scanner;

  /**
   * Sequential mode: record path statistics directly into {@link #pathSummaryWriter}'s deferred
   * machinery (the exact accumulation the cursor path uses; the standard pre-commit flush applies
   * it). Set when the writer is present and the resource enables statistics.
   */
  private final boolean statsToSummary;

  /**
   * Worker mode: per-chunk path-statistics partials, injected by the parallel importer when the
   * resource enables statistics and merged by the coordinator at chunk adoption. Mutually exclusive
   * with {@link #statsToSummary} (worker assemblers run without a summary writer).
   */
  private @Nullable ChunkPathStatsBatch pathStatsBatch;

  /** The key the NEXT sink creation must mint; every creation asserts and advances it. */
  private long expectedNextKey;

  // One frame per open container (document = frame 0). Parallel arrays, grown by doubling.
  private byte[] levelKind = new byte[INITIAL_DEPTH_CAPACITY];
  private long[] levelContainerKey = new long[INITIAL_DEPTH_CAPACITY];
  private long[] levelContentPcr = new long[INITIAL_DEPTH_CAPACITY];
  private long[] levelFirstChild = new long[INITIAL_DEPTH_CAPACITY];
  private long[] levelLastChild = new long[INITIAL_DEPTH_CAPACITY];
  private long[] levelChildCount = new long[INITIAL_DEPTH_CAPACITY];
  private int depth;

  /** Field name consumed from the reader, pending until its value token arrives. */
  private String pendingName;

  /**
   * (parent PCR, step name) → PCR memo with DEFERRED reference counting: a miss resolves through the
   * ordinary summary path (which counts that first occurrence), a hit only bumps a local pending
   * counter, and {@link #flushPendingPathReferences()} applies the accumulated repeats in one record
   * touch per path node BEFORE every epoch rotation and at the end of the run. The committed
   * reference counts are exactly per-occurrence counting's — the oracle's summary dump (references
   * included) pins it — while the measured 13.7%% of fill time spent in per-node summary resolution
   * collapses to a hash probe.
   */
  private final Long2ObjectOpenHashMap<Object2LongOpenHashMap<String>> pcrMemo = new Long2ObjectOpenHashMap<>();

  private final Long2IntOpenHashMap pendingPathReferences = new Long2IntOpenHashMap();

  /** Key AFTER the last record boundary accounted to the epoch machinery. */
  private long epochAccountedUpToKey;

  /** Member mode: first key of the member currently being driven. */
  private long memberStartKey;

  private long lastMemberStartKey() {
    return memberStartKey;
  }

  /** Member mode: the run's first key, for the first member's node-count computation. */
  private long expectedNextKeyAtRunStart;

  /**
   * MEMBER MODE (parallel import): frame 0 is a pre-existing top-level ARRAY rather than the
   * document, the drive consumes a sequence of that array's members, and the final member's right
   * sibling comes from OUTSIDE (the next chunk's first key) instead of the lookahead.
   */
  private final boolean memberMode;
  private final long memberRootKey;
  private final long memberRootPcr;
  private final long memberLeftBoundaryKey;
  private final long trailingSiblingKey;

  /** Member-mode statistics for the coordinator: members driven and the last member's node count. */
  private long membersDriven;
  private long lastMemberNodeCount;

  BulkJsonTreeAssembler(final BulkRecordSink sink, final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter,
      final boolean buildPathSummary, final BulkJsonScanner scanner, final long firstKey) {
    this(sink, pathSummaryWriter, buildPathSummary, scanner, firstKey, false, 0, 0, NULL_KEY, NULL_KEY);
  }

  BulkJsonTreeAssembler(final BulkRecordSink sink, final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter,
      final boolean buildPathSummary, final BulkJsonScanner scanner, final long firstKey, final boolean memberMode,
      final long memberRootKey, final long memberRootPcr, final long memberLeftBoundaryKey,
      final long trailingSiblingKey) {
    this.sink = sink;
    this.pathSummaryWriter = pathSummaryWriter;
    // Statistics require the summary to be BUILT here: without it every PCR is the 0/-1
    // sentinel, and observations would pile onto a nonsense path class.
    this.statsToSummary = pathSummaryWriter != null && buildPathSummary && pathSummaryWriter.isPathStatisticsEnabled();
    this.buildPathSummary = buildPathSummary;
    this.scanner = scanner;
    this.expectedNextKey = firstKey;
    this.epochAccountedUpToKey = firstKey;
    this.memberMode = memberMode;
    this.memberRootKey = memberRootKey;
    this.memberRootPcr = memberRootPcr;
    this.memberLeftBoundaryKey = memberLeftBoundaryKey;
    this.trailingSiblingKey = trailingSiblingKey;
  }

  /**
   * Worker mode: collect path statistics into {@code batch} (the coordinator merges it at chunk
   * adoption). Refused in sequential mode — there the summary writer itself accumulates and the two
   * routes must never double-count.
   */
  void collectPathStatsInto(final ChunkPathStatsBatch batch) {
    if (statsToSummary) {
      throw new IllegalStateException(
          "path statistics already flow into the summary writer — a batch would double-count");
    }
    this.pathStatsBatch = batch;
  }

  /** Pre-fills the PCR memo (worker mode: every path the chunk uses must already be resolved). */
  void prefillPcrMemo(final Long2ObjectOpenHashMap<Object2LongOpenHashMap<String>> resolved) {
    for (final var entry : resolved.long2ObjectEntrySet()) {
      final Object2LongOpenHashMap<String> copy = new Object2LongOpenHashMap<>(entry.getValue());
      copy.defaultReturnValue(-1);
      pcrMemo.put(entry.getLongKey(), copy);
    }
  }

  /** The coordinator's memo, snapshot after a counting run to hand to the chunk's builder. */
  Long2ObjectOpenHashMap<Object2LongOpenHashMap<String>> pcrMemoView() {
    return pcrMemo;
  }

  long membersDriven() {
    return membersDriven;
  }

  long lastMemberNodeCount() {
    return lastMemberNodeCount;
  }

  long nextKey() {
    return expectedNextKey;
  }

  /**
   * Assemble ONE top-level JSON value from {@code input} into the fresh resource behind {@code wtx}.
   * The caller commits.
   *
   * @param wtx a write transaction on a FRESH resource; must be the standard implementation, since
   *        bulk assembly drives its internal factory/summary/notification machinery
   * @param input the token source; consumed up to (and including) the top-level value's end
   */
  public static void assemble(final JsonNodeTrx wtx, final Reader input) {
    assemble(wtx, new BulkJsonScanner(input));
  }

  /** Test seam: inject a scanner (e.g. with a tiny buffer to stress refills inside tokens). */
  static void assemble(final JsonNodeTrx wtx, final BulkJsonScanner scanner) {
    if (!(wtx instanceof final JsonNodeTrxImpl impl)) {
      throw new IllegalArgumentException(
          "bulk assembly requires the standard JsonNodeTrx implementation, got " + wtx.getClass().getName());
    }
    refuseUnsupportedShape(impl);
    try {
      new BulkJsonTreeAssembler(new WtxBulkRecordSink(impl), impl.bulkPathSummaryWriter(), impl.bulkBuildPathSummary(),
          scanner, impl.getMaxNodeKey() + 1).run();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Refusal BEFORE any write: nothing may silently mix builders or half-load. */
  private static void refuseUnsupportedShape(final JsonNodeTrxImpl wtx) {
    final ResourceConfiguration config = wtx.getResourceSession().getResourceConfig();
    if (config.hashType != HashType.NONE) {
      throw new IllegalStateException("bulk assembly v0.1 supports hashType=NONE only, got " + config.hashType);
    }
    if (config.areDeweyIDsStored) {
      throw new IllegalStateException("bulk assembly v0.1 does not support DeweyIDs");
    }
    if (config.storeNodeHistory()) {
      throw new IllegalStateException("bulk assembly v0.1 does not support node history");
    }
    // Path statistics are SUPPORTED: the assembler records every leaf observation into the
    // summary writer's deferred machinery (the exact accumulation cursor ingestion uses; the
    // standard pre-commit flush applies it). See the leaf* methods.
    if (!wtx.moveToDocumentRoot() || wtx.hasFirstChild()) {
      throw new IllegalStateException("bulk assembly requires a FRESH resource (empty document root)");
    }
  }

  void run() throws IOException {
    expectedNextKeyAtRunStart = expectedNextKey;
    memberStartKey = expectedNextKey;
    if (memberMode) {
      // Frame 0: the pre-existing top-level array. The previous chunk's last member is the left
      // sibling of this chunk's first member; child bookkeeping here is chunk-local (the
      // coordinator fixes the array once at the very end with GLOBAL counts).
      levelKind[0] = LEVEL_ARRAY;
      levelContainerKey[0] = memberRootKey;
      levelContentPcr[0] = memberRootPcr;
      levelFirstChild[0] = NULL_KEY;
      levelLastChild[0] = memberLeftBoundaryKey;
      levelChildCount[0] = 0;
    } else {
      // Frame 0: the document. Its content PCR is 0, matching what the cursor path reports for
      // JSON_DOCUMENT context (getPathNodeKey(StructNode) returns 0 there).
      levelKind[0] = LEVEL_DOCUMENT;
      levelContainerKey[0] = Fixed.DOCUMENT_NODE_KEY.getStandardProperty();
      levelContentPcr[0] = 0;
      levelFirstChild[0] = NULL_KEY;
      levelLastChild[0] = NULL_KEY;
      levelChildCount[0] = 0;
    }
    depth = 0;

    loop: while (true) {
      final int event = scanner.next();
      switch (event) {
        case BulkJsonScanner.EVENT_BEGIN_OBJECT -> openContainer(true);
        case BulkJsonScanner.EVENT_BEGIN_ARRAY -> openContainer(false);
        case BulkJsonScanner.EVENT_NAME -> pendingName = scanner.name();
        case BulkJsonScanner.EVENT_STRING -> leafString(scanner.stringUtf8(), scanner.stringUtf8Length());
        case BulkJsonScanner.EVENT_NUMBER -> leafNumber();
        case BulkJsonScanner.EVENT_TRUE -> leafBoolean(true);
        case BulkJsonScanner.EVENT_FALSE -> leafBoolean(false);
        case BulkJsonScanner.EVENT_NULL -> leafNull();
        case BulkJsonScanner.EVENT_END_OBJECT, BulkJsonScanner.EVENT_END_ARRAY -> closeContainer();
        case BulkJsonScanner.EVENT_END_DOCUMENT -> {
          break loop;
        }
        default -> throw new IllegalStateException("unknown scanner event " + event);
      }
      if (!memberMode && depth == 0 && levelChildCount[0] > 0) {
        // One top-level value per assemble; a second is an input-shape refusal.
        break;
      }
    }

    if (depth != 0) {
      throw new IllegalStateException("bulk assembly ended with " + depth + " unclosed container(s)");
    }
    flushPendingPathReferences();
    if (!memberMode) {
      fixupDocument();
    }
  }

  // ==== containers ============================================================================

  private void openContainer(final boolean isObject) throws IOException {
    final byte parentLevelKind = levelKind[depth];
    final long parentKey = levelContainerKey[depth];
    final long leftSibKey = levelLastChild[depth];
    final long containerKey;
    final long contentPcr;

    if (parentLevelKind == LEVEL_OBJECT) {
      // Named structural: the fused OBJECT_NAMED_OBJECT / OBJECT_NAMED_ARRAY record.
      final String name = takePendingName();
      final long fieldPcr = resolveFieldPcr(name);
      if (isObject) {
        containerKey = assertMinted(sink.createObjectNamedObjectNode(parentKey, leftSibKey, fieldPcr, name));
        contentPcr = fieldPcr;
      } else {
        // The fused named array stores the __array__-layer PCR (see the cursor path's
        // insertObjectRecordStructuralAsFirstChild); its notification carries the SAME key.
        final long arrayPcr;
        if (!buildPathSummary) {
          arrayPcr = 0;
        } else {
          final long memoized = memoizedPcr(fieldPcr, "__array__");
          if (memoized >= 0) {
            arrayPcr = memoized;
          } else if (pathSummaryWriter == null) {
            throw new IllegalStateException(
                "array path under (" + fieldPcr + ") was not pre-resolved for this build chunk");
          } else {
            arrayPcr = pathSummaryWriter.getArrayChildPathNodeKey(fieldPcr);
            rememberPcr(fieldPcr, "__array__", arrayPcr);
          }
        }
        containerKey = assertMinted(sink.createObjectNamedArrayNode(parentKey, leftSibKey, arrayPcr, name));
        contentPcr = arrayPcr;
      }
      pushLevel(isObject
          ? LEVEL_OBJECT
          : LEVEL_ARRAY, containerKey, contentPcr);
      return;
    }

    // Unnamed structural under document or an array.
    if (isObject) {
      // Plain OBJECT is TRANSPARENT for path anchoring: its fields resolve under the parent
      // context, exactly like the cursor's stop-kind walk that passes straight through OBJECT.
      contentPcr = levelContentPcr[depth];
      containerKey = assertMinted(sink.createObjectNode(parentKey, leftSibKey, contentPcr));
      pushLevel(LEVEL_OBJECT, containerKey, contentPcr);
    } else {
      // One __array__ path step regardless of insert position (user-ruled unification, applied to
      // the cursor path in the same change): an array's path class depends on where it sits, so
      // [[1],[2]] puts both inner arrays in ONE class.
      final long arrayPcr;
      if (!buildPathSummary) {
        arrayPcr = 0;
      } else {
        final long parentPcr = levelContentPcr[depth];
        final long memoized = memoizedPcr(parentPcr, "__array__");
        if (memoized >= 0) {
          arrayPcr = memoized;
        } else if (pathSummaryWriter == null) {
          throw new IllegalStateException(
              "array path under (" + parentPcr + ") was not pre-resolved for this build chunk");
        } else {
          arrayPcr = pathSummaryWriter.getPathNodeKey(parentPcr, ARRAY_PATH_QNM, NodeKind.ARRAY);
          rememberPcr(parentPcr, "__array__", arrayPcr);
        }
      }
      containerKey = assertMinted(sink.createArrayNode(parentKey, leftSibKey, arrayPcr));
      pushLevel(LEVEL_ARRAY, containerKey, arrayPcr);
    }
  }

  private void closeContainer() throws IOException {
    final long containerKey = levelContainerKey[depth];
    final long firstChild = levelFirstChild[depth];
    final long lastChild = levelLastChild[depth];
    final long childCount = levelChildCount[depth];
    depth--;
    recordChildInParent(containerKey);

    // The container's rightSib is knowable exactly now: every descendant has minted, so the next
    // mint IS the following sibling (or nothing follows — where in member mode a TOP-LEVEL
    // member's successor is the next chunk's first key, supplied from outside).
    final long rightSibKey = peekSiblingFollows()
        ? expectedNextKey
        : (memberMode && depth == 0
            ? trailingSiblingKey
            : NULL_KEY);

    if (childCount == 0 && rightSibKey == NULL_KEY) {
      return; // the sink already wrote NULL child pointers and NULL rightSib — nothing changed
    }
    sink.fixupContainer(containerKey, firstChild, lastChild, childCount, rightSibKey);
  }

  private void fixupDocument() {
    if (levelChildCount[0] == 0) {
      return;
    }
    sink.fixupDocument(levelContainerKey[0], levelFirstChild[0], levelLastChild[0], levelChildCount[0]);
  }

  // ==== leaves ================================================================================

  private void leafString(final byte[] utf8, final int utf8Length) throws IOException {
    final long pcr;
    final long nodeKey;
    if (levelKind[depth] == LEVEL_OBJECT) {
      final String name = takePendingName();
      pcr = resolveFieldPcr(name);
      final long rightSibKey = predictedLeafRightSibling();
      nodeKey = assertMinted(sink.createObjectNamedStringNode(levelContainerKey[depth], levelLastChild[depth],
          rightSibKey, pcr, name, utf8, utf8Length));
    } else {
      pcr = levelContentPcr[depth];
      final long rightSibKey = predictedLeafRightSibling();
      nodeKey = assertMinted(
          sink.createStringNode(levelContainerKey[depth], levelLastChild[depth], rightSibKey, utf8, utf8Length, pcr));
    }
    recordChildInParent(nodeKey);
    if (statsToSummary) {
      pathSummaryWriter.recordValue(pcr, utf8, utf8Length, nodeKey);
    } else if (pathStatsBatch != null) {
      pathStatsBatch.recordString(pcr, utf8, utf8Length, nodeKey);
    }
  }

  private void leafNumber() throws IOException {
    final int numberType = scanner.numberType();
    switch (numberType) {
      case BulkJsonScanner.NUMBER_TYPE_INT -> leafIntegralNumber(scanner.integralNumberValue(), true);
      case BulkJsonScanner.NUMBER_TYPE_LONG -> leafIntegralNumber(scanner.integralNumberValue(), false);
      case BulkJsonScanner.NUMBER_TYPE_FALLBACK -> leafNumber(scanner.number());
      default -> throw new IllegalStateException("unknown scanner number type " + numberType);
    }
  }

  private void leafIntegralNumber(final long value, final boolean intValue) throws IOException {
    // Snapshot value/type before predictedLeafRightSibling() peeks the next token into the scanner's
    // scratch lane. Current and scratch are separate, but keeping only locals makes that lifetime
    // boundary explicit and prevents a future scanner refactor from aliasing adjacent numbers.
    final long primitiveValue = value;
    final boolean primitiveIsInt = intValue;
    final long pcr;
    final long nodeKey;
    if (levelKind[depth] == LEVEL_OBJECT) {
      final String name = takePendingName();
      pcr = resolveFieldPcr(name);
      final long rightSibKey = predictedLeafRightSibling();
      nodeKey = assertMinted(primitiveIsInt
          ? sink.createObjectNamedNumberNode(levelContainerKey[depth], levelLastChild[depth], rightSibKey, pcr, name,
              (int) primitiveValue)
          : sink.createObjectNamedNumberNode(levelContainerKey[depth], levelLastChild[depth], rightSibKey, pcr, name,
              primitiveValue));
    } else {
      pcr = levelContentPcr[depth];
      final long rightSibKey = predictedLeafRightSibling();
      nodeKey = assertMinted(primitiveIsInt
          ? sink.createNumberNode(levelContainerKey[depth], levelLastChild[depth], rightSibKey, (int) primitiveValue,
              pcr)
          : sink.createNumberNode(levelContainerKey[depth], levelLastChild[depth], rightSibKey, primitiveValue, pcr));
    }
    recordChildInParent(nodeKey);
    if (statsToSummary) {
      pathSummaryWriter.recordValue(pcr, primitiveValue, nodeKey);
    } else if (pathStatsBatch != null) {
      pathStatsBatch.recordLong(pcr, primitiveValue, nodeKey);
    }
  }

  private void leafNumber(final Number value) throws IOException {
    final long pcr;
    final long nodeKey;
    if (levelKind[depth] == LEVEL_OBJECT) {
      final String name = takePendingName();
      pcr = resolveFieldPcr(name);
      final long rightSibKey = predictedLeafRightSibling();
      nodeKey = assertMinted(sink.createObjectNamedNumberNode(levelContainerKey[depth], levelLastChild[depth],
          rightSibKey, pcr, name, value));
    } else {
      pcr = levelContentPcr[depth];
      final long rightSibKey = predictedLeafRightSibling();
      nodeKey =
          assertMinted(sink.createNumberNode(levelContainerKey[depth], levelLastChild[depth], rightSibKey, value, pcr));
    }
    recordChildInParent(nodeKey);
    if (statsToSummary) {
      pathSummaryWriter.recordNumberValue(pcr, value, nodeKey);
    } else if (pathStatsBatch != null) {
      pathStatsBatch.recordNumber(pcr, value, nodeKey);
    }
  }

  private void leafBoolean(final boolean value) throws IOException {
    final long pcr;
    final long nodeKey;
    if (levelKind[depth] == LEVEL_OBJECT) {
      final String name = takePendingName();
      pcr = resolveFieldPcr(name);
      final long rightSibKey = predictedLeafRightSibling();
      nodeKey = assertMinted(sink.createObjectNamedBooleanNode(levelContainerKey[depth], levelLastChild[depth],
          rightSibKey, pcr, name, value));
    } else {
      pcr = levelContentPcr[depth];
      final long rightSibKey = predictedLeafRightSibling();
      nodeKey = assertMinted(
          sink.createBooleanNode(levelContainerKey[depth], levelLastChild[depth], rightSibKey, value, pcr));
    }
    recordChildInParent(nodeKey);
    if (statsToSummary) {
      pathSummaryWriter.recordBooleanValue(pcr, value, nodeKey);
    } else if (pathStatsBatch != null) {
      pathStatsBatch.recordBoolean(pcr, value, nodeKey);
    }
  }

  private void leafNull() throws IOException {
    final long pcr;
    final long nodeKey;
    if (levelKind[depth] == LEVEL_OBJECT) {
      final String name = takePendingName();
      pcr = resolveFieldPcr(name);
      final long rightSibKey = predictedLeafRightSibling();
      nodeKey = assertMinted(
          sink.createObjectNamedNullNode(levelContainerKey[depth], levelLastChild[depth], rightSibKey, pcr, name));
    } else {
      pcr = levelContentPcr[depth];
      final long rightSibKey = predictedLeafRightSibling();
      nodeKey = assertMinted(sink.createNullNode(levelContainerKey[depth], levelLastChild[depth], rightSibKey, pcr));
    }
    recordChildInParent(nodeKey);
    if (statsToSummary) {
      pathSummaryWriter.recordNullValue(pcr, nodeKey);
    } else if (pathStatsBatch != null) {
      pathStatsBatch.recordNull(pcr, nodeKey);
    }
  }

  // ==== shared mechanics ======================================================================

  /**
   * A leaf occupies exactly one key, so if the lookahead shows another sibling, that sibling's key is
   * the one AFTER this leaf's. The prediction is verified when the sibling actually mints.
   */
  private long predictedLeafRightSibling() throws IOException {
    if (peekSiblingFollows()) {
      return expectedNextKey + 1;
    }
    return memberMode && depth == 0
        ? trailingSiblingKey
        : NULL_KEY;
  }

  /** After the current value is consumed: does another sibling follow in the open container? */
  private boolean peekSiblingFollows() throws IOException {
    final int next = scanner.peek();
    return next != BulkJsonScanner.EVENT_END_OBJECT && next != BulkJsonScanner.EVENT_END_ARRAY
        && next != BulkJsonScanner.EVENT_END_DOCUMENT;
  }

  private long resolveFieldPcr(final String name) {
    if (!buildPathSummary) {
      return 0;
    }
    final long parentPcr = levelContentPcr[depth];
    final long memoized = memoizedPcr(parentPcr, name);
    if (memoized >= 0) {
      return memoized;
    }
    if (pathSummaryWriter == null) {
      // Worker mode runs on a PRE-FILLED memo; a miss means the counting pass and this build saw
      // different paths — refuse loudly rather than resolve out of order.
      throw new IllegalStateException(
          "path (" + parentPcr + ", '" + name + "') was not pre-resolved for this build chunk");
    }
    // Same literal path kind the cursor passes for EVERY named step (fields of all value types).
    final long pcr = pathSummaryWriter.getPathNodeKey(parentPcr, new QNm(name), NodeKind.OBJECT_NAMED_OBJECT);
    rememberPcr(parentPcr, name, pcr);
    return pcr;
  }

  /** Memo hit: count the repeat locally and return the PCR; miss: {@code -1}. */
  private long memoizedPcr(final long parentPcr, final String name) {
    final Object2LongOpenHashMap<String> byName = pcrMemo.get(parentPcr);
    if (byName == null) {
      return -1;
    }
    final long pcr = byName.getLong(name);
    if (pcr >= 0) {
      pendingPathReferences.addTo(pcr, 1);
    }
    return pcr;
  }

  private void rememberPcr(final long parentPcr, final String name, final long pcr) {
    Object2LongOpenHashMap<String> byName = pcrMemo.get(parentPcr);
    if (byName == null) {
      byName = new Object2LongOpenHashMap<>();
      byName.defaultReturnValue(-1);
      pcrMemo.put(parentPcr, byName);
    }
    byName.put(name, pcr);
  }

  private void flushPendingPathReferences() {
    if (pendingPathReferences.isEmpty()) {
      return;
    }
    if (pathSummaryWriter == null) {
      // Worker mode: Stage A's counting pass over the same chars already counted every
      // occurrence; the worker's tallies are a byproduct and must not double-count.
      pendingPathReferences.clear();
      return;
    }
    for (final var iterator = pendingPathReferences.long2IntEntrySet().fastIterator(); iterator.hasNext();) {
      final var entry = iterator.next();
      pathSummaryWriter.addReferences(entry.getLongKey(), entry.getIntValue());
    }
    pendingPathReferences.clear();
  }

  private String takePendingName() {
    final String name = pendingName;
    if (name == null) {
      throw new IllegalStateException("value inside an object without a preceding field name");
    }
    pendingName = null;
    return name;
  }

  private long assertMinted(final long mintedKey) {
    if (mintedKey != expectedNextKey) {
      throw new IllegalStateException(
          "bulk key prediction violated: expected " + expectedNextKey + " but the sink minted " + mintedKey);
    }
    expectedNextKey++;
    return mintedKey;
  }

  private void recordChildInParent(final long childKey) {
    if (levelFirstChild[depth] == NULL_KEY) {
      levelFirstChild[depth] = childKey;
    }
    levelLastChild[depth] = childKey;
    levelChildCount[depth]++;
    if (memberMode && depth == 0) {
      membersDriven++;
      lastMemberNodeCount = expectedNextKey - lastMemberStartKey();
      memberStartKey = expectedNextKey;
    }
    if (depth == 1) {
      // A child of the TOP-LEVEL container completed — for an array-of-records import this is
      // exactly one record. Account its node count and let the sink rotate the intermediate
      // epoch under the SAME predicate the cursor path uses (count threshold OR TIL page-work
      // boundary). Safe point by construction: no leaf serialization is ever deferred, and the
      // only pending state is open containers' close-fixups — the same shape the cursor path
      // flushes under all the time. Forward-pointer predictions written before the flush refer to
      // keys this load WILL mint next (asserted at the mint), which is sound for uncommitted
      // pages only this transaction can read.
      final long mutations = expectedNextKey - epochAccountedUpToKey;
      if (mutations > 0 && mutations <= Integer.MAX_VALUE) {
        // Deferred path-reference deltas must land BEFORE the rotation that may flush the summary
        // pages, so every epoch's durable image carries counts the delta scheme has caught up on.
        flushPendingPathReferences();
        sink.accountRecords((int) mutations);
        epochAccountedUpToKey = expectedNextKey;
      }
    }
  }

  private void pushLevel(final byte kind, final long containerKey, final long contentPcr) {
    depth++;
    if (depth == levelKind.length) {
      final int grown = levelKind.length << 1;
      levelKind = Arrays.copyOf(levelKind, grown);
      levelContainerKey = Arrays.copyOf(levelContainerKey, grown);
      levelContentPcr = Arrays.copyOf(levelContentPcr, grown);
      levelFirstChild = Arrays.copyOf(levelFirstChild, grown);
      levelLastChild = Arrays.copyOf(levelLastChild, grown);
      levelChildCount = Arrays.copyOf(levelChildCount, grown);
    }
    levelKind[depth] = kind;
    levelContainerKey[depth] = containerKey;
    levelContentPcr[depth] = contentPcr;
    levelFirstChild[depth] = NULL_KEY;
    levelLastChild[depth] = NULL_KEY;
    levelChildCount[depth] = 0;
  }
}
