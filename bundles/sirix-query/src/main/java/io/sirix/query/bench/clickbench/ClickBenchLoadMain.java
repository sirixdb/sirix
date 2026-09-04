package io.sirix.query.bench.clickbench;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.sun.management.HotSpotDiagnosticMXBean;
import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.page.BulkAdoptionDiagnostics;
import io.sirix.access.trx.node.HashType;
import io.sirix.cache.Allocators;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.io.SharedArenas;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.settings.VersioningType;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * ClickBench loader: shreds the {@code hits} records into a SirixDB JSON resource and reports the
 * two figures the ClickBench harness asks a system for — {@code Load time} in seconds and
 * {@code Data size} in bytes.
 *
 * <pre>
 *   ClickBenchLoadMain &lt;dbDir&gt; &lt;source&gt;
 * </pre>
 * 
 * where {@code source} is any spelling {@link ClickBenchSource} accepts — a JSON array file, a
 * JSON-lines file (both optionally gzipped), or {@code generate:<rows>[:seed]} for the offline
 * synthetic dataset.
 *
 * <p>
 * Tunables, all system properties, defaulted to the fast-ingest configuration the other scale
 * benchmarks in this package use:
 * <ul>
 * <li>{@code -Dsirix.offheap.bytes} (default 24 GiB) — page buffer pool;</li>
 * <li>{@code -Dsirix.autoCommit.nodes} (default 1048576) — logical auto-commit threshold in nodes.
 * The async-flush import mode retains that logical threshold while bounding each storage-only flush
 * epoch to {@link AfterCommitState#MAX_ASYNC_FLUSH_NODE_COUNT} modifications;</li>
 * <li>{@code -DstorageType} (default FILE_CHANNEL for this benchmark) — selects the preallocated,
 * single-append-owner path that can release immutable projection payloads before the root commit.
 * MEMORY_MAPPED remains available explicitly, but its legacy physical-tail semantics cannot safely
 * prewrite rollbackable pages and therefore retain those payloads until final commit;</li>
 * <li>{@code -Dclickbench.projection} (default true) — build the projection index over the columns
 * the 43 queries touch, as part of the load. Without it the benchmark measures the row path alone
 * and no column or group-by kernel is reachable, which is what every earlier run did;</li>
 * <li>{@code -Dclickbench.projection.incremental} (must remain true when projection loading is
 * enabled) — the ClickBench loader accepts only the one-pass load-time build; a false value is
 * rejected rather than silently walking the completed 100M-row resource a second time. The loader
 * also snapshots the HOT rebuild counters immediately before ingestion and fails a projection run
 * if either projection subtree-rebuild counter advances;</li>
 * <li>{@code -DbuildPathSummary} (defaults to {@code clickbench.projection}) — the projection
 * builder resolves its field paths through the summary, so the two cannot disagree;</li>
 * <li>{@code -DbuildPathStatistics} (default false), {@code -DhashType} (default NONE) — structures
 * the analytical queries do not need;</li>
 * <li>{@code -DstoreNodeHistory} (default false) — ClickBench reads one immutable snapshot, so its
 * load does not maintain the per-record temporal revision index. Set true only for a non-standard
 * run that exercises Sirix temporal axes;</li>
 * <li>{@code -Dclickbench.normalizeSource} (default true) — adapt the official JSONEachRow quoting
 * and timestamp separator in the parser stream, without a second materialised input file;</li>
 * <li>{@code -Dclickbench.parallelImport} (default true for files, false for generated streams) —
 * use the general chunked bulk importer while retaining the same one-pass projection contract;</li>
 * <li>{@code -Dclickbench.preflightOnly} (default false) — validate the first source record and
 * exit before allocating off-heap memory or opening the target store;</li>
 * <li>{@code -Dclickbench.validate} (default true) — post-load type check of the first record. A
 * matching source preflight always runs before the target store is opened.</li>
 * </ul>
 *
 * <p>
 * {@code Load time} includes building the projection, reported separately on its own line too:
 * DuckDB likewise builds its per-column structures while ingesting, so charging ours to query time
 * instead would be a comparison in our favour that the protocol does not allow.
 *
 * <p>
 * The validation is not ceremony: the official {@code hits.json.gz} is produced by ClickHouse,
 * whose {@code JSONEachRow} format quotes 64-bit integers by default. A quoted {@code UserID}
 * shreds as a string node, and then Q19/Q40/Q41 quietly return nothing while every other query
 * still looks plausible. The default source normaliser exposes those strings as exact signed-long
 * tokens; strict mode ({@code -Dclickbench.normalizeSource=false}) rejects them. Either way, the
 * preflight happens before {@link BasicJsonDBStore} can replace an existing target.
 */
public final class ClickBenchLoadMain {

  /** Columns whose exact 64-bit integer value the queries depend on. */
  private static final List<String> INT64_COLUMNS =
      List.of("WatchID", "UserID", "FUniqID", "ParamPrice", "RefererHash", "URLHash");

  /** Columns that must arrive as ISO-8601 strings for the date predicates to work. */
  private static final List<String> DATE_COLUMNS =
      List.of("EventDate", "EventTime", "ClientEventTime", "LocalEventTime");

  /**
   * Process-wide HOT counters at one instant. A ClickBench load subtracts two snapshots so an earlier
   * operation in the same JVM cannot contaminate its evidence.
   */
  record HotMutationCounters(long completeStructuralFrontierSplice, long structuralValidationFailure,
      long structuralPropagationPreflightFailure, long mutationTraversalRefused,
      long structuralValidationOversizeSkipped) {

    static HotMutationCounters capture() {
      return new HotMutationCounters(AbstractHOTIndexWriter.COMPLETE_STRUCTURAL_FRONTIER_SPLICE.get(),
          AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get(),
          AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get(),
          AbstractHOTIndexWriter.MUTATION_TRAVERSAL_REFUSED.get(),
          AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_OVERSIZE_SKIPPED.get());
    }

    HotMutationDeltas deltasTo(final HotMutationCounters current) {
      if (current == null) {
        throw new NullPointerException("current");
      }
      return new HotMutationDeltas(
          nonNegativeDelta("COMPLETE_STRUCTURAL_FRONTIER_SPLICE", completeStructuralFrontierSplice,
              current.completeStructuralFrontierSplice),
          nonNegativeDelta("STRUCTURAL_VALIDATION_FAILURE", structuralValidationFailure,
              current.structuralValidationFailure),
          nonNegativeDelta("STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE", structuralPropagationPreflightFailure,
              current.structuralPropagationPreflightFailure),
          nonNegativeDelta("MUTATION_TRAVERSAL_REFUSED", mutationTraversalRefused, current.mutationTraversalRefused),
          nonNegativeDelta("STRUCTURAL_VALIDATION_OVERSIZE_SKIPPED", structuralValidationOversizeSkipped,
              current.structuralValidationOversizeSkipped));
    }

    private static long nonNegativeDelta(final String name, final long baseline, final long current) {
      if (current < baseline) {
        throw new IllegalStateException(
            name + " decreased during ClickBench ingestion: before=" + baseline + " after=" + current);
      }
      return current - baseline;
    }
  }

  /**
   * Per-load incremental-mutation evidence derived after the persisted projection acceptance check.
   */
  record HotMutationDeltas(long completeStructuralFrontierSplice, long structuralValidationFailure,
      long structuralPropagationPreflightFailure, long mutationTraversalRefused,
      long structuralValidationOversizeSkipped) {

    String logLine() {
      return "# HOT_INCREMENTAL_DELTAS COMPLETE_STRUCTURAL_FRONTIER_SPLICE=" + completeStructuralFrontierSplice
          + " STRUCTURAL_VALIDATION_FAILURE=" + structuralValidationFailure
          + " STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE=" + structuralPropagationPreflightFailure
          + " MUTATION_TRAVERSAL_REFUSED=" + mutationTraversalRefused + " STRUCTURAL_VALIDATION_OVERSIZE_SKIPPED="
          + structuralValidationOversizeSkipped;
    }

    void requireHealthyIncrementalMutations() {
      if (structuralValidationFailure != 0L || structuralPropagationPreflightFailure != 0L
          || mutationTraversalRefused != 0L) {
        throw new IllegalStateException(
            "ClickBench incremental HOT contract violated: " + "STRUCTURAL_VALIDATION_FAILURE="
                + structuralValidationFailure + " STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE="
                + structuralPropagationPreflightFailure + " MUTATION_TRAVERSAL_REFUSED=" + mutationTraversalRefused
                + "; every secondary-index mutation must complete through the canonical incremental route");
      }
    }
  }

  /** Ingestion start time and the counter baseline captured immediately before it. */
  private record LoadMeasurement(long startNanos, HotMutationCounters hotMutationCountersBefore) {
  }

  private ClickBenchLoadMain() {
    throw new AssertionError("no instances");
  }

  /**
   * How many records the source will deliver, for the projection's global-dictionary election.
   *
   * <p>
   * Exact for {@code generate:<rows>}; otherwise {@code -Dclickbench.expectedRows}, which for the
   * official {@code hits.json.gz} is <b>99,997,497</b>. It is not derivable from a gzip stream and
   * the election happens far too early to count, so an unhinted 100M load elects URL, Referer and
   * Title, watches their dictionaries outgrow the heap, and abandons the whole projection partway
   * through — a correct load that measures the row path. With the hint those three decline up front
   * and every other column stays indexed.
   * </p>
   */
  private static long expectedRows(final String source) {
    if (source.startsWith("generate:")) {
      final String[] parts = source.split(":");
      if (parts.length >= 2) {
        return Long.parseLong(parts[1]);
      }
    }
    return Long.getLong("clickbench.expectedRows", -1L);
  }

  /** Resolve the effective HotSpot value, rather than trusting possibly duplicated JVM arguments. */
  private static long effectiveVmOption(final String option) {
    final HotSpotDiagnosticMXBean diagnosticBean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
    if (diagnosticBean == null) {
      throw new IllegalStateException("The HFT gate requires the HotSpot diagnostic MXBean");
    }
    return Long.parseLong(diagnosticBean.getVMOption(option).getValue());
  }

  /** Resolve and validate a positive integer system property before measurement starts. */
  private static int positiveIntProperty(final String name, final int defaultValue) {
    final int value = Integer.getInteger(name, defaultValue);
    if (value <= 0) {
      throw new IllegalArgumentException("-D" + name + " must be > 0, got " + value);
    }
    return value;
  }

  private static int appendWorkers() {
    final int defaultWorkers = Math.min(2, Math.max(1, Runtime.getRuntime().availableProcessors() / 4));
    return positiveIntProperty("sirix.asyncFlush.appendParallelism", defaultWorkers);
  }

  /** Guards the single emission of the storage counter line. */
  private static final AtomicBoolean STORAGE_COUNTERS_PRINTED = new AtomicBoolean();

  /**
   * One line of the storage engine's adoption/flush diagnostics — general counters, not benchmark
   * mechanisms. {@code unstaged>0} means this configuration cannot stage overflow carriers and every
   * leaf holding one stays pinned until commit; {@code kvlPinnedByPromotion>0} or
   * {@code kvlPinnedAfterCap>0} means the flush lane fell back to pinning;
   * {@code kvlRetriedNextEpoch} shows the deferral mechanism engaged at all.
   */
  private static void printStorageCounters() {
    if (!STORAGE_COUNTERS_PRINTED.compareAndSet(false, true)) {
      return;
    }
    System.out.printf(Locale.ROOT,
        "# storage: adoptedCarriersStaged=%d unstaged=%d oversized=%d refused=%d kvlPinnedByPromotion=%d"
            + " kvlRetriedNextEpoch=%d kvlPinnedAfterCap=%d%n",
        BulkAdoptionDiagnostics.carriersStaged(), BulkAdoptionDiagnostics.carriersUnstaged(),
        BulkAdoptionDiagnostics.carriersOversized(), BulkAdoptionDiagnostics.carriersRefused(),
        TransactionIntentLog.kvlPagesPinnedByPromotion(), TransactionIntentLog.kvlPagesRetriedNextEpoch(),
        BulkAdoptionDiagnostics.kvlPagesPinnedAfterDeferralCap());
    System.out.flush();
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: ClickBenchLoadMain <dbDir> <source>");
      System.err.println("  source: <file.json[.gz]> | <file.jsonl[.gz]> | generate:<rows>[:seed]");
      System.exit(2);
      return;
    }
    final Path dbDir = Path.of(args[0]);
    final String source = args[1];
    final boolean normalizeSource = Boolean.parseBoolean(System.getProperty("clickbench.normalizeSource", "true"));
    final boolean parallelImport = Boolean.parseBoolean(
        System.getProperty("clickbench.parallelImport", String.valueOf(!source.startsWith("generate:"))));
    final ClickBenchSource.SourceValidation sourceValidation =
        ClickBenchSource.validateFirstRecord(source, normalizeSource);
    System.out.printf("# source preflight OK: %d columns, normalizedLongs=%d, normalizedTimestamps=%d, mode=%s%n",
        sourceValidation.columns(), sourceValidation.normalizedLongValues(), sourceValidation.normalizedTimestamps(),
        normalizeSource
            ? "normalize"
            : "strict");
    if (Boolean.getBoolean("clickbench.preflightOnly")) {
      return;
    }

    // A killed or crashed load still reports the storage counters (SIGTERM and System.exit run the
    // hook; SIGKILL and -XX:+ExitOnOutOfMemoryError do not). Idempotent: the normal path prints the
    // same line at HFT_MEASURE_END.
    Runtime.getRuntime()
           .addShutdownHook(new Thread(ClickBenchLoadMain::printStorageCounters, "clickbench-storage-counters"));
    final long offheap = Long.parseLong(System.getProperty("sirix.offheap.bytes", String.valueOf(24L << 30)));
    final int autoCommit = Integer.parseInt(System.getProperty("sirix.autoCommit.nodes", "1048576"));
    final StorageType storageType =
        StorageType.fromString(System.getProperty("storageType", StorageType.FILE_CHANNEL.name()));
    final VersioningType versioningType =
        VersioningType.fromString(System.getProperty("versioningType", VersioningType.FULL.name()));
    final boolean projection = Boolean.parseBoolean(System.getProperty("clickbench.projection", "true"));
    // ClickBench loading is one-pass only: the projection is declared before the shred and
    // maintained by it. A complete post-load walk is not an acceptable fallback at 100M rows, so a
    // stale invocation that still disables the incremental route must fail before opening the store.
    //
    // "One-pass" here means ONE PASS OVER THE LOADED RESOURCE, and this rule forbids exactly one
    // thing: deriving the projection by walking a FINISHED resource a second time, which re-decodes
    // every record single-threaded against a file the page cache cannot hold (~3x the whole load's
    // wall time at 100M). It does NOT forbid a pre-pass over the INPUT.
    //
    // The distinction matters because the trie lane depends on the second: -Dsirix.import.prepassRunner
    // runs ClickBenchLoadPrepassHook against the freshly created EMPTY resource and commits the
    // rank-ordered value dictionaries BEFORE the shred begins, so the record-page encoder has ids to
    // store from row one. That reads the input twice and the resource once, which is the opposite
    // shape from what this refusal exists to prevent -- and the alternative for the lane is the
    // streaming dictionary at 1,650 B/entry against the rank pass's 61, which at 100M turns an 11 GB
    // saving into a 19 GB regression. Do not read this rule as forbidding that.
    final boolean incrementalProjection =
        Boolean.parseBoolean(System.getProperty("clickbench.projection.incremental", "true"));
    if (projection && !incrementalProjection) {
      throw new IllegalArgumentException("clickbench.projection.incremental=false is not supported: ClickBench "
          + "projections must be built during the shred; no post-load rebuild will be run");
    }
    // The projection builder resolves its field paths through the path summary, so the two options
    // are not independent: a corpus loaded without a summary can never have a projection added, and
    // discovering that only when jn:create-projection-index throws costs a whole re-ingest. Default
    // the summary to whatever the projection needs, and reject the contradiction outright.
    final boolean pathSummary =
        Boolean.parseBoolean(System.getProperty("buildPathSummary", String.valueOf(projection)));
    if (projection && !pathSummary) {
      System.err.println("buildPathSummary=false cannot be combined with clickbench.projection=true — the "
          + "projection builder resolves its paths through the summary. Set one of them.");
      System.exit(2);
      return;
    }
    final boolean pathStatistics = Boolean.parseBoolean(System.getProperty("buildPathStatistics", "false"));
    final HashType hashType = HashType.fromString(System.getProperty("hashType", "NONE"));
    // The 43 ClickBench queries address the finished resource's current revision only. Maintaining
    // RECORD_TO_REVISIONS for every inserted node therefore consumes CPU, allocations and disk but
    // cannot serve a benchmark query. Keep an explicit opt-in for temporal diagnostic runs.
    final boolean storeNodeHistory = Boolean.parseBoolean(System.getProperty("storeNodeHistory", "false"));
    if (parallelImport && hashType != HashType.NONE) {
      throw new IllegalArgumentException("clickbench.parallelImport=true requires -DhashType=NONE, got " + hashType
          + "; set -Dclickbench.parallelImport=false for a hashed import");
    }
    if (parallelImport && storeNodeHistory) {
      throw new IllegalArgumentException(
          "clickbench.parallelImport=true does not support node history; set -DstoreNodeHistory=false or "
              + "-Dclickbench.parallelImport=false");
    }

    // Configuration refusals above deliberately run first: a bad benchmark invocation must fail
    // before reserving a multi-gigabyte native pool or opening/replacing its target database.
    final var allocator = Allocators.getInstance();
    allocator.init(offheap);

    final long sourceExpectedRows = projection
        ? expectedRows(source)
        : -1L;
    final boolean hftTelemetry = Boolean.getBoolean("sirix.hft.telemetry");
    final HftRuntimeEvidence.Build hftBuild = hftTelemetry
        ? HftRuntimeEvidence.capture(ClickBenchLoadMain.class)
        : null;
    final int pinnedTrieScanBudget = positiveIntProperty("sirix.asyncFlush.pinnedTrieSpillScanBudget", 1_024);
    final int pinnedTrieBatchCapacity = positiveIntProperty("sirix.asyncFlush.pinnedTrieSpillBatchCapacity", 64);
    // Resolve these before HFT_MEASURE_START. Loading the management bean or SharedArenas class
    // inside the marked region would contaminate the very GC evidence the marker is meant to bind.
    final String hftConfiguration = hftTelemetry
        ? String.format(Locale.ROOT,
            "# HFT_CONFIG globalDict=%s autoCommitNodes=%d asyncFlushNodeCap=%d "
                + "arenaStrategy=%s maxNewSizeBytes=%d "
                + "initialHeapBytes=%d maxHeapBytes=%d g1RegionSizeBytes=%d gcLogging=%s safepointLogging=%s "
                + "storage=%s importer=%s projectionMode=%s expectedRows=%d pinnedTrieScanBudget=%d "
                + "pinnedTrieBatchCapacity=%d versioningType=%s appendWorkers=%d appendQueueCapacity=%d",
            System.getProperty("sirix.projection.globalDict", "auto").toLowerCase(Locale.ROOT), autoCommit,
            AfterCommitState.MAX_ASYNC_FLUSH_NODE_COUNT, SharedArenas.strategy().name().toLowerCase(Locale.ROOT),
            effectiveVmOption("MaxNewSize"), effectiveVmOption("InitialHeapSize"), effectiveVmOption("MaxHeapSize"),
            effectiveVmOption("G1HeapRegionSize"), hftBuild.gcLogging(), hftBuild.safepointLogging(), storageType,
            parallelImport
                ? "parallel-bulk"
                : "jackson",
            projection
                ? "incremental"
                : "disabled",
            sourceExpectedRows, pinnedTrieScanBudget, pinnedTrieBatchCapacity, versioningType, appendWorkers(),
            positiveIntProperty("sirix.asyncFlush.appendQueueCapacity", 1))
        : null;

    Files.createDirectories(dbDir);
    System.out.printf("# ClickBench load: db=%s source=%s%n", dbDir, source);
    System.out.printf(
        "# offheap=%d MB autoCommit=%d storage=%s pathSummary=%s pathStatistics=%s hash=%s nodeHistory=%s importer=%s%n",
        offheap / (1L << 20), autoCommit, storageType, pathSummary, pathStatistics, hashType, storeNodeHistory,
        parallelImport
            ? "parallel-bulk"
            : "jackson");

    final LoadMeasurement loadMeasurement;
    try (var store = BasicJsonDBStore.newBuilder()
                                     .location(dbDir)
                                     .storageType(storageType)
                                     .versioningType(versioningType)
                                     .numberOfNodesBeforeAutoCommit(autoCommit)
                                     .buildPathSummary(pathSummary)
                                     .buildPathStatistics(pathStatistics)
                                     .hashType(hashType)
                                     .storeNodeHistory(storeNodeHistory)
                                     .build()) {
      if (parallelImport && source.startsWith("generate:")) {
        try (Reader input = ClickBenchSource.open(source)) {
          loadMeasurement = loadParallel(store, input, projection, sourceExpectedRows, hftConfiguration, hftBuild);
        }
      } else if (parallelImport) {
        try (InputStream input = ClickBenchSource.openParallelInput(source, normalizeSource)) {
          loadMeasurement = loadParallel(store, input, projection, sourceExpectedRows, hftConfiguration, hftBuild);
        }
      } else {
        try (ClickBenchSource.JacksonSource jsonSource = ClickBenchSource.openJackson(source, normalizeSource)) {
          // Machine-readable measurement boundary for the fixed-heap HFT gate. Store/source opening
          // intentionally happens first so JVM/library startup and gzip setup cannot masquerade as
          // ingestion old-gen pressure. The end marker is emitted only after close + explicit sync.
          loadMeasurement = loadJackson(store, jsonSource, projection, sourceExpectedRows, hftConfiguration, hftBuild);
        }
      }
    }

    // The projection index is part of LOADING, the same way DuckDB builds its own per-column
    // structures while ingesting: a run without it measures the row path and can say nothing about
    // any column or group-by kernel. Reported on its own line as well, so the ingest cost and the
    // index cost stay separable, and switchable off for a row-path A/B.
    if (!projection) {
      System.out.println("# projection: DISABLED (-Dclickbench.projection=false) — nothing will be served");
    } else {
      // dictProbes is the intern-table retention witness: a one-pass load keeps its dictionary
      // tables in memory until finalize, so interning can never reach the persistent radix and this
      // must print 0. A non-zero figure means the per-value durable-read regime is back — the shape
      // measured at ~85% of load CPU before the retention fix.
      //
      // This banner is NOT a success claim for the projection itself (task #55): the one way a
      // one-pass build fails while the LOAD still succeeds — a global dictionary breaching its byte
      // cap abandons the projection — used to report only through a LOGGER.warn the shipped
      // root=ERROR logback discards. The abandonment now ALSO prints '[proj] PROJECTION ABANDONED'
      // on stderr, which the banner names so a guard knows what to scan for.
      System.out.printf(
          "# projection: columns=%d globalDictColumns=%d dictProbes=%d built DURING the shred (one pass);"
              + " an abandonment prints '[proj] PROJECTION ABANDONED' on stderr%n",
          ClickBenchProjection.PROJECTED_COLUMNS.size(), ProjectionIndexBuilder.globalDictionaryColumnsBuilt(),
          ProjectionIndexBuilder.persistentDictionaryProbesReported());
    }

    // ClickBench's own driver syncs inside the measured window so "load time" means "the data is on
    // disk"; the store's close() already flushed, this makes the page cache write-back explicit.
    sync();
    final long loadEnd = System.nanoTime();
    System.out.println("# HFT_MEASURE_END");
    printStorageCounters();
    System.out.flush();

    // Verification is deliberately outside the ingestion timing and HFT GC window: it performs two
    // cold catalogue/directory walks after close + sync. It still runs before any success metric is
    // reported and never repairs or post-builds an absent projection, so failure is closed.
    if (projection) {
      final ClickBenchProjectionAcceptance.Verification verified =
          ClickBenchProjectionAcceptance.verify(dbDir, sourceExpectedRows);
      System.out.printf(
          "# projection acceptance OK: definition=%d revision=%d buildRevision=%d columns=%d rowGroups=%d rows=%d"
              + " (two cold persisted reopens)%n",
          verified.definitionId(), verified.revision(), verified.buildRevision(), verified.columns(),
          verified.rowGroups(), verified.rows());
    }

    // The projection acceptance above proves the result is persisted and servable. These deltas
    // prove how it got there. A complete-frontier splice is a legitimate bounded incremental
    // operation. Any failed validation, failed propagation preflight, or refused mutation is not.
    final HotMutationDeltas hotMutationDeltas =
        loadMeasurement.hotMutationCountersBefore().deltasTo(HotMutationCounters.capture());
    System.out.println(hotMutationDeltas.logLine());
    System.out.flush();
    hotMutationDeltas.requireHealthyIncrementalMutations();

    final double loadSeconds = (loadEnd - loadMeasurement.startNanos()) / 1e9;
    final long bytes = directorySize(dbDir);
    System.out.printf("Load time: %.3f%n", loadSeconds);
    System.out.printf("Data size: %d%n", bytes);

    if (Boolean.parseBoolean(System.getProperty("clickbench.validate", "true"))) {
      validate(dbDir);
    }
    System.exit(0);
  }

  private static LoadMeasurement loadParallel(final BasicJsonDBStore store, final InputStream input,
      final boolean projection, final long expectedRows, final String hftConfiguration,
      final HftRuntimeEvidence.Build hftBuild) {
    final HotMutationCounters hotMutationCountersBefore = HotMutationCounters.capture();
    final long start = startMeasurement(hftConfiguration, hftBuild);
    if (projection) {
      store.createParallel(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, input,
          ClickBenchProjection.spec(expectedRows));
    } else {
      store.createParallel(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, input);
    }
    return new LoadMeasurement(start, hotMutationCountersBefore);
  }

  private static LoadMeasurement loadParallel(final BasicJsonDBStore store, final Reader input,
      final boolean projection, final long expectedRows, final String hftConfiguration,
      final HftRuntimeEvidence.Build hftBuild) {
    final HotMutationCounters hotMutationCountersBefore = HotMutationCounters.capture();
    final long start = startMeasurement(hftConfiguration, hftBuild);
    if (projection) {
      store.createParallel(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, input,
          ClickBenchProjection.spec(expectedRows));
    } else {
      store.createParallel(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, input);
    }
    return new LoadMeasurement(start, hotMutationCountersBefore);
  }

  private static LoadMeasurement loadJackson(final BasicJsonDBStore store, final ClickBenchSource.JacksonSource source,
      final boolean projection, final long expectedRows, final String hftConfiguration,
      final HftRuntimeEvidence.Build hftBuild) {
    final HotMutationCounters hotMutationCountersBefore = HotMutationCounters.capture();
    final long start = startMeasurement(hftConfiguration, hftBuild);
    if (projection) {
      store.create(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, source.parser(),
          ClickBenchProjection.spec(expectedRows), source.ldjson());
    } else {
      store.create(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, source.parser(), source.ldjson());
    }
    return new LoadMeasurement(start, hotMutationCountersBefore);
  }

  private static long startMeasurement(final String hftConfiguration, final HftRuntimeEvidence.Build hftBuild) {
    System.out.println("# HFT_MEASURE_START");
    if (hftConfiguration != null) {
      System.out.println("# HFT_BUILD gitSha=" + hftBuild.gitSha() + " artifactSha256=" + hftBuild.artifactSha256());
      System.out.println(hftConfiguration);
    }
    System.out.flush();
    return System.nanoTime();
  }

  /**
   * Reads the first record back and fails loudly if the encoding is not the one the queries assume.
   *
   * @param dbDir the loaded database directory
   */
  private static void validate(final Path dbDir) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String query = ClickBenchQueries.wrap(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE,
          "subsequence(for $h in $hits[] return $h, 1, 1)");
      final Sequence result = new Query(chain, query).execute(ctx);
      final StringWriter out = new StringWriter();
      try (PrintWriter pw = new PrintWriter(out)) {
        new StringSerializer(pw).serialize(result);
      }
      final String first = out.toString().trim();
      if (first.isEmpty()) {
        throw new IllegalStateException("validation failed: the resource holds no records");
      }
      final JsonElement parsed = JsonParser.parseString(first);
      if (!parsed.isJsonObject()) {
        throw new IllegalStateException("validation failed: first record is not an object: " + first);
      }
      final JsonObject record = parsed.getAsJsonObject();
      final List<String> problems = new ArrayList<>();
      for (final String column : ClickBenchSchema.COLUMNS) {
        if (!record.has(column)) {
          problems.add("missing column " + column);
        }
      }
      for (final String column : INT64_COLUMNS) {
        final JsonElement value = record.get(column);
        if (value != null && value.isJsonPrimitive() && !((JsonPrimitive) value).isNumber()) {
          problems.add(column + " is not a JSON number (" + value
              + ") — 64-bit ids must not be quoted, or Q19/Q40/Q41 silently return nothing");
        }
      }
      for (final String column : DATE_COLUMNS) {
        final JsonElement value = record.get(column);
        if (value != null && value.isJsonPrimitive() && !((JsonPrimitive) value).isString()) {
          problems.add(column + " is not an ISO-8601 string (" + value + ")");
        }
      }
      if (!problems.isEmpty()) {
        throw new IllegalStateException("ClickBench encoding validation failed:\n  " + String.join("\n  ", problems));
      }
      System.out.printf("# validation OK: %d columns, exact 64-bit ids, ISO-8601 dates%n", record.size());
    }
  }

  /** Sums every file below {@code dir}; ClickBench counts indexes and logs, i.e. everything. */
  static long directorySize(final Path dir) throws IOException {
    try (Stream<Path> paths = Files.walk(dir)) {
      return paths.filter(Files::isRegularFile).mapToLong(path -> {
        try {
          return Files.size(path);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      }).sum();
    }
  }

  /** Best-effort {@code sync(1)}; ignored where it is not available. */
  private static void sync() {
    try {
      new ProcessBuilder("sync").inheritIO().start().waitFor();
    } catch (final IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      System.out.println("# note: could not run sync(1): " + e.getMessage());
    }
  }
}
