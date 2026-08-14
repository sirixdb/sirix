/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.benchmark;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.NodeStorageEngineReader;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.Constants;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Prices the cursor's page guard against the optimistic stamp that would replace it.
 *
 * <p>De-pinning the cursor — dropping {@code AbstractNodeReadOnlyTrx.currentPageGuard} and making
 * every accessor a snapshot/read/validate/retry against
 * {@link KeyValueLeafPage#readStamp()} — trades one cost for another, and which way the trade goes
 * is an empirical question, not an obvious one:
 *
 * <ul>
 *   <li>What it REMOVES is paid per <em>page switch</em>, not per hop. {@code moveToSingleton} has a
 *       within-page fast path that skips guard management entirely, so a walk that stays inside a
 *       1024-record page pays nothing today. Only a move that crosses a page boundary pays a guard
 *       release plus a {@code tryAcquireGuard} — and both are {@code synchronized} methods over an
 *       {@code AtomicInteger}, so the pair is two monitor acquisitions and two atomics.
 *   <li>What it ADDS is paid per <em>validated read</em>. A stamp proves nothing until it is
 *       validated, and the value it protects must not escape before then, so every accessor that
 *       reads through page memory — the four structural keys, the value, the name — needs its own
 *       validate, and the ones that miss have to redo the work on a re-resolved page.
 * </ul>
 *
 * <p>So the trade is (guard pair per page switch) against (stamp validate x accessors per node), and
 * it only pays when a workload switches pages often AND touches few fields per node. The stages
 * below measure each term separately rather than asserting a direction:
 *
 * <ul>
 *   <li>{@link #guardAcquireRelease} — the primitive being removed, on a resident page.
 *   <li>{@link #stampReadValidate} — the primitive being added, on the same page.
 *   <li>{@link #withinPageWalk} — {@code moveTo} over consecutive keys in ONE page: the fast path,
 *       zero guard traffic. The bind floor for this corpus.
 *   <li>{@link #pageSwitchWalk} — the same number of {@code moveTo} calls over the same slots, but
 *       each hop lands on a different page, so every hop pays the guard pair AND the page
 *       re-resolution. The delta over {@link #withinPageWalk} is the whole page-switch cost, of
 *       which the guard is only the {@link #guardAcquireRelease} share — read it as the ceiling on
 *       what de-pinning could possibly recover per hop.
 * </ul>
 *
 * <p>Self-contained: the corpus is shredded into a temp database in {@link #setUp}, so this runs
 * anywhere without a prebuilt store. Override with {@code -Dsirix.bench.corpus=/path/to.json}.
 *
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh -Pjmh.includes=CursorGuardCostBenchmark
 * </pre>
 *
 * <h2>Measured outcome</h2>
 *
 * <p>movies.json, 540,665 records over 527 record pages, 3 warmup + 5 measurement iterations x 2 s,
 * 1 fork, JDK 25. One run:
 *
 * <pre>
 *   withinPageWalk        25.9 +- 1.6  ns/hop   the bind floor, zero guard traffic
 *   pageSwitchWalk       247.9 +- 31.5 ns/hop   + guard pair + page re-resolution, EVERY hop
 *   guardAcquireRelease   31.6 +- 1.4  ns/op    the primitive de-pinning removes
 *   stampReadValidate      4.4 +- 0.2  ns/op    the primitive de-pinning adds, per read
 * </pre>
 *
 * <p><b>The trade does not pay.</b> A page switch costs 222.0 ns over the fast path, and the guard
 * pair is 31.6 ns of it — 14 % of the premium, 13 % of the hop. Removing that 31.6 ns per page
 * switch while adding 4.4 ns per validated read breaks even at 7.2 validated reads per page switch.
 * A cursor cannot get near that ratio: a record page holds 1024 records, so a walk with any locality
 * pays the guard once per ~1024 hops and would pay a validate on every accessor of every node. At
 * one validate per node that is 1024 x 4.4 = 4506 ns added against 31.6 ns removed, a ~140x loss.
 * Even in the shape most favourable to de-pinning — {@link #pageSwitchWalk}, a page switch on every
 * single hop — a node touching four structural keys and a value pays 5 x 4.4 = 22 ns to save 31.6,
 * netting 9.6 ns of 247.9, under 4 %. That is the ceiling, and only for a cursor with no locality
 * whatsoever.
 *
 * <p>What the numbers DO indict is the guard's implementation, not its existence: 31.6 ns for an
 * UNCONTENDED acquire/release pair is enormous. {@code tryAcquireGuard} is a {@code synchronized}
 * method delegating to {@code acquireGuard}, itself {@code synchronized} (a nested monitor entry),
 * each around a volatile flags read and an {@code AtomicInteger}; {@code releaseGuard} is a third
 * monitor plus an atomic decrement plus another volatile read. Folding count and flags into one word
 * behind a CAS would recover most of that on this path AND on
 * {@code NodeStorageEngineReader.getRecordPage}, which pays the same pair per page resolution —
 * without giving up pinning or putting a retry loop under every accessor.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {"-Xmx4g", "--add-modules", "jdk.incubator.vector",
    "--enable-native-access=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens", "java.base/java.nio=ALL-UNNAMED"})
@State(Scope.Benchmark)
public class CursorGuardCostBenchmark {

  /** Records per {@link KeyValueLeafPage}; a key and key+PAGE_SPAN are always on different pages. */
  private static final int PAGE_SPAN = 1 << Constants.NDP_NODE_COUNT_EXPONENT;

  /** Hops per walk invocation. Fixed so both walks do identical work at different localities. */
  private static final int HOPS = 4096;

  private Path dbPath;
  private Database<JsonResourceSession> database;
  private JsonResourceSession session;
  private JsonNodeReadOnlyTrx rtx;

  /** A resident page, guarded for the whole trial so the micro stages cannot race an eviction. */
  private KeyValueLeafPage page;

  /** Keys that all live in ONE page: consecutive slots of {@code basePage}. */
  private long[] samePageKeys;

  /** Keys that change page on EVERY hop, over the same slot numbers as {@link #samePageKeys}. */
  private long[] crossPageKeys;

  /** Repo-relative location of the default corpus; resolved against the working directory's parents. */
  private static final String DEFAULT_CORPUS = "bundles/sirix-core/src/test/resources/json/movies.json";

  /**
   * Locate the corpus without assuming where the forked JVM was started.
   *
   * <p>JMH forks run with the benchmark module as their working directory, not the repo root, so a
   * repo-relative default resolves to nothing. Walk up until the path exists.
   *
   * @return the corpus file
   */
  private static Path resolveCorpus() {
    final String override = System.getProperty("sirix.bench.corpus");
    if (override != null && !override.isBlank()) {
      return Paths.get(override);
    }
    for (Path dir = Paths.get("").toAbsolutePath(); dir != null; dir = dir.getParent()) {
      final Path candidate = dir.resolve(DEFAULT_CORPUS);
      if (Files.isReadable(candidate)) {
        return candidate;
      }
    }
    throw new IllegalStateException(
        "cannot find " + DEFAULT_CORPUS + " above " + Paths.get("").toAbsolutePath()
            + "; pass -Dsirix.bench.corpus=/path/to.json");
  }

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    final Path corpus = resolveCorpus();
    dbPath = Files.createTempDirectory("cursor-guard-cost");
    Databases.removeDatabase(dbPath);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    database = Databases.openJsonDatabase(dbPath);
    database.createResource(ResourceConfiguration.newBuilder("r").build());

    try (final JsonResourceSession manager = database.beginResourceSession("r");
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(trx, JsonShredder.createFileReader(corpus),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
      trx.commit();
    }

    session = database.beginResourceSession("r");
    rtx = session.beginNodeReadOnlyTrx();

    final long maxNodeKey = rtx.getMaxNodeKey();
    // Page 0 holds the document root and is only partly populated; start at page 1 so every key in
    // both sets resolves and the two walks do identical work.
    final int pageCount = (int) (maxNodeKey / PAGE_SPAN);
    final int usablePages = pageCount - 1;
    if (usablePages < 8) {
      throw new IllegalStateException(
          "corpus spans only " + pageCount + " record pages; need >= 9 to measure a page-switching walk");
    }

    // Both key sets visit the SAME slot sequence, so the per-slot bind work is identical. They
    // differ only in the page: samePageKeys stays on page 1 the whole way (the within-page fast
    // path), crossPageKeys advances the page on every single hop (guard pair + re-resolution).
    samePageKeys = new long[HOPS];
    crossPageKeys = new long[HOPS];
    for (int i = 0; i < HOPS; i++) {
      final int slot = i % PAGE_SPAN;
      final int pageIndex = 1 + (i % usablePages);
      samePageKeys[i] = (long) PAGE_SPAN + slot;
      crossPageKeys[i] = (long) pageIndex * PAGE_SPAN + slot;
    }

    // Every key must resolve, or the two walks would compare different amounts of work.
    int sameHits = 0;
    int crossHits = 0;
    for (int i = 0; i < HOPS; i++) {
      if (rtx.moveTo(samePageKeys[i])) {
        sameHits++;
      }
      if (rtx.moveTo(crossPageKeys[i])) {
        crossHits++;
      }
    }

    // Hold one guard on a resident page for the trial. The micro stages measure an ADDITIONAL
    // acquire/release pair on top of it, which is exactly what a page switch pays; keeping this one
    // held just stops the sweeper from reclaiming the page mid-measurement.
    final var reader = (NodeStorageEngineReader) rtx.getStorageEngineReader();
    final var location = reader.lookupSlotWithGuard(samePageKeys[0], IndexType.DOCUMENT, -1);
    if (location == null) {
      throw new IllegalStateException("no slot for the sampled key " + samePageKeys[0]);
    }
    page = location.page();

    System.out.printf("%n# maxNodeKey=%,d  pages=%,d  hops=%d  sameHits=%d  crossHits=%d%n",
                      maxNodeKey, pageCount, HOPS, sameHits, crossHits);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    rtx.close();
    session.close();
    database.close();
    Databases.removeDatabase(dbPath);
  }

  /**
   * The primitive de-pinning removes: one guard pair on a resident page.
   *
   * @return the acquire result, so nothing folds away
   */
  @Benchmark
  public boolean guardAcquireRelease() {
    final KeyValueLeafPage p = page;
    final boolean acquired = p.tryAcquireGuard();
    if (acquired) {
      p.releaseGuard();
    }
    return acquired;
  }

  /**
   * The primitive de-pinning adds, once per validated read.
   *
   * @return whether the stamp validated, so nothing folds away
   */
  @Benchmark
  public boolean stampReadValidate() {
    final KeyValueLeafPage p = page;
    final long stamp = p.readStamp();
    return p.validateStamp(stamp);
  }

  /**
   * The within-page fast path: {@code HOPS} binds with no guard traffic at all.
   *
   * @return the number of keys that resolved, so the loop cannot be eliminated
   */
  @Benchmark
  @OperationsPerInvocation(HOPS)
  public int withinPageWalk() {
    final JsonNodeReadOnlyTrx cursor = rtx;
    final long[] keys = samePageKeys;
    int visited = 0;
    for (int i = 0; i < HOPS; i++) {
      if (cursor.moveTo(keys[i])) {
        visited++;
      }
    }
    return visited;
  }

  /**
   * The same binds with a page switch on every hop: guard pair plus page re-resolution per hop.
   *
   * @return the number of keys that resolved, so the loop cannot be eliminated
   */
  @Benchmark
  @OperationsPerInvocation(HOPS)
  public int pageSwitchWalk() {
    final JsonNodeReadOnlyTrx cursor = rtx;
    final long[] keys = crossPageKeys;
    int visited = 0;
    for (int i = 0; i < HOPS; i++) {
      if (cursor.moveTo(keys[i])) {
        visited++;
      }
    }
    return visited;
  }
}
