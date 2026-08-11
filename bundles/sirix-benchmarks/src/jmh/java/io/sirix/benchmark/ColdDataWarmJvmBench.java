/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.benchmark;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.scan.SirixVectorizedExecutor;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * The regime between the two that get measured: JIT WARM, data COLD.
 *
 * <h2>Why neither existing harness answers it</h2>
 *
 * <p>
 * A JMH steady-state run has both warm — every record page is deserialized and resident, so a route
 * that avoids rebuilding pages saves nothing and its column read is pure addition. A fresh JVM has
 * both cold, so its number carries JIT warm-up and a first-touch of every page. Neither is the
 * shape a long-running server actually meets, which is a hot process reaching data that has fallen
 * out of cache.
 *
 * <p>
 * Separating them needs the eviction to happen INSIDE the process: a second JVM would hand back a
 * cold JIT along with the cold data, which is the measurement that already exists.
 *
 * <h2>Protocol</h2>
 *
 * <ol>
 * <li>Run the query {@code warmups} times, each with a DIFFERENT literal — the executor memoizes by
 * (source path, predicate), so repeating one query measures a hash map. Different literals compile
 * the same code and touch the same columns while leaving the memo cold.</li>
 * <li>Close the session and store, which drops SirixDB's own page and region caches.</li>
 * <li>Evict the OS page cache by reading unrelated files until more bytes have passed through it
 * than it can hold. {@code POSIX_FADV_DONTNEED} is not reachable from Java and {@code drop_caches}
 * needs root, so cache pressure is what is left.</li>
 * <li>Reopen and run the real query ONCE.</li>
 * </ol>
 *
 * <p>
 * Whether step 3 worked is not assumed: SirixDB reports bytes served from the device versus from
 * cache, and a run whose device bytes stayed near zero did not measure cold data and says so.
 *
 * <pre>
 *   java ... ColdDataWarmJvmBench &lt;store&gt; &lt;db&gt; &lt;warmups&gt; &lt;evictGiB&gt; '&lt;query with %s&gt;' &lt;literal&gt;
 * </pre>
 */
public final class ColdDataWarmJvmBench {

  private static final QNm DOC_VAR = new QNm("doc");
  private static final String RESOURCE = "movies";

  /** Literals used to warm the JIT without warming the result memo. */
  private static final String[] WARM_LITERALS =
      {"Silent", "Comedy", "Short", "Western", "Musical", "Romance", "Crime", "Family"};

  private ColdDataWarmJvmBench() {}

  public static void main(final String... args) throws Exception {
    if (args.length < 6) {
      System.err.println(
          "Usage: ColdDataWarmJvmBench <store> <db> <warmups> <evictGiB> " + "<queryTemplate with %s> <literal>");
      System.exit(2);
    }
    final Path store = Paths.get(args[0]);
    final String db = args[1];
    final int warmups = Integer.parseInt(args[2]);
    final int evictGiB = Integer.parseInt(args[3]);
    final String template = args[4];
    final String literal = args[5];

    for (int i = 0; i < warmups; i++) {
      final String lit = WARM_LITERALS[i % WARM_LITERALS.length];
      final long t0 = System.nanoTime();
      final String out = runOnce(store, db, String.format(template, lit));
      System.out.printf("  warmup %d (%s): %8.1f ms  %s%n", i, lit, (System.nanoTime() - t0) / 1e6, out.strip());
    }

    // SirixDB's buffer manager is GLOBAL — one instance shared by every database and resource, not
    // one per session — so closing the store above leaves every page and region exactly where it
    // was. A first attempt at this measurement missed that and reported 57 ms for a scan of 1.5 GB
    // of supposedly cold data, which is the giveaway that it never left the cache.
    Databases.clearGlobalCaches();

    if (evictGiB > 0) {
      final long t0 = System.nanoTime();
      final long read = evictPageCache(store, db, evictGiB * 1024L * 1024L * 1024L);
      System.out.printf("  evicted: read %.1f GiB of unrelated data in %.1f s%n", read / (double) (1 << 30),
          (System.nanoTime() - t0) / 1e9);
    }

    System.gc();
    final long t0 = System.nanoTime();
    final String out = runOnce(store, db, String.format(template, literal));
    System.out.printf("RESULT warm-jvm cold-data: %.1f ms  result=%s%n", (System.nanoTime() - t0) / 1e6, out.strip());
  }

  /**
   * Open, query, close. The close is load-bearing: SirixDB's page and region caches live in the
   * resource session, so keeping it open across the eviction would leave the data warm in the one
   * cache this is trying to empty.
   */
  private static String runOnce(final Path store, final String db, final String query) throws Exception {
    try (final BasicJsonDBStore s = BasicJsonDBStore.newBuilder().location(store).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(s);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(s)) {
      final JsonDBCollection coll = (JsonDBCollection) s.lookup(db);
      try (final JsonResourceSession session = coll.getDatabase().beginResourceSession(RESOURCE)) {
        final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session,
            session.getMostRecentRevisionNumber(), Runtime.getRuntime().availableProcessors());
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          ctx.bind(DOC_VAR, (Sequence) coll.getDocument());
          final ByteArrayOutputStream sink = new ByteArrayOutputStream();
          try (final PrintStream ps = new PrintStream(sink)) {
            new Query(chain, "declare variable $doc external; " + query).serialize(ctx, ps);
          }
          return sink.toString();
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
        }
      }
    }
  }

  /**
   * Push the resource out of the OS page cache by reading enough OTHER data through it.
   *
   * <p>
   * Everything in the store except the database under test, largest first, cycled until the target is
   * reached. Reading into a direct buffer that is thrown away each time keeps this from turning into
   * a heap benchmark of its own.
   *
   * @return bytes actually read
   */
  private static long evictPageCache(final Path store, final String db, final long targetBytes) throws Exception {
    final List<Path> victims = new ArrayList<>();
    final Path exclude = store.resolve(db);
    try (final var walk = Files.walk(store)) {
      walk.filter(Files::isRegularFile).filter(p -> !p.startsWith(exclude)).forEach(victims::add);
    }
    victims.sort((a, b) -> Long.compare(size(b), size(a)));
    if (victims.isEmpty()) {
      return 0L;
    }
    final ByteBuffer buf = ByteBuffer.allocateDirect(8 << 20);
    long total = 0;
    while (total < targetBytes) {
      final long before = total;
      for (final Path p : victims) {
        if (total >= targetBytes) {
          break;
        }
        try (final FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
          int n;
          while ((n = ch.read(buf.clear())) > 0) {
            total += n;
            if (total >= targetBytes) {
              break;
            }
          }
        } catch (final Exception ignored) {
          // A file that cannot be read is simply not a useful victim.
        }
      }
      if (total == before) {
        break; // nothing readable left; report what was achieved rather than spin
      }
    }
    return total;
  }

  private static long size(final Path p) {
    try {
      return Files.size(p);
    } catch (final Exception e) {
      return 0L;
    }
  }
}
