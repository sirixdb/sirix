package io.sirix.query.bench;

import io.brackit.query.Query;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.service.json.shredder.JsonShredder;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * End-to-end write-path smoke for GraalVM native-image builds.
 *
 * <p>Exercises the full lifecycle that historically failed in a native image because
 * {@link io.sirix.io.memorymapped.MMStorage} maps each generation into an
 * {@code Arena.ofShared()} (closing such an arena requires
 * {@code -H:+SharedArenaSupport} at image build time):
 *
 * <ol>
 *   <li>create a database + resource and shred a small JSON document (commit, revision 1),</li>
 *   <li>close everything, reopen from disk, read the document back via a query,</li>
 *   <li>append a subtree in a node transaction and commit (revision 2 — grows the data
 *       file, forcing an mmap <em>remap</em>: new arena generation, old generation closed),</li>
 *   <li>reopen again and verify both the new revision and time-travel to revision 1.</li>
 * </ol>
 *
 * <p>Intentionally uses the {@link BasicJsonDBStore} defaults, which select
 * {@link StorageType#MEMORY_MAPPED} on 64-bit Linux — the configuration whose reader path
 * crashes in a native image without shared-arena support.
 *
 * <p>Usage: {@code NativeWriteSmokeMain [dbDir]} (defaults to a fresh temp directory).
 * Exits 0 and prints {@code NATIVE WRITE SMOKE: OK} on success, exits 1 on any failure.
 */
public final class NativeWriteSmokeMain {

  private static final String DB = "smoke-db";
  private static final String RESOURCE = "smoke.jn";
  private static final String REV1_JSON = "[{\"i\":1,\"name\":\"a\"},{\"i\":2,\"name\":\"b\"}]";
  private static final String REV2_ELEMENT = "{\"i\":3,\"name\":\"c\"}";

  private NativeWriteSmokeMain() {
  }

  public static void main(final String[] args) throws Exception {
    final long start = System.nanoTime();
    try {
      run(args);
      System.out.printf("NATIVE WRITE SMOKE: OK (%d ms)%n", (System.nanoTime() - start) / 1_000_000L);
    } catch (final Throwable t) {
      t.printStackTrace();
      System.out.println("NATIVE WRITE SMOKE: FAILED");
      System.exit(1);
    }
    // Daemon/cleaner threads must not keep the process alive.
    System.exit(0);
  }

  private static void run(final String[] args) throws Exception {
    final Path dbDir = args.length > 0 ? Path.of(args[0]) : Files.createTempDirectory("sirix-write-smoke");
    Files.createDirectories(dbDir);

    // Keep the off-heap budget small — this is a smoke, not a benchmark.
    Allocators.getInstance().init(2L << 30);

    System.out.println("# dbDir = " + dbDir);

    // ---- Phase A: create + shred + commit (revision 1), then read back in-process. ----
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create(DB, RESOURCE, REV1_JSON);
      System.out.println("PASS create+shred+commit (revision 1)");

      expect("2", query(chain, ctx, "count(jn:doc('" + DB + "','" + RESOURCE + "')[])"), "rev1 count (in-process)");
      final String doc = query(chain, ctx, "jn:doc('" + DB + "','" + RESOURCE + "')");
      expectContains(doc, "\"i\":2", "rev1 content (in-process)");
      System.out.println("PASS read-back in-process: " + doc);
    }

    // ---- Phase B: reopen from disk, append a subtree, commit revision 2. ----
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(dbDir).build()) {
      final JsonDBCollection coll = (JsonDBCollection) store.lookup(DB);
      if (coll == null) {
        throw new IllegalStateException("lookup of database '" + DB + "' after reopen returned null");
      }
      try (final JsonResourceSession session = coll.getDatabase().beginResourceSession(RESOURCE)) {
        final StorageType storageType = session.getResourceConfig().storageType;
        System.out.println("# storageType = " + storageType);
        if (System.getProperty("storageType") == null && storageType != StorageType.MEMORY_MAPPED) {
          throw new IllegalStateException(
              "expected MEMORY_MAPPED storage on this platform but got " + storageType
                  + " — the smoke would not exercise the shared-arena mmap path");
        }
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          if (!wtx.moveToFirstChild()) {
            throw new IllegalStateException("document root has no child (expected top-level array)");
          }
          wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(REV2_ELEMENT), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
        if (session.getMostRecentRevisionNumber() < 2) {
          throw new IllegalStateException(
              "expected at least 2 revisions after second commit, got " + session.getMostRecentRevisionNumber());
        }
      }
      System.out.println("PASS append+commit (revision 2)");
    }

    // ---- Phase C: reopen once more; verify revision 2 and time-travel to revision 1. ----
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      expect("3", query(chain, ctx, "count(jn:doc('" + DB + "','" + RESOURCE + "')[])"), "rev2 count (reopened)");
      final String rev2 = query(chain, ctx, "jn:doc('" + DB + "','" + RESOURCE + "')");
      expectContains(rev2, "\"i\":3", "rev2 content (reopened)");
      expect("2", query(chain, ctx, "count(jn:doc('" + DB + "','" + RESOURCE + "',1)[])"), "rev1 count (time-travel)");
      System.out.println("PASS reopen+read-back (rev2 + time-travel rev1): " + rev2);
    }
  }

  private static String query(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryStr) {
    final var sequence = new Query(chain, queryStr).evaluate(ctx);
    final var buf = IOUtils.createBuffer();
    try (final var serializer = new StringSerializer(buf)) {
      serializer.serialize(sequence);
    }
    return buf.toString();
  }

  private static void expect(final String expected, final String actual, final String what) {
    if (!expected.equals(actual)) {
      throw new AssertionError(what + ": expected <" + expected + "> but got <" + actual + ">");
    }
  }

  private static void expectContains(final String haystack, final String needle, final String what) {
    if (haystack == null || !haystack.contains(needle)) {
      throw new AssertionError(what + ": expected output containing <" + needle + "> but got <" + haystack + ">");
    }
  }
}
