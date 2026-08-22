package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.GlobalValueDictionary;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/**
 * Verdict probe for the maintenance-commit metadata loss: DECLINE or WRONG ANSWER?
 *
 * <p>
 * {@code ProjectionIndexChangeListener#applyIncremental} rewrites metadata slot 0 through the 6-arg
 * {@link io.sirix.index.projection.ProjectionIndexMetadata} constructor, which carries neither
 * {@code valueDictionaryHeaderKeys} nor {@code setValueRowCounts} — while the {@code columnKinds} it
 * copies forward still say {@code COLUMN_KIND_STRING_GLOBAL}. So after ONE ordinary update commit a
 * store describes global columns whose dictionary is no longer reachable. The severity depends
 * entirely on what the read routes then do, and that is what this measures rather than argues:
 *
 * <ul>
 * <li>if every route DECLINES and the generic pipeline answers, it is a performance defect;</li>
 * <li>if any route answers from ids it cannot resolve, it is a correctness defect on a shipping
 * default ({@code sirix.projection.globalDict=auto}) and jumps the queue.</li>
 * </ul>
 *
 * <p>
 * The oracle is the generic pipeline, recomputed AFTER the update as well as before — the update
 * changes the data, so the pre-update answer is not a valid expectation for the post-update query.
 * The serving counters are read on both sides because they are the only thing that distinguishes a
 * route that ran from one that silently fell back; equal answers alone cannot, since the generic
 * pipeline is correct either way. The corpus and query shapes are lifted from
 * {@link GlobalValueDictionaryServingTest}, which already establishes that all three are SERVED on a
 * freshly built store — that is the control this probe needs.
 */
public final class GlobalDictMaintenanceVerdictTest extends AbstractJsonTest {

  private static final String GLOBAL_DICT_PROPERTY = "sirix.projection.globalDict";

  /** Enough rows and enough distinct values that AUTO's minimum-entries floor is cleared. */
  private static final int ROWS = 12_000;

  private static String corpus() {
    final StringBuilder sb = new StringBuilder(ROWS * 70).append('[');
    for (int i = 0; i < ROWS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"kind\":\"")
        .append(i % 3 == 0
            ? "commit"
            : i % 3 == 1
                ? "identity"
                : "account")
        .append("\",\"did\":\"did:plc:")
        .append(i)
        .append("\",\"n\":")
        .append(i % 977)
        .append('}');
    }
    return sb.append(']').toString();
  }

  private static final String TOP_K = """
        subsequence(
          for $e in jn:doc('json-path1','serving.jn')[]
          where $e.kind eq 'commit'
          let $k := $e.did
          group by $k
          let $first := min($e.n)
          order by $first
          return {"did": $k, "first": $first}, 1, 3)
      """;

  private static final String PER_GROUP_DISTINCT = """
        for $e in jn:doc('json-path1','serving.jn')[]
        where $e.n ge 0
        let $k := $e.kind
        group by $k
        let $c := count($e)
        let $u := count(distinct-values($e.did))
        order by $c descending
        return {"kind": $k, "count": $c, "users": $u}
      """;

  private static final String EQUALITY =
      "count(for $e in jn:doc('json-path1','serving.jn')[] where $e.did eq \"did:plc:4711\" return $e)";

  private static final String EQUALITY_MISS =
      "count(for $e in jn:doc('json-path1','serving.jn')[] where $e.did eq \"did:plc:nope\" return $e)";

  @BeforeEach
  public void clearBefore() {
    System.clearProperty(GLOBAL_DICT_PROPERTY);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearAfter() {
    System.clearProperty(GLOBAL_DICT_PROPERTY);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  /** One shape's evidence: what the generic pipeline said, what the routes said, and whether they ran. */
  private record Evidence(String shape, String generic, String served, String auto, long counterDelta) {
    boolean agrees() {
      return generic.equals(served);
    }

    boolean autoAgrees() {
      return generic.equals(auto);
    }

    private static String verdict(final String generic, final String actual) {
      if (generic.equals(actual)) {
        return "AGREES";
      }
      return actual.startsWith("THREW")
          ? "THREW"
          : "WRONG";
    }

    @Override
    public String toString() {
      return String.format("%-18s explicit=%-6s autoWired=%-6s counterDelta=%d", shape, verdict(generic, served),
          verdict(generic, auto), counterDelta);
    }
  }

  @Test
  public void aMaintenanceCommitOnAGlobalDictStoreRemainsServed() throws IOException {
    Assertions.assertEquals(1, buildUnderDefault(),
        "AUTO must have elected exactly one global column (did) — without it this probe tests nothing");

    final Evidence[] before = probeAll("BEFORE");
    updateOneRecordAndCommit();
    // The catalog caches metadata per revision; drop it so the probe reads what the commit wrote.
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final Evidence[] after = probeAll("AFTER");

    System.out.println("=== global-dict maintenance verdict ===");
    for (final Evidence e : before) {
      System.out.println("  BEFORE " + e);
    }
    for (final Evidence e : after) {
      System.out.println("  AFTER  " + e);
    }

    // Control: on the freshly built store every shape must both agree AND have run. If a shape did
    // not run before the update, its counter says nothing about the update afterwards.
    for (final Evidence e : before) {
      Assertions.assertTrue(e.agrees(), "pre-update disagreement in " + e.shape() + " — the fixture is broken, "
          + "not the maintenance path");
      Assertions.assertTrue(e.counterDelta() > 0,
          "pre-update " + e.shape() + " was never served, so this probe cannot tell a post-update decline from a "
              + "route that was never taken");
    }

    // POST-FIX EXPECTATION. Before the anchors were carried forward, this loop asserted only that
    // the answers stayed correct, and they did — by DECLINING, with every counter at zero, and with
    // the equality shape throwing outright once a session was open. Now the maintenance commit must
    // leave the store fully usable: same answers as the generic pipeline AND the routes still
    // running. The counter is what separates the two, which is why correctness alone is not the
    // gate here.
    for (final Evidence e : after) {
      Assertions.assertTrue(e.agrees(),
          "explicit-executor arm did not answer correctly after the maintenance commit in " + e.shape()
              + "\n  generic: " + e.generic() + "\n  served : " + e.served());
      Assertions.assertTrue(e.autoAgrees(),
          "AUTO-WIRED (shipping default) arm did not answer correctly after the maintenance commit in " + e.shape()
              + " — this is the configuration a real user is in.\n  generic  : " + e.generic() + "\n  autoWired: "
              + e.auto());
      Assertions.assertTrue(e.counterDelta() > 0,
          e.shape() + " answered correctly but its projection route did not run after maintenance");
    }
  }

  @Test
  public void maintenanceInternsANewGlobalValueAndKeepsItsRouteServing() throws IOException {
    Assertions.assertEquals(1, buildUnderDefault());
    final String newValue = "did:plc:brand-new-maintained-value";
    updateGlobalValueAndCommit(newValue);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String query = "count(for $e in jn:doc('json-path1','serving.jn')[] where $e.did eq \""
        + newValue + "\" return $e)";
    final Evidence evidence = probe("AFTER", "EQ_NEW", query, Counter.PROJECTION_COUNTS);
    Assertions.assertTrue(evidence.agrees(), evidence.toString());
    Assertions.assertTrue(evidence.autoAgrees(), evidence.toString());
    Assertions.assertTrue(evidence.counterDelta() > 0, evidence.toString());

    try (final Database<JsonResourceSession> database =
        Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession("serving.jn");
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
      final ProjectionIndexMetadata metadata =
          ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), 0, 0L));
      Assertions.assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, metadata.columnKinds()[1]);
      final long headerKey = metadata.valueDictionaryHeaderKey(1);
      final int id = GlobalValueDictionary.probe(headerKey, newValue.getBytes(StandardCharsets.UTF_8),
          rtx.getStorageEngineReader());
      Assertions.assertTrue(id > 0);
      Assertions.assertEquals(newValue,
          GlobalValueDictionary.value(headerKey, id, rtx.getStorageEngineReader()));
    }
  }

  /**
   * The shape that once produced a THROW, isolated so the observation is either reproduced or
   * retired rather than left as folklore.
   *
   * <p>
   * The first run of this probe reported {@code bit:BIDY0300: column 1 is STRING_GLOBAL, but the EQ
   * predicate still carries a string literal — it was never resolved to a dictionary id}. It has not
   * reappeared under any later structure. The one thing that run did and the others do not is open a
   * resource session against the store BEFORE evaluating, which is what a long-lived reader does and
   * what may resolve the catalog at a revision the query then compiles against. If the throw is
   * real, it is a hard query failure on shipping defaults; if it is not, the verdict is a clean
   * decline. Either answer is worth having, so this asserts NOTHING and reports what happened.
   * </p>
   */
  @Test
  public void theOnceObservedThrowIsEitherReproducedOrRetired() throws IOException {
    buildUnderDefault();
    updateOneRecordAndCommit();
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    try (final BasicJsonDBStore store =
        BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      // Opened BEFORE the query compiles — the one thing the throwing run did differently.
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("serving.jn");
      Assertions.assertNotNull(session);
      for (final String q : new String[] {EQUALITY, EQUALITY_MISS, TOP_K, PER_GROUP_DISTINCT}) {
        String outcome;
        try {
          outcome = "answered: " + evaluateQuery(chain, ctx, q).trim();
        } catch (final RuntimeException thrown) {
          outcome = "THREW: " + rootMessage(thrown);
        }
        System.out.println("  [session-first, auto-wired] " + outcome);
      }
    }
  }

  /**
   * Can the maintenance path write a GLOBAL column at all? This decides whether the ticket's
   * proposed fix is safe.
   *
   * <p>
   * {@code ProjectionIndexRowGroupPage#internGlobal} throws when no
   * {@code GlobalValueDictionaryWriter} is attached, and the maintenance path in
   * {@code ProjectionIndexChangeListener#applyIncremental} builds its tail row group with
   * {@code new ProjectionIndexRowGroupPage(defKinds)} and never attaches one — the writers live only
   * inside a build. If that is right, maintenance cannot mint an id for a new or changed value in a
   * global column, and carrying {@code valueDictionaryHeaderKeys} forward would point readers at a
   * dictionary that does not describe what maintenance wrote. Reports rather than asserts.
   * </p>
   */
  @Test
  public void canMaintenanceWriteAGlobalColumnAtAll() throws IOException {
    buildUnderDefault();
    System.out.println("  [maintenance-write] change the GLOBAL column's own value:");
    System.out.println("    " + attemptWrite(wtx -> {
      Assertions.assertTrue(wtx.moveToDocumentRoot());
      Assertions.assertTrue(wtx.moveToFirstChild()); // array
      Assertions.assertTrue(wtx.moveToFirstChild()); // record 0
      Assertions.assertTrue(wtx.moveToFirstChild()); // "kind"
      Assertions.assertTrue(wtx.moveToRightSibling()); // "did" — the global column
      wtx.setStringValue("did:plc:brand-new-value-never-interned");
    }));

    buildUnderDefault();
    System.out.println("  [maintenance-write] change a NON-global column (the control):");
    System.out.println("    " + attemptWrite(wtx -> {
      Assertions.assertTrue(wtx.moveToDocumentRoot());
      Assertions.assertTrue(wtx.moveToFirstChild()); // array
      Assertions.assertTrue(wtx.moveToFirstChild()); // record 0
      Assertions.assertTrue(wtx.moveToFirstChild()); // "kind" — a per-leaf DICT column
      wtx.setStringValue("maintained");
    }));
  }

  /**
   * Is the ticket's proposed remedy SAFE? Simulates it at the data level.
   *
   * <p>
   * The proposal is to carry {@code valueDictionaryHeaderKeys} (and {@code setValueRowCounts})
   * forward through the 8-arg constructor, which restores the reader's fast path. But maintenance
   * has no {@code GlobalValueDictionaryWriter} — the writers exist only inside a build — so it
   * cannot mint an id for a value the dictionary has never seen. If it nonetheless patches the row
   * in place, then re-pointing readers at the dictionary makes them decode a cell the dictionary
   * cannot explain, and today's honest decline becomes a silent WRONG ANSWER.
   * </p>
   *
   * <p>
   * So: change the global column to a never-interned value, commit, write the ORIGINAL anchors back
   * over slot 0 (exactly what the fix would leave behind), and ask the fast path about both the new
   * value and a surviving old one, with the generic pipeline as the oracle. Reports; asserting a
   * verdict here would prejudge the design decision this is meant to inform.
   * </p>
   */
  @Test
  public void isTheProposedRemedySafeForARowMaintenanceRewrote() throws IOException {
    buildUnderDefault();
    final long[] originalAnchors = readAnchors();
    System.out.println("  [remedy-probe] anchors as built: " + java.util.Arrays.toString(originalAnchors));

    final String newValue = "did:plc:never-interned-by-any-build";
    attemptWrite(wtx -> {
      Assertions.assertTrue(wtx.moveToDocumentRoot());
      Assertions.assertTrue(wtx.moveToFirstChild());
      Assertions.assertTrue(wtx.moveToFirstChild());
      Assertions.assertTrue(wtx.moveToFirstChild());   // "kind"
      Assertions.assertTrue(wtx.moveToRightSibling()); // "did" — the global column
      wtx.setStringValue(newValue);
    });

    restoreAnchors(originalAnchors);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    System.out.println("  [remedy-probe] anchors restored: " + describeDictionariesFresh());

    for (final String[] shape : new String[][] {
        {"the value maintenance wrote", "count(for $e in jn:doc('json-path1','serving.jn')[] where $e.did eq \""
            + newValue + "\" return $e)"},
        {"a value it did not touch",
            "count(for $e in jn:doc('json-path1','serving.jn')[] where $e.did eq \"did:plc:4711\" return $e)"}}) {
      final String generic = evaluateIsolated(shape[1], Arm.GENERIC).trim();
      final String fast = evaluateIsolated(shape[1], Arm.AUTO_WIRED).trim();
      System.out.printf("  [remedy-probe] %-28s generic=%-24s fast=%-24s %s%n", shape[0], generic, fast,
          generic.equals(fast)
              ? "AGREE"
              : "*** DISAGREE ***");
    }
  }

  /** The persisted dictionary anchors, as built. */
  private long[] readAnchors() {
    try (final Database<JsonResourceSession> database =
        Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession("serving.jn");
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
      final ProjectionIndexMetadata meta =
          ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), 0, 0L));
      final long[] anchors = new long[meta.columnKinds().length];
      for (int c = 0; c < anchors.length; c++) {
        anchors[c] = meta.valueDictionaryHeaderKey(c);
      }
      return anchors;
    }
  }

  /** Write slot 0 back with the anchors present — the state the proposed fix would produce. */
  private void restoreAnchors(final long[] anchors) {
    try (final Database<JsonResourceSession> database =
        Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession("serving.jn");
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexMetadata current = ProjectionIndexMetadata.parse(
          ProjectionIndexHOTStorage.readBlob(wtx.getStorageEngineReader(), 0, 0L));
      final ProjectionIndexMetadata patched = new ProjectionIndexMetadata(current.rootPath(), current.fieldPaths(),
          current.fieldNames(), current.columnKinds(), current.rowGroupCount(), current.buildRevision(),
          current.setValueRowCounts(), anchors);
      new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0).putBlob(0, patched.serialize());
      wtx.commit();
    }
  }

  private String describeDictionariesFresh() {
    try (final Database<JsonResourceSession> database =
        Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession("serving.jn")) {
      return describeDictionaries(session);
    }
  }

  /** What a write does to the store. */
  @FunctionalInterface
  private interface Write {
    void apply(JsonNodeTrx wtx);
  }

  /** Run {@code write} and commit, reporting the outcome instead of propagating it. */
  private String attemptWrite(final Write write) {
    try (final Database<JsonResourceSession> database =
        Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession("serving.jn")) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        write.apply(wtx);
        wtx.commit();
      }
      return "committed OK — metadata now: " + describeDictionaries(session);
    } catch (final RuntimeException thrown) {
      return "THREW: " + rootMessage(thrown);
    }
  }

  /** Build the corpus + index under the DEFAULT mode; returns how many columns went global. */
  private int buildUnderDefault() throws IOException {
    JsonTestHelper.deleteEverything();
    query("jn:store('json-path1','serving.jn','" + corpus() + "')");
    query("""
          let $doc := jn:doc('json-path1','serving.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/kind', '/[]/did', '/[]/n'), ('string', 'string', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    final int global = ProjectionIndexBuilder.globalDictionaryColumnsBuilt();
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    return global;
  }

  /**
   * One ordinary update commit — the whole trigger. Nothing here is projection-aware; that is the
   * point, because any writer touching a maintained resource takes this path. Reports whether the
   * dictionary anchors survived it, which is the premise the severity question rests on.
   */
  private void updateOneRecordAndCommit() {
    try (final Database<JsonResourceSession> database =
        Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession("serving.jn")) {
      System.out.println("  metadata BEFORE commit: " + describeDictionaries(session));
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        Assertions.assertTrue(wtx.moveToDocumentRoot());
        Assertions.assertTrue(wtx.moveToFirstChild()); // top-level ARRAY
        Assertions.assertTrue(wtx.moveToFirstChild()); // record 0
        Assertions.assertTrue(wtx.moveToFirstChild()); // first field — "kind"
        wtx.setStringValue("maintained");
        wtx.commit();
      }
      System.out.println("  metadata AFTER  commit: " + describeDictionaries(session));
    }
  }

  private void updateGlobalValueAndCommit(final String value) {
    try (final Database<JsonResourceSession> database =
        Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession("serving.jn");
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      Assertions.assertTrue(wtx.moveToDocumentRoot());
      Assertions.assertTrue(wtx.moveToFirstChild());
      Assertions.assertTrue(wtx.moveToFirstChild());
      Assertions.assertTrue(wtx.moveToFirstChild());
      Assertions.assertTrue(wtx.moveToRightSibling());
      wtx.setStringValue(value);
      wtx.commit();
    }
  }

  /** The persisted metadata's view of its own dictionaries, at the session's newest revision. */
  private static String describeDictionaries(final JsonResourceSession session) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
      final ProjectionIndexMetadata meta =
          ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), 0, 0L));
      if (meta == null) {
        return "no metadata";
      }
      final StringBuilder sb = new StringBuilder(128);
      // columnKinds too: the store's own kind array and the column's encoded kind were measured
      // disagreeing (array says 2 = STRING_DICT, the data says 5 = STRING_GLOBAL), and this says
      // whether the metadata blob is the one carrying the wrong value or whether it still reads 5
      // and the disagreement is introduced below it.
      sb.append("columnKinds=").append(java.util.Arrays.toString(meta.columnKinds())).append(' ');
      sb.append("hasValueDictionaries=").append(meta.hasValueDictionaries()).append(" headerKeys=[");
      for (int col = 0; col < 3; col++) {
        if (col > 0) {
          sb.append(',');
        }
        sb.append(meta.valueDictionaryHeaderKey(col));
      }
      return sb.append(']').toString();
    } catch (final RuntimeException unreadable) {
      return "unreadable: " + unreadable;
    }
  }

  /**
   * Print the caller chain that reached a refusal, filtered to our own frames.
   *
   * <p>
   * Reachability attribution, not decoration: the open question is WHICH pipeline lands in the
   * predicate-count route when {@code -Dsirix.query.autoVectorize=false} is set. If it is the
   * generic pipeline, then making that route decline would change what the differential oracle
   * itself does — and a route that declines into itself either regresses infinitely or answers
   * wrongly. If it is a third, auto-wired shortcut, the oracle is independent and the fix is safe.
   * </p>
   */
  private static void printCallerChain(final String label, final Throwable thrown) {
    Throwable root = thrown;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    System.out.println("  [chain] " + label + " root=" + root.getClass().getSimpleName());
    int shown = 0;
    for (final StackTraceElement frame : root.getStackTrace()) {
      final String cls = frame.getClassName();
      if (!cls.startsWith("io.sirix") && !cls.startsWith("io.brackit")) {
        continue;
      }
      System.out.println("  [chain]     " + cls + "#" + frame.getMethodName() + ":" + frame.getLineNumber());
      if (++shown >= 22) {
        System.out.println("  [chain]     ...");
        break;
      }
    }
  }

  /** The deepest message in the chain — the one naming the actual refusal. */
  private static String rootMessage(final Throwable thrown) {
    Throwable t = thrown;
    while (t.getCause() != null && t.getCause() != t) {
      t = t.getCause();
    }
    return t.getClass().getSimpleName() + ": " + t.getMessage();
  }

  private static String evaluateQuery(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryStr)
      throws IOException {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final PrintWriter printWriter = new PrintWriter(out)) {
      new Query(chain, queryStr).serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString();
    }
  }

  private Evidence[] probeAll(final String phase) throws IOException {
    return new Evidence[] {probe(phase, "TOP_K", TOP_K, Counter.NUMERIC_GROUP_BY),
        probe(phase, "DISTINCT", PER_GROUP_DISTINCT, Counter.GROUP_DISTINCT),
        probe(phase, "EQ_HIT", EQUALITY, Counter.PROJECTION_COUNTS),
        probe(phase, "EQ_MISS", EQUALITY_MISS, Counter.PROJECTION_COUNTS)};
  }

  /** Which serving counter tells whether a given shape's route actually ran. */
  private enum Counter {
    NUMERIC_GROUP_BY {
      @Override
      long read() {
        return SirixVectorizedExecutor.numericGroupByServedCount();
      }
    },
    GROUP_DISTINCT {
      @Override
      long read() {
        return SirixVectorizedExecutor.groupDistinctServedCount();
      }
    },
    PROJECTION_COUNTS {
      @Override
      long read() {
        return SirixVectorizedExecutor.projectionCountsServed();
      }
    };

    abstract long read();
  }

  /**
   * Evaluate {@code q} with no executor (the oracle), then with one installed, and report both plus
   * the counter movement. The install is what wires the analytical routes in at all — without it
   * every query takes the generic pipeline and a flat counter would prove nothing.
   */
  private Evidence probe(final String phase, final String shape, final String q, final Counter counter)
      throws IOException {
    // The oracle must be the GENERIC pipeline, and merely not installing an executor does not give
    // one: the compile chain AUTO-WIRES the analytical routes from the store, so an "oracle" taken
    // that way is the very path under test. -Dsirix.query.autoVectorize=false is the only switch
    // that compiles every query generically (SirixCompileChain:112), and it is read at COMPILE
    // time, so it has to be set before the chain is built.
    // ORDER IS LOAD-BEARING, and finding that out cost a contradictory pair of runs. The AUTO-WIRED
    // arm goes FIRST because that is the real sequence — a writer commits, then somebody queries —
    // and because the handle/metadata caches are re-populated by whichever arm touches the store
    // first. Running it last let an earlier arm resolve the handle and cache its decision, which
    // masked a failure that appears when the auto-wired path is the first reader after the commit.
    final String auto = evaluateIsolated(q, Arm.AUTO_WIRED);
    final String generic = evaluateIsolated(q, Arm.GENERIC);
    final long start = counter.read();
    final String served = evaluateIsolated(q, Arm.EXPLICIT_EXECUTOR);
    final long delta = counter.read() - start;
    return new Evidence(phase + "/" + shape, generic, served, auto, delta);
  }

  /** Which pipeline an evaluation runs through. */
  private enum Arm {
    /** {@code -Dsirix.query.autoVectorize=false} — the oracle. */
    GENERIC,
    /** An executor installed by hand, as the serving suites do. */
    EXPLICIT_EXECUTOR,
    /** Stock configuration: the compile chain wires the routes in by itself. */
    AUTO_WIRED
  }

  /**
   * Evaluate {@code q} in its own store/context/chain, either through the generic pipeline or
   * through the analytical routes with an executor installed. A throw is recorded as the answer
   * rather than propagated: the question is what EVERY shape does, and stopping at the first
   * failure would hide whether the others decline cleanly.
   */
  private String evaluateIsolated(final String q, final Arm arm) throws IOException {
    if (arm == Arm.GENERIC) {
      System.setProperty("sirix.query.autoVectorize", "false");
    }
    try (final BasicJsonDBStore store =
        BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      SirixVectorizedExecutor executor = null;
      if (arm == Arm.EXPLICIT_EXECUTOR) {
        final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
        final JsonResourceSession session = collection.getDatabase().beginResourceSession("serving.jn");
        executor = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
      }
      try {
        return evaluateQuery(chain, ctx, q);
      } catch (final RuntimeException thrown) {
        printCallerChain(arm.name(), thrown);
        return "THREW: " + rootMessage(thrown);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (executor != null) {
          executor.close();
        }
      }
    } finally {
      if (arm == Arm.GENERIC) {
        System.clearProperty("sirix.query.autoVectorize");
      }
    }
  }
}
