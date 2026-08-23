package io.sirix.query;

import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * The column fill's segment-chain fetch splits its per-leaf work — descriptor lookup, page read,
 * integrity check — across contiguous leaf ranges once a store is large enough. At the 97k-leaf
 * scale that took a chain's serial phase from ~1.05 s to ~0.53 s; below the threshold it stays
 * serial, which is why no other suite here reaches the parallel arm at all.
 *
 * <p>
 * The threshold is property-driven precisely so this test can drive the parallel arm on a fixture
 * that builds in a second, and the assertions are on
 * {@link ProjectionColumnStore#parallelChainFetchCount()} rather than on the arithmetic — a fast
 * path that silently declines is the failure mode this codebase keeps meeting.
 */
public final class ParallelChainFetchTest extends AbstractJsonTest {

  private static final String MIN_LEAVES = "sirix.projection.chainFetchMinLeaves";
  private static final String RANGE_LEAVES = "sirix.projection.chainFetchRangeLeaves";

  /** Enough records that the resource spans several row-group leaves — one leaf cannot be split. */
  private static final int RECORDS = 4000;

  @BeforeEach
  public void clearProjectionStateBefore() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearProjectionStateAfter() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    System.clearProperty(MIN_LEAVES);
    System.clearProperty(RANGE_LEAVES);
  }

  @Test
  public void rangedChainFetchDecodesExactlyWhatTheSerialOneDoes() throws IOException {
    buildProjection();

    // Arm A: forced serial — the threshold is above the store's leaf count.
    System.setProperty(MIN_LEAVES, Integer.toString(Integer.MAX_VALUE));
    final long serialSplits = ProjectionColumnStore.parallelChainFetchCount();
    final Filled serial = fill();
    Assertions.assertEquals(serialSplits, ProjectionColumnStore.parallelChainFetchCount(),
        "the serial arm must not split a single chain");
    Assertions.assertTrue(serial.leaves() > 1,
        "the fixture must span several leaves or the split has nothing to prove — got " + serial.leaves());

    // Arm B: one worker per leaf, so every chain of this fixture takes the ranged path.
    System.setProperty(MIN_LEAVES, "1");
    System.setProperty(RANGE_LEAVES, "1");
    final long before = ProjectionColumnStore.parallelChainFetchCount();
    final Filled parallel = fill();
    Assertions.assertTrue(ProjectionColumnStore.parallelChainFetchCount() > before,
        "the ranged arm must actually engage — the threshold properties are the only thing gating it");

    Assertions.assertEquals(serial.leaves(), parallel.leaves(), "both arms must see the same leaf count");
    for (int i = 0; i < serial.leaves(); i++) {
      final ProjectionColumnStore.ColumnSlice a = serial.ages()[i];
      final ProjectionColumnStore.ColumnSlice b = parallel.ages()[i];
      Assertions.assertEquals(a.rowCount(), b.rowCount(), "long slice " + i + " row count");
      Assertions.assertArrayEquals(a.numericValues(), b.numericValues(), "long slice " + i + " values");
      Assertions.assertArrayEquals(a.presenceWords(), b.presenceWords(), "long slice " + i + " presence");

      final ProjectionColumnStore.ColumnSlice c = serial.depts()[i];
      final ProjectionColumnStore.ColumnSlice d = parallel.depts()[i];
      Assertions.assertEquals(c.rowCount(), d.rowCount(), "string slice " + i + " row count");
      Assertions.assertArrayEquals(c.stringDictIds(), d.stringDictIds(), "string slice " + i + " dict ids");
      Assertions.assertArrayEquals(c.dictOffsets(), d.dictOffsets(), "string slice " + i + " dict offsets");
      Assertions.assertTrue(dictBytesEqual(c, d), "string slice " + i + " dictionary bytes");
    }
  }

  /**
   * The dictionary is a flat run addressed by the offsets, and a RAW-mode dictionary's array IS the
   * whole segment — so only the addressed span is meaningful, not the array's length.
   */
  private static boolean dictBytesEqual(final ProjectionColumnStore.ColumnSlice a,
      final ProjectionColumnStore.ColumnSlice b) {
    if (a.dictBytes() == null || b.dictBytes() == null) {
      return a.dictBytes() == b.dictBytes();
    }
    final int[] offs = a.dictOffsets();
    if (offs == null || offs.length == 0) {
      return true;
    }
    final int from = offs[0];
    final int to = offs[offs.length - 1];
    return Arrays.equals(a.dictBytes(), from, to, b.dictBytes(), from, to);
  }

  /** One cold fill of a long column and a string column, through a freshly opened session. */
  private Filled fill() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    try (final BasicJsonDBStore store =
        BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build()) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sales.jn");
      try {
        final String resourceKey = session.getResourceConfig().getResource().toString();
        final int revision = session.getMostRecentRevisionNumber();
        final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session, resourceKey,
            revision, new String[] {"[]"}, new String[] {"age"});
        Assertions.assertNotNull(handle, "the projection must be loadable");
        final ProjectionColumnStore columnStore = handle.columnStoreOrNull();
        Assertions.assertNotNull(columnStore, "the catalog must build a lazy handle");
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
        // A string column exercises the OPTIONAL dict chain beside the required body chain, which is
        // the arm where an absent-per-leaf segment has to survive the split.
        return new Filled(columnStore.leafCount(), columnStore.column(handle.columnOf("age"), fetcher),
            columnStore.column(handle.columnOf("dept"), fetcher));
      } finally {
        session.close();
      }
    }
  }

  private record Filled(int leaves, ProjectionColumnStore.ColumnSlice[] ages,
      ProjectionColumnStore.ColumnSlice[] depts) {
  }

  private void buildProjection() throws IOException {
    final StringBuilder records = new StringBuilder(RECORDS * 48);
    records.append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        records.append(',');
      }
      records.append("{\"age\": ")
             .append(20 + i % 60)
             .append(", \"dept\": \"")
             .append(DEPTS[i % DEPTS.length])
             .append("\"}");
    }
    records.append(']');
    query("jn:store('json-path1','sales.jn','" + records + "')");
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/age', '/[]/dept'), ('long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
  }

  private static final String[] DEPTS = {"Eng", "Sales", "HR", "Ops", "Legal"};
}
