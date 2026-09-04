package io.sirix.index;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.Assert.assertEquals;

/**
 * Equality lookups against a numeric CAS index.
 *
 * <p>
 * A HOT index key is a byte string that carries the value's type id, so a probe key has to be typed
 * like the entries the index stores rather than like whatever atomic the caller passed — otherwise
 * an {@code xs:decimal} index probed with a numerically equal {@code xs:double} builds a different
 * key and the lookup silently returns nothing.
 * </p>
 */
public final class JsonCASNumericLookupTest {

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static long count(final Iterator<NodeReferences> hits) {
    long total = 0;
    while (hits.hasNext()) {
      total += hits.next().getNodeKeys().getLongCardinality();
    }
    return total;
  }

  /** Shred {@code json}, index {@code path} as {@code contentType}, and hand back a prober. */
  private static Prober index(final String json, final String path, final Type contentType, final JsonNodeTrx trx,
      final JsonResourceSession manager) {
    new JsonShredder.Builder(trx, JsonShredder.createStringReader(json),
        InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();

    final JsonIndexController indexController = manager.getWtxIndexController(trx.getRevisionNumber());
    indexController.createIndexes(Set.of(IndexDefs.createCASIdxDef(false, contentType,
        Collections.singleton(parse(path, PathParser.Type.JSON)), 0, IndexDef.DbType.JSON)), trx);
    trx.commit();
    return new Prober(indexController, trx, indexController.getIndexes().getIndexDef(0, IndexType.CAS), path);
  }

  private record Prober(JsonIndexController indexController, JsonNodeTrx trx, IndexDef indexDef, String path) {
    long equalTo(final Atomic key) {
      return count(indexController.openCASIndex(trx.getStorageEngineReader(), indexDef,
          indexController.createCASFilter(Set.of(path), key, SearchMode.EQUAL, new JsonPCRCollector(trx))));
    }

    long all() {
      return count(indexController.openCASIndex(trx.getStorageEngineReader(), indexDef,
          indexController.createCASFilter(Set.of(), null, SearchMode.EQUAL, new JsonPCRCollector(trx))));
    }
  }

  @Test
  public void decimalIndexIsFoundByAnyNumericProbeType() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      final Prober prober =
          index("[{\"v\":2.33},{\"v\":7.5},{\"v\":2.33},{\"other\":1}]", "/[]/v", Type.DEC, trx, manager);

      assertEquals("every v must be indexed", 3, prober.all());
      assertEquals("probe with xs:decimal", 2, prober.equalTo(new Dec(new BigDecimal("2.33"))));
      assertEquals("probe with xs:double", 2, prober.equalTo(new Dbl(2.33)));
      assertEquals("probe with xs:double, singleton value", 1, prober.equalTo(new Dbl(7.5)));
      assertEquals("value not in the index", 0, prober.equalTo(new Dbl(9.25)));
    }
  }

  @Test
  public void integerIndexIsFoundByAnyNumericProbeType() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      final Prober prober = index("[{\"n\":3},{\"n\":10},{\"n\":3}]", "/[]/n", Type.INR, trx, manager);

      assertEquals("every n must be indexed", 3, prober.all());
      assertEquals("probe with xs:int", 2, prober.equalTo(new Int32(3)));
      assertEquals("probe with xs:long", 1, prober.equalTo(new Int64(10)));
      assertEquals("probe with xs:decimal", 2, prober.equalTo(new Dec(new BigDecimal("3"))));
      assertEquals("value not in the index", 0, prober.equalTo(new Int32(4)));
    }
  }
}
