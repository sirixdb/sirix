package io.sirix.query.budget;

import io.brackit.query.Query;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jsonitem.object.AbstractObject;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.optimizer.CheapFirstConjunctStage;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.query.json.JsonDBObject;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

/** Counts actual field evaluations through a stored-document decorator. */
@Isolated
final class CheapFirstConjunctWorkBudgetTest {
  private static final int ROWS = 100;
  @TempDir
  Path directory;

  @Test
  void cheapPredicateLimitsTimestampEvaluationToSurvivors() {
    final String query = "for $r in jn:doc('input','rows')[]"
        + " where $r.id eq 1 and xs:dateTime($r.stamp) le xs:dateTime('2024-06-01T00:00:00Z')"
        + " and xs:dateTime('2023-01-01T00:00:00Z') lt xs:dateTime($r.stamp) return $r.id";
    final Reads optimized = evaluate(query, true);
    final Reads baseline = evaluate(query, false);
    assertEquals("1", optimized.answer);
    assertEquals(optimized.answer, baseline.answer);
    assertEquals(ROWS + 1, optimized.id, "one cheap test per row, plus the returned id");
    assertEquals(2, optimized.stamp, "both casts run only on the one survivor");
    assertTrue(baseline.stamp >= ROWS, "rule-off capture proves the read seam is live");
    assertEquals(List.of(0, 20, 20), conjunctCosts(optimized.ast));
  }

  @Test
  void correlatedTemporalOpenFiltersEachFreshRowBeforeItsTimestampCasts() {
    final String query =
        "for $e in jn:doc('epochs','days')[]" + " for $r in jn:open('input','rows',xs:dateTime($e.ts))[]"
            + " where $r.id eq 1 and xs:dateTime($r.stamp) le xs:dateTime('2024-06-01T00:00:00Z')"
            + " and xs:dateTime('2023-01-01T00:00:00Z') lt xs:dateTime($r.stamp) return $r.id";
    final Reads optimized = evaluate(query, true);
    final Reads baseline = evaluate(query, false);
    assertEquals("1 1", optimized.answer);
    assertEquals(optimized.answer, baseline.answer);
    assertEquals(2 * (ROWS + 1), optimized.id);
    assertEquals(4, optimized.stamp, "two casts per survivor, across two correlated opens");
    assertTrue(baseline.stamp >= 2 * ROWS, "rule-off capture proves the correlated read seam is live");
    assertEquals(List.of(0, 20, 20), conjunctCosts(optimized.ast));
  }

  private Reads evaluate(final String query, final boolean cheapFirst) {
    final String previous = System.getProperty(CheapFirstConjunctStage.ENABLED_PROPERTY);
    System.setProperty(CheapFirstConjunctStage.ENABLED_PROPERTY, Boolean.toString(cheapFirst));
    final Reads reads = new Reads();
    try (final BasicJsonDBStore actual =
        BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build()) {
      actual.create("epochs", "days", "[{\"ts\":\"9999-01-01T00:00:00Z\"},{\"ts\":\"9999-01-01T00:00:00Z\"}]");
      final StringBuilder json = new StringBuilder(ROWS * 48).append('[');
      for (int i = 1; i <= ROWS; i++) {
        if (i > 1)
          json.append(',');
        json.append("{\"id\":").append(i).append(",\"stamp\":\"2024-01-01T00:00:00Z\"}");
      }
      final JsonDBCollection collection = actual.create("input", "rows", json.append(']').toString());
      final JsonDBArray array = (JsonDBArray) collection.getDocument("rows");
      final JsonDBArray counted = mock(JsonDBArray.class, withSettings().stubOnly().defaultAnswer(invocation -> {
        try {
          final var value = invocation.getMethod().invoke(array, invocation.getArguments());
          return invocation.getMethod().getName().equals("at") && value instanceof Object object
              ? new CountingObject(object, reads)
              : value;
        } catch (final InvocationTargetException exception) {
          throw exception.getCause();
        }
      }));
      final JsonDBCollection documents =
          mock(JsonDBCollection.class, withSettings().stubOnly().defaultAnswer(invocation -> {
            if (invocation.getMethod().getName().equals("getDocument"))
              return counted;
            try {
              return invocation.getMethod().invoke(collection, invocation.getArguments());
            } catch (final InvocationTargetException exception) {
              throw exception.getCause();
            }
          }));
      final BasicJsonDBStore store =
          mock(BasicJsonDBStore.class, withSettings().stubOnly().defaultAnswer(invocation -> {
            if (invocation.getMethod().getName().equals("lookup") && "input".equals(invocation.getArgument(0)))
              return documents;
            try {
              return invocation.getMethod().invoke(actual, invocation.getArguments());
            } catch (final InvocationTargetException exception) {
              throw exception.getCause();
            }
          }));
      final StringWriter out = new StringWriter();
      try (final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final PrintWriter writer = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, writer);
        writer.flush();
        reads.answer = out.toString().trim();
        reads.ast = chain.getOptimizedAST();
      }
      return reads;
    } finally {
      if (previous == null)
        System.clearProperty(CheapFirstConjunctStage.ENABLED_PROPERTY);
      else
        System.setProperty(CheapFirstConjunctStage.ENABLED_PROPERTY, previous);
    }
  }

  private static List<Integer> conjunctCosts(final AST node) {
    if (node.getType() == XQ.AndExpr) {
      final List<Integer> costs = new ArrayList<>();
      flattenCosts(node, costs);
      return costs;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final List<Integer> costs = conjunctCosts(node.getChild(i));
      if (!costs.isEmpty()) {
        return costs;
      }
    }
    return List.of();
  }

  private static void flattenCosts(final AST node, final List<Integer> costs) {
    if (node.getType() == XQ.AndExpr) {
      for (int i = 0; i < node.getChildCount(); i++) {
        flattenCosts(node.getChild(i), costs);
      }
    } else {
      costs.add(CheapFirstConjunctStage.cost(node));
    }
  }

  private static final class Reads {
    private int id;
    private int stamp;
    private String answer;
    private AST ast;
  }

  private static final class CountingObject extends AbstractObject implements JsonDBItem {
    private final Object delegate;
    private final Reads reads;

    private CountingObject(final Object delegate, final Reads reads) {
      this.delegate = delegate;
      this.reads = reads;
    }

    @Override
    public JsonNodeReadOnlyTrx getTrx() {
      return ((JsonDBItem) delegate).getTrx();
    }

    @Override
    public JsonResourceSession getResourceSession() {
      return ((JsonDBItem) delegate).getResourceSession();
    }

    @Override
    public long getNodeKey() {
      return ((JsonDBItem) delegate).getNodeKey();
    }

    @Override
    public JsonDBCollection getCollection() {
      return ((JsonDBItem) delegate).getCollection();
    }

    @Override
    public Sequence get(final QNm field) {
      if (field.getLocalName().equals("id")) {
        reads.id++;
      } else if (field.getLocalName().equals("stamp")) {
        reads.stamp++;
      }
      return delegate.get(field);
    }

    @Override
    public Object replace(final QNm name, final Sequence value) {
      return delegate.replace(name, value);
    }

    @Override
    public Object rename(final QNm from, final QNm to) {
      return delegate.rename(from, to);
    }

    @Override
    public Object insert(final QNm name, final Sequence value) {
      return delegate.insert(name, value);
    }

    @Override
    public Object remove(final QNm name) {
      return delegate.remove(name);
    }

    @Override
    public Object remove(final IntNumeric index) {
      return delegate.remove(index);
    }

    @Override
    public Object remove(final int index) {
      return delegate.remove(index);
    }

    @Override
    public Sequence value(final IntNumeric index) {
      return delegate.value(index);
    }

    @Override
    public Sequence value(final int index) {
      return delegate.value(index);
    }

    @Override
    public Array names() {
      return delegate.names();
    }

    @Override
    public Array values() {
      return delegate.values();
    }

    @Override
    public QNm name(final IntNumeric index) {
      return delegate.name(index);
    }

    @Override
    public QNm name(final int index) {
      return delegate.name(index);
    }

    @Override
    public IntNumeric length() {
      return delegate.length();
    }

    @Override
    public int len() {
      return delegate.len();
    }
  }
}
