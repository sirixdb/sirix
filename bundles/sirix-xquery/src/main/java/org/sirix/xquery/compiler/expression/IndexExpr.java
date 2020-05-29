package org.sirix.xquery.compiler.expression;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.array.DArray;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.IndexDef;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonItemFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public final class IndexExpr implements Expr {

  private final String databaseName;

  private final String resourceName;

  private final Integer revision;

  private final Map<IndexDef, List<Path<QNm>>> indexDefsToPaths;

  public IndexExpr(final Map<String, Object> properties) {
    requireNonNull(properties);
    databaseName = (String) properties.get("databaseName");
    resourceName = (String) properties.get("resourceName");
    revision = (Integer) properties.get("revision");
    indexDefsToPaths = (Map<IndexDef, List<Path<QNm>>>) properties.get("indexDefs");
  }

  @Override
  public Sequence evaluate(QueryContext ctx, Tuple tuple) throws QueryException {
    final var jsonItemStore = ((SirixQueryContext) ctx).getJsonItemStore();

    final JsonDBCollection jsonCollection = jsonItemStore.lookup(databaseName);
    final var database = jsonCollection.getDatabase();

    final var manager = database.openResourceManager(resourceName);
    final var indexController = revision == -1
        ? manager.getRtxIndexController(manager.getMostRecentRevisionNumber())
        : manager.getRtxIndexController(revision);

    final JsonNodeReadOnlyTrx rtx =
        revision == -1 ? manager.beginNodeReadOnlyTrx() : manager.beginNodeReadOnlyTrx(revision);
    final var nodeKeys = new ArrayList<Long>();

    for (final Map.Entry<IndexDef, List<Path<QNm>>> entrySet : indexDefsToPaths.entrySet()) {
      final var pathStrings = entrySet.getValue().stream().map(Path::toString).collect(toSet());
      final var nodeReferencesIterator = indexController.openPathIndex(rtx.getPageTrx(),
                                                                       entrySet.getKey(),
                                                                       indexController.createPathFilter(pathStrings,
                                                                                                        rtx));

      nodeReferencesIterator.forEachRemaining(currentNodeReferences -> nodeKeys.addAll(currentNodeReferences.getNodeKeys()));
    }

    final var sequence = new ArrayList<Sequence>();

    final var jsonItemFactory = new JsonItemFactory();

    nodeKeys.forEach(nodeKey -> {
      rtx.moveTo(nodeKey).trx().moveToFirstChild();
      sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
    });

    if (sequence.size() == 0) {
      return null;
    }

    return new DArray(sequence.toArray(new Sequence[] {}));
  }

  @Override
  public Item evaluateToItem(QueryContext ctx, Tuple tuple) throws QueryException {
    return null;
  }

  @Override
  public boolean isUpdating() {
    return false;
  }

  @Override
  public boolean isVacuous() {
    return false;
  }
}
