package org.sirix.xquery.compiler.expression;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.array.DArray;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.ExprUtil;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.cas.CASFilter;
import org.sirix.index.path.json.JsonPCRCollector;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.function.jn.JNFun;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonItemFactory;

import java.util.*;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public final class IndexExpr implements Expr {

  private final String databaseName;

  private final String resourceName;

  private final Integer revision;

  private final Map<IndexDef, List<Path<QNm>>> indexDefsToPaths;

  private final Map<String, Object> properties;

  public IndexExpr(final Map<String, Object> properties) {
    this.properties = properties;
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
    var nodeKeys = new ArrayList<Long>();

    boolean hasArrayInPath = false;
    final var indexType = (IndexType) properties.get("indexType");
    final var indexTypeToNodeKeys = new HashMap<IndexDef, List<Long>>();

    for (final Map.Entry<IndexDef, List<Path<QNm>>> entrySet : indexDefsToPaths.entrySet()) {
      final var paths = entrySet.getValue();

      if (hasArrayInPath == false) {
        hasArrayInPath = paths.stream().anyMatch(path -> path.steps().stream().anyMatch(isArrayStep()));
      }

      final var pathStrings = entrySet.getValue().stream().map(Path::toString).collect(toSet());

      switch (indexType) {
        case PATH -> {
          final Iterator<NodeReferences> nodeReferencesIterator = indexController.openPathIndex(rtx.getPageTrx(),
                                                                                                entrySet.getKey(),
                                                                                                indexController.createPathFilter(
                                                                                                    pathStrings,
                                                                                                    rtx));
          final var finalNodeKeys = nodeKeys;
          nodeReferencesIterator.forEachRemaining(currentNodeReferences -> finalNodeKeys.addAll(currentNodeReferences.getNodeKeys()));
        }
        case CAS -> {
          final var atomic = (Atomic) properties.get("atomic");
          final var comparisonType = (String) properties.get("comparator");

          final SearchMode searchMode;

          if ("ValueCompGT".equals(comparisonType)) {
            searchMode = SearchMode.GREATER;
          } else if ("ValueCompLT".equals(comparisonType)) {
            searchMode = SearchMode.LOWER;
          } else if ("ValueCompEQ".equals(comparisonType)) {
            searchMode = SearchMode.EQUAL;
          } else if ("ValueCompGE".equals(comparisonType)) {
            searchMode = SearchMode.GREATER_OR_EQUAL;
          } else if ("ValueCompLE".equals(comparisonType)) {
            searchMode = SearchMode.LOWER_OR_EQUAL;
          } else {
            throw new IllegalStateException("Unexpected value: " + comparisonType);
          }

          final var casFilter =
              new CASFilter(new HashSet<>(entrySet.getValue()), atomic, searchMode, new JsonPCRCollector(rtx));

          final Iterator<NodeReferences> nodeReferencesIterator =
              indexController.openCASIndex(rtx.getPageTrx(), entrySet.getKey(), casFilter);

          final var finalNodeKeys = nodeKeys;
          nodeReferencesIterator.forEachRemaining(currentNodeReferences -> finalNodeKeys.addAll(currentNodeReferences.getNodeKeys()));

          indexTypeToNodeKeys.put(entrySet.getKey(), nodeKeys);

          nodeKeys = new ArrayList<>();
        }
        case NAME -> {
        }
        default -> throw new IllegalStateException("Index type " + indexType + " not known");
      }
    }

    final var sequence = new ArrayList<Sequence>();
    final var jsonItemFactory = new JsonItemFactory();

    switch (indexType) {
      case PATH -> {
        nodeKeys.forEach(nodeKey -> {
          rtx.moveTo(nodeKey).trx().moveToFirstChild();
          sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
        });
      }
      case CAS -> {
        indexDefsToPaths.keySet().forEach(indexDef -> {
          final var indexDefToPredicateLevel = (Map<IndexDef, Integer>) properties.get("predicateLevel");
          final var predicateLevel = indexDefToPredicateLevel.get(indexDef);
          final var nodeKeysOfIndex = indexTypeToNodeKeys.get(indexDef);
          nodeKeysOfIndex.forEach(nodeKey -> {
            // TODO: We can skip this traversal once we store a DeweyID <=> nodeKey mapping.
            // Then we can simply clip the DeweyID with the given path level and get the corresponding nodeKey.
            rtx.moveTo(nodeKey);
            rtx.moveToParent();
            for (int i = 0; i < predicateLevel; i++) {
              rtx.moveToParent();

              if (rtx.isObject() && i + 1 < predicateLevel) {
                rtx.moveToParent();
              }
            }
            sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
          });
        });
      }
      case NAME -> {
      }
      default -> throw new QueryException(JNFun.ERR_INVALID_INDEX_TYPE, "Index type not known: " + indexType);
    }

    if (sequence.size() == 0) {
      return null;
    }

    final var hasBitArrayValuesFunction =
        indexType == IndexType.CAS ? (boolean) properties.get("hasBitArrayValuesFunction") : false;

    if (sequence.size() == 1 && (!hasArrayInPath || hasBitArrayValuesFunction)) {
      return ExprUtil.asItem(sequence.get(0));
    }

    return new DArray(sequence.toArray(new Sequence[] {}));
  }

  private Predicate<Path.Step<QNm>> isArrayStep() {
    return step -> Path.Axis.CHILD_ARRAY.equals(step.getAxis()) || Path.Axis.DESC_ARRAY.equals(step.getAxis());
  }

  @Override
  public Item evaluateToItem(QueryContext ctx, Tuple tuple) throws QueryException {
    final var res = evaluate(ctx, tuple);
    if (res instanceof Item) {
      return (Item) res;
    }
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
