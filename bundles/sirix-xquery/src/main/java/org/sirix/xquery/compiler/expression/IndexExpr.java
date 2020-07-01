package org.sirix.xquery.compiler.expression;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.sequence.BaseIter;
import org.brackit.xquery.sequence.LazySequence;
import org.brackit.xquery.util.ExprUtil;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.cas.CASFilter;
import org.sirix.index.cas.CASFilterRange;
import org.sirix.index.path.json.JsonPCRCollector;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.function.jn.JNFun;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonItemFactory;

import java.util.*;

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

    final var indexType = (IndexType) properties.get("indexType");
    final var indexTypeToNodeKeys = new HashMap<IndexDef, List<Long>>();
    final var arrayIndexes = (Map<String, Deque<Integer>>) properties.get("arrayIndexes");

    for (final Map.Entry<IndexDef, List<Path<QNm>>> entrySet : indexDefsToPaths.entrySet()) {
      final var pathStrings = entrySet.getValue().stream().map(Path::toString).collect(toSet());

      switch (indexType) {
        case PATH -> {
          final Iterator<NodeReferences> nodeReferencesIterator = indexController.openPathIndex(rtx.getPageTrx(),
                                                                                                entrySet.getKey(),
                                                                                                indexController.createPathFilter(
                                                                                                    pathStrings,
                                                                                                    rtx));

          checkIfIndexNodeIsApplicable(manager, rtx, arrayIndexes, nodeReferencesIterator, nodeKeys);
        }
        case CAS -> {
          final var atomic = (Atomic) properties.get("atomic");
          final var comparisonType = (String) properties.get("comparator");
          final Atomic atomicUpperBound = (Atomic) properties.get("upperBoundAtomic");
          final String comparisonUpperBound = (String) properties.get("upperBoundComparator");

          if (atomicUpperBound != null && comparisonUpperBound != null) {
            final SearchMode searchMode = getSearchMode(comparisonType);
            final SearchMode searchModeUpperBound = getSearchMode(comparisonUpperBound);

            if ((searchMode != SearchMode.GREATER && searchMode != SearchMode.GREATER_OR_EQUAL)
                || (searchModeUpperBound != SearchMode.LOWER && searchModeUpperBound != SearchMode.LOWER_OR_EQUAL)) {
              throw new QueryException(JNFun.ERR_INVALID_ARGUMENT, new QNm("Search mode not supported."));
            }

            final var casFilter = new CASFilterRange(new HashSet<>(entrySet.getValue()),
                                                     atomic,
                                                     atomicUpperBound,
                                                     searchMode == SearchMode.GREATER_OR_EQUAL,
                                                     searchModeUpperBound == SearchMode.LOWER_OR_EQUAL,
                                                     new JsonPCRCollector(rtx));

            final Iterator<NodeReferences> nodeReferencesIterator =
                indexController.openCASIndex(rtx.getPageTrx(), entrySet.getKey(), casFilter);

            checkIfIndexNodeIsApplicable(manager, rtx, arrayIndexes, nodeReferencesIterator, nodeKeys);

            indexTypeToNodeKeys.put(entrySet.getKey(), nodeKeys);

            nodeKeys = new ArrayList<>();
          } else {
            final SearchMode searchMode = getSearchMode(comparisonType);

            final var casFilter =
                new CASFilter(new HashSet<>(entrySet.getValue()), atomic, searchMode, new JsonPCRCollector(rtx));

            final Iterator<NodeReferences> nodeReferencesIterator =
                indexController.openCASIndex(rtx.getPageTrx(), entrySet.getKey(), casFilter);

            checkIfIndexNodeIsApplicable(manager, rtx, arrayIndexes, nodeReferencesIterator, nodeKeys);

            indexTypeToNodeKeys.put(entrySet.getKey(), nodeKeys);

            nodeKeys = new ArrayList<>();
          }
        }
        case NAME -> {
        }
        default -> throw new IllegalStateException("Index type " + indexType + " not known");
      }
    }

    final var sequence = new ArrayList<Sequence>();
    final var jsonItemFactory = new JsonItemFactory();

    switch (indexType) {
      case PATH -> nodeKeys.forEach(nodeKey -> {
        rtx.moveTo(nodeKey).trx().moveToFirstChild();
        sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
      });
      case CAS -> indexDefsToPaths.keySet().forEach(indexDef -> {
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
      case NAME -> {
      }
      default -> throw new QueryException(JNFun.ERR_INVALID_INDEX_TYPE, "Index type not known: " + indexType);
    }

    if (sequence.size() == 0) {
      return null;
    }

    return new LazySequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          int i;

          @Override
          public Item next() {
            if (i < sequence.size()) {
              final var item = sequence.get(i++);

              return item.evaluateToItem(ctx, tuple);
            }
            return null;
          }

          @Override
          public void close() {
          }
        };
      }
    };
  }

  private SearchMode getSearchMode(String comparisonType) {
    final SearchMode searchMode;

    if ("ValueCompGT".equals(comparisonType) || "GeneralCompGT".equals(comparisonType)) {
      searchMode = SearchMode.GREATER;
    } else if ("ValueCompLT".equals(comparisonType) || "GeneralCompLT".equals(comparisonType)) {
      searchMode = SearchMode.LOWER;
    } else if ("ValueCompEQ".equals(comparisonType) || "GeneralCompEQ".equals(comparisonType)) {
      searchMode = SearchMode.EQUAL;
    } else if ("ValueCompGE".equals(comparisonType) || "GeneralCompGE".equals(comparisonType)) {
      searchMode = SearchMode.GREATER_OR_EQUAL;
    } else if ("ValueCompLE".equals(comparisonType) || "GeneralCompLE".equals(comparisonType)) {
      searchMode = SearchMode.LOWER_OR_EQUAL;
    } else {
      throw new IllegalStateException("Unexpected value: " + comparisonType);
    }

    return searchMode;
  }

  private void checkIfIndexNodeIsApplicable(JsonResourceManager manager, JsonNodeReadOnlyTrx rtx,
      Map<String, Deque<Integer>> arrayIndexes, Iterator<NodeReferences> nodeReferencesIterator, List<Long> nodeKeys) {
    try (final var pathSummary = revision == -1 ? manager.openPathSummary() : manager.openPathSummary(revision)) {
      nodeReferencesIterator.forEachRemaining(currentNodeReferences -> {
        final var currNodeKeys = new HashSet<>(currentNodeReferences.getNodeKeys());
        // if array indexes are given (only some might be specified we have to drop false positive nodes
        if (arrayIndexes != null && !arrayIndexes.isEmpty()) {
          currentNodeReferences.getNodeKeys().forEach(nodeKey -> {
            rtx.moveTo(nodeKey);
            if (rtx.isStringValue() || rtx.isNumberValue() || rtx.isBooleanValue() || rtx.isNullValue()) {
              rtx.moveToParent();
            }

            pathSummary.moveTo(rtx.getPathNodeKey());
            final var path = pathSummary.getPath();
            final var steps = path.steps();

            outer:
            for (int i = steps.size() - 1; i >= 0; i--) {
              final var step = steps.get(i);

              if (step.getAxis() == Path.Axis.CHILD_ARRAY) {
                final int currentIndex = i;
                int j = i - 1;
                // nested child arrays
                while (steps.get(j).getAxis() == Path.Axis.CHILD_ARRAY) {
                  j--;
                  i--;
                }

                final Deque<Integer> tempIndexes = arrayIndexes.get(steps.get(j).getValue().getLocalName());
                final Deque<Integer> indexes = tempIndexes == null ? null : new ArrayDeque<>(tempIndexes);

                if (indexes == null) {
                  // no array indexes given
                  while (j < currentIndex) {
                    j++;
                    rtx.moveToParent();
                  }
                } else {
                  // at least some array indexes are given for the specific object key node
                  int y = 0;
                  for (int m = 0, length = currentIndex - j - indexes.size(); m < length; m++) {
                    // for instance =>foo[[0]]=>bar   in a path /foo/[]/[]/[]/bar meaning at least one index is not specified
                    y++;
                    rtx.moveToParent();
                  }
                  for (int l = currentIndex, length = j + y; l > length; l--) {
                    // remaining with array indexes specified
                    final Integer index = indexes.pop();
                    boolean hasMoved = true;
                    for (int k = 0; k < index && hasMoved; k++) {
                      hasMoved = rtx.moveToLeftSibling().hasMoved();
                    }
                    if (!hasMoved || rtx.hasLeftSibling()) {
                      currNodeKeys.remove(nodeKey);
                      break outer;
                    }
                    rtx.moveToParent();
                  }
                }
              }

              rtx.moveToParent();
              if (rtx.isObject() && i - 1 > 0 && steps.get(i - 1).getAxis() == Path.Axis.CHILD) {
                rtx.moveToParent();
              }
            }
          });
        }
        nodeKeys.addAll(currNodeKeys);
      });
    }
  }

  @Override
  public Item evaluateToItem(QueryContext ctx, Tuple tuple) throws QueryException {
    return ExprUtil.asItem(evaluate(ctx, tuple));
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
