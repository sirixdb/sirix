package org.sirix.xquery.compiler.expression;

import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.sequence.ItemSequence;
import org.brackit.xquery.util.ExprUtil;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.SearchMode;
import org.sirix.index.cas.CASFilter;
import org.sirix.index.cas.CASFilterRange;
import org.sirix.index.name.NameFilter;
import org.sirix.index.path.json.JsonPCRCollector;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.compiler.optimizer.walker.json.Paths;
import org.sirix.xquery.compiler.optimizer.walker.json.QueryPathSegment;
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
    //noinspection unchecked
    indexDefsToPaths = (Map<IndexDef, List<Path<QNm>>>) properties.get("indexDefs");
  }

  @Override
  public Sequence evaluate(QueryContext ctx, Tuple tuple) throws QueryException {
    final var jsonItemStore = ((SirixQueryContext) ctx).getJsonItemStore();

    final JsonDBCollection jsonCollection = jsonItemStore.lookup(databaseName);
    final var database = jsonCollection.getDatabase();

    final var manager = database.beginResourceSession(resourceName);
    final var indexController = revision == -1
        ? manager.getRtxIndexController(manager.getMostRecentRevisionNumber())
        : manager.getRtxIndexController(revision);

    final JsonNodeReadOnlyTrx rtx =
        revision == -1 ? manager.beginNodeReadOnlyTrx() : manager.beginNodeReadOnlyTrx(revision);
    var nodeKeys = new ArrayList<Long>();

    final var indexType = (IndexType) properties.get("indexType");
    final var indexTypeToNodeKeys = new HashMap<IndexDef, List<Long>>();
    @SuppressWarnings("unchecked") final var pathSegmentNamesToArrayIndexes =
        (Deque<QueryPathSegment>) properties.get("pathSegmentNamesToArrayIndexes");

    for (final Map.Entry<IndexDef, List<Path<QNm>>> entrySet : indexDefsToPaths.entrySet()) {
      final var pathStrings = entrySet.getValue().stream().map(Path::toString).collect(toSet());

      switch (indexType) {
        case PATH -> {
          final Iterator<NodeReferences> nodeReferencesIterator = indexController.openPathIndex(rtx.getPageTrx(),
                                                                                                entrySet.getKey(),
                                                                                                indexController.createPathFilter(
                                                                                                    pathStrings,
                                                                                                    rtx));

          checkIfIndexNodeIsApplicable(manager,
                                       rtx,
                                       pathSegmentNamesToArrayIndexes,
                                       nodeReferencesIterator,
                                       nodeKeys,
                                       false);
        }
        case CAS -> {
          final var atomic = (Atomic) properties.get("atomic");
          final var comparisonType = (String) properties.get("comparator");
          final Atomic atomicUpperBound = (Atomic) properties.get("upperBoundAtomic");
          final String comparisonUpperBound = (String) properties.get("upperBoundComparator");
          final SearchMode searchMode = getSearchMode(comparisonType);

          if (atomicUpperBound != null && comparisonUpperBound != null) {
            final SearchMode searchModeUpperBound = getSearchMode(comparisonUpperBound);

            if ((searchMode != SearchMode.GREATER && searchMode != SearchMode.GREATER_OR_EQUAL) || (
                searchModeUpperBound != SearchMode.LOWER && searchModeUpperBound != SearchMode.LOWER_OR_EQUAL)) {
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

            checkIfIndexNodeIsApplicable(manager,
                                         rtx,
                                         pathSegmentNamesToArrayIndexes,
                                         nodeReferencesIterator,
                                         nodeKeys,
                                         false);

          } else {

            final var casFilter =
                new CASFilter(new HashSet<>(entrySet.getValue()), atomic, searchMode, new JsonPCRCollector(rtx));

            final Iterator<NodeReferences> nodeReferencesIterator =
                indexController.openCASIndex(rtx.getPageTrx(), entrySet.getKey(), casFilter);

            checkIfIndexNodeIsApplicable(manager,
                                         rtx,
                                         pathSegmentNamesToArrayIndexes,
                                         nodeReferencesIterator,
                                         nodeKeys,
                                         false);

          }
          indexTypeToNodeKeys.put(entrySet.getKey(), nodeKeys);
          nodeKeys = new ArrayList<>();
        }
        case NAME -> {
          final Iterator<NodeReferences> nodeReferencesIterator = indexController.openNameIndex(rtx.getPageTrx(),
                                                                                                entrySet.getKey(),
                                                                                                new NameFilter(Set.of(
                                                                                                    entrySet.getValue()
                                                                                                            .get(
                                                                                                                entrySet.getValue()
                                                                                                                        .size()
                                                                                                                    - 1)
                                                                                                            .tail()),
                                                                                                               Set.of()));

          checkIfIndexNodeIsApplicable(manager,
                                       rtx,
                                       pathSegmentNamesToArrayIndexes,
                                       nodeReferencesIterator,
                                       nodeKeys,
                                       true);

        }
        default -> throw new IllegalStateException("Index type " + indexType + " not known");
      }
    }

    final var sequence = new ArrayList<Item>();
    final var jsonItemFactory = new JsonItemFactory();

    switch (indexType) {
      case PATH, NAME -> nodeKeys.forEach(nodeKey -> {
        rtx.moveTo(nodeKey);
        final Deque<Integer> arrayIndexes = pathSegmentNamesToArrayIndexes.getLast().arrayIndexes();
        if (arrayIndexes.isEmpty()) {
          rtx.moveToFirstChild();
          sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
        } else if (arrayIndexes.getFirst() == Integer.MIN_VALUE) {
          if (rtx.moveToFirstChild()) {
            do {
              sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
            } while (rtx.moveToRightSibling());
          }
        } else {
          var index = arrayIndexes.getFirst();
          index = index < 0 ? (int) (rtx.getChildCount() + index) : index;
          boolean hasMoved = rtx.moveToFirstChild();
          assert hasMoved;
          int k = 1;
          for (; k <= index; k++) {
            hasMoved = rtx.moveToRightSibling();
            assert hasMoved;
          }
          sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
        }
      });
      case CAS -> indexDefsToPaths.keySet().forEach(indexDef -> {
        final var predicateLeafNode = (AST) properties.get("predicateLeafNode");
        @SuppressWarnings(
            "unchecked") final var indexDefToPredicateLevel = (Map<IndexDef, Integer>) properties.get("predicateLevel");
        final var predicateLevel = indexDefToPredicateLevel.get(indexDef);
        final var nodeKeysOfIndex = indexTypeToNodeKeys.get(indexDef);
        nodeKeysOfIndex.forEach(nodeKey -> {
          // TODO: We can skip this traversal once we store a DeweyID <=> nodeKey mapping.
          // Then we can simply clip the DeweyID with the given path level and get the corresponding nodeKey.
          rtx.moveTo(nodeKey);
          rtx.moveToParent();
          for (int i = 1; i < predicateLevel; i++) {
            rtx.moveToParent();

            if (rtx.isObject() && i + 1 < predicateLevel) {
              rtx.moveToParent();
            }
          }
          if (predicateLeafNode != null && predicateLeafNode.getParent().getType() != XQ.ArrayAccess) {
            rtx.moveToParent();
          }
          sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
        });
      });
      default -> throw new QueryException(JNFun.ERR_INVALID_INDEX_TYPE, "Index type not known: " + indexType);
    }

    if (sequence.size() == 0) {
      return null;
    }

    return new ItemSequence(sequence.toArray(new Item[0]));
  }

  private SearchMode getSearchMode(String comparisonType) {
    return switch (comparisonType) {
      case "ValueCompGT", "GeneralCompGT" -> SearchMode.GREATER;
      case "ValueCompLT", "GeneralCompLT" -> SearchMode.LOWER;
      case "ValueCompEQ", "GeneralCompEQ" -> SearchMode.EQUAL;
      case "ValueCompGE", "GeneralCompGE" -> SearchMode.GREATER_OR_EQUAL;
      case "ValueCompLE", "GeneralCompLE" -> SearchMode.LOWER_OR_EQUAL;
      case null, default -> throw new IllegalStateException("Unexpected value: " + comparisonType);
    };
  }

  private void checkIfIndexNodeIsApplicable(JsonResourceSession manager, JsonNodeReadOnlyTrx rtx,
      Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes, Iterator<NodeReferences> nodeReferencesIterator,
      List<Long> nodeKeys, boolean checkPathBecauseOfFieldNameChecks) {
    final long numberOfArrayIndexes = getNumberOfArrayIndexes(pathSegmentNamesToArrayIndexes);
    try (final var pathSummary = revision == -1 ? manager.openPathSummary() : manager.openPathSummary(revision)) {
      nodeReferencesIterator.forEachRemaining(currentNodeReferences -> {
        final var currNodeKeys = new LongLinkedOpenHashSet(currentNodeReferences.getNodeKeys().toArray());
        // if array numberOfArrayIndexes are given (only some might be specified we have to drop false positive nodes
        if (numberOfArrayIndexes != 0 || checkPathBecauseOfFieldNameChecks) {
          final var nodeKeyIter =  currentNodeReferences.getNodeKeys().getLongIterator();
          while (nodeKeyIter.hasNext()) {
            final var nodeKey = nodeKeyIter.next();
            final var currentPathSegmentNamesToArrayIndexes = new ArrayDeque<QueryPathSegment>();
            pathSegmentNamesToArrayIndexes.forEach(pathSegmentNameToArrayIndex -> {
              final var currentIndexes = new ArrayDeque<>(pathSegmentNameToArrayIndex.arrayIndexes());
              currentPathSegmentNamesToArrayIndexes.addLast(new QueryPathSegment(pathSegmentNameToArrayIndex.pathSegmentName(), currentIndexes));
            });

            rtx.moveTo(nodeKey);
            if (rtx.isStringValue() || rtx.isNumberValue() || rtx.isBooleanValue() || rtx.isNullValue()) {
              rtx.moveToParent();
            }

            final var pathNodeKey = rtx.getPathNodeKey();
            pathSummary.moveTo(pathNodeKey);
            final var path = pathSummary.getPath();

            if (checkPathBecauseOfFieldNameChecks) {
              if (Paths.isPathNodeNotAQueryResult(pathSegmentNamesToArrayIndexes, pathSummary, pathNodeKey)) {
                currNodeKeys.remove(nodeKey);
                continue;
              }
            }

            assert path != null;
            final var steps = path.steps();

            outer:
            for (int i = steps.size() - 1; i >= 0; i--) {
              final var step = steps.get(i);

              boolean moveToParent = true;
              final int currentIndex = i;

              if (step.getAxis() == Path.Axis.CHILD_ARRAY) {
                int j = i - 1;
                // nested child arrays
                while (j >= 0 && steps.get(j).getAxis() == Path.Axis.CHILD_ARRAY) {
                  j--;
                  i--;
                }

                final Deque<Integer> tempIndexes;

                assert currentPathSegmentNamesToArrayIndexes.peekLast() != null;
                tempIndexes = currentPathSegmentNamesToArrayIndexes.peekLast().arrayIndexes();
                final Deque<Integer> indexes = tempIndexes == null ? null : new ArrayDeque<>(tempIndexes);

                if (indexes == null) {
                  // no array numberOfArrayIndexes given
                  while (j < currentIndex) {
                    j++;
                    rtx.moveToParent();
                  }
                } else {
                  // at least some array numberOfArrayIndexes are given for the specific object key node
                  int y = 0;
                  for (int m = 0, length = currentIndex - j - indexes.size(); m < length; m++) {
                    // for instance =>foo[[0]]=>bar   in a path /foo/[]/[]/[]/bar meaning at least one index is not specified
                    y++;
                    rtx.moveToParent();
                  }
                  for (int l = currentIndex, length = j + y; l > length; l--) {
                    // remaining with array numberOfArrayIndexes specified
                    int index = indexes.pop();

                    if (index != Integer.MIN_VALUE) {
                      index = index < 0 ? (int) (rtx.getChildCount() + index) : index;
                      if (currentIndex == steps.size() - 1) {
                        boolean hasMoved = rtx.moveToFirstChild();
                        int k = 0;
                        for (; k <= index && hasMoved; k++) {
                          hasMoved = rtx.moveToRightSibling();
                        }
                        if (k - 1 != index) {
                          currNodeKeys.remove(nodeKey);
                          break outer;
                        }
                      } else {
                        boolean hasMoved = true;
                        for (int k = 0; k < index && hasMoved; k++) {
                          hasMoved = rtx.moveToLeftSibling();
                        }
                        if (!hasMoved || rtx.hasLeftSibling()) {
                          currNodeKeys.remove(nodeKey);
                          break outer;
                        }
                      }
                    } else if (l == steps.size() - 1) {
                      moveToParent = false;
                    }
                    rtx.moveToParent();
                  }
                }
              } else {
                currentPathSegmentNamesToArrayIndexes.removeLast();
              }

              // if not the last step is an array unboxing
              if (moveToParent) {
                rtx.moveToParent();
              }

              if (rtx.isObject() && i - 1 > 0 && steps.get(i - 1).getAxis() == Path.Axis.CHILD_OBJECT_FIELD) {
                rtx.moveToParent();
              }
            }
          }
        }
        nodeKeys.addAll(currNodeKeys);
      });
    }
  }

  private long getNumberOfArrayIndexes(Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes) {
    return pathSegmentNamesToArrayIndexes.stream()
                                         .map(QueryPathSegment::arrayIndexes)
                                         .filter(arrayIndexes -> !arrayIndexes.isEmpty())
                                         .count();
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
