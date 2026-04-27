package io.sirix.query.compiler.expression;

import io.sirix.query.compiler.optimizer.walker.json.Paths;
import io.sirix.query.function.jn.JNFun;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.ExprUtil;
import io.brackit.query.util.path.Path;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.cas.CASFilter;
import io.sirix.index.cas.CASFilterRange;
import io.sirix.index.name.NameFilter;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.optimizer.walker.json.QueryPathSegment;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonItemFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    // noinspection unchecked
    indexDefsToPaths = (Map<IndexDef, List<Path<QNm>>>) properties.get("indexDefs");
  }

  @Override
  public Sequence evaluate(QueryContext ctx, Tuple tuple) throws QueryException {
    final var jsonItemStore = ((SirixQueryContext) ctx).getJsonItemStore();

    final JsonDBCollection jsonCollection = jsonItemStore.lookup(databaseName);
    final var database = jsonCollection.getDatabase();

    final var resourceSession = database.beginResourceSession(resourceName);
    final JsonNodeReadOnlyTrx rtx;
    try {
      rtx = revision == -1
          ? resourceSession.beginNodeReadOnlyTrx()
          : resourceSession.beginNodeReadOnlyTrx(revision);
    } catch (final Exception e) {
      resourceSession.close();
      throw e;
    }
    try {
      final var indexController = revision == -1
          ? resourceSession.getRtxIndexController(resourceSession.getMostRecentRevisionNumber())
          : resourceSession.getRtxIndexController(revision);
      var nodeKeys = new ArrayList<Long>();

      final var indexType = (IndexType) properties.get("indexType");
      final var indexTypeToNodeKeys = new HashMap<IndexDef, List<Long>>();
      @SuppressWarnings("unchecked")
      final var pathSegmentNamesToArrayIndexes =
          (Deque<QueryPathSegment>) properties.get("pathSegmentNamesToArrayIndexes");

      for (final Map.Entry<IndexDef, List<Path<QNm>>> entrySet : indexDefsToPaths.entrySet()) {
        final var pathStrings = entrySet.getValue().stream().map(Path::toString).collect(toSet());

        switch (indexType) {
          case PATH -> {
            final Iterator<NodeReferences> nodeReferencesIterator = indexController.openPathIndex(rtx.getStorageEngineReader(),
                entrySet.getKey(), indexController.createPathFilter(pathStrings, rtx));

            checkIfIndexNodeIsApplicable(resourceSession, rtx, pathSegmentNamesToArrayIndexes, nodeReferencesIterator, nodeKeys,
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

              if ((searchMode != SearchMode.GREATER && searchMode != SearchMode.GREATER_OR_EQUAL)
                  || (searchModeUpperBound != SearchMode.LOWER && searchModeUpperBound != SearchMode.LOWER_OR_EQUAL)) {
                throw new QueryException(JNFun.ERR_INVALID_ARGUMENT, new QNm("Search mode not supported."));
              }

              final var casFilter = new CASFilterRange(new HashSet<>(entrySet.getValue()), atomic, atomicUpperBound,
                  searchMode == SearchMode.GREATER_OR_EQUAL, searchModeUpperBound == SearchMode.LOWER_OR_EQUAL,
                  new JsonPCRCollector(rtx));

              final Iterator<NodeReferences> nodeReferencesIterator =
                  indexController.openCASIndex(rtx.getStorageEngineReader(), entrySet.getKey(), casFilter);

              checkIfIndexNodeIsApplicable(resourceSession, rtx, pathSegmentNamesToArrayIndexes, nodeReferencesIterator, nodeKeys,
                  false);

            } else {

              final var casFilter =
                  new CASFilter(new HashSet<>(entrySet.getValue()), atomic, searchMode, new JsonPCRCollector(rtx));

              final Iterator<NodeReferences> nodeReferencesIterator =
                  indexController.openCASIndex(rtx.getStorageEngineReader(), entrySet.getKey(), casFilter);

              checkIfIndexNodeIsApplicable(resourceSession, rtx, pathSegmentNamesToArrayIndexes, nodeReferencesIterator, nodeKeys,
                  false);

            }
            indexTypeToNodeKeys.put(entrySet.getKey(), nodeKeys);
            nodeKeys = new ArrayList<>();
          }
          case NAME -> {
            final Iterator<NodeReferences> nodeReferencesIterator =
                indexController.openNameIndex(rtx.getStorageEngineReader(), entrySet.getKey(),
                    new NameFilter(Set.of(entrySet.getValue().get(entrySet.getValue().size() - 1).tail()), Set.of()));

            checkIfIndexNodeIsApplicable(resourceSession, rtx, pathSegmentNamesToArrayIndexes, nodeReferencesIterator, nodeKeys,
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
          // iter#32 P2: fused OBJECT_NAMED_OBJECT (52) / OBJECT_NAMED_ARRAY (53) carry the
          // OBJECT_KEY+nested-value pair on a single record. The legacy two-level pattern
          // descended via moveToFirstChild to land on the OBJECT/ARRAY value; under fusion
          // the indexed node IS already that value. Same applies for the primitive-fused
          // OBJECT_NAMED_BOOLEAN/NUMBER/STRING/NULL (48-51) — JsonItemFactory dispatches on
          // the fused kind and synthesises the typed atomic. Skip moveToFirstChild so we
          // hand the cursor to the factory in its proper position.
          final boolean isFusedRecord = rtx.getKind().isFusedAnyNamed();
          if (arrayIndexes.isEmpty()) {
            if (!isFusedRecord) {
              rtx.moveToFirstChild();
            }
            sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
          } else if (arrayIndexes.getFirst() == Integer.MIN_VALUE) {
            if (rtx.moveToFirstChild()) {
              do {
                sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
              } while (rtx.moveToRightSibling());
            }
          } else {
            var index = arrayIndexes.getFirst();
            index = index < 0
                ? (int) (rtx.getChildCount() + index)
                : index;
            if (!rtx.moveToFirstChild()) {
              throw new QueryException(JNFun.ERR_INVALID_ARGUMENT,
                  new QNm("Index expression: moveToFirstChild failed for nodeKey " + nodeKey));
            }
            int k = 1;
            for (; k <= index; k++) {
              if (!rtx.moveToRightSibling()) {
                throw new QueryException(JNFun.ERR_INVALID_ARGUMENT,
                    new QNm("Index expression: moveToRightSibling failed at position " + k
                        + " of " + index + " for nodeKey " + nodeKey));
              }
            }
            sequence.add(jsonItemFactory.getSequence(rtx, jsonCollection));
          }
        });
        case CAS -> indexDefsToPaths.keySet().forEach(indexDef -> {
          final var predicateLeafNode = (AST) properties.get("predicateLeafNode");
          @SuppressWarnings("unchecked")
          final var indexDefToPredicateLevel = (Map<IndexDef, Integer>) properties.get("predicateLevel");
          final var predicateLevel = indexDefToPredicateLevel.get(indexDef);
          final var nodeKeysOfIndex = indexTypeToNodeKeys.get(indexDef);
          nodeKeysOfIndex.forEach(nodeKey -> {
            // TODO: We can skip this traversal once we store a DeweyID <=> nodeKey mapping.
            // Then we can simply clip the DeweyID with the given path level and get the corresponding nodeKey.
            rtx.moveTo(nodeKey);
            // iter#32 fusion: the legacy CAS index emitted entries on the primitive VALUE node
            // (STRING_VALUE etc.) whose parent was OBJECT_KEY. Under fusion the indexed entry
            // is on the fused OBJECT_NAMED_* record itself — which already plays the OBJECT_KEY
            // role. The first moveToParent below is the legacy "VALUE → OBJECT_KEY" step; for
            // a fused record it would skip one level too high, so emit a synthetic stay-put.
            final boolean indexedOnFusedPrimitive = rtx.getKind().isFusedObjectNamed();
            if (!indexedOnFusedPrimitive) {
              rtx.moveToParent();
            }
            for (int i = 1; i < predicateLevel; i++) {
              rtx.moveToParent();

              // Bare OBJECT only — the fused OBJECT_NAMED_OBJECT already collapses the
              // OBJECT_KEY+OBJECT pair, so the extra hop is illegal under fusion.
              if (rtx.getKind() == NodeKind.OBJECT && i + 1 < predicateLevel) {
                rtx.moveToParent();
              }
            }
            if (predicateLeafNode != null && predicateLeafNode.getParent().getType() != XQ.ArrayAccess) {
              // iter#32 P2 fusion: the legacy "skip OBJECT_KEY layer to its containing OBJECT"
              // hop becomes a no-op when the predicate target is itself a fused
              // OBJECT_NAMED_OBJECT — the named-OBJECT pair is already represented by the same
              // record. Skip the FINAL hop to avoid over-shooting into the parent OBJECT.
              if (rtx.getKind() != NodeKind.OBJECT_NAMED_OBJECT
                  && rtx.getKind() != NodeKind.OBJECT_NAMED_ARRAY) {
                rtx.moveToParent();
              }
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
    } catch (final Exception e) {
      // Close resources with proper exception suppression to avoid masking the original error
      try { rtx.close(); } catch (final Exception s) { e.addSuppressed(s); }
      try { resourceSession.close(); } catch (final Exception s) { e.addSuppressed(s); }
      throw e;
    }
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

  private void checkIfIndexNodeIsApplicable(JsonResourceSession resourceSession, JsonNodeReadOnlyTrx rtx,
      Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes, Iterator<NodeReferences> nodeReferencesIterator,
      List<Long> nodeKeys, boolean checkPathBecauseOfFieldNameChecks) {
    final long numberOfArrayIndexes = getNumberOfArrayIndexes(pathSegmentNamesToArrayIndexes);
    try (final var pathSummary = revision == -1
        ? resourceSession.openPathSummary()
        : resourceSession.openPathSummary(revision)) {
      nodeReferencesIterator.forEachRemaining(currentNodeReferences -> {
        final var currNodeKeys = new LongLinkedOpenHashSet(currentNodeReferences.getNodeKeys().toArray());
        // if array numberOfArrayIndexes are given (only some might be specified we have to drop false
        // positive nodes
        if (numberOfArrayIndexes != 0 || checkPathBecauseOfFieldNameChecks) {
          final var nodeKeyIter = currentNodeReferences.getNodeKeys().getLongIterator();
          while (nodeKeyIter.hasNext()) {
            final var nodeKey = nodeKeyIter.next();
            final var currentPathSegmentNamesToArrayIndexes = new ArrayDeque<QueryPathSegment>();
            pathSegmentNamesToArrayIndexes.forEach(pathSegmentNameToArrayIndex -> {
              final var currentIndexes = new ArrayDeque<>(pathSegmentNameToArrayIndex.arrayIndexes());
              currentPathSegmentNamesToArrayIndexes.addLast(
                  new QueryPathSegment(pathSegmentNameToArrayIndex.pathSegmentName(), currentIndexes));
            });

            rtx.moveTo(nodeKey);
            // Fused OBJECT_NAMED_* records play the OBJECT_KEY role — their pathNodeKey lives
            // on the fused node itself. Only move to parent for legacy unfused primitives,
            // where the indexed node is a VALUE whose parent is the OBJECT_KEY.
            if (!rtx.isObjectKey()
                && (rtx.isStringValue() || rtx.isNumberValue() || rtx.isBooleanValue() || rtx.isNullValue())) {
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

            // iter#32 P2 fusion: OBJECT_NAMED_ARRAY anchors its pathNodeKey at the
            // {@code __array__/ARRAY} layer (so child fields nest correctly), but its OWN
            // identity is the OBJECT_KEY-layer field (e.g. "tada"). The path summary path
            // therefore ends with a trailing {@code []}; the query segment's name ("tada")
            // matches the FIELD step before that trailing {@code []}. Skip the trailing
            // {@code []} when walking back so the segment pointers line up.
            int startIdx = steps.size() - 1;
            if (rtx.getKind() == NodeKind.OBJECT_NAMED_ARRAY
                && startIdx >= 1
                && steps.get(startIdx).getAxis() == Path.Axis.CHILD_ARRAY) {
              startIdx--;
            }
            outer: for (int i = startIdx; i >= 0; i--) {
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

                final var lastSegment = currentPathSegmentNamesToArrayIndexes.peekLast();
                if (lastSegment == null) {
                  currNodeKeys.remove(nodeKey);
                  break outer;
                }
                tempIndexes = lastSegment.arrayIndexes();
                final Deque<Integer> indexes = tempIndexes == null
                    ? null
                    : new ArrayDeque<>(tempIndexes);

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
                    // for instance =>foo[[0]]=>bar in a path /foo/[]/[]/[]/bar meaning at least one index is not
                    // specified
                    y++;
                    rtx.moveToParent();
                  }
                  for (int l = currentIndex, length = j + y; l > length; l--) {
                    // remaining with array numberOfArrayIndexes specified
                    int index = indexes.pop();

                    if (index != Integer.MIN_VALUE) {
                      index = index < 0
                          ? (int) (rtx.getChildCount() + index)
                          : index;
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
                // iter#32 P2 fusion: OBJECT_NAMED_ARRAY collapses the ARRAY+OBJECT_KEY
                // pair into one record. After a CHILD_ARRAY step's inner-loop landed
                // on the ARRAY-equivalent (the fused record itself), the outer
                // moveToParent here would skip over the OBJECT_KEY layer too — but under
                // fusion that layer is already part of the same record. Skip ONLY for
                // the CHILD_ARRAY follow-up, not for FIELD steps.
                final boolean fusedSkip = step.getAxis() == Path.Axis.CHILD_ARRAY
                    && rtx.getKind() == NodeKind.OBJECT_NAMED_ARRAY;
                if (!fusedSkip) {
                  rtx.moveToParent();
                }
              }

              // Legacy two-record OBJECT_KEY+OBJECT pair needs an extra moveToParent here
              // (OBJECT layer → OBJECT_KEY layer). Under fusion the single OBJECT_NAMED_OBJECT
              // record collapses both layers — the parent moveToParent already lands on the
              // GRANDPARENT object, so the extra hop would over-shoot. Restrict the extra hop
              // to bare OBJECT records only.
              if (rtx.getKind() == NodeKind.OBJECT
                  && i - 1 > 0
                  && steps.get(i - 1).getAxis() == Path.Axis.CHILD_OBJECT_FIELD) {
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
