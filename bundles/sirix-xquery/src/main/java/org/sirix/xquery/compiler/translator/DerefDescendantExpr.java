package org.sirix.xquery.compiler.translator;

import com.google.common.collect.ImmutableSet;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.Bits;
import org.brackit.xquery.sequence.BaseIter;
import org.brackit.xquery.sequence.ItemSequence;
import org.brackit.xquery.sequence.LazySequence;
import org.brackit.xquery.util.ExprUtil;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.json.Object;
import org.checkerframework.checker.index.qual.NonNegative;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.NestedAxis;
import org.sirix.axis.SelfAxis;
import org.sirix.axis.concurrent.ConcurrentAxis;
import org.sirix.axis.concurrent.ConcurrentUnionAxis;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.json.JsonNameFilter;
import org.sirix.axis.filter.json.ObjectKeyFilter;
import org.sirix.exception.SirixException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.NodeKind;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.stream.json.SirixJsonStream;

import java.util.*;

class DerefDescendantExpr implements Expr {

  private record PathSegment(QNm name, boolean isArray) {
  }

  private record PathData(BitSet matchingPathNodeKeys, int level) {
  }

  /**
   * Map with PCR <=> matching nodes.
   */
  private final Map<Long, PathData> filterMap;

  private final Expr context;
  private final Expr field;

  public DerefDescendantExpr(Expr context, Expr field) {
    this.context = context;
    this.field = field;
    filterMap = new HashMap<>();
  }

  @Override
  public Sequence evaluate(QueryContext ctx, Tuple tuple) {
    Sequence sequence = context.evaluate(ctx, tuple);

    if (sequence instanceof ItemSequence itemSequence) {
      return getLazySequence(ctx, tuple, itemSequence.iterate());
    }

    if (sequence instanceof LazySequence lazySequence) {
      return getLazySequence(ctx, tuple, lazySequence.iterate());
    }

    if (!(sequence instanceof Object) && !(sequence instanceof Array)) {
      return null;
    }

    Item itemField = field.evaluateToItem(ctx, tuple);
    if (itemField == null) {
      return null;
    }

    final var recordFieldAsString = getRecordFieldAsString(itemField);
    final var jsonDBItem = (JsonDBItem) sequence;
    final var rtx = jsonDBItem.getTrx();

    return getLazySequence(recordFieldAsString, jsonDBItem, rtx);
  }

  @SuppressWarnings("ConstantConditions")
  @Nullable
  private SirixJsonLazySequence getLazySequence(String recordFieldAsString, JsonDBItem jsonDBItem,
      JsonNodeReadOnlyTrx rtx) {
    final var resourceManager = rtx.getResourceManager();
    if (resourceManager.getResourceConfig().withPathSummary && rtx.hasChildren()) {
      try {
        final int revisionNumber = rtx.getRevisionNumber();
        final var nodeKey = jsonDBItem.getNodeKey();
        final var nodeKind = jsonDBItem instanceof Object ? NodeKind.OBJECT : NodeKind.ARRAY;
        final long pcr;
        if (nodeKind == NodeKind.OBJECT) {
          rtx.moveToParent();
          pcr = rtx.isDocumentRoot() ? 0 : rtx.getPathNodeKey();
          rtx.moveTo(jsonDBItem.getNodeKey());
        } else {
          pcr = rtx.getPathNodeKey();
        }
        final PathData data = filterMap.get(pcr);
        BitSet matches = data == null ? null : data.matchingPathNodeKeys;
        int startLevel = data == null ? 0 : data.level;
        try (final PathSummaryReader reader = resourceManager.openPathSummary(rtx.getRevisionNumber())) {
          if (matches == null) {
            if (pcr != 0) {
              reader.moveTo(pcr);
            }
            startLevel = reader.getLevel();
            final int level = startLevel + 1;
            final QNm name = new QNm(recordFieldAsString);
            matches = reader.match(name, level);
            filterMap.put(pcr, new PathData(matches, startLevel));
          }
          // No matches.
          if (matches.cardinality() == 0) {
            return null;
          }
          // One match.
          if (matches.cardinality() == 1) {
            final long pcr2 = matches.nextSetBit(0);
            reader.moveTo(pcr2);
            assert reader.getPathNode() != null;
            final int matchLevel = reader.getPathNode().getLevel();

            // Match at the same level.
            if (matchLevel == startLevel) {
              return getLazySequence(new SirixJsonStream(new SelfAxis(rtx), jsonDBItem.getCollection()));
            }
            // Match at the next level (single child-path).
            if (matchLevel == startLevel + 1) {
              if (reader.getPathNode().getKind() == NodeKind.ARRAY) {
                return getLazySequence(new SirixJsonStream(new ChildAxis(rtx), jsonDBItem.getCollection()));
              }
              if (nodeKind == NodeKind.ARRAY) {
                var axis = new NestedAxis(new FilterAxis<>(new NestedAxis(new ChildAxis(rtx), new ChildAxis(rtx)),
                                                           new ObjectKeyFilter(rtx),
                                                           new JsonNameFilter(rtx, recordFieldAsString)),
                                          new ChildAxis(rtx));
                return getLazySequence(new SirixJsonStream(axis, jsonDBItem.getCollection()));
              }
              var axis = new NestedAxis(new FilterAxis<>(new ChildAxis(rtx),
                                                         new ObjectKeyFilter(rtx),
                                                         new JsonNameFilter(rtx, recordFieldAsString)),
                                        new ChildAxis(rtx));
              return getLazySequence(new SirixJsonStream(axis, jsonDBItem.getCollection()));
            }
            // Match at a level below the child level.
            final Deque<PathSegment> pathSegments = getPathSegments(matchLevel, startLevel, reader);
            return getLazySequence(new SirixJsonStream(buildQuery(pathSegments,
                                                                  resourceManager,
                                                                  revisionNumber,
                                                                  nodeKey), jsonDBItem.getCollection()));
          }
          // More than one match.
          boolean onSameLevel = true;
          int i = matches.nextSetBit(0);
          reader.moveTo(i);
          int level = reader.getLevel();
          final int tmpLevel = level;
          for (i = matches.nextSetBit(i + 1); i >= 0; i = matches.nextSetBit(i + 1)) {
            reader.moveTo(i);
            level = reader.getLevel();
            if (level != tmpLevel) {
              onSameLevel = false;
              break;
            }
          }

          final Deque<org.sirix.api.Axis> axisQueue = new ArrayDeque<>(matches.cardinality());
          if (onSameLevel) {
            // Matches on same level.
            for (int j = level; (j > startLevel && nodeKind == NodeKind.OBJECT) || (j >= startLevel
                && nodeKind == NodeKind.ARRAY); j--) {
              final var newRtx = resourceManager.beginNodeReadOnlyTrx(rtx.getRevisionNumber());
              final var newRtx2 = resourceManager.beginNodeReadOnlyTrx(rtx.getRevisionNumber());
              newRtx2.moveTo(nodeKey);

              // Build an immutable set and turn it into a list for sorting.
              final ImmutableSet.Builder<PathSegment> pathSegmentBuilder = ImmutableSet.builder();
              for (i = matches.nextSetBit(0); i >= 0; i = matches.nextSetBit(i + 1)) {
                reader.moveTo(i);
                for (int k = level; k > j; k--) {
                  reader.moveToParent();
                }
                pathSegmentBuilder.add(new PathSegment(reader.getName(), reader.getPathKind() == NodeKind.ARRAY));
              }

              final List<PathSegment> pathSegmentsList = pathSegmentBuilder.build().asList();
              final var pathSegment = pathSegmentsList.get(0);
              final QNm name = pathSegment.name;
              boolean sameName = true;
              for (int k = 1; k < pathSegmentsList.size(); k++) {
                if (pathSegmentsList.get(k).name.atomicCmp(name) != 0) {
                  sameName = false;
                  break;
                }
              }

              org.sirix.api.Axis axis;

              if (pathSegment.isArray) {
                axis = new ConcurrentAxis<>(newRtx, new ChildAxis(newRtx2));
              } else {
                if (j == startLevel && nodeKind == NodeKind.ARRAY) {
                  if (sameName) {
                    axis = new FilterAxis<>(new NestedAxis(new ChildAxis(newRtx2), new ChildAxis(newRtx2)),
                                            new ObjectKeyFilter(newRtx2),
                                            new JsonNameFilter(newRtx2, pathSegment.name));
                  } else {
                    axis = new FilterAxis<>(new NestedAxis(new ChildAxis(newRtx2), new ChildAxis(newRtx2)),
                                            new ObjectKeyFilter(newRtx2));
                  }
                } else {
                  if (sameName) {
                    axis = new FilterAxis<>(new ChildAxis(newRtx2),
                                            new ObjectKeyFilter(newRtx2),
                                            new JsonNameFilter(newRtx2, pathSegment.name));
                  } else {
                    axis = new FilterAxis<>(new ChildAxis(newRtx2), new ObjectKeyFilter(newRtx2));
                  }
                }

                axis = new NestedAxis(new ConcurrentAxis<>(newRtx, axis), new ChildAxis(newRtx));
              }

              axisQueue.push(axis);
            }

            org.sirix.api.Axis axis = axisQueue.pop();
            for (int k = 0, size = axisQueue.size(); k < size; k++) {
              axis = new NestedAxis(axis, axisQueue.pop());
            }
            return getLazySequence(new SirixJsonStream(axis, jsonDBItem.getCollection()));
          } else {
            // Matches on different levels.
            level = startLevel;
            for (i = matches.nextSetBit(0); i >= 0; i = matches.nextSetBit(i + 1)) {
              reader.moveTo(i);
              assert reader.getPathNode() != null;
              final int matchLevel = reader.getPathNode().getLevel();

              // Match at the same level.
              if (matchLevel == level) {
                axisQueue.addLast(new SelfAxis(rtx));
              }
              // Match at the next level (single child-path).
              if (matchLevel == level + 1) {
                final var newConcurrentRtx = resourceManager.beginNodeReadOnlyTrx(revisionNumber);
                newConcurrentRtx.moveTo(nodeKey);
                final var newConcurrentRtx1 = resourceManager.beginNodeReadOnlyTrx(revisionNumber);

                if (reader.getPathNode().getKind() == NodeKind.ARRAY) {
                  axisQueue.addLast(new ConcurrentAxis<>(newConcurrentRtx1, new ChildAxis(rtx)));
                } else {
                  final FilterAxis<JsonNodeReadOnlyTrx> filterAxis;
                  if (nodeKind == NodeKind.ARRAY) {
                    filterAxis = new FilterAxis<>(new NestedAxis(new ChildAxis(newConcurrentRtx),
                                                                 new ChildAxis(newConcurrentRtx)),
                                                  new ObjectKeyFilter(newConcurrentRtx),
                                                  new JsonNameFilter(newConcurrentRtx, recordFieldAsString));

                    axisQueue.addLast(new NestedAxis(new ConcurrentAxis<>(newConcurrentRtx1, filterAxis),
                                                     new ChildAxis(newConcurrentRtx1)));
                  } else {
                    filterAxis = new FilterAxis<>(new ChildAxis(newConcurrentRtx),
                                                  new ObjectKeyFilter(newConcurrentRtx),
                                                  new JsonNameFilter(newConcurrentRtx, recordFieldAsString));

                    axisQueue.addLast(new NestedAxis(new ConcurrentAxis<>(newConcurrentRtx1, filterAxis),
                                                     new ChildAxis(newConcurrentRtx1)));
                  }
                }
              }
              // Match at a level below the child level.
              else {
                final Deque<PathSegment> pathSegments = getPathSegments(matchLevel, level, reader);
                axisQueue.addLast(buildQuery(pathSegments, resourceManager, revisionNumber, nodeKey));
              }
            }

            var newRtx = resourceManager.beginNodeReadOnlyTrx(revisionNumber);
            org.sirix.api.Axis axis = new ConcurrentUnionAxis<>(newRtx, axisQueue.pollFirst(), axisQueue.pollFirst());
            final int size = axisQueue.size();
            for (i = 0; i < size; i++) {
              newRtx = resourceManager.beginNodeReadOnlyTrx(revisionNumber);
              axis = new ConcurrentUnionAxis<>(newRtx, axis, axisQueue.pollFirst());
            }
            return getLazySequence(new SirixJsonStream(axis, jsonDBItem.getCollection()));
          }
        }
      } catch (final SirixException e) {
        throw new QueryException(new QNm(e.getMessage()), e);
      }
    } else if (rtx.getChildCount() == 0) {
      return null;
    } else {
      final var filterAxis =
          new NestedAxis(new FilterAxis<>(new DescendantAxis(rtx), new JsonNameFilter(rtx, recordFieldAsString)),
                         new ChildAxis(rtx));
      return getLazySequence(new SirixJsonStream(filterAxis, jsonDBItem.getCollection()));
    }
  }

  // Get all path segments on the path up to level.
  private static Deque<PathSegment> getPathSegments(final @NonNegative int matchLevel, final @NonNegative int level,
      final PathSummaryReader reader) {
    // Match at a level below this level which is not a direct child.
    final Deque<PathSegment> pathSegments = new ArrayDeque<>(matchLevel - level);
    for (int i = matchLevel; i > level; i--) {
      pathSegments.push(new PathSegment(reader.getName(), reader.getPathKind() == NodeKind.ARRAY));
      reader.moveToParent();
    }
    return pathSegments;
  }

  @NotNull
  private SirixJsonLazySequence getLazySequence(final SirixJsonStream stream) {
    return new SirixJsonLazySequence(stream);
  }

  // Build the query.
  private static org.sirix.api.Axis buildQuery(final Deque<PathSegment> pathSegments,
      final ResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager, final int revisionNumber,
      final long nodeKey) {
    final var concurrentRtx = resourceManager.beginNodeReadOnlyTrx(revisionNumber);
    concurrentRtx.moveTo(nodeKey);
    final var concurrentRtx1 = resourceManager.beginNodeReadOnlyTrx(revisionNumber);
    var pathSegment = pathSegments.pop();
    org.sirix.api.Axis axis;
    if (pathSegments.isEmpty()) {
      if (pathSegment.isArray()) {
        axis = new ConcurrentAxis<>(concurrentRtx1, new ChildAxis(concurrentRtx));
      } else {
        if (concurrentRtx.getKind() == NodeKind.ARRAY) {
          axis = new FilterAxis<>(new NestedAxis(new ChildAxis(concurrentRtx), new ChildAxis(concurrentRtx)),
                                  new ObjectKeyFilter(concurrentRtx),
                                  new JsonNameFilter(concurrentRtx, pathSegment.name));
          axis = new NestedAxis(new ConcurrentAxis<>(concurrentRtx1, axis), new ChildAxis(concurrentRtx1));
        } else {
          axis = new FilterAxis<>(new ChildAxis(concurrentRtx),
                                  new ObjectKeyFilter(concurrentRtx),
                                  new JsonNameFilter(concurrentRtx, pathSegment.name));
          axis = new NestedAxis(new ConcurrentAxis<>(concurrentRtx1, axis), new ChildAxis(concurrentRtx1));
        }
      }
    } else {
      if (pathSegment.isArray()) {
        axis = new ChildAxis(concurrentRtx);
      } else {
        if (concurrentRtx.getKind() == NodeKind.ARRAY) {
          axis = new FilterAxis<>(new NestedAxis(new ChildAxis(concurrentRtx), new ChildAxis(concurrentRtx)),
                                  new ObjectKeyFilter(concurrentRtx),
                                  new JsonNameFilter(concurrentRtx, pathSegment.name));
          axis = new NestedAxis(axis, new ChildAxis(concurrentRtx));
        } else {
          axis = new FilterAxis<>(new ChildAxis(concurrentRtx),
                                  new ObjectKeyFilter(concurrentRtx),
                                  new JsonNameFilter(concurrentRtx, pathSegment.name));
          axis = new NestedAxis(axis, new ChildAxis(concurrentRtx));
        }
      }
    }
    for (int i = 0, size = pathSegments.size(); i < size; i++) {
      pathSegment = pathSegments.pop();

      if (!pathSegment.isArray) {
        axis = new NestedAxis(axis,
                              new FilterAxis<>(new ChildAxis(concurrentRtx),
                                               new ObjectKeyFilter(concurrentRtx),
                                               new JsonNameFilter(concurrentRtx, pathSegment.name)));
      }

      if (i == size - 1) {
        axis = new NestedAxis(new ConcurrentAxis<>(concurrentRtx1, axis), new ChildAxis(concurrentRtx1));
      } else {
        axis = new NestedAxis(axis, new ChildAxis(concurrentRtx));
      }
    }

    return axis;
  }

  private LazySequence getLazySequence(final QueryContext ctx, final Tuple tuple, final Iter iter) {
    return new LazySequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          private SirixJsonLazySequence lazySequence;
          private Iter lazySequenceIter;
          private final Map<NodeKind, SirixJsonLazySequence> nodeKindToLazySequence = new HashMap<>();

          @Override
          public Item next() {
            if (lazySequenceIter != null) {
              final Item item = lazySequenceIter.next();

              if (item != null) {
                return item;
              }
            }

            Item item;
            while ((item = iter.next()) != null) {
              if (!(item instanceof Array) && !(item instanceof Object)) {
                continue;
              }

              Item itemField = field.evaluateToItem(ctx, tuple);
              if (itemField == null) {
                continue;
              }

              final var recordFieldAsString = getRecordFieldAsString(itemField);
              final var jsonDBItem = (JsonDBItem) item;
              final var rtx = jsonDBItem.getTrx();
              rtx.moveTo(jsonDBItem.getNodeKey());

              if (lazySequence == null || nodeKindToLazySequence.get(rtx.getKind()) == null) {
                lazySequence = getLazySequence(recordFieldAsString, jsonDBItem, rtx);
                nodeKindToLazySequence.put(rtx.getKind(), lazySequence);
                if (lazySequence == null) {
                  continue;
                }
              } else {
                lazySequence.getAxis().reset(rtx.getNodeKey());
              }

              lazySequenceIter = lazySequence.iterate();
              Item currItem;
              while ((currItem = lazySequenceIter.next()) != null) {
                if (currItem != null) {
                  return currItem;
                }
              }
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

  private String getRecordFieldAsString(Item itemField) {
    if (itemField instanceof QNm qNmField) {
      return qNmField.stringValue();
    } else if (itemField instanceof Atomic atomicField) {
      return atomicField.stringValue();
    } else {
      throw new QueryException(Bits.BIT_ILLEGAL_OBJECT_FIELD, "Illegal object itemField reference: %s", itemField);
    }
  }

  @Override
  public Item evaluateToItem(QueryContext ctx, Tuple tuple) throws QueryException {
    return ExprUtil.asItem(evaluate(ctx, tuple));
  }

  @Override
  public boolean isUpdating() {
    if (context.isUpdating()) {
      return true;
    }
    return field.isUpdating();
  }

  @Override
  public boolean isVacuous() {
    return false;
  }

  public String toString() {
    return "==>" + field;
  }
}
