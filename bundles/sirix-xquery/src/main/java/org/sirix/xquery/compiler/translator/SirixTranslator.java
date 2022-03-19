package org.sirix.xquery.compiler.translator;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.Bits;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.translator.TopDownTranslator;
import org.brackit.xquery.expr.Accessor;
import org.brackit.xquery.node.stream.EmptyStream;
import org.brackit.xquery.sequence.BaseIter;
import org.brackit.xquery.sequence.ItemSequence;
import org.brackit.xquery.sequence.LazySequence;
import org.brackit.xquery.util.Cfg;
import org.brackit.xquery.util.ExprUtil;
import org.brackit.xquery.xdm.*;
import org.brackit.xquery.xdm.json.Object;
import org.brackit.xquery.xdm.node.Node;
import org.brackit.xquery.xdm.type.NodeType;
import org.checkerframework.checker.index.qual.NonNegative;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.*;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.json.JsonNameFilter;
import org.sirix.axis.filter.json.ObjectKeyFilter;
import org.sirix.axis.filter.xml.*;
import org.sirix.axis.temporal.*;
import org.sirix.exception.SirixException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.NodeKind;
import org.sirix.service.xml.xpath.expr.UnionAxis;
import org.sirix.xquery.compiler.XQExt;
import org.sirix.xquery.compiler.expression.IndexExpr;
import org.sirix.xquery.json.JsonDBObject;
import org.sirix.xquery.node.XmlDBNode;
import org.sirix.xquery.stream.json.SirixJsonStream;
import org.sirix.xquery.stream.node.SirixNodeStream;
import org.sirix.xquery.stream.node.TemporalSirixNodeStream;

import java.util.*;

/**
 * Translates queries (optimizes currently path-expressions if {@code OPTIMIZE} is set to true).
 *
 * @author Johannes Lichtenberger
 */
public final class SirixTranslator extends TopDownTranslator {

  /**
   * Optimize accessors or not.
   */
  public static final boolean OPTIMIZE = Cfg.asBool("org.sirix.xquery.optimize.accessor", true);

  /**
   * Number of children (needed as a threshold to lookup in path summary if a path exists at all).
   */
  public static final int CHILD_THRESHOLD = Cfg.asInt("org.sirix.xquery.optimize.child.threshold", 1);

  /**
   * Constructor.
   *
   * @param options options map
   */
  public SirixTranslator(final Map<QNm, Str> options) {
    super(options);
  }

  protected Expr anyExpr(AST node) throws QueryException {
    if (node.getType() == XQExt.IndexExpr) {
      return indexExpr(node);
    } else if (node.getType() == XQ.DerefDescendantExpr) {
      return derefDescendantExpr(node);
    }
    return super.anyExpr(node);
  }

  protected Expr derefDescendantExpr(AST node) throws QueryException {
    Expr object = expr(node.getChild(0), true);
    Expr field = expr(node.getChild(1), true);
    return new DerefDescendant(object, field);
  }

  private Expr indexExpr(AST node) {
    return new IndexExpr(node.getProperties());
  }

  @Override
  protected Accessor axis(final AST node) {
    if (!OPTIMIZE) {
      return super.axis(node);
    }
    return switch (node.getType()) {
      case XQ.DESCENDANT -> new DescOrSelf(Axis.DESCENDANT);
      case XQ.DESCENDANT_OR_SELF -> new DescOrSelf(Axis.DESCENDANT_OR_SELF);
      case XQ.CHILD -> new Child(Axis.CHILD);
      case XQ.ATTRIBUTE -> new Attribute(Axis.ATTRIBUTE);
      case XQ.PARENT -> new Parent(Axis.PARENT);
      case XQ.ANCESTOR -> new AncestorOrSelf(Axis.ANCESTOR);
      case XQ.ANCESTOR_OR_SELF -> new AncestorOrSelf(Axis.ANCESTOR_OR_SELF);
      case XQ.FOLLOWING -> new Following(Axis.FOLLOWING);
      case XQ.FOLLOWING_SIBLING -> new FollowingSibling(Axis.FOLLOWING_SIBLING);
      case XQ.PRECEDING -> new Preceding(Axis.PRECEDING);
      case XQ.PRECEDING_SIBLING -> new PrecedingSibling(Axis.PRECEDING_SIBLING);
      case XQ.FUTURE -> new Future(Axis.FUTURE);
      case XQ.FUTURE_OR_SELF -> new Future(Axis.FUTURE_OR_SELF);
      case XQ.PAST -> new Past(Axis.PAST);
      case XQ.PAST_OR_SELF -> new Past(Axis.PAST_OR_SELF);
      case XQ.PREVIOUS -> new Previous(Axis.PREVIOUS);
      case XQ.NEXT -> new Next(Axis.NEXT);
      case XQ.ALL_TIMES -> new AllTime(Axis.ALL_TIME);
      case XQ.FIRST -> new First(Axis.FIRST);
      case XQ.LAST -> new Last(Axis.LAST);
      default -> super.axis(node);
    };
  }

  /**
   * {@code first::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class Last extends Accessor {
    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public Last(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis = new LastAxis<>(rtx.getResourceManager(), rtx);
      return new TemporalSirixNodeStream(SirixTranslator.getTemporalAxis(test, rtx, axis), dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis = new LastAxis<>(rtx.getResourceManager(), rtx);
      return new TemporalSirixNodeStream(axis, dbNode.getCollection());
    }
  }

  /**
   * {@code first::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class First extends Accessor {
    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public First(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis = new FirstAxis<>(rtx.getResourceManager(), rtx);
      return new TemporalSirixNodeStream(SirixTranslator.getTemporalAxis(test, rtx, axis), dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis = new FirstAxis<>(rtx.getResourceManager(), rtx);
      return new TemporalSirixNodeStream(axis, dbNode.getCollection());
    }
  }

  /**
   * {@code next::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class Next extends Accessor {
    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public Next(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis = new NextAxis<>(rtx.getResourceManager(), rtx);
      return new TemporalSirixNodeStream(SirixTranslator.getTemporalAxis(test, rtx, axis), dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis = new NextAxis<>(rtx.getResourceManager(), rtx);
      return new TemporalSirixNodeStream(axis, dbNode.getCollection());
    }
  }

  /**
   * {@code previous::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class Previous extends Accessor {
    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public Previous(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
          new PreviousAxis<>(rtx.getResourceManager(), rtx);
      return new TemporalSirixNodeStream(SirixTranslator.getTemporalAxis(test, rtx, axis), dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
          new PreviousAxis<>(rtx.getResourceManager(), rtx);
      return new TemporalSirixNodeStream(axis, dbNode.getCollection());
    }
  }

  /**
   * {@code all-time::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class AllTime extends Accessor {
    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public AllTime(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
          new AllTimeAxis<>(rtx.getResourceManager(), rtx);
      return new TemporalSirixNodeStream(SirixTranslator.getTemporalAxis(test, rtx, axis), dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
          new AllTimeAxis<>(rtx.getResourceManager(), rtx);
      return new TemporalSirixNodeStream(axis, dbNode.getCollection());
    }
  }

  /**
   * {@code past::} and {@code past-or-self::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class Past extends Accessor {
    /**
     * Determine if self is included or not.
     */
    private final IncludeSelf mSelf;

    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public Past(final Axis axis) {
      super(axis);
      mSelf = axis == Axis.PAST ? IncludeSelf.NO : IncludeSelf.YES;
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
          new PastAxis<>(rtx.getResourceManager(), rtx, mSelf);
      return new TemporalSirixNodeStream(SirixTranslator.getTemporalAxis(test, rtx, axis), dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
          new PastAxis<>(rtx.getResourceManager(), rtx, mSelf);
      return new TemporalSirixNodeStream(axis, dbNode.getCollection());
    }
  }

  /**
   * {@code future::} and {@code future-or-self::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class Future extends Accessor {
    /**
     * Determine if self is included or not.
     */
    private final IncludeSelf includeSelf;

    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public Future(final Axis axis) {
      super(axis);
      includeSelf = axis == Axis.FUTURE ? IncludeSelf.NO : IncludeSelf.YES;
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
          new FutureAxis<>(rtx.getResourceManager(), rtx, includeSelf);
      return new TemporalSirixNodeStream(SirixTranslator.getTemporalAxis(test, rtx, axis), dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
          new FutureAxis<>(rtx.getResourceManager(), rtx, includeSelf);
      return new TemporalSirixNodeStream(axis, dbNode.getCollection());
    }
  }

  /**
   * {@code preceding::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class Preceding extends Accessor {
    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public Preceding(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(SirixTranslator.getAxis(test, rtx, new PrecedingAxis(rtx)), dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(new PrecedingAxis(rtx), dbNode.getCollection());
    }
  }

  /**
   * {@code preceding-sibling::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class PrecedingSibling extends Accessor {
    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public PrecedingSibling(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(SirixTranslator.getAxis(test, rtx, new PrecedingSiblingAxis(rtx)),
                                 dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(new PrecedingSiblingAxis(rtx), dbNode.getCollection());
    }
  }

  /**
   * {@code following-sibling::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class FollowingSibling extends Accessor {
    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public FollowingSibling(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(SirixTranslator.getAxis(test, rtx, new FollowingSiblingAxis(rtx)),
                                 dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(new FollowingSiblingAxis(rtx), dbNode.getCollection());
    }
  }

  /**
   * {@code following::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class Following extends Accessor {
    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public Following(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(SirixTranslator.getAxis(test, rtx, new FollowingAxis(rtx)), dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(new FollowingAxis(rtx), dbNode.getCollection());
    }
  }

  /**
   * {@code ancestor::} and {@code ancestor-or-self::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class AncestorOrSelf extends Accessor {
    /**
     * Determine if self is included or not.
     */
    private final IncludeSelf includeSelf;

    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public AncestorOrSelf(final Axis axis) {
      super(axis);
      includeSelf = axis == Axis.ANCESTOR ? IncludeSelf.NO : IncludeSelf.YES;
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(SirixTranslator.getAxis(test, rtx, new AncestorAxis(rtx, includeSelf)),
                                 dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(new AncestorAxis(rtx, includeSelf), dbNode.getCollection());
    }
  }

  /**
   * {@code attribute::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class Parent extends Accessor {
    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public Parent(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(SirixTranslator.getAxis(test, rtx, new ParentAxis(rtx)), dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(new ParentAxis(rtx), dbNode.getCollection());
    }
  }

  /**
   * {@code attribute::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class Attribute extends Accessor {
    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public Attribute(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(new FilterAxis<>(new AttributeAxis(rtx), new XmlNameFilter(rtx, test.getQName())),
                                 dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(new AttributeAxis(rtx), dbNode.getCollection());
    }
  }

  /**
   * {@code child::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class Child extends Accessor {
    /**
     * Map with PCR <=> matching nodes.
     */
    private final Map<Long, BitSet> filterMap;

    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public Child(final Axis axis) {
      super(axis);
      filterMap = new HashMap<>();
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      if (rtx.getResourceManager().getResourceConfig().withPathSummary && test.getNodeKind() == Kind.ELEMENT
          && test.getQName() != null && rtx.getChildCount() > CHILD_THRESHOLD) {
        try {
          final long pcr = dbNode.getPCR();
          BitSet matches = filterMap.get(pcr);
          final PathSummaryReader reader = rtx.getResourceManager().openPathSummary(rtx.getRevisionNumber());
          if (matches == null) {
            reader.moveTo(pcr);
            final int level = reader.getLevel() + 1;
            final QNm name = test.getQName();
            matches = reader.match(name, level);
            filterMap.put(pcr, matches);
          }
          // No matches.
          if (matches.cardinality() == 0) {
            reader.close();
            return new EmptyStream<>();
          }
          reader.close();
        } catch (final SirixException e) {
          throw new QueryException(new QNm(e.getMessage()), e);
        }
      }

      return new SirixNodeStream(SirixTranslator.getAxis(test, rtx, new ChildAxis(rtx)), dbNode.getCollection());
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(new ChildAxis(rtx), dbNode.getCollection());
    }
  }

  /**
   * {@code descendant::} and {@code descendant-or-self::} path-optimizations.
   *
   * @author Johannes Lichtenberger
   */
  private static final class DescOrSelf extends Accessor {
    /**
     * Determines if current node is included (-or-self part).
     */
    private final IncludeSelf self;

    /**
     * Map with PCR <=> matching nodes.
     */
    private final Map<Long, BitSet> mFilterMap;

    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    public DescOrSelf(final Axis axis) {
      super(axis);
      self = axis == Axis.DESCENDANT_OR_SELF ? IncludeSelf.YES : IncludeSelf.NO;
      mFilterMap = new HashMap<>();
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      if (rtx.getResourceManager().getResourceConfig().withPathSummary && test.getNodeKind() == Kind.ELEMENT
          && test.getQName() != null) {
        try {
          final long pcr = dbNode.getPCR();
          BitSet matches = mFilterMap.get(pcr);
          final PathSummaryReader reader = rtx.getResourceManager().openPathSummary(rtx.getRevisionNumber());
          if (matches == null) {
            reader.moveTo(pcr);
            final int level = self == IncludeSelf.YES ? reader.getLevel() : reader.getLevel() + 1;
            final QNm name = test.getQName();
            matches = reader.match(name, level);
            mFilterMap.put(pcr, matches);
          }
          // No matches.
          if (matches.cardinality() == 0) {
            reader.close();
            return new EmptyStream<>();
          }
          // One match.
          if (matches.cardinality() == 1) {
            final int level = getLevel(dbNode);
            final long pcr2 = matches.nextSetBit(0);
            reader.moveTo(pcr2);
            assert reader.getPathNode() != null;
            final int matchLevel = reader.getPathNode().getLevel();
            // Match at the same level.
            if (self == IncludeSelf.YES && matchLevel == level) {
              reader.close();
              return new SirixNodeStream(new SelfAxis(rtx), dbNode.getCollection());
            }
            // Match at the next level (single child-path).
            if (matchLevel == level + 1) {
              reader.close();
              return new SirixNodeStream(new FilterAxis<>(new ChildAxis(rtx),
                                                          new ElementFilter(rtx),
                                                          new XmlNameFilter(rtx, test.getQName().toString())),
                                         dbNode.getCollection());
            }
            // Match at a level below the child level.
            final Deque<QNm> names = getNames(matchLevel, level, reader);
            reader.close();
            return new SirixNodeStream(buildQuery(rtx, names), dbNode.getCollection());
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
          // Matches on same level.
          final Deque<org.sirix.api.Axis> axisQueue = new ArrayDeque<>(matches.cardinality());
          if (onSameLevel) {
            for (int j = level, nodeLevel = getLevel(dbNode); j > nodeLevel; j--) {
              // Build an immutable set and turn it into a list for sorting.
              final Builder<QNm> pathNodeQNmBuilder = ImmutableSet.builder();
              for (i = matches.nextSetBit(0); i >= 0; i = matches.nextSetBit(i + 1)) {
                reader.moveTo(i);
                for (int k = level; k > j; k--) {
                  reader.moveToParent();
                }
                pathNodeQNmBuilder.add(Objects.requireNonNull(reader.getName()));
              }
              final List<QNm> pathNodeQNmsList = pathNodeQNmBuilder.build().asList();
              final QNm name = pathNodeQNmsList.get(0);
              boolean sameName = true;
              for (int k = 1; k < pathNodeQNmsList.size(); k++) {
                if (pathNodeQNmsList.get(k).atomicCmp(name) != 0) {
                  sameName = false;
                  break;
                }
              }

              axisQueue.push(sameName
                                 ? new FilterAxis<>(new ChildAxis(rtx),
                                                    new ElementFilter(rtx),
                                                    new XmlNameFilter(rtx, name))
                                 : new FilterAxis<>(new ChildAxis(rtx), new ElementFilter(rtx)));
            }

            org.sirix.api.Axis axis = axisQueue.pop();
            for (int k = 0, size = axisQueue.size(); k < size; k++) {
              axis = new NestedAxis(axis, axisQueue.pop());
            }
            reader.close();
            return new SirixNodeStream(axis, dbNode.getCollection());
          } else {
            // Matches on different levels.
            // TODO: Use ConcurrentUnionAxis.
            level = getLevel(dbNode);
            for (i = matches.nextSetBit(0); i >= 0; i = matches.nextSetBit(i + 1)) {
              reader.moveTo(i);
              assert reader.getPathNode() != null;
              final int matchLevel = reader.getPathNode().getLevel();

              // Match at the same level.
              if (self == IncludeSelf.YES && matchLevel == level) {
                axisQueue.addLast(new SelfAxis(rtx));
              }
              // Match at the next level (single child-path).
              else if (matchLevel == level + 1) {
                axisQueue.addLast(new FilterAxis<>(new ChildAxis(rtx),
                                                   new ElementFilter(rtx),
                                                   new XmlNameFilter(rtx, test.getQName().toString())));
              }
              // Match at a level below the child level.
              else {
                final Deque<QNm> names = getNames(matchLevel, level, reader);
                axisQueue.addLast(buildQuery(rtx, names));
              }
            }
            org.sirix.api.Axis axis = new UnionAxis(rtx, axisQueue.pollFirst(), axisQueue.pollFirst());
            final int size = axisQueue.size();
            for (i = 0; i < size; i++) {
              axis = new UnionAxis(rtx, axis, axisQueue.pollFirst());
            }
            reader.close();
            return new SirixNodeStream(axis, dbNode.getCollection());
          }
        } catch (final SirixException e) {
          throw new QueryException(new QNm(e.getMessage()), e);
        }
      }
      return super.performStep(node, test);
    }

    // Get all names on the path up to level.
    private static Deque<QNm> getNames(final @NonNegative int matchLevel, final @NonNegative int level,
        final PathSummaryReader reader) {
      // Match at a level below this level which is not a direct child.
      final Deque<QNm> names = new ArrayDeque<>(matchLevel - level);
      for (int i = matchLevel; i > level; i--) {
        names.push(reader.getName());
        reader.moveToParent();
      }
      return names;
    }

    // Build the query.
    private static org.sirix.api.Axis buildQuery(final XmlNodeReadOnlyTrx rtx, final Deque<QNm> names) {
      org.sirix.api.Axis axis =
          new FilterAxis<>(new ChildAxis(rtx), new ElementFilter(rtx), new XmlNameFilter(rtx, names.pop()));
      for (int i = 0, size = names.size(); i < size; i++) {
        axis = new NestedAxis(axis,
                              new FilterAxis<>(new ChildAxis(rtx),
                                               new ElementFilter(rtx),
                                               new XmlNameFilter(rtx, names.pop())));
      }
      return axis;
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(new DescendantAxis(rtx, self), dbNode.getCollection());
    }
  }

  private static int getLevel(XmlDBNode dbNode) {
    return dbNode.getDeweyID().getLevel();
  }

  private static org.sirix.axis.AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> getTemporalAxis(
      final NodeType test, final XmlNodeReadOnlyTrx trx,
      final org.sirix.axis.AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> innerAxis) {
    final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis;

    switch (test.getNodeKind()) {
      case COMMENT:
        axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new CommentFilter(trx));
        break;
      case PROCESSING_INSTRUCTION:
        if (test.getQName() == null) {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new PIFilter(trx));
        } else {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis,
                                                     new PIFilter(trx),
                                                     new XmlNameFilter(trx, test.getQName()));
        }
        break;
      case ELEMENT:
        if (test.getQName() == null) {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new ElementFilter(trx));
        } else {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis,
                                                     new ElementFilter(trx),
                                                     new XmlNameFilter(trx, test.getQName()));
        }
        break;
      case TEXT:
        axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new TextFilter(trx));
        break;
      case NAMESPACE:
        if (test.getQName() == null) {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new NamespaceFilter(trx));
        } else {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis,
                                                     new NamespaceFilter(trx),
                                                     new XmlNameFilter(trx, test.getQName()));
        }
        break;
      case ATTRIBUTE:
        if (test.getQName() == null) {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis, new AttributeFilter(trx));
        } else {
          axis = new TemporalXmlNodeReadFilterAxis<>(innerAxis,
                                                     new AttributeFilter(trx),
                                                     new XmlNameFilter(trx, test.getQName()));
        }
        break;
      case DOCUMENT:
        return new TemporalXmlNodeReadFilterAxis<>(innerAxis, new DocumentRootNodeFilter(trx));
      default:
        throw new AssertionError(); // Must not happen.
    }

    return axis;
  }

  private static org.sirix.api.Axis getAxis(final NodeType test, final XmlNodeReadOnlyTrx trx,
      final org.sirix.api.Axis innerAxis) {
    final FilterAxis<XmlNodeReadOnlyTrx> axis;

    switch (test.getNodeKind()) {
      case COMMENT:
        axis = new FilterAxis<>(innerAxis, new CommentFilter(trx));
        break;
      case PROCESSING_INSTRUCTION:
        if (test.getQName() == null) {
          axis = new FilterAxis<>(innerAxis, new PIFilter(trx));
        } else {
          axis = new FilterAxis<>(innerAxis, new PIFilter(trx), new XmlNameFilter(trx, test.getQName()));
        }
        break;
      case ELEMENT:
        if (test.getQName() == null) {
          axis = new FilterAxis<>(innerAxis, new ElementFilter(trx));
        } else {
          axis = new FilterAxis<>(innerAxis, new ElementFilter(trx), new XmlNameFilter(trx, test.getQName()));
        }
        break;
      case TEXT:
        axis = new FilterAxis<>(innerAxis, new TextFilter(trx));
        break;
      case NAMESPACE:
        if (test.getQName() == null) {
          axis = new FilterAxis<>(innerAxis, new NamespaceFilter(trx));
        } else {
          axis = new FilterAxis<>(innerAxis, new NamespaceFilter(trx), new XmlNameFilter(trx, test.getQName()));
        }
        break;
      case ATTRIBUTE:
        if (test.getQName() == null) {
          axis = new FilterAxis<>(innerAxis, new AttributeFilter(trx));
        } else {
          axis = new FilterAxis<>(innerAxis, new AttributeFilter(trx), new XmlNameFilter(trx, test.getQName()));
        }
        break;
      case DOCUMENT:
        axis = new FilterAxis<>(innerAxis, new DocumentRootNodeFilter(trx));
        break;
      default:
        throw new AssertionError(); // Must not happen.
    }

    return axis;
  }

  private static class SirixJsonLazySequence extends LazySequence {

    private final SirixJsonStream stream;

    SirixJsonLazySequence(final SirixJsonStream stream) {
      this.stream = stream;
    }

    org.sirix.api.Axis getAxis() {
      return stream.getAxis();
    }

    @Override
    public Iter iterate() {
      return new BaseIter() {
        @Override
        public Item next() {
          return stream.next();
        }

        @Override
        public void close() {
        }
      };
    }
  }

  private static class DerefDescendant implements Expr {

    private record PathSegment(QNm name, boolean isArray) {
    }

    private record PathData(BitSet matchingPathNodeKeys, int level) {
    }

    /**
     * Map with PCR <=> matching nodes.
     */
    private final Map<Long, PathData> filterMap;

    private final Expr object;
    private final Expr field;

    public DerefDescendant(Expr object, Expr field) {
      this.object = object;
      this.field = field;
      filterMap = new HashMap<>();
    }

    @Override
    public Sequence evaluate(QueryContext ctx, Tuple tuple) throws QueryException {
      Sequence sequence = object.evaluate(ctx, tuple);

      if (sequence instanceof ItemSequence itemSequence) {
        return getLazySequence(ctx, tuple, itemSequence.iterate());
      }

      if (sequence instanceof LazySequence lazySequence) {
        return getLazySequence(ctx, tuple, lazySequence.iterate());
      }

      if (!(sequence instanceof Object object)) {
        return null;
      }

      Item itemField = field.evaluateToItem(ctx, tuple);
      if (itemField == null) {
        return null;
      }

      final var recordFieldAsString = getRecordFieldAsString(itemField);
      final var jsonDBObject = (JsonDBObject) object;
      final var rtx = jsonDBObject.getTrx();

      return getLazySequence(recordFieldAsString, jsonDBObject, rtx);
    }

    @SuppressWarnings("ConstantConditions")
    @Nullable
    private SirixJsonLazySequence getLazySequence(String recordFieldAsString, JsonDBObject jsonDBObject,
        JsonNodeReadOnlyTrx rtx) {
      if (rtx.getResourceManager().getResourceConfig().withPathSummary && rtx.hasChildren()) {
        try {
          rtx.moveToParent();
          final long pcr = rtx.isDocumentRoot() ? 0 : rtx.getPathNodeKey();
          rtx.moveTo(jsonDBObject.getNodeKey());
          final PathData data = filterMap.get(pcr);
          BitSet matches = data == null ? null : data.matchingPathNodeKeys;
          int startLevel = data == null ? 0 : data.level;
          try (final PathSummaryReader reader = rtx.getResourceManager().openPathSummary(rtx.getRevisionNumber())) {
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
              final int level = startLevel;
              final long pcr2 = matches.nextSetBit(0);
              reader.moveTo(pcr2);
              assert reader.getPathNode() != null;
              final int matchLevel = reader.getPathNode().getLevel();

              // Match at the same level.
              if (matchLevel == level) {
                return getLazySequence(new SirixJsonStream(new SelfAxis(rtx), jsonDBObject.getCollection()));
              }
              // Match at the next level (single child-path).
              if (matchLevel == level + 1) {
                return getLazySequence(new SirixJsonStream(new NestedAxis(new FilterAxis<>(new ChildAxis(rtx),
                                                                                           new ObjectKeyFilter(rtx),
                                                                                           new JsonNameFilter(rtx,
                                                                                                              recordFieldAsString)),
                                                                          new ChildAxis(rtx)),
                                                           jsonDBObject.getCollection()));
              }
              // Match at a level below the child level.
              final Deque<PathSegment> pathSegments = getPathSegments(matchLevel, level, reader);
              return getLazySequence(new SirixJsonStream(buildQuery(rtx, pathSegments), jsonDBObject.getCollection()));
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
            // Matches on same level.
            final Deque<org.sirix.api.Axis> axisQueue = new ArrayDeque<>(matches.cardinality());
            if (onSameLevel) {
              for (int j = level, nodeLevel = startLevel; j > nodeLevel; j--) {
                // Build an immutable set and turn it into a list for sorting.
                final Builder<PathSegment> pathSegmentBuilder = ImmutableSet.builder();
                for (i = matches.nextSetBit(0); i >= 0; i = matches.nextSetBit(i + 1)) {
                  reader.moveTo(i);
                  for (int k = level; k > j; k--) {
                    reader.moveToParent();
                  }
                  pathSegmentBuilder.add(Objects.requireNonNull(new PathSegment(reader.getName(),
                                                                                reader.getPathKind()
                                                                                    == NodeKind.ARRAY)));
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
                  axis = new ChildAxis(rtx);
                } else {
                  if (sameName) {
                    axis = new FilterAxis<>(new ChildAxis(rtx), new ObjectKeyFilter(rtx), new JsonNameFilter(rtx, pathSegment.name));
                  } else {
                    axis = new FilterAxis<>(new ChildAxis(rtx), new ObjectKeyFilter(rtx));
                  }

                  axis = new NestedAxis(axis, new ChildAxis(rtx));
                }

                axisQueue.push(axis);
              }

              org.sirix.api.Axis axis = axisQueue.pop();
              for (int k = 0, size = axisQueue.size(); k < size; k++) {
                axis = new NestedAxis(axis, axisQueue.pop());
              }
              return getLazySequence(new SirixJsonStream(axis, jsonDBObject.getCollection()));
            } else {
              // Matches on different levels.
              // TODO: Use ConcurrentUnionAxis.
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
                  axisQueue.addLast(new NestedAxis(new FilterAxis<>(new ChildAxis(rtx),
                                                                    new ObjectKeyFilter(rtx),
                                                                    new JsonNameFilter(rtx, recordFieldAsString)),
                                                   new ChildAxis(rtx)));
                }
                // Match at a level below the child level.
                else {
                  final Deque<PathSegment> pathSegments = getPathSegments(matchLevel, level, reader);
                  axisQueue.addLast(buildQuery(rtx, pathSegments));
                }
              }

              org.sirix.api.Axis axis = new UnionAxis(rtx, axisQueue.pollFirst(), axisQueue.pollFirst());
              final int size = axisQueue.size();
              for (i = 0; i < size; i++) {
                axis = new UnionAxis(rtx, axis, axisQueue.pollFirst());
              }
              return getLazySequence(new SirixJsonStream(axis, jsonDBObject.getCollection()));
            }
          }
        } catch (final SirixException e) {
          throw new QueryException(new QNm(e.getMessage()), e);
        }
      } else if (rtx.getChildCount() == 0) {
        return null;
      } else {
        final var filterAxis =
            new NestedAxis(new FilterAxis(new DescendantAxis(rtx), new JsonNameFilter(rtx, recordFieldAsString)),
                           new ChildAxis(rtx));
        return getLazySequence(new SirixJsonStream(filterAxis, jsonDBObject.getCollection()));
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
    private static org.sirix.api.Axis buildQuery(final JsonNodeReadOnlyTrx rtx, final Deque<PathSegment> pathSegments) {
      var pathSegment = pathSegments.pop();
      org.sirix.api.Axis axis =
          new FilterAxis<>(new ChildAxis(rtx), new ObjectKeyFilter(rtx), new JsonNameFilter(rtx, pathSegment.name));
      axis = new NestedAxis(axis, new ChildAxis(rtx));
      for (int i = 0, size = pathSegments.size(); i < size; i++) {
        pathSegment = pathSegments.pop();

        if (!pathSegment.isArray) {
          axis = new NestedAxis(axis,
                                new FilterAxis(new ChildAxis(rtx),
                                               new ObjectKeyFilter(rtx),
                                               new JsonNameFilter(rtx, pathSegment.name)));
        }

        axis = new NestedAxis(axis, new ChildAxis(rtx));
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
                if (!(item instanceof Object object)) {
                  continue;
                }

                Item itemField = field.evaluateToItem(ctx, tuple);
                if (itemField == null) {
                  continue;
                }

                final var recordFieldAsString = getRecordFieldAsString(itemField);
                final var jsonDBObject = (JsonDBObject) object;
                final var rtx = jsonDBObject.getTrx();

                if (lazySequence == null) {
                  lazySequence = getLazySequence(recordFieldAsString, jsonDBObject, rtx);
                  lazySequenceIter = lazySequence.iterate();
                } else {
                  lazySequence.getAxis().reset(rtx.getNodeKey());
                  lazySequenceIter = lazySequence.iterate();
                }

                final Item currItem = lazySequenceIter.next();

                if (currItem != null) {
                  return currItem;
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
      if (object.isUpdating()) {
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
}
