package io.sirix.query.compiler.translator;

import java.util.LinkedHashSet;
import java.util.Set;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.axis.AbstractAxis;
import io.sirix.axis.AncestorAxis;
import io.sirix.axis.AttributeAxis;
import io.sirix.axis.ChildAxis;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.FollowingAxis;
import io.sirix.axis.FollowingSiblingAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.NestedAxis;
import io.sirix.axis.ParentAxis;
import io.sirix.axis.PrecedingAxis;
import io.sirix.axis.PrecedingSiblingAxis;
import io.sirix.axis.SelfAxis;
import io.sirix.axis.filter.AbstractFilter;
import io.sirix.axis.filter.FilterAxis;
import io.sirix.axis.filter.xml.AttributeFilter;
import io.sirix.axis.filter.xml.CommentFilter;
import io.sirix.axis.filter.xml.DocumentRootNodeFilter;
import io.sirix.axis.filter.xml.ElementFilter;
import io.sirix.axis.filter.xml.TextFilter;
import io.sirix.axis.filter.xml.XmlNameFilter;
import io.sirix.exception.SirixException;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.page.NamePage;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.compiler.expression.IndexExpr;
import io.sirix.query.compiler.expression.GuardedConjunctExpr;
import io.sirix.query.compiler.expression.ConjunctInputs;
import io.sirix.query.compiler.optimizer.CheapFirstConjunctStage;
import io.sirix.query.compiler.expression.VectorizedPipelineExpr;
import io.sirix.query.function.jn.index.scan.ScanValidTimeIndex;
import io.sirix.query.function.jn.temporal.OpenBitemporal;
import io.brackit.query.function.FunctionExpr;
import io.sirix.query.node.XmlDBNode;
import io.sirix.query.stream.node.SirixNodeStream;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.compiler.translator.PipelineStrategy;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.compiler.translator.TopDownTranslator;
import io.brackit.query.expr.DeclVariable;
import io.brackit.query.expr.BoundVariable;
import io.brackit.query.operator.Operator;
import io.sirix.query.compiler.operator.HashMembershipJoin;
import io.sirix.query.compiler.operator.HashMembershipJoin.Binding;
import io.brackit.query.module.Namespaces;
import io.sirix.query.compiler.optimizer.ComputedAggregateDetectionStage;
import io.sirix.query.compiler.optimizer.HashMembershipStage;
import io.sirix.query.scan.SirixExecutorProvider;
import io.brackit.query.expr.Accessor;
import io.brackit.query.jdm.Axis;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Kind;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.jdm.type.AnyNodeType;
import io.brackit.query.jdm.type.AttributeType;
import io.brackit.query.jdm.type.NodeType;
import io.brackit.query.node.stream.EmptyStream;
import io.brackit.query.util.Cfg;
import io.brackit.query.util.Whitespace;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

import static io.sirix.query.stream.node.TemporalSirixNodeStream.isSimpleNodeTest;

/**
 * Translates queries (optimizes currently path-expressions if {@code OPTIMIZE} is set to true).
 *
 * @author Johannes Lichtenberger
 */
public class SirixTranslator extends TopDownTranslator {

  /**
   * Optimize accessors or not.
   */
  public static final boolean OPTIMIZE = Cfg.asBool("org.sirix.xquery.optimize.accessor", true);

  /**
   * Number of children (needed as a threshold to lookup in path summary if a path exists at all).
   */
  public static final int CHILD_THRESHOLD = Cfg.asInt("org.sirix.xquery.optimize.child.threshold", 100_000);

  /**
   * Number of descendants (needed as a threshold to lookup in path summary if a path exists at all).
   */
  public static final int DESCENDANT_THRESHOLD = Cfg.asInt("org.sirix.xquery.optimize.child.threshold", -1);

  /**
   * Constructor using sequential (default) pipeline strategy.
   *
   * @param options options map
   */
  public SirixTranslator(final Map<QNm, Str> options) {
    this(options, new SirixPipelineStrategy());
  }

  /**
   * Constructor with explicit pipeline strategy. Pass a
   * {@link io.brackit.query.compiler.translator.BlockPipelineStrategy} to enable parallel
   * (block-based) FLWOR execution via Brackit's ForkJoinPool model.
   *
   * @param options options map
   * @param pipelineStrategy the strategy for pipelines without membership joins; membership joins use
   *        {@link SirixPipelineStrategy}
   */
  public SirixTranslator(final Map<QNm, Str> options, final PipelineStrategy pipelineStrategy) {
    super(options, (node, compiler) -> SirixPipelineStrategy.hasMembershipJoin(node)
        ? new SirixPipelineStrategy().compilePipeExpr(node, compiler)
        : pipelineStrategy.compilePipeExpr(node, compiler));
  }

  @Override
  protected Expr anyExpr(AST node) throws QueryException {
    if (node.getType() == XQExt.HashMembershipJoin) {
      // A marker outside a physical selection (e.g. a subsequently folded expression) remains exact.
      return expr(node.getChild(3), true);
    } else if (node.getType() == XQExt.IndexExpr) {
      return indexExpr(node);
    } else if (node.getType() == XQExt.VectorizedPipelineExpr) {
      return new VectorizedPipelineExpr(node.getProperties());
    } else if (node.getType() == XQ.DerefDescendantExpr) {
      return derefDescendantExpr(node);
    }
    return super.anyExpr(node);
  }

  HashMembershipJoin membershipJoin(final Operator in, final AST node) {
    final AST scopeNode = node.getChild(1);
    final Expr scope = expr(scopeNode, true);
    final Binding binding = new Binding(scope);
    if (!(scope instanceof DeclVariable)) {
      table.resolve((QNm) scopeNode.getValue(), binding);
    }
    return new HashMembershipJoin(in, expr(node.getChild(0), true), binding, expr(node.getChild(2), true),
        expr(node.getChild(3), true), (QNm) node.getProperty(HashMembershipStage.FIELD),
        node.checkProperty(HashMembershipStage.ANTI));
  }

  int bindingCount() {
    return table.bound().length;
  }

  Expr pipelineReturn(final AST node, final int initialBindings) {
    final Expr result = anyExpr(node);
    for (int count = table.bound().length - initialBindings; count > 0; count--) {
      table.unbind();
    }
    return result;
  }

  @Override
  protected Expr andExpr(final AST node) throws QueryException {
    final Expr ordered = super.andExpr(node);
    final AST original = (AST) node.getProperty(CheapFirstConjunctStage.ORIGINAL);
    return original == null
        ? ordered
        : new GuardedConjunctExpr(ordered, super.andExpr(original),
            new ConjunctInputs((QNm[]) node.getProperty(CheapFirstConjunctStage.INPUTS),
                (QNm[]) node.getProperty(CheapFirstConjunctStage.CAPTURED),
                (QNm[]) node.getProperty(CheapFirstConjunctStage.DEFAULTS), table,
                Boolean.TRUE.equals(node.getProperty(CheapFirstConjunctStage.NATIVE_STORE))));
  }

  @Override
  protected Expr derefDescendantExpr(AST node) throws QueryException {
    Expr object = expr(node.getChild(0), true);
    Expr field = expr(node.getChild(1), true);
    return new DerefDescendantExpr(object, field);
  }


  private static final Set<String> COMPUTED_AGG_FUNCS = Set.of("sum", "avg", "min", "max", "count");

  /**
   * Optimizer-marked valid-time scans retain deferred point evaluation so empty arrays do not
   * evaluate the point expression.
   *
   * <p>
   * {@code sum|avg|min|max|count(<computed pipe>)} — an aggregate call whose sole argument is a
   * pipeline {@link ComputedAggregateDetectionStage} annotated as a servable computed-expression
   * fold. Emits the projection-served expression with the GENERIC function call compiled alongside as
   * the runtime fallback. Unmarked calls retain the generic translation.
   */
  @Override
  protected Expr functionCall(AST node) throws QueryException {
    if (OpenBitemporal.OPEN_BITEMPORAL_SLICE.equals(node.getValue()) && node.getChildCount() == 7
        && node.checkProperty(OpenBitemporal.INTERNAL_SLICE)) {
      final Expr[] arguments = new Expr[7];
      for (int i = 0; i < arguments.length; i++) {
        arguments[i] = expr(node.getChild(i), true);
      }
      return new FunctionExpr(ctx, OpenBitemporal.forSlice(), arguments);
    }
    if (ScanValidTimeIndex.SCAN_VALID_TIME_INDEX.equals(node.getValue()) && node.getChildCount() == 5
        && node.checkProperty(ScanValidTimeIndex.DEFERRED_POINT)) {
      final AST pointNode = node.getChild(1);
      Expr point = expr(pointNode, true);
      if (pointNode.getType() == XQ.FunctionCall) {
        try {
          point = new DateTime(Whitespace.normalizeXML11(pointNode.getChild(0).getStringValue()));
        } catch (final QueryException ignored) {
        }
      }
      final SirixValidTimeScanExpr scan = new SirixValidTimeScanExpr(ctx, expr(node.getChild(0), true), point,
          ((Str) node.getChild(2).getValue()).stringValue(), ((Str) node.getChild(3).getValue()).stringValue(),
          ((IntNumeric) node.getChild(4).getValue()).intValue());
      if (point instanceof BoundVariable variable) {
        table.resolve(variable.getName(), scan);
      }
      return scan;
    }
    if (node.getChildCount() == 1 && node.getValue() instanceof QNm fn && node.getChild(0).getType() == XQ.PipeExpr
        && Boolean.TRUE.equals(node.getChild(0).getProperty(ComputedAggregateDetectionStage.COMPUTED_AGG))
        && COMPUTED_AGG_FUNCS.contains(fn.getLocalName())
        && SequentialPipelineStrategy.getVectorizedExecutor() instanceof SirixExecutorProvider executor) {
      // Built-in aggregates only: unprefixed calls resolve to the JSONiq default function
      // namespace, fn:* to the XQuery one — both are the builtins. A user-defined
      // local:sum must never be served with fn:sum semantics.
      final String ns = fn.getNamespaceURI();
      if (ns == null || ns.isEmpty() || Namespaces.FN_NSURI.equals(ns) || Namespaces.DEFAULT_FN_NSURI.equals(ns)) {
        final AST pipe = node.getChild(0);
        final String[] sourcePath = (String[]) pipe.getProperty("VECTORIZED_SOURCE_PATH_PREFIX");
        final SourceRef sourceRef = (SourceRef) pipe.getProperty("VECTORIZED_SOURCE_REF");
        final String[] fields = (String[]) pipe.getProperty(ComputedAggregateDetectionStage.COMPUTED_AGG_FIELDS);
        final int[] code = (int[]) pipe.getProperty(ComputedAggregateDetectionStage.COMPUTED_AGG_CODE);
        final long[] consts = (long[]) pipe.getProperty(ComputedAggregateDetectionStage.COMPUTED_AGG_CONSTS);
        if (sourcePath != null && fields != null && code != null && consts != null
            && SirixPipelineStrategy.acceptsOrRuntimeCheckable(executor, sourceRef)) {
          // Admit a VARIABLE (external-variable) source at compile time and re-verify its actual
          // binding per evaluation — the same runtime gate the four pipeline serving exprs use.
          final Expr generic = super.functionCall(node);
          return new SirixComputedAggregateExpr(executor, sourcePath,
              (PredicateNode) pipe.getProperty("VECTORIZED_PREDICATE_TREE"), fn.getLocalName(), fields, code, consts,
              sourceRef, generic);
        }
      }
    }
    return super.functionCall(node);
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
      default -> super.axis(node);
    };
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
    private Preceding(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      if (!isSimpleNodeTest(test)) {
        return super.performStep(node, test);
      }
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
    private PrecedingSibling(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      if (!isSimpleNodeTest(test)) {
        return super.performStep(node, test);
      }
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
    private FollowingSibling(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      if (!isSimpleNodeTest(test)) {
        return super.performStep(node, test);
      }
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
    private Following(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      if (!isSimpleNodeTest(test)) {
        return super.performStep(node, test);
      }
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
    private AncestorOrSelf(final Axis axis) {
      super(axis);
      includeSelf = axis == Axis.ANCESTOR
          ? IncludeSelf.NO
          : IncludeSelf.YES;
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      if (!isSimpleNodeTest(test)) {
        return super.performStep(node, test);
      }
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
    private Parent(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      if (!isSimpleNodeTest(test)) {
        return super.performStep(node, test);
      }
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
    private Attribute(final Axis axis) {
      super(axis);
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      if (!(node instanceof final XmlDBNode dbNode)) {
        return Accessor.ATTRIBUTE.performStep(node, test);
      }
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      final AttributeAxis axis = new AttributeAxis(rtx);
      if (test instanceof AnyNodeType) {
        return new SirixNodeStream(axis, dbNode.getCollection());
      }
      if (test instanceof AttributeType && test.getType() == null) {
        final QNm name = test.getQName();
        if (name == null) {
          return new SirixNodeStream(axis, dbNode.getCollection());
        }
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final NamePage names = reader.getNamePage(reader.getActualRevisionRootPage());
        final String localName = name.getLocalName();
        final String namespaceURI = Objects.toString(name.getNamespaceURI(), "");
        final int localNameKey = names.keyForName(localName, NodeKind.ATTRIBUTE, reader);
        final int namespaceKey = names.keyForName(namespaceURI, NodeKind.NAMESPACE, reader);
        // Rename and dictionary collision-chain changes can leave equivalent expanded names
        // with different keys. Keep resolved-name equality when the key fast path misses.
        return new SirixNodeStream(new FilterAxis<>(axis, new AbstractFilter<XmlNodeReadOnlyTrx>(rtx) {
          @Override
          public boolean filter() {
            return (rtx.getLocalNameKey() == localNameKey || localName.equals(rtx.nameForKey(rtx.getLocalNameKey())))
                && (rtx.getURIKey() == namespaceKey
                    || namespaceURI.equals(Objects.toString(rtx.getNamespaceURI(), "")));
          }
        }), dbNode.getCollection());
      }
      return new KindFilter(test, new SirixNodeStream(axis, dbNode.getCollection()));
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node) {
      if (!(node instanceof final XmlDBNode dbNode)) {
        return Accessor.ATTRIBUTE.performStep(node);
      }
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      return new SirixNodeStream(new AttributeAxis(rtx), dbNode.getCollection());
    }
  }

  /**
   * Worker-local path-summary matches. Compiled accessors are shared by parallel FLWOR execution, so
   * document identity, cache lookup and publication must stay in the same worker.
   */
  private static final class PathSummaryMatches {
    private final Long2ObjectMap<BitSet> matchesByPath = new Long2ObjectOpenHashMap<>();
    private final IncludeSelf self;
    private long databaseId;
    private long resourceId;
    private int revision;
    private @Nullable QNm name;

    private PathSummaryMatches(final IncludeSelf self) {
      this.self = self;
    }

    private BitSet match(final XmlNodeReadOnlyTrx rtx, final PathSummaryReader reader, final QNm qName) {
      final long pcr = rtx.isDocumentRoot()
          ? Fixed.DOCUMENT_NODE_KEY.getStandardProperty()
          : rtx.getPathNodeKey();
      final ResourceConfiguration configuration = rtx.getResourceSession().getResourceConfig();
      final int currentRevision = rtx.getRevisionNumber();
      if (databaseId != configuration.getDatabaseId() || resourceId != configuration.getID()
          || revision != currentRevision || !qName.equals(name)) {
        matchesByPath.clear();
        databaseId = configuration.getDatabaseId();
        resourceId = configuration.getID();
        revision = currentRevision;
        name = qName;
      }
      BitSet matches = matchesByPath.get(pcr);
      if (matches == null) {
        reader.moveTo(pcr);
        final int contextLevel = reader.getLevel();
        final int minLevel = self == IncludeSelf.YES
            ? contextLevel
            : contextLevel + 1;
        matches = reader.match(qName, minLevel, NodeKind.ELEMENT);
        for (int candidate = matches.nextSetBit(0); candidate >= 0; candidate = matches.nextSetBit(candidate + 1)) {
          long ancestorKey = candidate;
          PathNode ancestor = reader.getPathNodeForPathNodeKey(ancestorKey);
          while (ancestor != null && ancestor.getLevel() > contextLevel) {
            ancestorKey = ancestor.getParentKey();
            ancestor = reader.getPathNodeForPathNodeKey(ancestorKey);
          }
          if (ancestorKey != pcr) {
            matches.clear(candidate);
          }
        }
        matchesByPath.put(pcr, matches);
      }
      return matches;
    }
  }

  /**
   * {@code child::} optimization.
   *
   * @author Johannes Lichtenberger
   */
  private static final class Child extends Accessor {
    /**
     * Worker-local cache; see {@link PathSummaryMatches} for the ownership invariant.
     */
    private final ThreadLocal<PathSummaryMatches> filterMap;

    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    private Child(final Axis axis) {
      super(axis);
      filterMap = ThreadLocal.withInitial(() -> new PathSummaryMatches(IncludeSelf.NO));
    }

    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      if (!isSimpleNodeTest(test)) {
        return super.performStep(node, test);
      }
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      if (rtx.getResourceSession().getResourceConfig().withPathSummary && test.getNodeKind() == Kind.ELEMENT
          && test.getQName() != null && (rtx.isElement() || rtx.isDocumentRoot())
          && rtx.getChildCount() > CHILD_THRESHOLD) {
        try {
          final PathSummaryReader reader = rtx.getResourceSession().openPathSummary(rtx.getRevisionNumber());
          final BitSet matches = filterMap.get().match(rtx, reader, test.getQName());
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
     * Worker-local cache; see {@link PathSummaryMatches} for the ownership invariant.
     */
    private final ThreadLocal<PathSummaryMatches> filterMap;

    /**
     * Constructor.
     *
     * @param axis the axis to evaluate
     */
    private DescOrSelf(final Axis axis) {
      super(axis);
      self = axis == Axis.DESCENDANT_OR_SELF
          ? IncludeSelf.YES
          : IncludeSelf.NO;
      filterMap = ThreadLocal.withInitial(() -> new PathSummaryMatches(self));
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public Stream<? extends Node<?>> performStep(final Node<?> node, final NodeType test) {
      if (!isSimpleNodeTest(test)) {
        return super.performStep(node, test);
      }
      final XmlDBNode dbNode = (XmlDBNode) node;
      final XmlNodeReadOnlyTrx rtx = dbNode.getTrx();
      if (rtx.getResourceSession().getResourceConfig().withPathSummary && test.getNodeKind() == Kind.ELEMENT
          && test.getQName() != null && (rtx.isElement() || rtx.isDocumentRoot())
          && rtx.getDescendantCount() > DESCENDANT_THRESHOLD) {
        try {
          final PathSummaryReader reader = rtx.getResourceSession().openPathSummary(rtx.getRevisionNumber());
          final BitSet matches = filterMap.get().match(rtx, reader, test.getQName());
          final boolean matchingSelf = self == IncludeSelf.YES && rtx.isElement()
              && matches.get((int) rtx.getPathNodeKey()) && test.getQName().equals(rtx.getName());
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
              return matchingSelf
                  ? new SirixNodeStream(new SelfAxis(rtx), dbNode.getCollection())
                  : new EmptyStream<>();
            }
            // Match at the next level (single child-path).
            if (matchLevel == level + 1) {
              reader.close();
              return new SirixNodeStream(
                  new FilterAxis<>(new ChildAxis(rtx), new ElementFilter(rtx), new XmlNameFilter(rtx, test.getQName())),
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
          final Deque<AbstractAxis> axisQueue = new ArrayDeque<>(matches.cardinality());
          if (onSameLevel) {
            for (int j = level, nodeLevel = getLevel(dbNode); j > nodeLevel; j--) {
              // Build a set and turn it into a list for sorting.
              final Set<QNm> pathNodeQNmSet = new LinkedHashSet<>();
              for (i = matches.nextSetBit(0); i >= 0; i = matches.nextSetBit(i + 1)) {
                reader.moveTo(i);
                for (int k = level; k > j; k--) {
                  reader.moveToParent();
                }
                pathNodeQNmSet.add(Objects.requireNonNull(reader.getName()));
              }
              final List<QNm> pathNodeQNmsList = List.copyOf(pathNodeQNmSet);
              final QNm name = pathNodeQNmsList.get(0);
              boolean sameName = true;
              for (int k = 1; k < pathNodeQNmsList.size(); k++) {
                if (pathNodeQNmsList.get(k).atomicCmp(name) != 0) {
                  sameName = false;
                  break;
                }
              }

              axisQueue.push(sameName
                  ? new FilterAxis<>(new ChildAxis(rtx), new ElementFilter(rtx), new XmlNameFilter(rtx, name))
                  : new FilterAxis<>(new ChildAxis(rtx), new ElementFilter(rtx)));
            }

            AbstractAxis axis = axisQueue.pop();
            for (int k = 0, size = axisQueue.size(); k < size; k++) {
              axis = new NestedAxis(axis, axisQueue.pop());
            }
            reader.close();
            return new SirixNodeStream(axis, dbNode.getCollection());
          } else {
            // Matches on different levels.
            level = getLevel(dbNode);
            for (i = matches.nextSetBit(0); i >= 0; i = matches.nextSetBit(i + 1)) {
              reader.moveTo(i);
              assert reader.getPathNode() != null;
              final int matchLevel = reader.getPathNode().getLevel();

              // Match at the same level.
              if (self == IncludeSelf.YES && matchLevel == level) {
                if (matchingSelf) {
                  axisQueue.addLast(new SelfAxis(rtx));
                }
              }
              // Match at the next level (single child-path).
              else if (matchLevel == level + 1) {
                axisQueue.addLast(new FilterAxis<>(new ChildAxis(rtx), new ElementFilter(rtx),
                    new XmlNameFilter(rtx, test.getQName())));
              }
              // Match at a level below the child level.
              else {
                final Deque<QNm> names = getNames(matchLevel, level, reader);
                axisQueue.addLast(buildQuery(rtx, names));
              }
            }
            while (axisQueue.size() > 1) {
              axisQueue.addLast(new OrderedUnionAxis(rtx, axisQueue.removeFirst(), axisQueue.removeFirst()));
            }
            reader.close();
            return new SirixNodeStream(axisQueue.removeFirst(), dbNode.getCollection());
          }
        } catch (final SirixException e) {
          throw new QueryException(new QNm(e.getMessage()), e);
        }
      }
      return super.performStep(node, test);
    }

    // Get all names on the path up to level.
    private static Deque<QNm> getNames(final int matchLevel, final int level, final PathSummaryReader reader) {
      // Match at a level below this level which is not a direct child.
      final Deque<QNm> names = new ArrayDeque<>(matchLevel - level);
      for (int i = matchLevel; i > level; i--) {
        names.push(reader.getName());
        reader.moveToParent();
      }
      return names;
    }

    // Build the query.
    private static AbstractAxis buildQuery(final XmlNodeReadOnlyTrx rtx, final Deque<QNm> names) {
      AbstractAxis axis =
          new FilterAxis<>(new ChildAxis(rtx), new ElementFilter(rtx), new XmlNameFilter(rtx, names.pop()));
      for (int i = 0, size = names.size(); i < size; i++) {
        axis = new NestedAxis(axis,
            new FilterAxis<>(new ChildAxis(rtx), new ElementFilter(rtx), new XmlNameFilter(rtx, names.pop())));
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

  private static int getLevel(final XmlDBNode dbNode) {
    final XmlNodeReadOnlyTrx rtx = dbNode.getRtx();
    final SirixDeweyID deweyID = rtx.getDeweyID();
    if (deweyID != null) {
      return deweyID.getLevel();
    }
    final long nodeKey = rtx.getNodeKey();
    int level = 0;
    try {
      while (rtx.moveToParent()) {
        level++;
      }
      return level;
    } finally {
      rtx.moveTo(nodeKey);
    }
  }

  private static AbstractAxis getAxis(final NodeType test, final XmlNodeReadOnlyTrx trx, final AbstractAxis innerAxis) {
    final FilterAxis<XmlNodeReadOnlyTrx> axis;

    switch (test.getNodeKind()) {
      case COMMENT -> axis = new FilterAxis<>(innerAxis, new CommentFilter(trx));
      case ELEMENT -> {
        if (test.getQName() == null) {
          axis = new FilterAxis<>(innerAxis, new ElementFilter(trx));
        } else {
          axis = new FilterAxis<>(innerAxis, new ElementFilter(trx), new XmlNameFilter(trx, test.getQName()));
        }
      }
      case TEXT -> axis = new FilterAxis<>(innerAxis, new TextFilter(trx));
      case ATTRIBUTE -> {
        if (test.getQName() == null) {
          axis = new FilterAxis<>(innerAxis, new AttributeFilter(trx));
        } else {
          axis = new FilterAxis<>(innerAxis, new AttributeFilter(trx), new XmlNameFilter(trx, test.getQName()));
        }
      }
      case DOCUMENT -> axis = new FilterAxis<>(innerAxis, new DocumentRootNodeFilter(trx));
      default -> throw new AssertionError(); // Must not happen.
    }

    return axis;
  }
}
