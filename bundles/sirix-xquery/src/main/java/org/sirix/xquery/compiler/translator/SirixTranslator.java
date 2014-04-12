package org.sirix.xquery.compiler.translator;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnegative;

import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.translator.TopDownTranslator;
import org.brackit.xquery.expr.Accessor;
import org.brackit.xquery.node.stream.EmptyStream;
import org.brackit.xquery.util.Cfg;
import org.brackit.xquery.xdm.Axis;
import org.brackit.xquery.xdm.Kind;
import org.brackit.xquery.xdm.Node;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.type.NodeType;
import org.sirix.api.NodeReadTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.axis.AncestorAxis;
import org.sirix.axis.AttributeAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.FollowingAxis;
import org.sirix.axis.FollowingSiblingAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.NestedAxis;
import org.sirix.axis.ParentAxis;
import org.sirix.axis.PrecedingSiblingAxis;
import org.sirix.axis.SelfAxis;
import org.sirix.axis.filter.AttributeFilter;
import org.sirix.axis.filter.CommentFilter;
import org.sirix.axis.filter.DocumentRootNodeFilter;
import org.sirix.axis.filter.ElementFilter;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.NameFilter;
import org.sirix.axis.filter.NamespaceFilter;
import org.sirix.axis.filter.PIFilter;
import org.sirix.axis.filter.TemporalFilterAxis;
import org.sirix.axis.filter.TextFilter;
import org.sirix.axis.temporal.AllTimeAxis;
import org.sirix.axis.temporal.FirstAxis;
import org.sirix.axis.temporal.FutureAxis;
import org.sirix.axis.temporal.LastAxis;
import org.sirix.axis.temporal.NextAxis;
import org.sirix.axis.temporal.PastAxis;
import org.sirix.axis.temporal.PreviousAxis;
import org.sirix.exception.SirixException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.service.xml.xpath.expr.UnionAxis;
import org.sirix.xquery.node.DBNode;
import org.sirix.xquery.stream.SirixStream;
import org.sirix.xquery.stream.TemporalSirixStream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

/**
 * Translates queries (optimizes currently path-expressions if {@code OPTIMIZE}
 * is set to true).
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixTranslator extends TopDownTranslator {

	/** Optimize accessors or not. */
	public static final boolean OPTIMIZE = Cfg.asBool(
			"org.sirix.xquery.optimize.accessor", true);

	/**
	 * Number of children (needed as a threshold to lookup in path summary if a
	 * path exists at all).
	 */
	public static final int CHILD_THRESHOLD = Cfg.asInt(
			"org.sirix.xquery.optimize.child.threshold", 50);

	/**
	 * Constructor.
	 *
	 * @param options
	 *          options map
	 */
	public SirixTranslator(final Map<QNm, Str> options) {
		super(options);
	}

	@Override
	protected Accessor axis(final AST node) throws QueryException {
		if (!OPTIMIZE) {
			return super.axis(node);
		}
		switch (node.getType()) {
		case XQ.DESCENDANT:
			return new DescOrSelf(Axis.DESCENDANT);
		case XQ.DESCENDANT_OR_SELF:
			return new DescOrSelf(Axis.DESCENDANT_OR_SELF);
		case XQ.CHILD:
			return new Child(Axis.CHILD);
		case XQ.ATTRIBUTE:
			return new Attribute(Axis.ATTRIBUTE);
		case XQ.PARENT:
			return new Parent(Axis.PARENT);
		case XQ.ANCESTOR:
			return new AncestorOrSelf(Axis.ANCESTOR);
		case XQ.ANCESTOR_OR_SELF:
			return new AncestorOrSelf(Axis.ANCESTOR_OR_SELF);
		case XQ.FOLLOWING:
			return new Following(Axis.FOLLOWING);
		case XQ.FOLLOWING_SIBLING:
			return new FollowingSibling(Axis.FOLLOWING_SIBLING);
		case XQ.PRECEDING_SIBLING:
			return new PrecedingSibling(Axis.PRECEDING_SIBLING);
		case XQ.FUTURE:
			return new Future(Axis.FUTURE);
		case XQ.FUTURE_OR_SELF:
			return new Future(Axis.FUTURE_OR_SELF);
		case XQ.PAST:
			return new Past(Axis.PAST);
		case XQ.PAST_OR_SELF:
			return new Past(Axis.PAST_OR_SELF);
		case XQ.PREVIOUS:
			return new Previous(Axis.PREVIOUS);
		case XQ.NEXT:
			return new Next(Axis.NEXT);
		case XQ.ALL_TIME:
			return new AllTime(Axis.ALL_TIME);
		case XQ.FIRST:
			return new First(Axis.FIRST);
		case XQ.LAST:
			return new Last(Axis.LAST);
		default:
			return super.axis(node);
		}
	}

	/**
	 * {@code first::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class Last extends Accessor {
		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public Last(final Axis axis) {
			super(axis);
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new LastAxis(rtx);
			return new TemporalSirixStream(SirixTranslator.this.getTemporalAxis(test,
					rtx, axis), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new LastAxis(rtx);
			return new TemporalSirixStream(axis, dbNode.getCollection());
		}
	}

	/**
	 * {@code first::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class First extends Accessor {
		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public First(final Axis axis) {
			super(axis);
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new FirstAxis(rtx);
			return new TemporalSirixStream(SirixTranslator.this.getTemporalAxis(test,
					rtx, axis), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new FirstAxis(rtx);
			return new TemporalSirixStream(axis, dbNode.getCollection());
		}
	}

	/**
	 * {@code next::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class Next extends Accessor {
		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public Next(final Axis axis) {
			super(axis);
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new NextAxis(rtx);
			return new TemporalSirixStream(SirixTranslator.this.getTemporalAxis(test,
					rtx, axis), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new NextAxis(rtx);
			return new TemporalSirixStream(axis, dbNode.getCollection());
		}
	}

	/**
	 * {@code previous::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class Previous extends Accessor {
		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public Previous(final Axis axis) {
			super(axis);
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new PreviousAxis(rtx);
			return new TemporalSirixStream(SirixTranslator.this.getTemporalAxis(test,
					rtx, axis), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new PreviousAxis(rtx);
			return new TemporalSirixStream(axis, dbNode.getCollection());
		}
	}

	/**
	 * {@code all-time::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class AllTime extends Accessor {
		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public AllTime(final Axis axis) {
			super(axis);
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new AllTimeAxis(rtx);
			return new TemporalSirixStream(SirixTranslator.this.getTemporalAxis(test,
					rtx, axis), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new AllTimeAxis(rtx);
			return new TemporalSirixStream(axis, dbNode.getCollection());
		}
	}

	/**
	 * {@code past::} and {@code past-or-self::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class Past extends Accessor {
		/** Determine if self is included or not. */
		private final IncludeSelf mSelf;

		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public Past(final Axis axis) {
			super(axis);
			mSelf = axis == Axis.PAST ? IncludeSelf.NO : IncludeSelf.YES;
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new PastAxis(rtx, mSelf);
			return new TemporalSirixStream(SirixTranslator.this.getTemporalAxis(test,
					rtx, axis), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new PastAxis(rtx, mSelf);
			return new TemporalSirixStream(axis, dbNode.getCollection());
		}
	}

	/**
	 * {@code future::} and {@code future-or-self::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class Future extends Accessor {
		/** Determine if self is included or not. */
		private final IncludeSelf mSelf;

		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public Future(final Axis axis) {
			super(axis);
			mSelf = axis == Axis.FUTURE ? IncludeSelf.NO : IncludeSelf.YES;
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new FutureAxis(rtx, mSelf);
			return new TemporalSirixStream(SirixTranslator.this.getTemporalAxis(test,
					rtx, axis), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			final AbstractTemporalAxis axis = new FutureAxis(rtx, mSelf);
			return new TemporalSirixStream(axis, dbNode.getCollection());
		}
	}

	/**
	 * {@code preceding-sibling::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class PrecedingSibling extends Accessor {
		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public PrecedingSibling(final Axis axis) {
			super(axis);
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(SirixTranslator.this.getAxis(test, rtx,
					new PrecedingSiblingAxis(rtx)), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(new PrecedingSiblingAxis(rtx),
					dbNode.getCollection());
		}
	}

	/**
	 * {@code following-sibling::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class FollowingSibling extends Accessor {
		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public FollowingSibling(final Axis axis) {
			super(axis);
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(SirixTranslator.this.getAxis(test, rtx,
					new FollowingSiblingAxis(rtx)), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(new FollowingSiblingAxis(rtx),
					dbNode.getCollection());
		}
	}

	/**
	 * {@code following::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class Following extends Accessor {
		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public Following(final Axis axis) {
			super(axis);
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(SirixTranslator.this.getAxis(test, rtx,
					new FollowingAxis(rtx)), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(new FollowingAxis(rtx), dbNode.getCollection());
		}
	}

	/**
	 * {@code ancestor::} and {@code ancestor-or-self::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class AncestorOrSelf extends Accessor {
		/** Determine if self is included or not. */
		private final IncludeSelf mSelf;

		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public AncestorOrSelf(final Axis axis) {
			super(axis);
			mSelf = axis == Axis.ANCESTOR ? IncludeSelf.NO : IncludeSelf.YES;
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(SirixTranslator.this.getAxis(test, rtx,
					new AncestorAxis(rtx, mSelf)), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(new AncestorAxis(rtx, mSelf),
					dbNode.getCollection());
		}
	}

	/**
	 * {@code attribute::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class Parent extends Accessor {
		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public Parent(final Axis axis) {
			super(axis);
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(SirixTranslator.this.getAxis(test, rtx,
					new ParentAxis(rtx)), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(new ParentAxis(rtx), dbNode.getCollection());
		}
	}

	/**
	 * {@code attribute::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class Attribute extends Accessor {
		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public Attribute(final Axis axis) {
			super(axis);
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(new FilterAxis(new AttributeAxis(rtx),
					new NameFilter(rtx, test.getQName())), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(new AttributeAxis(rtx), dbNode.getCollection());
		}
	}

	/**
	 * {@code child::} optimization.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class Child extends Accessor {
		/**
		 * Map with PCR <=> matching nodes.
		 */
		private final Map<Long, BitSet> mFilterMap;

		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public Child(final Axis axis) {
			super(axis);
			mFilterMap = new HashMap<>();
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			if (test.getNodeKind() == Kind.ELEMENT
					&& rtx.getChildCount() > CHILD_THRESHOLD) {
				try {
					final long pcr = dbNode.getPCR();
					BitSet matches = mFilterMap.get(pcr);
					final PathSummaryReader reader = rtx.getSession().openPathSummary(
							rtx.getRevisionNumber());
					if (matches == null) {
						reader.moveTo(pcr);
						final int level = reader.getLevel() + 1;
						final QNm name = test.getQName();
						matches = reader.match(name, level);
						mFilterMap.put(pcr, matches);
					}
					// No matches.
					if (matches.cardinality() == 0) {
						reader.close();
						return new EmptyStream<DBNode>();
					}
					reader.close();
				} catch (final SirixException e) {
					throw new QueryException(new QNm(e.getMessage()), e);
				}
			}

			return new SirixStream(SirixTranslator.this.getAxis(test, rtx,
					new ChildAxis(rtx)), dbNode.getCollection());
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(new ChildAxis(rtx), dbNode.getCollection());
		}
	}

	/**
	 * {@code descendant::} and {@code descendant-or-self::} path-optimizations.
	 *
	 * @author Johannes Lichtenberger
	 *
	 */
	private class DescOrSelf extends Accessor {
		/**
		 * Determines if current node is included (-or-self part).
		 */
		private final IncludeSelf mSelf;

		/**
		 * Map with PCR <=> matching nodes.
		 */
		private final Map<Long, BitSet> mFilterMap;

		/**
		 * Constructor.
		 *
		 * @param axis
		 *          the axis to evaluate
		 */
		public DescOrSelf(final Axis axis) {
			super(axis);
			mSelf = axis == Axis.DESCENDANT_OR_SELF ? IncludeSelf.YES
					: IncludeSelf.NO;
			mFilterMap = new HashMap<>();
		}

		@Override
		public Stream<? extends Node<?>> performStep(final Node<?> node,
				final NodeType test) throws QueryException {
			if (test.getNodeKind() == Kind.ELEMENT) {
				final DBNode dbNode = (DBNode) node;
				try {
					final long pcr = dbNode.getPCR();
					BitSet matches = mFilterMap.get(pcr);
					final NodeReadTrx rtx = dbNode.getTrx();
					final PathSummaryReader reader = rtx.getSession().openPathSummary(
							rtx.getRevisionNumber());
					if (matches == null) {
						reader.moveTo(pcr);
						final int level = mSelf == IncludeSelf.YES ? reader.getLevel()
								: reader.getLevel() + 1;
						final QNm name = test.getQName();
						matches = reader.match(name, level);
						mFilterMap.put(pcr, matches);
					}
					// No matches.
					if (matches.cardinality() == 0) {
						reader.close();
						return new EmptyStream<DBNode>();
					}
					// One match.
					if (matches.cardinality() == 1) {
						final int level = dbNode.getDeweyID().get().getLevel();
						final long pcr2 = matches.nextSetBit(0);
						reader.moveTo(pcr2);
						final int matchLevel = reader.getPathNode().getLevel();
						// Match at the same level.
						if (mSelf == IncludeSelf.YES && matchLevel == level) {
							reader.close();
							return new SirixStream(new SelfAxis(rtx), dbNode.getCollection());
						}
						// Match at the next level (single child-path).
						if (matchLevel == level + 1) {
							reader.close();
							return new SirixStream(new FilterAxis(new ChildAxis(rtx),
									new ElementFilter(rtx), new NameFilter(rtx, test.getQName()
											.toString())), dbNode.getCollection());
						}
						// Match at a level below the child level.
						final Deque<QNm> names = getNames(matchLevel, level, reader);
						reader.close();
						return new SirixStream(buildQuery(rtx, names),
								dbNode.getCollection());
					}
					// More than one match.
					boolean onSameLevel = true;
					int i = matches.nextSetBit(0);
					reader.moveTo(i);
					int level = reader.getLevel();
					int tmpLevel = level;
					for (i = matches.nextSetBit(i + 1); i >= 0; i = matches
							.nextSetBit(i + 1)) {
						reader.moveTo(i);
						level = reader.getLevel();
						if (level != tmpLevel) {
							onSameLevel = false;
							break;
						}
					}
					// Matches on same level.
					if (onSameLevel) {
						final Deque<org.sirix.api.Axis> axisQueue = new ArrayDeque<>(
								matches.cardinality());
						for (int j = level, nodeLevel = dbNode.getDeweyID().get()
								.getLevel(); j > nodeLevel; j--) {
							// Build an immutable set and turn it into a list
							// for sorting.
							Builder<QNm> pathNodeQNmBuilder = ImmutableSet.<QNm> builder();
							for (i = matches.nextSetBit(0); i >= 0; i = matches
									.nextSetBit(i + 1)) {
								reader.moveTo(i);
								for (int k = level; k > j; k--) {
									reader.moveToParent();
								}
								pathNodeQNmBuilder.add(reader.getName());
							}
							final List<QNm> pathNodeQNmsList = pathNodeQNmBuilder.build()
									.asList();
							final QNm name = pathNodeQNmsList.get(0);
							boolean sameName = true;
							for (int k = 1; k < pathNodeQNmsList.size(); k++) {
								if (pathNodeQNmsList.get(k).atomicCmp(name) != 0) {
									sameName = false;
									break;
								}
							}

							axisQueue.push(sameName ? new FilterAxis(new ChildAxis(rtx),
									new ElementFilter(rtx), new NameFilter(rtx, name))
									: new FilterAxis(new ChildAxis(rtx), new ElementFilter(rtx)));
						}

						org.sirix.api.Axis axis = axisQueue.pop();
						for (int k = 0, size = axisQueue.size(); k < size; k++) {
							axis = new NestedAxis(axis, axisQueue.pop());
						}
						reader.close();
						return new SirixStream(axis, dbNode.getCollection());
						// return new SirixStream(new FilterAxis(new
						// DescendantAxis(rtx,
						// mSelf),
						// new ElementFilter(rtx), new NameFilter(rtx,
						// test.getQName()
						// .toString())), dbNode.getCollection());
					} else {
						// Matches on different levels.
						// TODO: Use ConcurrentUnionAxis.
						final Deque<org.sirix.api.Axis> axisQueue = new ArrayDeque<>(
								matches.cardinality());
						level = dbNode.getDeweyID().get().getLevel();
						for (i = matches.nextSetBit(0); i >= 0; i = matches
								.nextSetBit(i + 1)) {
							reader.moveTo(i);
							final int matchLevel = reader.getPathNode().getLevel();

							// Match at the same level.
							if (mSelf == IncludeSelf.YES && matchLevel == level) {
								axisQueue.addLast(new SelfAxis(rtx));
							}
							// Match at the next level (single child-path).
							else if (matchLevel == level + 1) {
								axisQueue.addLast(new FilterAxis(new ChildAxis(rtx),
										new ElementFilter(rtx), new NameFilter(rtx, test.getQName()
												.toString())));
							}
							// Match at a level below the child level.
							else {
								final Deque<QNm> names = getNames(matchLevel, level, reader);
								axisQueue.addLast(buildQuery(rtx, names));
							}
						}
						org.sirix.api.Axis axis = new UnionAxis(rtx, axisQueue.pollFirst(),
								axisQueue.pollFirst());
						final int size = axisQueue.size();
						for (i = 0; i < size; i++) {
							axis = new UnionAxis(rtx, axis, axisQueue.pollFirst());
						}
						reader.close();
						return new SirixStream(axis, dbNode.getCollection());
					}
				} catch (final SirixException e) {
					throw new QueryException(new QNm(e.getMessage()), e);
				}
			}
			return super.performStep(node, test);
		}

		// Get all names on the path up to level.
		private Deque<QNm> getNames(final @Nonnegative int matchLevel,
				final @Nonnegative int level, final PathSummaryReader reader) {
			// Match at a level below this level which is not a direct child.
			final Deque<QNm> names = new ArrayDeque<>(matchLevel - level);
			for (int i = matchLevel; i > level; i--) {
				names.push(reader.getName());
				reader.moveToParent();
			}
			return names;
		}

		// Build the query.
		private org.sirix.api.Axis buildQuery(final NodeReadTrx rtx,
				final Deque<QNm> names) {
			org.sirix.api.Axis axis = new FilterAxis(new ChildAxis(rtx),
					new ElementFilter(rtx), new NameFilter(rtx, names.pop()));
			for (int i = 0, size = names.size(); i < size; i++) {
				axis = new NestedAxis(axis, new FilterAxis(new ChildAxis(rtx),
						new ElementFilter(rtx), new NameFilter(rtx, names.pop())));
			}
			return axis;
		}

		@Override
		public Stream<? extends Node<?>> performStep(Node<?> node)
				throws QueryException {
			final DBNode dbNode = (DBNode) node;
			final NodeReadTrx rtx = dbNode.getTrx();
			return new SirixStream(new DescendantAxis(rtx, mSelf),
					dbNode.getCollection());
		}
	}

	private org.sirix.axis.AbstractTemporalAxis getTemporalAxis(
			final NodeType test, final NodeReadTrx trx,
			final org.sirix.axis.AbstractTemporalAxis innerAxis) {
		TemporalFilterAxis axis = null;
		switch (test.getNodeKind()) {
		case COMMENT:
			axis = new TemporalFilterAxis(innerAxis, new CommentFilter(trx));
			break;
		case PROCESSING_INSTRUCTION:
			if (test.getQName() == null) {
				axis = new TemporalFilterAxis(innerAxis, new PIFilter(trx));
			} else {
				axis = new TemporalFilterAxis(innerAxis, new PIFilter(trx),
						new NameFilter(trx, test.getQName()));
			}
			break;
		case ELEMENT:
			if (test.getQName() == null) {
				axis = new TemporalFilterAxis(innerAxis, new ElementFilter(trx));
			} else {
				axis = new TemporalFilterAxis(innerAxis, new ElementFilter(trx),
						new NameFilter(trx, test.getQName()));
			}
			break;
		case TEXT:
			axis = new TemporalFilterAxis(innerAxis, new TextFilter(trx));
			break;
		case NAMESPACE:
			if (test.getQName() == null) {
				axis = new TemporalFilterAxis(innerAxis, new NamespaceFilter(trx));
			} else {
				axis = new TemporalFilterAxis(innerAxis, new NamespaceFilter(trx),
						new NameFilter(trx, test.getQName()));
			}
			break;
		case ATTRIBUTE:
			if (test.getQName() == null) {
				axis = new TemporalFilterAxis(innerAxis, new AttributeFilter(trx));
			} else {
				axis = new TemporalFilterAxis(innerAxis, new AttributeFilter(trx),
						new NameFilter(trx, test.getQName()));
			}
			break;
		case DOCUMENT:
			axis = new TemporalFilterAxis(innerAxis, new DocumentRootNodeFilter(trx));
		}
		return axis;
	}

	private org.sirix.api.Axis getAxis(final NodeType test,
			final NodeReadTrx trx, final org.sirix.api.Axis innerAxis) {
		FilterAxis axis = null;
		switch (test.getNodeKind()) {
		case COMMENT:
			axis = new FilterAxis(innerAxis, new CommentFilter(trx));
			break;
		case PROCESSING_INSTRUCTION:
			if (test.getQName() == null) {
				axis = new FilterAxis(innerAxis, new PIFilter(trx));
			} else {
				axis = new FilterAxis(innerAxis, new PIFilter(trx), new NameFilter(trx,
						test.getQName()));
			}
			break;
		case ELEMENT:
			if (test.getQName() == null) {
				axis = new FilterAxis(innerAxis, new ElementFilter(trx));
			} else {
				axis = new FilterAxis(innerAxis, new ElementFilter(trx),
						new NameFilter(trx, test.getQName()));
			}
			break;
		case TEXT:
			axis = new FilterAxis(innerAxis, new TextFilter(trx));
			break;
		case NAMESPACE:
			if (test.getQName() == null) {
				axis = new FilterAxis(innerAxis, new NamespaceFilter(trx));
			} else {
				axis = new FilterAxis(innerAxis, new NamespaceFilter(trx),
						new NameFilter(trx, test.getQName()));
			}
			break;
		case ATTRIBUTE:
			if (test.getQName() == null) {
				axis = new FilterAxis(innerAxis, new AttributeFilter(trx));
			} else {
				axis = new FilterAxis(innerAxis, new AttributeFilter(trx),
						new NameFilter(trx, test.getQName()));
			}
			break;
		case DOCUMENT:
			axis = new FilterAxis(innerAxis, new DocumentRootNodeFilter(trx));
		}
		return axis;
	}
}
