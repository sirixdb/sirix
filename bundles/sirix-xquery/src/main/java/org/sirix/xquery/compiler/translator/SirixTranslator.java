package org.sirix.xquery.compiler.translator;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

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
import org.brackit.xquery.xdm.Node;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.type.NodeType;
import org.sirix.api.NodeReadTrx;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.NestedAxis;
import org.sirix.axis.SelfAxis;
import org.sirix.axis.filter.ElementFilter;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.NameFilter;
import org.sirix.exception.SirixException;
import org.sirix.index.path.PathSummaryReader;
import org.sirix.service.xml.xpath.expr.UnionAxis;
import org.sirix.xquery.node.DBNode;
import org.sirix.xquery.stream.SirixStream;

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
	 * Constructor.
	 * 
	 * @param options
	 *          options map
	 */
	public SirixTranslator(final @Nonnull Map<QNm, Str> options) {
		super(options);
	}

	@Override
	protected Accessor axis(final @Nonnull AST node) throws QueryException {
		if (!OPTIMIZE) {
			return super.axis(node);
		}
		switch (node.getType()) {
		case XQ.DESCENDANT:
			return new DescOrSelf(Axis.DESCENDANT);
		case XQ.DESCENDANT_OR_SELF:
			return new DescOrSelf(Axis.DESCENDANT_OR_SELF);
		default:
			return super.axis(node);
		}
	}

	/**
	 * {@code descendant::} and {@code descendant-or-self::} path-optimizations.
	 * 
	 * @author Johannes Lichtenberger
	 * 
	 */
	private static class DescOrSelf extends Accessor {
		/**
		 * Determines if current node is included (-or-self part).
		 */
		private IncludeSelf mSelf;

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
		public DescOrSelf(final @Nonnull Axis axis) {
			super(axis);
			mSelf = axis == Axis.DESCENDANT_OR_SELF ? IncludeSelf.YES
					: IncludeSelf.NO;
			mFilterMap = new HashMap<>();
		}

		@Override
		public Stream<? extends Node<?>> performStep(final @Nonnull Node<?> node,
				final @Nonnull NodeType test) throws QueryException {
			final DBNode dbNode = (DBNode) node;
			try {
				final long pcr = dbNode.getPCR();
				BitSet matches = mFilterMap.get(pcr);
				final NodeReadTrx rtx = dbNode.getTrx();
				final PathSummaryReader reader = rtx.getSession().openPathSummary(
						rtx.getRevisionNumber());
				if (matches == null) {
					reader.moveTo(pcr);
					final int level = reader.getLevel();
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
					return new SirixStream(buildQuery(rtx, names), dbNode.getCollection());
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
					for (int j = level, nodeLevel = dbNode.getDeweyID().get().getLevel(); j > nodeLevel; j--) {
						// Build an immutable set and turn it into a list for sorting.
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
					// return new SirixStream(new FilterAxis(new DescendantAxis(rtx,
					// mSelf),
					// new ElementFilter(rtx), new NameFilter(rtx, test.getQName()
					// .toString())), dbNode.getCollection());
				} else {
					// Matches on different levels.
					// TODO: Use ConcurrentUnionAxis.
					final Deque<org.sirix.api.Axis> axisQueue = new ArrayDeque<>(
							matches.cardinality());
					level = dbNode.getDeweyID().get().getLevel();
					for (i = matches.nextSetBit(0); i >= 0; i = matches.nextSetBit(i + 1)) {
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
				// TODO: exception handling.
				e.printStackTrace();
				return null;
			}
		}

		// Get all names on the path up to level.
		private Deque<QNm> getNames(final @Nonnegative int matchLevel,
				final @Nonnegative int level, final @Nonnull PathSummaryReader reader) {
			// Match at a level below this level which is not a direct child.
			final Deque<QNm> names = new ArrayDeque<>(matchLevel - level);
			for (int i = matchLevel; i > level; i--) {
				names.push(reader.getName());
				reader.moveToParent();
			}
			return names;
		}

		// Build the query.
		private org.sirix.api.Axis buildQuery(final @Nonnull NodeReadTrx rtx,
				final @Nonnull Deque<QNm> names) {
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
			return null;
		}
	}
}
