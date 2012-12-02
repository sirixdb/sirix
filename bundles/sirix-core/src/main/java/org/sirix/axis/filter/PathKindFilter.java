package org.sirix.axis.filter;

import static com.google.common.base.Preconditions.checkArgument;

import javax.annotation.Nonnull;

import org.sirix.api.NodeReadTrx;
import org.sirix.index.path.PathSummaryReader;
import org.sirix.node.Kind;

/**
 * Path filter for {@link PathSummaryReader}, filtering specific path types.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class PathKindFilter extends AbstractFilter {

	/** Type to filter. */
	private Kind mType;

	/**
	 * Constructor. Initializes the internal state.
	 * 
	 * @param rtx
	 *          transaction this filter is bound to
	 * @param type
	 *          type to match
	 */
	public PathKindFilter(final @Nonnull NodeReadTrx rtx,
			final @Nonnull Kind type) {
		super(rtx);
		checkArgument(rtx instanceof PathSummaryReader);
		mType = type;
	}

	@Override
	public boolean filter() {
		return mType == getTrx().getPathKind();
	}
}
