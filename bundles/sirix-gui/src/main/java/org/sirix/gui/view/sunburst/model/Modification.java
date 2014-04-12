package org.sirix.gui.view.sunburst.model;

import static com.google.common.base.Preconditions.checkArgument;

import javax.annotation.concurrent.Immutable;

/**
 * Immutable container class for counting descendants, modifications... of one
 * node.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
@Immutable
public final class Modification {
	/** Number of modifications in the subtree. */
	private final int mModifications;

	/**
	 * Determines if modifications must be subtracted in {@link ITraverseModel#}
	 * or not.
	 */
	private final boolean mSubtract;

	/** Number of descendants in the subtree including the actual node. */
	private final int mDescendants;

	/** Empty modification instance. */
	public static final Modification EMPTY_MODIFICATION = new Modification(0, 0,
			false);

	/**
	 * Constructor.
	 * 
	 * @param pModifications
	 *          number of modifications in the subtree
	 * @param pDescendants
	 *          number of descendants in the subtree
	 * @param pSubtract
	 *          determines if modifications must be subtracted
	 */
	public Modification(final int pModifications, final int pDescendants,
			final boolean pSubtract) {
		checkArgument(pModifications >= 0, "paramModifications must be >= 0!");
		checkArgument(pDescendants >= 0, "paramDescendants must be >= 0!");
		mModifications = pModifications;
		mSubtract = pSubtract;
		mDescendants = pDescendants;
	}

	/**
	 * Get modification count.
	 * 
	 * @return modification count
	 */
	public int getModifications() {
		return mModifications;
	}

	/**
	 * Determines if needs to subtract ({@link mSubtract}).
	 * 
	 * @return {@link mSubtract}
	 */
	public boolean isSubtract() {
		return mSubtract;
	}

	/**
	 * Get descendant-or-self count.
	 * 
	 * @return descendant-or-self count
	 */
	public int getDescendants() {
		return mDescendants;
	}
}
