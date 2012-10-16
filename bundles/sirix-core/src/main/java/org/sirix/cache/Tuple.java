package org.sirix.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.page.PageKind;

import com.google.common.base.Objects;

/**
 * Tuple used for caches.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class Tuple {
	/** Node key. */
	private long mKey;

	/** Page kind. */
	private PageKind mPage;

	/**
	 * Constructor.
	 * 
	 * @param pKey
	 *          unique node key
	 * @param pPage
	 *          page type
	 */
	public Tuple(final long pKey, final @Nonnull PageKind pPage) {
		mKey = pKey;
		mPage = checkNotNull(pPage);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mKey, mPage);
	}

	@Override
	public boolean equals(final @Nullable Object pObj) {
		if (pObj instanceof Tuple) {
			final Tuple otherTuple = (Tuple) pObj;
			return Objects.equal(mKey, otherTuple.mKey)
					&& Objects.equal(mPage, otherTuple.mPage);
		}
		return false;
	}

	/**
	 * Get the node key.
	 * 
	 * @return node key
	 */
	public long getKey() {
		return mKey;
	}

	/**
	 * Get the page kind.
	 * 
	 * @return page kind
	 */
	public PageKind getPage() {
		return mPage;
	}
}
