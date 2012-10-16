package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.NodeCursor;

import com.google.common.base.Objects;

/**
 * Determines that a {@link NodeCursor} has been moved.
 * 
 * @author Johannes Lichtenberger
 * 
 * @param <T>
 *          the cursor instance
 */
public class Moved<T extends NodeCursor> extends Move<T> {

	/** {@link NodeCursor} implementation. */
	private final T mNodeCursor;

	/**
	 * Constructor.
	 * 
	 * @param pNodeCursor
	 *          the cursor which has been moved
	 */
	public Moved(final @Nonnull T pNodeCursor) {
		mNodeCursor = checkNotNull(pNodeCursor);
	}

	@Override
	public boolean hasMoved() {
		return true;
	}

	@Override
	public T get() {
		return mNodeCursor;
	}

	@Override
	public boolean equals(final @Nullable Object object) {
		if (object instanceof Moved) {
			final Moved<?> other = (Moved<?>) object;
			return mNodeCursor.equals(other.mNodeCursor);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return 0x598df91c + mNodeCursor.hashCode();
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("cursor", mNodeCursor).toString();
	}
}
