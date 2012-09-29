package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.sirix.api.INodeCursor;

/**
 * Determines if the {@link INodeCursor} moved to a node or not. Based on the
 * idea of providing a wrapper just like in Google Guava's {@link Optional}
 * class.
 * 
 * @author Johannes Lichtenberger
 * 
 * @param <T>
 *          type parameter, the cursor
 */
public abstract class Move<T extends INodeCursor> {
	/**
	 * Returns a {@link Moved} instance with no contained reference.
	 */
	@SuppressWarnings("unchecked")
	public static <T extends INodeCursor> Move<T> notMoved() {
		return (Move<T>) NotMoved.INSTANCE;
	}

	/**
	 * Returns a {@code Moved} instance containing the given non-null reference.
	 */
	public static <T extends INodeCursor> Moved<T> moved(final @Nonnull T pMoved) {
		return new Moved<T>(checkNotNull(pMoved));
	}

	/**
	 * Determines if the cursor has moved.
	 * 
	 * @return {@code true} if it has moved, {@code false} otherwise
	 */
	public abstract boolean hasMoved();

	/**
	 * Get the cursor reference.
	 * 
	 * @return cursor reference
	 */
	public abstract T get();
}
