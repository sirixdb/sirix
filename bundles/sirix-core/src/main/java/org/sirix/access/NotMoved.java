package org.sirix.access;

import javax.annotation.Nullable;

import org.sirix.api.INodeCursor;

/**
 * Determines that a {@link INodeCursor} hasn't moved to the node.
 * 
 * @author Johannes Lichtenberger
 *
 */
public class NotMoved extends Move<INodeCursor> {
	/** Singleton instance. */
	static final NotMoved INSTANCE = new NotMoved();

	/** Private constructor. */
	private NotMoved() {
	}

	@Override
	public boolean hasMoved() {
		return false;
	}

	@Override
	public INodeCursor get() {
		throw new IllegalStateException(
				"Optional.get() cannot be called on an absent value");
	}

	@Override
	public boolean equals(final @Nullable Object pObject) {
		return pObject == this;
	}

	@Override
	public int hashCode() {
		return 0x598df91c;
	}

	private Object readResolve() {
		return INSTANCE;
	}
}
