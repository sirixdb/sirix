package org.sirix.node;

import javax.annotation.Nonnegative;

import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.StructNode;

import com.google.common.base.MoreObjects;

/**
 * Skeletal implementation of {@link StructNode} interface.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractStructForwardingNode extends
		AbstractForwardingNode implements StructNode {

	/** Constructor for use by subclasses. */
	protected AbstractStructForwardingNode() {
	}

	/** {@link StructNodeDelegate} instance. */
	protected abstract StructNodeDelegate structDelegate();

	/**
	 * Getting the inlying {@link NodeDelegate}.
	 *
	 * @return the inlying {@link NodeDelegate} instance
	 */
	public StructNodeDelegate getStructNodeDelegate() {
		return structDelegate();
	}

	@Override
	public boolean hasFirstChild() {
		return structDelegate().hasFirstChild();
	}

	@Override
	public boolean hasLeftSibling() {
		return structDelegate().hasLeftSibling();
	}

	@Override
	public boolean hasRightSibling() {
		return structDelegate().hasRightSibling();
	}

	@Override
	public long getChildCount() {
		return structDelegate().getChildCount();
	}

	@Override
	public long getFirstChildKey() {
		return structDelegate().getFirstChildKey();
	}

	@Override
	public long getLeftSiblingKey() {
		return structDelegate().getLeftSiblingKey();
	}

	@Override
	public long getRightSiblingKey() {
		return structDelegate().getRightSiblingKey();
	}

	@Override
	public void setRightSiblingKey(final long key) {
		structDelegate().setRightSiblingKey(key);
	}

	@Override
	public void setLeftSiblingKey(final long key) {
		structDelegate().setLeftSiblingKey(key);
	}

	@Override
	public void setFirstChildKey(final long key) {
		structDelegate().setFirstChildKey(key);
	}

	@Override
	public void decrementChildCount() {
		structDelegate().decrementChildCount();
	}

	@Override
	public void incrementChildCount() {
		structDelegate().incrementChildCount();
	}

	@Override
	public long getDescendantCount() {
		return structDelegate().getDescendantCount();
	}

	@Override
	public void decrementDescendantCount() {
		structDelegate().decrementDescendantCount();
	}

	@Override
	public void incrementDescendantCount() {
		structDelegate().incrementDescendantCount();
	}

	@Override
	public void setDescendantCount(final @Nonnegative long descendantCount) {
		structDelegate().setDescendantCount(descendantCount);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("nodeDelegate", super.toString())
				.add("structDelegate", structDelegate().toString()).toString();
	}

}
