package org.sirix.node;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.immutable.ImmutableComment;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

import com.google.common.base.Objects;

/**
 * Comment node implementation.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class CommentNode extends AbstractStructForwardingNode implements
		ValueNode {

	/** {@link StructNodeDelegate} reference. */
	private final StructNodeDelegate mStructNodeDel;

	/** {@link ValNodeDelegate} reference. */
	private final ValNodeDelegate mValDel;

	/** Value of the node. */
	private byte[] mValue;

	/**
	 * Constructor for TextNode.
	 * 
	 * @param pDel
	 *          delegate for {@link Node} implementation
	 * @param valDel
	 *          delegate for {@link ValueNode} implementation
	 * @param structDel
	 *          delegate for {@link StructNode} implementation
	 */
	public CommentNode(final ValNodeDelegate valDel,
			final StructNodeDelegate structDel) {
		mStructNodeDel = checkNotNull(structDel);
		mValDel = checkNotNull(valDel);
	}

	@Override
	public Kind getKind() {
		return Kind.COMMENT;
	}

	@Override
	public byte[] getRawValue() {
		if (mValue == null) {
			mValue = mValDel.getRawValue();
		}
		return mValue;
	}

	@Override
	public void setValue(final byte[] value) {
		mValue = null;
		mValDel.setValue(value);
	}

	@Override
	public long getFirstChildKey() {
		return Fixed.NULL_NODE_KEY.getStandardProperty();
	}

	@Override
	public VisitResult acceptVisitor(final Visitor visitor) {
		return visitor.visit(ImmutableComment.of(this));
	}

	@Override
	public void decrementChildCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void incrementChildCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getDescendantCount() {
		return 0;
	}

	@Override
	public void decrementDescendantCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void incrementDescendantCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDescendantCount(final long descendantCount) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mStructNodeDel.getNodeDelegate(), mValDel);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof CommentNode) {
			final CommentNode other = (CommentNode) obj;
			return Objects.equal(mStructNodeDel.getNodeDelegate(),
					other.getNodeDelegate())
					&& mValDel.equals(other.mValDel);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("node delegate", mStructNodeDel.getNodeDelegate())
				.add("value delegate", mValDel).toString();
	}

	public ValNodeDelegate getValNodeDelegate() {
		return mValDel;
	}

	@Override
	protected NodeDelegate delegate() {
		return mStructNodeDel.getNodeDelegate();
	}

	@Override
	protected StructNodeDelegate structDelegate() {
		return mStructNodeDel;
	}
	
	@Override
	public String getValue() {
		return new String(mValDel.getRawValue(), Constants.DEFAULT_ENCODING);
	}

}
