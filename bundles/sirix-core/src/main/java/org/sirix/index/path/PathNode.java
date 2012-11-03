package org.sirix.index.path;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.AbstractStructForwardingNode;
import org.sirix.node.Kind;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.NameNode;

import com.google.common.base.Objects;

/**
 * Path node in the {@link PathSummary}.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class PathNode extends AbstractStructForwardingNode implements NameNode {

	/** {@link NodeDelegate} instance. */
	private final NodeDelegate mNodeDel;

	/** {@link StructNodeDelegate} instance. */
	private final StructNodeDelegate mStructNodeDel;

	/** {@link NameNodeDelegate} instance. */
	private final NameNodeDelegate mNameNodeDel;

	/** Kind of node to index. */
	private final Kind mKind;

	/** Number of references to this path node. */
	private int mReferences;

	/** Level of this path node. */
	private int mLevel;

	/**
	 * Constructor.
	 * 
	 * @param nodeDel
	 *          {@link NodeDelegate} instance
	 * @param structNodeDel
	 *          {@link StructNodeDelegate} instance
	 * @param nameNodeDel
	 *          {@link NameNodeDelegate} instance
	 * @param kind
	 *          kind of node to index
	 * @param references
	 *          number of references to this path node
	 * @param level
	 *          level of this path node
	 */
	public PathNode(@Nonnull final NodeDelegate nodeDel,
			@Nonnull final StructNodeDelegate structNodeDel,
			@Nonnull final NameNodeDelegate nameNodeDel, @Nonnull final Kind kind,
			@Nonnegative final int references, @Nonnegative final int level) {
		mNodeDel = checkNotNull(nodeDel);
		mStructNodeDel = checkNotNull(structNodeDel);
		mNameNodeDel = checkNotNull(nameNodeDel);
		mKind = checkNotNull(kind);
		checkArgument(references > 0, "pReferences must be > 0!");
		mReferences = references;
		mLevel = level;
	}

	/**
	 * Level of this path node.
	 * 
	 * @return level of this path node
	 */
	public int getLevel() {
		return mLevel;
	}

	/**
	 * Get the number of references to this path node.
	 * 
	 * @return number of references
	 */
	public int getReferences() {
		return mReferences;
	}

	/**
	 * Set the reference count.
	 * 
	 * @param references
	 *          number of references
	 */
	public void setReferenceCount(final @Nonnegative int references) {
		checkArgument(references > 0, "pReferences must be > 0!");
		mReferences = references;
	}

	/**
	 * Increment the reference count.
	 */
	public void incrementReferenceCount() {
		mReferences++;
	}

	/**
	 * Decrement the reference count.
	 */
	public void decrementReferenceCount() {
		if (mReferences <= 1) {
			throw new IllegalStateException();
		}
		mReferences--;
	}

	/**
	 * Get the kind of path (element, attribute or namespace).
	 * 
	 * @return path kind
	 */
	public Kind getPathKind() {
		return mKind;
	}

	@Override
	public Kind getKind() {
		return Kind.PATH;
	}

	@Override
	public int getNameKey() {
		return mNameNodeDel.getNameKey();
	}

	@Override
	public int getURIKey() {
		return mNameNodeDel.getURIKey();
	}

	@Override
	public void setNameKey(final int nameKey) {
		mNameNodeDel.setNameKey(nameKey);
	}

	@Override
	public void setURIKey(final int uriKey) {
		mNameNodeDel.setURIKey(uriKey);
	}

	@Override
	public VisitResultType acceptVisitor(final @Nonnull Visitor visitor) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected StructNodeDelegate structDelegate() {
		return mStructNodeDel;
	}

	@Override
	protected NodeDelegate delegate() {
		return mNodeDel;
	}

	/**
	 * Get the name node delegate.
	 * 
	 * @return name node delegate.
	 */
	public NameNodeDelegate getNameNodeDelegate() {
		return mNameNodeDel;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mNodeDel, mNameNodeDel);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof PathNode) {
			final PathNode other = (PathNode) obj;
			return Objects.equal(mNodeDel, other.mNodeDel)
					&& Objects.equal(mNameNodeDel, other.mNameNodeDel);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("node delegate", mNodeDel)
				.add("struct delegate", mStructNodeDel)
				.add("name delegate", mNameNodeDel).add("references", mReferences)
				.add("kind", mKind).add("level", mLevel).toString();
	}

	@Override
	public void setPathNodeKey(final long pNodeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getPathNodeKey() {
		throw new UnsupportedOperationException();
	}

}
