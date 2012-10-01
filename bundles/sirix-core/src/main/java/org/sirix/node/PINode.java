package org.sirix.node;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.immutable.ImmutablePI;
import org.sirix.node.interfaces.INameNode;
import org.sirix.node.interfaces.IValNode;

import com.google.common.base.Objects;

/**
 * <h1>PINode</h1>
 * 
 * <p>
 * Node representing a processing instruction.
 * </p>
 */
public final class PINode extends AbsStructForwardingNode implements IValNode,
		INameNode {

	/** Delegate for name node information. */
	private final NameNodeDelegate mNameDel;

	/** Delegate for val node information. */
	private final ValNodeDelegate mValDel;

	/** Delegate for structural node information. */
	private final StructNodeDelegate mStructDel;

	/**
	 * Creating an attribute.
	 * 
	 * @param pStructDel
	 *          {@link StructNodeDelegate} to be set
	 * @param pNameDel
	 *          {@link NameNodeDelegate} to be set
	 * @param pValDel
	 *          {@link ValNodeDelegate} to be set
	 * 
	 */
	public PINode(@Nonnull final StructNodeDelegate pStructDel,
			@Nonnull final NameNodeDelegate pNameDel,
			@Nonnull final ValNodeDelegate pValDel) {
		mStructDel = checkNotNull(pStructDel);
		mNameDel = checkNotNull(pNameDel);
		mValDel = pValDel;
	}

	@Override
	public EKind getKind() {
		return EKind.PROCESSING;
	}

	@Override
	public EVisitResult acceptVisitor(final @Nonnull IVisitor pVisitor) {
		return pVisitor.visit(ImmutablePI.of(this));
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("structDel", mStructDel)
				.add("nameDel", mNameDel).add("valDel", mValDel).toString();
	}

	@Override
	public int getNameKey() {
		return mNameDel.getNameKey();
	}

	@Override
	public int getURIKey() {
		return mNameDel.getURIKey();
	}

	@Override
	public void setNameKey(final int pNameKey) {
		mNameDel.setNameKey(pNameKey);
	}

	@Override
	public void setURIKey(final int pUriKey) {
		mNameDel.setURIKey(pUriKey);
	}

	@Override
	public byte[] getRawValue() {
		return mValDel.getRawValue();
	}

	@Override
	public void setValue(@Nonnull final byte[] pVal) {
		mValDel.setValue(pVal);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mNameDel, mValDel);
	}

	@Override
	public boolean equals(@Nullable final Object pObj) {
		boolean retVal = false;
		if (pObj instanceof PINode) {
			final PINode other = (PINode) pObj;
			retVal = Objects.equal(mNameDel, other.mNameDel)
					&& Objects.equal(mValDel, other.mValDel);
		}
		return retVal;
	}

	@Override
	public void setPathNodeKey(@Nonnegative final long pPathNodeKey) {
		mNameDel.setPathNodeKey(pPathNodeKey);
	}

	@Override
	public long getPathNodeKey() {
		return mNameDel.getPathNodeKey();
	}

	/**
	 * Getting the inlying {@link NameNodeDelegate}.
	 * 
	 * @return the {@link NameNodeDelegate} instance
	 */
	NameNodeDelegate getNameNodeDelegate() {
		return mNameDel;
	}

	/**
	 * Getting the inlying {@link ValNodeDelegate}.
	 * 
	 * @return the {@link ValNodeDelegate} instance
	 */
	ValNodeDelegate getValNodeDelegate() {
		return mValDel;
	}

	@Override
	protected NodeDelegate delegate() {
		return mStructDel.getNodeDelegate();
	}

	@Override
	protected StructNodeDelegate structDelegate() {
		return mStructDel;
	}
}