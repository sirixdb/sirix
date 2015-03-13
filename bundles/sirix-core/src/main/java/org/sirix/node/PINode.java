package org.sirix.node;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageReadTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.immutable.ImmutablePI;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.settings.Constants;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * <h1>PINode</h1>
 *
 * <p>
 * Node representing a processing instruction.
 * </p>
 */
public final class PINode extends AbstractStructForwardingNode implements
		ValueNode, NameNode {

	/** Delegate for name node information. */
	private final NameNodeDelegate mNameDel;

	/** Delegate for val node information. */
	private final ValNodeDelegate mValDel;

	/** Delegate for structural node information. */
	private final StructNodeDelegate mStructDel;

	/** {@link PageReadTrx} reference. */
	private final PageReadTrx mPageReadTrx;

	/**
	 * Creating an attribute.
	 *
	 * @param structDel
	 *          {@link StructNodeDelegate} to be set
	 * @param nameDel
	 *          {@link NameNodeDelegate} to be set
	 * @param valDel
	 *          {@link ValNodeDelegate} to be set
	 *
	 */
	public PINode(final StructNodeDelegate structDel,
			final NameNodeDelegate nameDel, final ValNodeDelegate valDel,
			final PageReadTrx pageReadTrx) {
		assert structDel != null : "structDel must not be null!";
		mStructDel = structDel;
		assert nameDel != null : "nameDel must not be null!";
		mNameDel = nameDel;
		assert valDel != null : "valDel must not be null!";
		mValDel = valDel;
		assert pageReadTrx != null : "pageReadTrx must not be null!";
		mPageReadTrx = pageReadTrx;
	}

	@Override
	public Kind getKind() {
		return Kind.PROCESSING_INSTRUCTION;
	}

	@Override
	public VisitResult acceptVisitor(final Visitor visitor) {
		return visitor.visit(ImmutablePI.of(this));
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("structDel", mStructDel)
				.add("nameDel", mNameDel).add("valDel", mValDel).toString();
	}

	@Override
	public int getPrefixKey() {
		return mNameDel.getPrefixKey();
	}

	@Override
	public int getLocalNameKey() {
		return mNameDel.getLocalNameKey();
	}

	@Override
	public int getURIKey() {
		return mNameDel.getURIKey();
	}

	@Override
	public void setPrefixKey(final int prefixKey) {
		mNameDel.setPrefixKey(prefixKey);
	}

	@Override
	public void setLocalNameKey(final int localNameKey) {
		mNameDel.setLocalNameKey(localNameKey);
	}

	@Override
	public void setURIKey(final int uriKey) {
		mNameDel.setURIKey(uriKey);
	}

	@Override
	public byte[] getRawValue() {
		return mValDel.getRawValue();
	}

	@Override
	public void setValue(final byte[] value) {
		mValDel.setValue(value);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mNameDel, mValDel);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof PINode) {
			final PINode other = (PINode) obj;
			return Objects.equal(mNameDel, other.mNameDel)
					&& Objects.equal(mValDel, other.mValDel);
		}
		return false;
	}

	@Override
	public void setPathNodeKey(final @Nonnegative long pathNodeKey) {
		mNameDel.setPathNodeKey(pathNodeKey);
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

	@Override
	public QNm getName() {
		final String uri = mPageReadTrx.getName(mNameDel.getURIKey(),
				Kind.NAMESPACE);
		final int prefixKey = mNameDel.getPrefixKey();
		final String prefix = prefixKey == -1 ? "" : mPageReadTrx.getName(
				prefixKey, Kind.PROCESSING_INSTRUCTION);
		final int localNameKey = mNameDel.getLocalNameKey();
		final String localName = localNameKey == -1 ? "" : mPageReadTrx.getName(
				localNameKey, Kind.PROCESSING_INSTRUCTION);
		return new QNm(uri, prefix, localName);
	}

	@Override
	public String getValue() {
		return new String(mValDel.getRawValue(), Constants.DEFAULT_ENCODING);
	}
}