package org.sirix.service.xml.shredder;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.INodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.EKind;
import org.sirix.settings.EFixed;

/**
 * Skeleton implementation of {@link IShredder} interface methods.
 * 
 * All methods throw {@link NullPointerException}s in case of {@code null}
 * values for reference parameters and check the arguments, whereas in case they
 * are not valid a {@link IllegalArgumentException} is thrown.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * @author Marc Kramis, Seabix GmbH
 * 
 */
public abstract class AbsShredder implements IShredder<String, QName> {

	/** Sirix {@link INodeWriteTrx}. */
	private final INodeWriteTrx mWtx;

	/** Keeps track of visited keys. */
	private final Deque<Long> mParents;

	/** Determines the import location of a new node. */
	private EInsert mInsertLocation;

	/**
	 * Constructor.
	 * 
	 * @throws NullPointerException
	 *           if {@code pWtx} is {@code null}
	 */
	public AbsShredder(final @Nonnull INodeWriteTrx pWtx,
			final @Nonnull EInsert pInsertLocation) {
		mWtx = checkNotNull(pWtx);
		mInsertLocation = checkNotNull(pInsertLocation);
		mParents = new ArrayDeque<>();
		mParents.push(EFixed.NULL_NODE_KEY.getStandardProperty());
	}

	@Override
	public void processStartTag(final @Nonnull QName pName) throws SirixException {
		final QName name = checkNotNull(pName);
		long key = -1;
		switch (mInsertLocation) {
		case ASFIRSTCHILD:
			if (mParents.peek() == EFixed.NULL_NODE_KEY.getStandardProperty()) {
				key = mWtx.insertElementAsFirstChild(name).getNode().getNodeKey();
			} else {
				key = mWtx.insertElementAsRightSibling(name).getNode().getNodeKey();
			}
			break;
		case ASRIGHTSIBLING:
			if (mWtx.getNode().getKind() == EKind.DOCUMENT_ROOT
					|| mWtx.getNode().getParentKey() == EFixed.DOCUMENT_NODE_KEY
							.getStandardProperty()) {
				throw new IllegalStateException(
						"Subtree can not be inserted as sibling of document root or the root-element!");
			}
			key = mWtx.insertElementAsRightSibling(name).getNode().getNodeKey();
			mInsertLocation = EInsert.ASFIRSTCHILD;
			break;
		case ASLEFTSIBLING:
			if (mWtx.getNode().getKind() == EKind.DOCUMENT_ROOT
					|| mWtx.getNode().getParentKey() == EFixed.DOCUMENT_NODE_KEY
							.getStandardProperty()) {
				throw new IllegalStateException(
						"Subtree can not be inserted as sibling of document root or the root-element!");
			}
			key = mWtx.insertElementAsLeftSibling(name).getNode().getNodeKey();
			mInsertLocation = EInsert.ASFIRSTCHILD;
			break;
		}

		mParents.pop();
		mParents.push(key);
		mParents.push(EFixed.NULL_NODE_KEY.getStandardProperty());
	}

	@Override
	public void processText(final @Nonnull String pText) throws SirixException {
		final String text = checkNotNull(pText);
		long key;
		if (!text.isEmpty()) {
			if (mParents.peek() == EFixed.NULL_NODE_KEY.getStandardProperty()) {
				key = mWtx.insertTextAsFirstChild(text).getNode().getNodeKey();
			} else {
				key = mWtx.insertTextAsRightSibling(text).getNode().getNodeKey();
			}

			mParents.pop();
			mParents.push(key);
		}
	}

	@Override
	public void processEndTag(final @Nonnull QName pName) {
		mParents.pop();
		mWtx.moveTo(mParents.peek());
	}

	@Override
	public void processEmptyElement(final @Nonnull QName pName)
			throws SirixException {
		processStartTag(pName);
		processEndTag(pName);
	}
}
