package org.sirix.service.xml.shredder;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.settings.Fixed;

/**
 * Skeleton implementation of {@link Shredder} interface methods.
 * 
 * All methods throw {@link NullPointerException}s in case of {@code null}
 * values for reference parameters and check the arguments, whereas in case they
 * are not valid a {@link IllegalArgumentException} is thrown.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * @author Marc Kramis, Seabix GmbH
 * 
 */
public abstract class AbsShredder implements Shredder<String, QName> {

	/** Sirix {@link NodeWriteTrx}. */
	private final NodeWriteTrx mWtx;

	/** Keeps track of visited keys. */
	private final Deque<Long> mParents;

	/** Determines the import location of a new node. */
	private Insert mInsertLocation;

	/**
	 * Constructor.
	 * 
	 * @throws NullPointerException
	 *           if {@code pWtx} is {@code null}
	 */
	public AbsShredder(final @Nonnull NodeWriteTrx wtx,
			final @Nonnull Insert insertLocation) {
		mWtx = checkNotNull(wtx);
		mInsertLocation = checkNotNull(insertLocation);
		mParents = new ArrayDeque<>();
		mParents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
	}

	@Override
	public void processComment(final @Nonnull String commentValue)
			throws SirixException {
		final String value = checkNotNull(commentValue);
		long key;
		if (!value.isEmpty()) {
			if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
				key = mWtx.insertCommentAsFirstChild(value).getNodeKey();
			} else {
				key = mWtx.insertCommentAsRightSibling(value).getNodeKey();
			}

			mParents.pop();
			mParents.push(key);
		}
	}

	@Override
	public void processPI(final @Nonnull String processingContent,
			final @Nonnull String processingTarget) throws SirixException {
		final String content = checkNotNull(processingContent);
		final String target = checkNotNull(processingTarget);
		long key;
		if (!target.isEmpty()) {
			if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
				key = mWtx.insertPIAsFirstChild(target, content).getNodeKey();
			} else {
				key = mWtx.insertPIAsRightSibling(target, content).getNodeKey();
			}

			mParents.pop();
			mParents.push(key);
		}
	}

	@Override
	public void processText(final @Nonnull String textValue) throws SirixException {
		final String text = checkNotNull(textValue);
		long key;
		if (!text.isEmpty()) {
			if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
				key = mWtx.insertTextAsFirstChild(text).getNodeKey();
			} else {
				key = mWtx.insertTextAsRightSibling(text).getNodeKey();
			}

			mParents.pop();
			mParents.push(key);
		}
	}

	@Override
	public void processStartTag(final @Nonnull QName elementName) throws SirixException {
		final QName name = checkNotNull(elementName);
		long key = -1;
		switch (mInsertLocation) {
		case ASFIRSTCHILD:
			if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
				key = mWtx.insertElementAsFirstChild(name).getNodeKey();
			} else {
				key = mWtx.insertElementAsRightSibling(name).getNodeKey();
			}
			break;
		case ASRIGHTSIBLING:
			if (mWtx.getKind() == Kind.DOCUMENT_ROOT
					|| mWtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY
							.getStandardProperty()) {
				throw new IllegalStateException(
						"Subtree can not be inserted as sibling of document root or the root-element!");
			}
			key = mWtx.insertElementAsRightSibling(name).getNodeKey();
			mInsertLocation = Insert.ASFIRSTCHILD;
			break;
		case ASLEFTSIBLING:
			if (mWtx.getKind() == Kind.DOCUMENT_ROOT
					|| mWtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY
							.getStandardProperty()) {
				throw new IllegalStateException(
						"Subtree can not be inserted as sibling of document root or the root-element!");
			}
			key = mWtx.insertElementAsLeftSibling(name).getNodeKey();
			mInsertLocation = Insert.ASFIRSTCHILD;
			break;
		}

		mParents.pop();
		mParents.push(key);
		mParents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
	}

	@Override
	public void processEndTag(final @Nonnull QName elementName) {
		mParents.pop();
		mWtx.moveTo(mParents.peek());
	}

	@Override
	public void processEmptyElement(final @Nonnull QName alementName)
			throws SirixException {
		processStartTag(alementName);
		processEndTag(alementName);
	}
}
