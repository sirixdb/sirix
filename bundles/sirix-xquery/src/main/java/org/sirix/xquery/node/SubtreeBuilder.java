package org.sirix.xquery.node;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import javax.annotation.Nonnull;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.node.parser.SubtreeHandler;
import org.brackit.xquery.node.parser.SubtreeListener;
import org.brackit.xquery.xdm.AbstractTemporalNode;
import org.brackit.xquery.xdm.DocumentException;
import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.shredder.AbstractShredder;
import org.sirix.service.xml.shredder.Insert;

/**
 * Subtree builder to build a new tree.
 * 
 * @author Johannes Lichtenberger
 * 
 * @param <E>
 *          temporal node which extends {@link AbstractTemporalNode}
 */
public final class SubtreeBuilder extends AbstractShredder implements
		SubtreeHandler {

	/** {@link SubtreeProcessor} for listeners. */
	private final SubtreeProcessor<AbstractTemporalNode<DBNode>> mSubtreeProcessor;

	/** Sirix {@link NodeWriteTrx}. */
	private final NodeWriteTrx mWtx;

	/** Stack for saving the parent nodes. */
	private final Deque<DBNode> mParents;

	/** Collection. */
	private final DBCollection mCollection;

	/** First element. */
	private boolean mFirst;

	/** Start node key. */
	private long mStartNodeKey;

	/**
	 * Constructor.
	 * 
	 * @param wtx
	 *          Sirix {@link IWriteTransaction}
	 * @param insertPos
	 *          determines how to insert (as a right sibling, first child or left
	 *          sibling)
	 * @param listeners
	 *          listeners which implement
	 * @throws SirixException
	 *           if constructor couldn't be fully constructed because building a
	 *           new reading transaction failed (might indicate that a few
	 */
	public SubtreeBuilder(
			final DBCollection collection,
			final NodeWriteTrx wtx,
			final Insert insertPos,
			final List<SubtreeListener<? super AbstractTemporalNode<DBNode>>> listeners)
			throws SirixException {
		super(wtx, insertPos);
		mCollection = checkNotNull(collection);
		mSubtreeProcessor = new SubtreeProcessor<AbstractTemporalNode<DBNode>>(
				checkNotNull(listeners));
		mWtx = checkNotNull(wtx);
		mParents = new ArrayDeque<>();
		mFirst = true;
	}

	/**
	 * Get start node key.
	 * 
	 * @return start node key
	 */
	public long getStartNodeKey() {
		return mStartNodeKey;
	}

	@Override
	public void begin() throws DocumentException {
		try {
			mSubtreeProcessor.notifyBegin();
		} catch (final DocumentException e) {
			mSubtreeProcessor.notifyFail();
			throw e;
		}
	}

	@Override
	public void end() throws DocumentException {
		try {
			mSubtreeProcessor.notifyEnd();
		} catch (final DocumentException e) {
			mSubtreeProcessor.notifyFail();
			throw e;
		}
	}

	@Override
	public void beginFragment() throws DocumentException {
		try {
			mSubtreeProcessor.notifyBeginFragment();
		} catch (final DocumentException e) {
			mSubtreeProcessor.notifyFail();
			throw e;
		}
	}

	@Override
	public void endFragment() throws DocumentException {
		try {
			mSubtreeProcessor.notifyEndFragment();
		} catch (final DocumentException e) {
			mSubtreeProcessor.notifyFail();
			throw e;
		}
	}

	@Override
	public void startDocument() throws DocumentException {
		try {
			mSubtreeProcessor.notifyBeginDocument();
		} catch (final DocumentException e) {
			mSubtreeProcessor.notifyFail();
			throw e;
		}
	}

	@Override
	public void endDocument() throws DocumentException {
		try {
			mSubtreeProcessor.notifyEndDocument();
		} catch (final DocumentException e) {
			mSubtreeProcessor.notifyFail();
			throw e;
		}
	}

	@Override
	public void fail() throws DocumentException {
		mSubtreeProcessor.notifyFail();
	}

	@Override
	public void startMapping(final String prefix, final String uri)
			throws DocumentException {
	}

	@Override
	public void endMapping(final String prefix) throws DocumentException {
	}

	@Override
	public void comment(final Atomic content) throws DocumentException {
		try {
			processComment(content.asStr().stringValue());
			if (mFirst) {
				mFirst = false;
				mStartNodeKey = mWtx.getNodeKey();
			}
			mSubtreeProcessor.notifyComment(new DBNode(mWtx, mCollection));
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public void processingInstruction(final QNm target, final Atomic content)
			throws DocumentException {
		try {
			processPI(content.asStr().stringValue(), target.getLocalName());
			mSubtreeProcessor.notifyProcessingInstruction(new DBNode(mWtx,
					mCollection));
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public void startElement(final QNm name) throws DocumentException {
		try {
			processStartTag(name);
			if (mFirst) {
				mFirst = false;
				mStartNodeKey = mWtx.getNodeKey();
			}
			final DBNode node = new DBNode(mWtx, mCollection);
			mParents.push(node);
			mSubtreeProcessor.notifyStartElement(node);
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public void endElement(final QNm name) throws DocumentException {
		processEndTag(name);
		final DBNode node = mParents.pop();
		mSubtreeProcessor.notifyEndElement(node);
	}

	@Override
	public void text(final Atomic content) throws DocumentException {
		try {
			processText(content.stringValue());
			mSubtreeProcessor.notifyText(new DBNode(mWtx, mCollection));
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public void attribute(final QNm name, final Atomic value)
			throws DocumentException {
		try {
			mWtx.insertAttribute(name, value.stringValue());
			mWtx.moveToParent();
			mSubtreeProcessor.notifyAttribute(new DBNode(mWtx, mCollection));
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

}
