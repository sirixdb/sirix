package org.sirix.xquery.node;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.node.parser.SubtreeHandler;
import org.brackit.xquery.node.parser.SubtreeListener;
import org.brackit.xquery.xdm.DocumentException;
import org.sirix.api.INodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.shredder.AbsShredder;
import org.sirix.service.xml.shredder.EInsert;

/**
 * Subtree builder to build a new tree.
 * 
 * @author Johannes Lichtenberger
 * 
 * @param <E>
 *          temporal node which extends {@link AbsTemporalNode}
 */
public final class SubtreeBuilder<E extends AbsTemporalNode> extends
		AbsShredder implements SubtreeHandler {

	/** {@link SubtreeProcessor} for listeners. */
	private final SubtreeProcessor<AbsTemporalNode> mSubtreeProcessor;

	/** Sirix {@link INodeWriteTrx}. */
	private final INodeWriteTrx mWtx;

	/** Stack for saving the parent nodes. */
	private final Deque<DBNode> mParents;

	/** Collection. */
	private final DBCollection<? extends AbsTemporalNode> mCollection;
	
	/** First element. */
	private boolean mFirst;
	
	/** Start node key. */
	private long mStartNodeKey;

	/**
	 * Constructor.
	 * 
	 * @param pWtx
	 *          Sirix {@link IWriteTransaction}
	 * @param pInsert
	 *          determines how to insert (as a right sibling, first child or left
	 *          sibling)
	 * @param pListeners
	 *          listeners which implement
	 * @throws SirixException
	 *           if constructor couldn't be fully constructed because building a
	 *           new reading transaction failed (might indicate that a few
	 */
	public SubtreeBuilder(
			final @Nonnull DBCollection<? extends AbsTemporalNode> pCollection,
			final @Nonnull INodeWriteTrx pWtx, final @Nonnull EInsert pInsert,
			final @Nonnull List<SubtreeListener<? super AbsTemporalNode>> pListeners)
			throws SirixException {
		super(pWtx, pInsert);
		mCollection = checkNotNull(pCollection);
		mSubtreeProcessor = new SubtreeProcessor<AbsTemporalNode>(
				checkNotNull(pListeners));
		mWtx = checkNotNull(pWtx);
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
	public void startMapping(final String pPrefix, final String pUri)
			throws DocumentException {
	}

	@Override
	public void endMapping(final String pPrefix) throws DocumentException {
	}

	@Override
	public void comment(final Atomic pContent) throws DocumentException {
		try {
			processComment(pContent.asStr().stringValue());
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
	public void processingInstruction(final QNm pTarget, final Atomic pContent)
			throws DocumentException {
		try {
			processPI(pContent.asStr().stringValue(), pTarget.localName);
			mSubtreeProcessor.notifyProcessingInstruction(new DBNode(mWtx, mCollection));
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public void startElement(final QNm pName) throws DocumentException {
		try {
			processStartTag(new QName(pName.nsURI, pName.localName, pName.prefix));
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
	public void endElement(final QNm pName) throws DocumentException {
		processEndTag(new QName(pName.nsURI, pName.localName, pName.prefix));
		final DBNode node = mParents.pop();
		mSubtreeProcessor.notifyEndElement(node);
	}

	@Override
	public void text(final Atomic pContent) throws DocumentException {
		try {
			processText(pContent.stringValue());
			mSubtreeProcessor.notifyText(new DBNode(mWtx, mCollection));
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public void attribute(final QNm pName, final Atomic pValue)
			throws DocumentException {
		try {
			mWtx.insertAttribute(
					new QName(pName.nsURI, pName.localName, pName.prefix),
					pValue.stringValue());
			mWtx.moveToParent();
			mSubtreeProcessor.notifyAttribute(new DBNode(mWtx, mCollection));
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

}
