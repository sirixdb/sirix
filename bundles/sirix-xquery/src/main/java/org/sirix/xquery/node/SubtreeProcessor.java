package org.sirix.xquery.node;

import java.util.ArrayList;
import java.util.List;

import org.brackit.xquery.node.parser.SubtreeListener;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Node;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Johannes Lichtenberger
 * 
 * @param <E>
 */
public class SubtreeProcessor<E extends Node<E>> {
	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(SubtreeProcessor.class));
	
	private final List<SubtreeListener<? super E>> mListeners;

	public SubtreeProcessor(final List<SubtreeListener<? super E>> listeners) {
		mListeners = new ArrayList<SubtreeListener<? super E>>();
		if (listeners != null) {
			for (final SubtreeListener<? super E> listener : listeners) {
				mListeners.add(listener);
			}
		}
	}

	public void notifyBegin() throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.begin();
		}
	}

	public void notifyEnd() throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.end();
		}
	}

	public void notifyBeginDocument() throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.startDocument();
		}
	}

	public void notifyEndDocument() throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.endDocument();
		}
	}

	public void notifyBeginFragment() throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.beginFragment();
		}
	}

	public void notifyEndFragment() throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.endFragment();
		}
	}

	public void notifyFail() {
		for (final SubtreeListener<? super E> listener : mListeners) {
			try {
				listener.fail();
			} catch (final DocumentException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}
		}
	}

	public void notifyStartElement(E node) throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.startElement(node);
		}
	}

	public void notifyEndElement(E node) throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.endElement(node);
		}
	}

	public void notifyAttribute(E node) throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.attribute(node);
		}
	}

	public void notifyText(E node) throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.text(node);
		}
	}

	public void notifyComment(E node) throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.comment(node);
		}
	}

	public void notifyProcessingInstruction(E node) throws DocumentException {
		for (SubtreeListener<? super E> listener : mListeners) {
			listener.processingInstruction(node);
		}
	}
}
