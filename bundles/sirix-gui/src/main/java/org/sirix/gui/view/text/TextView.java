/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.gui.view.text;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.sirix.gui.GUIConstants.ATTRIBUTE_COLOR;
import static org.sirix.gui.GUIConstants.ELEMENT_COLOR;
import static org.sirix.gui.GUIConstants.NAMESPACE_COLOR;
import static org.sirix.gui.GUIConstants.NEWLINE;
import static org.sirix.gui.GUIConstants.TEXT_COLOR;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.AdjustmentEvent;
import java.awt.event.AdjustmentListener;
import java.util.Iterator;

import javax.swing.JComponent;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;
import javax.swing.ScrollPaneConstants;
import javax.swing.text.BadLocationException;
import javax.swing.text.Style;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.NodeReadTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.exception.SirixException;
import org.sirix.gui.GUI;
import org.sirix.gui.GUIProp;
import org.sirix.gui.ReadDB;
import org.sirix.gui.view.DiffAxis;
import org.sirix.gui.view.View;
import org.sirix.gui.view.ViewNotifier;
import org.sirix.gui.view.ViewUtilities;
import org.sirix.gui.view.VisualItem;
import org.sirix.gui.view.VisualItemAxis;
import org.sirix.service.xml.serialize.StAXSerializer;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * 
 * <p>
 * Basic text view.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class TextView extends JScrollPane implements View {

	/**
	 * SerialUID.
	 */
	private static final long serialVersionUID = -5001983007463504219L;

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(TextView.class));

	/** Name of the view. */
	private static final String NAME = "TextView";

	// ======= Global member variables =====

	/** {@link TextView} instance. */
	private static TextView mView;

	/** {@link JTextPane}, which displays XML data. */
	private final JTextPane mText = new JTextPane();

	/** {@link ViewNotifier} which notifies views of changes. */
	private final ViewNotifier mNotifier;

	/** Main {@link GUI} window. */
	private final GUI mGUI;

	/** {@link XMLEventReader} reference. */
	private transient XMLEventReader mSerializer;

	/** Temporal adjustment value. */
	private transient int mTempValue;

	/** Lines changed during adjustment. */
	private transient int mLineChanges;

	/** State to serialize data. */
	private enum State {
		/** Initial state. */
		INITIAL,

		/** Update state. */
		UPDATE,
	};

	/** Determines if a node has children or not. */
	private enum Child {
		/** Node has no children. */
		NOCHILD,

		/** Node has children. */
		CHILD,
	}

	/** {@link NodeReadTrx} reference. */
	private transient NodeReadTrx mRtx;

	/** Temporary level after initial filling of the text area. */
	private transient int mTempLevel;

	/** The {@link Dimension}. */
	private transient Dimension mDimension;

	/** Determines if it's an empty element or not. */
	private transient boolean mEmptyElement;

	private transient boolean mTempEmptyElement;

	/** Diff axis. */
	private transient VisualItemAxis mAxis;

	/** DAO reference. */
	private transient ReadDB mDb;

	/** Temp {@link StAXSerializer} reference. */
	private transient StAXSerializer mTempSerializer;

	/**
	 * Private constructor, called from singleton factory method.
	 * 
	 * @param pNotifier
	 *          {@link ViewNotifier} to notify views of changes etc.pp.
	 */
	private TextView(final ViewNotifier pNotifier) {
		mNotifier = pNotifier;
		mNotifier.add(this);
		mGUI = pNotifier.getGUI();

		// Setup text field.
		mText.setEditable(false);
		mText.setCaretPosition(0);

		// Create a scroll pane and add the XML text area to it.
		setViewportView(mText);
		setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);
		setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
	}

	/**
	 * Singleton factory method.
	 * 
	 * @param pNotifier
	 *          {@link ViewNotifier} to notify views of changes etc.pp.
	 * @return {@link TextView} instance.
	 */
	public synchronized static TextView getInstance(final ViewNotifier pNotifier) {
		if (mView == null) {
			mView = new TextView(pNotifier);
		}

		return mView;
	}

	/**
	 * Not supported.
	 * 
	 * @see Object#clone()
	 */
	@Override
	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException();
	}

	@Override
	public boolean isVisible() {
		return GUIProp.EShowViews.SHOWTEXT.getValue();
	}

	@Override
	public String name() {
		return NAME;
	};

	@Override
	public JComponent component() {
		return this;
	}

	@Override
	public void dispose() {
		try {
			if (mRtx != null) {
				mRtx.close();
			}
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
		final JScrollBar bar = this.getVerticalScrollBar();
		for (final AdjustmentListener listener : bar.getAdjustmentListeners()) {
			bar.removeAdjustmentListener(listener);
		}
	}

	@Override
	public void refreshInit() {
		refreshUpdate(Optional.<VisualItemAxis> absent());
	}

	@Override
	public void refreshUpdate(final Optional<VisualItemAxis> pAxis) {
		checkNotNull(pAxis);
		dispose();
		mAxis = null;
		final Dimension mainFrame = mGUI.getSize();
		mText.setSize(new Dimension(mainFrame.width - 16, mainFrame.height
				- mGUI.getJMenuBar().getHeight() - 37));
		if (pAxis.isPresent()) {
			mAxis = pAxis.get();
		}
		mDb = checkNotNull(mGUI).getReadDB();
		try {
			if (mRtx != null) {
				mRtx.close();
			}
			mRtx = mDb.getSession().beginNodeReadTrx(mDb.getRevisionNumber());
			mRtx.moveTo(mDb.getNodeKey());

			mText.setText("");
			if (mDimension != null) {
				mText.setSize(mDimension);
			}
			mTempValue = 0;
			mLineChanges = 0;
			serialize();
			mText.setCaretPosition(0);
			repaint();
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
		addAdjustmentListener();
	}

	/**
	 * Add an {@link AdjustmentListener} which handles addition of text once
	 * scrolled.
	 */
	private void addAdjustmentListener() {
		final JScrollBar vertScrollBar = getVerticalScrollBar();
		vertScrollBar.setValue(vertScrollBar.getMinimum());

		if (vertScrollBar.getAdjustmentListeners().length == 0) {
			vertScrollBar.addAdjustmentListener(new AdjustmentListener() {
				@Override
				public void adjustmentValueChanged(final AdjustmentEvent pEvt) {
					/*
					 * getValueIsAdjusting() returns true if the user is currently
					 * dragging the scrollbar's knob and has not picked a final value.
					 */
					if (pEvt.getValueIsAdjusting()) {
						// The user is dragging the knob.
						return;
					}

					final int lineHeight = mText.getFontMetrics(mText.getFont())
							.getHeight();
					final int value = pEvt.getValue();
					final int result = value - mTempValue;
					mLineChanges = result / lineHeight;

					LOGWRAPPER.debug("line changes: " + mLineChanges);

					if (mLineChanges > 0) {
						try {
							mLineChanges += 5;
							processStAX(State.UPDATE);
						} catch (final XMLStreamException | BadLocationException e) {
							LOGWRAPPER.error(e.getMessage(), e);
						}
					}

					mTempValue = value;
				}
			});
		}
	}

	/**
	 * Serialize a tree.
	 */
	private void serialize() {
		final NodeReadTrx rtx = mRtx;

		// Style document.
		final StyledDocument doc = (StyledDocument) mText.getDocument();
		final Style styleElements = doc.addStyle("elements", null);
		StyleConstants.setForeground(styleElements, ELEMENT_COLOR);
		final Style styleNamespaces = doc.addStyle("attributes", null);
		StyleConstants.setForeground(styleNamespaces, NAMESPACE_COLOR);
		final Style styleAttributes = doc.addStyle("attributes", null);
		StyleConstants.setForeground(styleAttributes, ATTRIBUTE_COLOR);
		final Style styleText = doc.addStyle("text", null);
		StyleConstants.setForeground(styleText, TEXT_COLOR);

		final long nodeKey = rtx.getNodeKey();

		try {
			switch (rtx.getKind()) {
			case DOCUMENT:
			case ELEMENT:
				mText.setText("");
				if (mAxis == null) {
					mSerializer = new StAXSerializer(rtx);
				} else {
					mSerializer = new StAXDiffSerializer(
							new DiffAxis(IncludeSelf.YES, mDb.getSession().beginNodeReadTrx(
									mDb.getCompareRevisionNumber()), rtx, mAxis));
				}
				processStAX(State.INITIAL);
				break;
			case TEXT:
				rtx.moveTo(nodeKey);
				mText.setText("");
				doc.insertString(doc.getLength(), new String(rtx.getRawValue()),
						styleText);
				break;
			case NAMESPACE:
				// Move transaction to parent of given namespace node.
				rtx.moveToParent();
				mText.setText("");

				final long nNodeKey = rtx.getNodeKey();
				for (int i = 0, namespCount = rtx.getNamespaceCount(); i < namespCount; i++) {
					rtx.moveToNamespace(i);
					if (rtx.getNodeKey() == nodeKey) {
						break;
					}
					rtx.moveTo(nNodeKey);
				}

				if (rtx.nameForKey(rtx.getPrefixKey()).length() == 0) {
					doc.insertString(
							doc.getLength(),
							new StringBuilder().append("xmlns='")
									.append(rtx.nameForKey(rtx.getURIKey())).append("'")
									.toString(), styleNamespaces);
				} else {
					doc.insertString(doc.getLength(), new StringBuilder()
							.append("xmlns:").append(rtx.nameForKey(rtx.getPrefixKey()))
							.append("='").append(rtx.nameForKey(rtx.getURIKey())).append("'")
							.toString(), styleNamespaces);
				}
				break;
			case ATTRIBUTE:
				// Move transaction to parent of given attribute node.
				rtx.moveToParent();
				mText.setText("");

				final long aNodeKey = rtx.getNodeKey();
				for (int i = 0, attsCount = rtx.getAttributeCount(); i < attsCount; i++) {
					rtx.moveToAttribute(i);
					if (rtx.getNodeKey() == nodeKey) {
						break;
					}
					rtx.moveTo(aNodeKey);
				}

				// Display value.
				final String attPrefix = rtx.getName().getPrefix();
				final QNm attQName = rtx.getName();

				if (attPrefix == null || attPrefix.isEmpty()) {
					doc.insertString(doc.getLength(),
							new StringBuilder().append(attQName.getLocalName()).append("='")
									.append(rtx.getValue()).append("'").toString(),
							styleAttributes);
				} else {
					doc.insertString(
							doc.getLength(),
							new StringBuilder().append(attPrefix).append(":")
									.append(attQName.getLocalName()).append("='")
									.append(rtx.getValue()).append("'").toString(),
							styleAttributes);
				}
				break;
			default:
				throw new IllegalStateException("Node kind not known!");
			}
		} catch (final SirixException | BadLocationException | XMLStreamException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	/**
	 * Process StAX output.
	 * 
	 * @param pState
	 *          {@link State} enum, which determines if an initial or update of
	 *          the view occurs.
	 * @throws XMLStreamException
	 *           if any parsing exception occurs
	 * @throws BadLocationException
	 *           if inserting strings into the {@link JTextPane} fails
	 */
	private void processStAX(final State pState) throws XMLStreamException,
			BadLocationException {
		assert pState != null;

		final GUIProp prop = new GUIProp();
		final int indent = prop.getIndentSpaces();
		final StringBuilder spaces = new StringBuilder();
		for (int i = 0; i < indent; i++) {
			spaces.append(" ");
		}
		final String indentSpaces = spaces.toString();

		// Style document.
		final StyledDocument doc = (StyledDocument) mText.getDocument();
		final Style styleElements = doc.addStyle("elements", null);
		StyleConstants.setForeground(styleElements, ELEMENT_COLOR);
		final Style styleNamespaces = doc.addStyle("namespaces", null);
		StyleConstants.setForeground(styleNamespaces, NAMESPACE_COLOR);
		final Style styleAttributes = doc.addStyle("attributes", null);
		StyleConstants.setForeground(styleAttributes, ATTRIBUTE_COLOR);
		final Style styleText = doc.addStyle("text", null);
		StyleConstants.setForeground(styleText, TEXT_COLOR);

		final Style[] styles = new Style[] { styleElements, styleNamespaces,
				styleAttributes, styleText };

		boolean doIndent = true;

		assert mSerializer != null;
		switch (pState) {
		case INITIAL:
			// Initialize variables.
			final int lineHeight = mText.getFontMetrics(this.getFont()).getHeight();
			final int frameHeight = mText.getHeight();
			int level = -1;
			long height = 0;

			while (mSerializer.hasNext() && height < frameHeight) {
				final XMLEvent event = mSerializer.nextEvent();
				if (mSerializer instanceof StAXDiffSerializer) {
					style(((StAXDiffSerializer) mSerializer).getDiff(), styles);
				}

				switch (event.getEventType()) {
				case XMLStreamConstants.START_DOCUMENT:
					doIndent = true;
					break;
				case XMLStreamConstants.END_DOCUMENT:
					if (mTempSerializer != null) {
						mSerializer = mTempSerializer;
						mTempSerializer = null;
					}
					break;
				case XMLStreamConstants.START_ELEMENT:
					level++;
					if (mSerializer instanceof StAXDiffSerializer) {
						if (((StAXDiffSerializer) mSerializer).getDiff() == DiffType.UPDATED) {
							if (doIndent) {
								doIndent = false;
							} else {
								if (!mTempEmptyElement) {
									level--;
								}
								doIndent = true;
							}
						} else {
							doIndent = true;
						}
					}
					indent(doc, level, indentSpaces);
					mEmptyElement = processStartTag(event, doc);
					if (mSerializer instanceof StAXDiffSerializer
							&& ((StAXDiffSerializer) mSerializer).getDiff() == DiffType.UPDATED) {
						mTempEmptyElement = mEmptyElement;
					} else {
						mTempEmptyElement = false;
					}
					height += lineHeight;
					break;
				case XMLStreamConstants.END_ELEMENT:
					if (mSerializer instanceof StAXDiffSerializer) {
						if (((StAXDiffSerializer) mSerializer).getDiff() == DiffType.UPDATED) {
							if (doIndent) {
								doIndent = false;
							} else {
								if (!mEmptyElement) {
									level++;
								}
								doIndent = true;
							}
						} else {
							doIndent = true;
						}
					}
					if (mEmptyElement) {
						mEmptyElement = false;
					} else {
						processEndTag(event, level, indentSpaces, doc, styleElements);
					}
					level--;
					break;
				case XMLStreamConstants.CHARACTERS:
					doIndent = true;
					mTempEmptyElement = false;
					level++;
					indent(doc, level, indentSpaces);
					level--;
					doc.insertString(doc.getLength(), event.asCharacters().getData()
							+ NEWLINE, styleText);
					height += lineHeight;
					break;
				default:
					// Empty.
				}
			}
			mTempLevel = level;
			mDimension = mText.getSize();
			break;
		case UPDATE:
			for (int i = 0; i < mLineChanges && mSerializer.hasNext(); i++) {
				final XMLEvent event = mSerializer.nextEvent();
				if (mSerializer instanceof StAXDiffSerializer) {
					style(((StAXDiffSerializer) mSerializer).getDiff(), styles);
				}

				switch (event.getEventType()) {
				case XMLStreamConstants.START_DOCUMENT:
					doIndent = true;
					break;
				case XMLStreamConstants.END_DOCUMENT:
					if (mTempSerializer != null) {
						mSerializer = mTempSerializer;
						mTempSerializer = null;
					}
					break;
				case XMLStreamConstants.START_ELEMENT:
					mTempLevel++;
					if (mSerializer instanceof StAXDiffSerializer) {
						if (((StAXDiffSerializer) mSerializer).getDiff() == DiffType.UPDATED) {
							if (doIndent) {
								doIndent = false;
							} else {
								if (!mTempEmptyElement) {
									mTempLevel--;
								}
								doIndent = true;
							}
						} else {
							doIndent = true;
						}
					}
					mTempEmptyElement = false;
					indent(doc, mTempLevel, indentSpaces);
					mEmptyElement = processStartTag(event, doc);
					if (mSerializer instanceof StAXDiffSerializer
							&& ((StAXDiffSerializer) mSerializer).getDiff() == DiffType.UPDATED) {
						mTempEmptyElement = mEmptyElement;
					} else {
						mTempEmptyElement = false;
					}
					LOGWRAPPER.debug("start elem: "
							+ event.asStartElement().getName().getLocalPart() + " "
							+ mEmptyElement);
					break;
				case XMLStreamConstants.END_ELEMENT:
					if (mSerializer instanceof StAXDiffSerializer) {
						if (((StAXDiffSerializer) mSerializer).getDiff() == DiffType.UPDATED) {
							if (doIndent) {
								doIndent = false;
							} else {
								if (!mTempEmptyElement) {
									mTempLevel++;
								}
								doIndent = true;
							}
						} else {
							doIndent = true;
						}
					}
					if (mEmptyElement) {
						mEmptyElement = false;
					} else {
						processEndTag(event, mTempLevel, indentSpaces, doc, styleElements);
					}
					mTempLevel--;
					break;
				case XMLStreamConstants.CHARACTERS:
					doIndent = true;
					mTempEmptyElement = false;
					mTempLevel++;
					indent(doc, mTempLevel, indentSpaces);
					mTempLevel--;
					doc.insertString(doc.getLength(), event.asCharacters().getData()
							+ NEWLINE, styleText);
					LOGWRAPPER.debug("characters: " + event.asCharacters().getData());
					break;
				default:
					// Empty.
				}
			}
			break;
		default:
			// Do nothing.
		}
	}

	/**
	 * Style for all kinds of diffs.
	 * 
	 * @param diff
	 *          the diff kind
	 * @param pStyle
	 *          all available styles
	 */
	private void style(final DiffType pDiff, final Style[] pStyle) {
		assert pStyle != null;
		switch (pDiff) {
		case UPDATED:
			final Color green = new Color(40, 220, 40, 50);
			for (final Style style : pStyle) {
				StyleConstants.setBackground(style, green);
			}
			break;
		case REPLACEDOLD:
			final Color darkPink = new Color(255, 10, 220, 100);
			for (final Style style : pStyle) {
				StyleConstants.setBackground(style, darkPink);
			}
			break;
		case REPLACEDNEW:
			final Color pink = new Color(250, 60, 150, 50);
			for (final Style style : pStyle) {
				StyleConstants.setBackground(style, pink);
			}
			break;
		case MOVEDFROM:
		case MOVEDTO:
			for (final Style style : pStyle) {
				StyleConstants.setBackground(style, Color.YELLOW);
			}
			break;
		case INSERTED:
			final Color blue = new Color(40, 40, 220, 80);
			for (final Style style : pStyle) {
				StyleConstants.setBackground(style, blue);
			}
			break;
		case SAME:
		case SAMEHASH:
			for (final Style style : pStyle) {
				StyleConstants.setBackground(style, Color.WHITE);
			}
			break;
		case DELETED:
			final Color red = new Color(255, 40, 20, 90);
			for (final Style style : pStyle) {
				StyleConstants.setBackground(style, red);
			}
			break;
		}
	}

	/**
	 * Process start tag.
	 * 
	 * @param pEvent
	 *          {@link XMLEvent} reference
	 * @param pDoc
	 *          {@link StyledDocument} reference
	 * @return if it's an empty element or not
	 * @throws XMLStreamException
	 *           if the sirix StAXSerializer fails
	 */
	private boolean processStartTag(final XMLEvent pEvent,
			final StyledDocument pDoc) throws XMLStreamException {
		assert pEvent != null;
		assert pDoc != null;
		final StartElement startTag = pEvent.asStartElement();
		boolean emptyElement = false;
		LOGWRAPPER.debug("startTag: " + startTag);
		if (mSerializer.peek().getEventType() == XMLStreamConstants.END_ELEMENT) {
			processStartTag(startTag, pDoc, Child.NOCHILD);
			emptyElement = true;
		} else {
			processStartTag(startTag, pDoc, Child.CHILD);
		}
		return emptyElement;
	}

	/**
	 * Process end tag.
	 * 
	 * @param pEvent
	 *          {@link XMLEvent} reference
	 * @param pLevel
	 *          level in the tree
	 * @param pIndentSpaces
	 *          determines how many spaces to indent
	 * @param pDoc
	 *          {@link StyledDocument} reference
	 * @param pStyleElements
	 *          {@link Style} reference
	 * @throws BadLocationException
	 *           if insertion of string fails
	 */
	private void processEndTag(final XMLEvent pEvent, final int pLevel,
			final String pIndentSpaces, final StyledDocument pDoc,
			final Style pStyleElements) throws BadLocationException {
		assert pEvent != null;
		// assert pLevel >= 0;
		assert pIndentSpaces != null;
		assert pDoc != null;
		assert pStyleElements != null;
		final EndElement endTag = pEvent.asEndElement();
		final QName name = endTag.getName();
		LOGWRAPPER.debug("endTag: " + endTag);
		indent(pDoc, pLevel, pIndentSpaces);
		pDoc.insertString(
				pDoc.getLength(),
				new StringBuilder()
						.append("</")
						.append(
								ViewUtilities.qNameToString(new QNm(name.getNamespaceURI(),
										name.getPrefix(), name.getLocalPart()))).append(">")
						.append(NEWLINE).toString(), pStyleElements);
	}

	/**
	 * Generate a String representation from a {@link StartElement}.
	 * 
	 * @param pStartTag
	 *          The {@link StartElement} to serialize.
	 * @param pDoc
	 *          The {@link StyledDocument} from the {@link JTextPane} instance.
	 * @param pHasChild
	 *          {@link Child}.
	 */
	private void processStartTag(final StartElement pStartTag,
			final StyledDocument pDoc, final Child pHasChild) {
		assert pStartTag != null;
		assert pDoc != null;
		assert pHasChild != null;

		try {
			final QName name = pStartTag.getName();
			final String qName = ViewUtilities.qNameToString(new QNm(name
					.getNamespaceURI(), name.getPrefix(), name.getLocalPart()));
			pDoc.insertString(pDoc.getLength(), new StringBuilder("<").append(qName)
					.toString(), pDoc.getStyle("elements"));

			// Insert a space if namespaces or attributes follow.
			if (pStartTag.getAttributes().hasNext()
					|| pStartTag.getNamespaces().hasNext()) {
				pDoc.insertString(pDoc.getLength(), " ", pDoc.getStyle("elements"));
			}

			// Process namespaces.
			for (final Iterator<?> namespaces = pStartTag.getNamespaces(); namespaces
					.hasNext();) {
				final Namespace ns = (Namespace) namespaces.next();
				if (ns.getPrefix().isEmpty()) {
					pDoc.insertString(pDoc.getLength(), new StringBuilder(" xmlns=\"")
							.append(ns.getNamespaceURI()).append("\"").toString(),
							pDoc.getStyle("namespaces"));
				} else {
					pDoc.insertString(pDoc.getLength(), new StringBuilder(" xmlns:")
							.append(ns.getPrefix()).append("=\"")
							.append(ns.getNamespaceURI()).append("\"").toString(),
							pDoc.getStyle("namespaces"));
				}

				if (pStartTag.getAttributes().hasNext()) {
					pDoc.insertString(pDoc.getLength(), " ", pDoc.getStyle("elements"));
				}
			}

			// Process attributes.
			for (final Iterator<?> attributes = pStartTag.getAttributes(); attributes
					.hasNext();) {
				final Attribute att = (Attribute) attributes.next();
				final QName attName = att.getName();

				pDoc.insertString(
						pDoc.getLength(),
						new StringBuilder()
								.append(
										ViewUtilities.qNameToString(new QNm(attName
												.getNamespaceURI(), attName.getPrefix(), attName
												.getLocalPart()))).append("=\"").append(att.getValue())
								.append("\"").toString(), pDoc.getStyle("attributes"));

				if (attributes.hasNext()) {
					pDoc.insertString(pDoc.getLength(), " ", pDoc.getStyle("elements"));
				}
			}

			switch (pHasChild) {
			case CHILD:
				pDoc.insertString(pDoc.getLength(), ">" + NEWLINE,
						pDoc.getStyle("elements"));
				break;
			case NOCHILD:
				pDoc.insertString(pDoc.getLength(), "/>" + NEWLINE,
						pDoc.getStyle("elements"));
				break;
			default:
				break;
			}
		} catch (final BadLocationException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	/**
	 * Indent serialized output.
	 * 
	 * @param pDocument
	 *          {@link StyledDocument}.
	 * @param pLevel
	 *          Current level in the tree.
	 * @param pIndentSpaces
	 *          Determines how many spaces to indent at every level.
	 */
	private void indent(final StyledDocument pDocument, final int pLevel,
			final String pIndentSpaces) {
		assert pDocument != null;
		assert pLevel > -1;
		LOGWRAPPER.debug("LEVEL: " + pLevel);
		assert pIndentSpaces != null;
		try {
			for (int i = 0; i < pLevel; i++) {
				pDocument.insertString(pDocument.getLength(), pIndentSpaces,
						pDocument.addStyle(null, null));
			}
		} catch (final BadLocationException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	@Override
	public Dimension getPreferredSize() {
		assert mGUI != null;
		final Dimension mainFrame = mGUI.getSize();
		mText.setSize(new Dimension(mainFrame.width - 16, mainFrame.height
				- mGUI.getJMenuBar().getHeight() - 37));
		return new Dimension(mainFrame.width - 16, mainFrame.height
				- mGUI.getJMenuBar().getHeight() - 37);
	}

	@Override
	public void hover(final VisualItem pItem) {
	}

	@Override
	public void resize() {
	}
}
