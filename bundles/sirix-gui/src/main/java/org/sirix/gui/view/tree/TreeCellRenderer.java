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

package org.sirix.gui.view.tree;

import static org.sirix.gui.GUIConstants.ATTRIBUTE_COLOR;
import static org.sirix.gui.GUIConstants.DOC_COLOR;
import static org.sirix.gui.GUIConstants.ELEMENT_COLOR;

import java.awt.Color;
import java.awt.Component;

import javax.swing.JTree;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.xml.XMLConstants;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.NodeReadTrx;
import org.sirix.exception.SirixException;
import org.sirix.gui.ReadDB;
import org.sirix.node.interfaces.Node;

/**
 * 
 * <p>
 * Customized tree cell renderer to render nodes nicely.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class TreeCellRenderer extends DefaultTreeCellRenderer {

	/**
	 * Generated UID.
	 */
	private static final long serialVersionUID = -6242168246410260644L;

	/** White color. */
	private static final Color WHITE = new Color(255, 255, 255);

	/** Treetant reading transaction {@link NodeReadTrx}. */
	private transient NodeReadTrx mRTX;

	/** Path to file. */
	private final String mPATH;

	/**
	 * Constructor.
	 * 
	 * @param pReadDB
	 *          {@link ReadDB} instance
	 */
	TreeCellRenderer(final ReadDB pReadDB) {
		setOpenIcon(null);
		setClosedIcon(null);
		setLeafIcon(null);
		setBackgroundNonSelectionColor(null);
		setTextSelectionColor(Color.red);

		try {
			mRTX = pReadDB.getSession().beginNodeReadTrx(pReadDB.getRevisionNumber());
		} catch (final SirixException exc) {
			exc.printStackTrace();
		}

		mRTX.moveTo(pReadDB.getNodeKey());
		mPATH = pReadDB.getDatabase().getDatabaseConfig().getFile().getName();
	}

	@Override
	public Component getTreeCellRendererComponent(final JTree pTree,
			Object pValue, final boolean pSel, final boolean pExpanded,
			final boolean pLeaf, final int pRow, final boolean pHasFocus)
			throws IllegalStateException {
		final Node node = (Node) pValue;

		final long key = node.getNodeKey();

		switch (node.getKind()) {
		case ELEMENT:
			mRTX.moveTo(node.getNodeKey());
			final String prefix = mRTX.getName().getPrefix();
			final QNm qName = mRTX.getName();

			if (prefix == null || prefix.equals(XMLConstants.DEFAULT_NS_PREFIX)) {
				final String localPart = qName.getLocalName();

				if (mRTX.hasFirstChild()) {
					pValue = new StringBuilder("<").append(localPart).append(">")
							.toString();
				} else {
					pValue = new StringBuilder("<").append(localPart).append("/>")
							.toString();
				}
			} else {
				pValue = new StringBuilder("<").append(prefix).append(":")
						.append(qName.getLocalName()).append(">").toString();
			}

			break;
		case ATTRIBUTE:
			// Move transaction to parent of the attribute node.
			mRTX.moveTo(node.getParentKey());
			final long aNodeKey = node.getNodeKey();
			for (int i = 0, attsCount = mRTX.getAttributeCount(); i < attsCount; i++) {
				mRTX.moveToAttribute(i);
				if (mRTX.getNodeKey() == key) {
					break;
				}
				mRTX.moveTo(aNodeKey);
			}

			// Display value.
			final String attPrefix = mRTX.getName().getPrefix();
			final QNm attQName = mRTX.getName();

			if (attPrefix == null || attPrefix.equals("")) {
				pValue = new StringBuilder("@").append(attQName.getLocalName())
						.append("='").append(mRTX.getValue()).append("'").toString();
			} else {
				pValue = new StringBuilder("@").append(attPrefix).append(":")
						.append(attQName.getLocalName()).append("='")
						.append(mRTX.getValue()).append("'").toString();
			}

			break;
		case NAMESPACE:
			// Move transaction to parent the namespace node.
			mRTX.moveTo(node.getParentKey());
			final long nNodeKey = node.getNodeKey();
			for (int i = 0, namespCount = mRTX.getNamespaceCount(); i < namespCount; i++) {
				mRTX.moveToNamespace(i);
				if (mRTX.getNodeKey() == key) {
					break;
				}
				mRTX.moveTo(nNodeKey);
			}

			if (mRTX.nameForKey(mRTX.getPrefixKey()).length() == 0) {
				pValue = new StringBuilder("xmlns='")
						.append(mRTX.nameForKey(mRTX.getURIKey())).append("'").toString();
			} else {
				pValue = new StringBuilder("xmlns:")
						.append(mRTX.nameForKey(mRTX.getPrefixKey())).append("='")
						.append(mRTX.nameForKey(mRTX.getURIKey())).append("'").toString();
			}
			break;
		case TEXT:
			mRTX.moveTo(node.getNodeKey());
			pValue = mRTX.getValue();
			break;
		case COMMENT:
			mRTX.moveTo(node.getNodeKey());
			pValue = new StringBuilder("<!-- ").append(mRTX.getValue())
					.append(" -->").toString();
			break;
		case PROCESSING_INSTRUCTION:
			mRTX.moveTo(node.getNodeKey());
			pValue = new StringBuilder("<? ").append(mRTX.getValue()).append(" ?>")
					.toString();
			break;
		case DOCUMENT:
			pValue = "Doc: " + mPATH;
			break;
		case WHITESPACE:
			break;
		default:
			throw new IllegalStateException("Node kind not known!");
		}

		pValue += new StringBuilder(" [").append(key).append("]").toString();

		super.getTreeCellRendererComponent(pTree, pValue, pSel, pExpanded, pLeaf,
				pRow, pHasFocus);
		setBackground(null);
		setBackgroundNonSelectionColor(null);
		setBackgroundSelectionColor(null);
		if (!selected) {
			switch (node.getKind()) {
			case DOCUMENT:
				setForeground(DOC_COLOR);
				break;
			case ELEMENT:
				setForeground(ELEMENT_COLOR);
				break;
			case ATTRIBUTE:
				setForeground(ATTRIBUTE_COLOR);
				break;
			default:
				// Do nothing.
			}
		} else {
			setForeground(WHITE);
		}

		return this;
	}

	@Override
	public Color getBackground() {
		return null;// new Color(255, 255, 255);
	}
}
