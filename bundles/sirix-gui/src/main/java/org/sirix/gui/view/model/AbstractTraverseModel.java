package org.sirix.gui.view.model;

import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.NodeReadTrx;
import org.sirix.gui.view.AbstractObservableComponent;
import org.sirix.gui.view.model.interfaces.TraverseModel;
import org.sirix.node.Kind;

/**
 * Skeletal implementation of {@link TraverseModel}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbstractTraverseModel extends AbstractObservableComponent
		implements TraverseModel {
	/**
	 * Fill an attribute list with entries.
	 * 
	 * @param rtx
	 *          Sirix {@link NodeReadTrx}
	 * @return {@link List} of {@link Attribute}s
	 */
	protected List<Attribute> fillAttributes(final NodeReadTrx rtx) {
		assert rtx != null;
		final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
		// Casting to ElementNode is always safe because we don't iterate over
		// attributes in the axis.
		assert rtx.getKind() == Kind.ELEMENT;
		final int attNumber = rtx.getAttributeCount();
		final List<Attribute> attributes = new ArrayList<>(attNumber);
		for (int i = 0; i < attNumber; i++) {
			rtx.moveToAttribute(i);
			final QNm name = rtx.getName();
			attributes.add(eventFactory.createAttribute(
					new QName(name.getNamespaceURI(), name.getLocalName(), name
							.getPrefix()), rtx.getValue()));
			rtx.moveToParent();
		}
		return attributes;
	}

	/**
	 * Fill a namespace list with entries.
	 * 
	 * @param pRtx
	 *          Sirix {@link NodeReadTrx}
	 * @return {@link List} of {@link Namespace}s
	 */
	protected List<Namespace> fillNamespaces(final NodeReadTrx pRtx) {
		assert pRtx != null;
		final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
		// Casting to ElementNode is always safe because we don't iterate over
		// attributes in the axis.
		assert pRtx.getKind() == Kind.ELEMENT;
		final int nspNumber = pRtx.getNamespaceCount();
		final List<Namespace> namespaces = new ArrayList<>(nspNumber);
		for (int i = 0; i < nspNumber; i++) {
			pRtx.moveToNamespace(i);
			final QNm qName = pRtx.getName();
			namespaces.add(eventFactory.createNamespace(qName.getPrefix(),
					qName.getNamespaceURI()));
			pRtx.moveToParent();
		}
		return namespaces;
	}
}
