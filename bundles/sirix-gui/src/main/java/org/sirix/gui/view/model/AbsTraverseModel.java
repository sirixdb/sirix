package org.sirix.gui.view.model;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;

import org.sirix.api.INodeReadTrx;
import org.sirix.gui.view.AbsObservableComponent;
import org.sirix.gui.view.model.interfaces.ITraverseModel;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;

/**
 * Skeletal implementation of {@link ITraverseModel}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbsTraverseModel extends AbsObservableComponent implements ITraverseModel {
  /**
   * Fill an attribute list with entries.
   * 
   * @param pRtx
   *          Sirix {@link INodeReadTrx}
   * @return {@link List} of {@link Attribute}s
   */
  protected List<Attribute> fillAttributes(@Nonnull final INodeReadTrx pRtx) {
    assert pRtx != null;
    final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
    // Casting to ElementNode is always safe because we don't iterate over attributes in the axis.
    assert pRtx.getNode().getKind() == EKind.ELEMENT;
    final int attNumber = ((ElementNode)pRtx.getNode()).getAttributeCount();
    final List<Attribute> attributes = new ArrayList<>(attNumber);
    for (int i = 0; i < attNumber; i++) {
      pRtx.moveToAttribute(i);
      attributes
        .add(eventFactory.createAttribute(pRtx.getQNameOfCurrentNode(), pRtx.getValueOfCurrentNode()));
      pRtx.moveToParent();
    }
    return attributes;
  }

  /**
   * Fill a namespace list with entries.
   * 
   * @param pRtx
   *          Sirix {@link INodeReadTrx}
   * @return {@link List} of {@link Namespace}s
   */
  protected List<Namespace> fillNamespaces(@Nonnull final INodeReadTrx pRtx) {
    assert pRtx != null;
    final XMLEventFactory eventFactory = XMLEventFactory.newInstance();
    // Casting to ElementNode is always safe because we don't iterate over attributes in the axis.
    assert pRtx.getNode().getKind() == EKind.ELEMENT;
    final int nspNumber = ((ElementNode)pRtx.getNode()).getNamespaceCount();
    final List<Namespace> namespaces = new ArrayList<>(nspNumber);
    for (int i = 0; i < nspNumber; i++) {
      pRtx.moveToNamespace(i);
      final QName qName = pRtx.getQNameOfCurrentNode();
      namespaces.add(eventFactory.createNamespace(qName.getPrefix(), qName.getNamespaceURI()));
      pRtx.moveToParent();
    }
    return namespaces;
  }
}
