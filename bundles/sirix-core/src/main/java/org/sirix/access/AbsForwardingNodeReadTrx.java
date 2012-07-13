package org.sirix.access;

import com.google.common.collect.ForwardingObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.IItemList;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.ISession;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.service.xml.xpath.AtomicValue;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbsForwardingNodeReadTrx extends ForwardingObject
  implements INodeReadTrx {

  /** Constructor for use by subclasses. */
  protected AbsForwardingNodeReadTrx() {
  }

  @Override
  protected abstract INodeReadTrx delegate();

  @Override
  public IItemList<AtomicValue> getItemList() {
    return delegate().getItemList();
  }

  @Override
  public long getMaxNodeKey() throws TTIOException {
    return delegate().getMaxNodeKey();
  }

  @Override
  public INode getNode() {
    return delegate().getNode();
  }

  @Override
  public void close() throws AbsTTException {
    delegate().close();
  }

  @Override
  public QName getQNameOfCurrentNode() {
    return delegate().getQNameOfCurrentNode();
  }

  @Override
  public long getRevisionNumber() throws TTIOException {
    return delegate().getRevisionNumber();
  }

  @Override
  public long getRevisionTimestamp() throws TTIOException {
    return delegate().getRevisionTimestamp();
  }

  @Override
  public ISession getSession() {
    return delegate().getSession();
  }

  @Override
  public IStructNode getStructuralNode() {
    return delegate().getStructuralNode();
  }

  @Override
  public long getTransactionID() {
    return delegate().getTransactionID();
  }

  @Override
  public String getTypeOfCurrentNode() {
    return delegate().getTypeOfCurrentNode();
  }

  @Override
  public String getValueOfCurrentNode() {
    return delegate().getValueOfCurrentNode();
  }

  @Override
  public boolean isClosed() {
    return delegate().isClosed();
  }

  @Override
  public int keyForName(@Nonnull String pName) {
    return delegate().keyForName(pName);
  }

  @Override
  public boolean moveTo(long pKey) {
    return delegate().moveTo(pKey);
  }

  @Override
  public boolean moveToAttribute(@Nonnegative int pIndex) {
    return delegate().moveToAttribute(pIndex);
  }

  @Override
  public boolean moveToAttributeByName(@Nonnull QName pQName) {
    return delegate().moveToAttributeByName(pQName);
  }

  @Override
  public boolean moveToDocumentRoot() {
    return delegate().moveToDocumentRoot();
  }

  @Override
  public boolean moveToFirstChild() {
    return delegate().moveToFirstChild();
  }

  @Override
  public boolean moveToLeftSibling() {
    return delegate().moveToLeftSibling();
  }

  @Override
  public boolean moveToNamespace(@Nonnegative int pIndex) {
    return delegate().moveToNamespace(pIndex);
  }

  @Override
  public boolean moveToNextFollowing() {
    return delegate().moveToNextFollowing();
  }

  @Override
  public boolean moveToParent() {
    return delegate().moveToParent();
  }

  @Override
  public boolean moveToRightSibling() {
    return delegate().moveToRightSibling();
  }

  @Override
  public String nameForKey(int pKey) {
    return delegate().nameForKey(pKey);
  }

  @Override
  public byte[] rawNameForKey(int pKey) {
    return delegate().rawNameForKey(pKey);
  }

  @Override
  public INodeReadTrx cloneInstance() throws AbsTTException {
    return delegate().cloneInstance();
  }
  
  @Override
  public int getNameCount() {
    return delegate().getNameCount();
  }
}
