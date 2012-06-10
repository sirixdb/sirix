package org.treetank.access;

import com.google.common.collect.ForwardingObject;

import javax.xml.namespace.QName;

import org.treetank.api.IItemList;
import org.treetank.api.INodeReadTrx;
import org.treetank.api.ISession;
import org.treetank.exception.AbsTTException;
import org.treetank.exception.TTIOException;
import org.treetank.node.interfaces.INode;
import org.treetank.node.interfaces.IStructNode;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbsForwardingNodeReadTrx extends ForwardingObject implements INodeReadTrx {

  /** Constructor for use by subclasses. */
  protected AbsForwardingNodeReadTrx() {
  }

  @Override
  protected abstract INodeReadTrx delegate();

  @Override
  public IItemList<INode> getItemList() {
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
  public int keyForName(String pName) {
    return delegate().keyForName(pName);
  }

  @Override
  public boolean moveTo(long pKey) {
    return delegate().moveTo(pKey);
  }

  @Override
  public boolean moveToAttribute(int pIndex) {
    return delegate().moveToAttribute(pIndex);
  }

  @Override
  public boolean moveToAttributeByNameKey(int pNameKey) {
    return delegate().moveToAttributeByNameKey(pNameKey);
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
  public boolean moveToNamespace(int pIndex) {
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
}
