package org.sirix.access;

import com.google.common.base.Optional;
import com.google.common.collect.ForwardingObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.IItemList;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.ISession;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.node.EKind;
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
  public int keyForName(final @Nonnull String pName) {
    return delegate().keyForName(pName);
  }

  @Override
  public boolean moveTo(long pKey) {
    return delegate().moveTo(pKey);
  }

  @Override
  public boolean moveToAttribute(final @Nonnegative int pIndex) {
    return delegate().moveToAttribute(pIndex);
  }

  @Override
  public boolean moveToAttributeByName(final @Nonnull QName pQName) {
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
  public boolean moveToNamespace(final @Nonnegative int pIndex) {
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
  public boolean moveToLastChild() {
    return delegate().moveToLastChild();
  }

  @Override
  public String nameForKey(final int pKey) {
    return delegate().nameForKey(pKey);
  }

  @Override
  public byte[] rawNameForKey(final int pKey) {
    return delegate().rawNameForKey(pKey);
  }

  @Override
  public INodeReadTrx cloneInstance() throws AbsTTException {
    return delegate().cloneInstance();
  }
  
  @Override
  public int getNameCount(final @Nonnull String pName, final @Nonnull EKind pKind) {
    return delegate().getNameCount(pName, pKind);
  }
  
  @Override
  public Optional<IStructNode> moveToAndGetFirstChild() {
    return delegate().moveToAndGetFirstChild();
  }
  
  @Override
  public Optional<IStructNode> moveToAndGetLastChild() {
    return delegate().moveToAndGetLastChild();
  }
  
  @Override
  public Optional<IStructNode> moveToAndGetLeftSibling() {
    return delegate().moveToAndGetLeftSibling();
  }
  
  @Override
  public Optional<IStructNode> moveToAndGetRightSibling() {
    return delegate().moveToAndGetRightSibling();
  }
  
  @Override
  public Optional<IStructNode> moveToAndGetParent() {
    return delegate().moveToAndGetParent();
  }
  
  @Override
  public Optional<IStructNode> getFirstChild() {
    return delegate().getFirstChild();
  }
  
  @Override
  public Optional<IStructNode> getLastChild() {
    return delegate().getLastChild();
  }
  
  @Override
  public Optional<IStructNode> getLeftSibling() {
    return delegate().getLeftSibling();
  }
  
  @Override
  public Optional<IStructNode> getParent() {
    return delegate().getParent();
  }
  
  @Override
  public Optional<IStructNode> getRightSibling() {
    return delegate().getRightSibling();
  }
}
