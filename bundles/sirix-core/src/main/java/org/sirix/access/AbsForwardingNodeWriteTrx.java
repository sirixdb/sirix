package org.sirix.access;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;

import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.exception.AbsTTException;
import org.sirix.service.xml.shredder.EInsert;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbsForwardingNodeWriteTrx extends
  AbsForwardingNodeReadTrx implements INodeWriteTrx {

  /** Constructor for use by subclasses. */
  protected AbsForwardingNodeWriteTrx() {
  }

  @Override
  protected abstract INodeWriteTrx delegate();

  @Override
  public void abort() throws AbsTTException {
    delegate().abort();
  }

  @Override
  public void close() throws AbsTTException {
    delegate().close();
  }

  @Override
  public void commit() throws AbsTTException {
    delegate().commit();
  }

  @Override
  public INodeWriteTrx moveSubtreeToLeftSibling(long pFromKey)
    throws AbsTTException {
    return delegate().moveSubtreeToLeftSibling(pFromKey);
  }

  @Override
  public INodeWriteTrx moveSubtreeToRightSibling(long pFromKey)
    throws AbsTTException {
    return delegate().moveSubtreeToRightSibling(pFromKey);
  }

  @Override
  public INodeWriteTrx moveSubtreeToFirstChild(long pFromKey)
    throws AbsTTException {
    return delegate().moveSubtreeToFirstChild(pFromKey);
  }

  @Override
  public INodeWriteTrx copySubtreeAsFirstChild(@Nonnull INodeReadTrx pRtx)
    throws AbsTTException {
    return delegate().copySubtreeAsFirstChild(pRtx);
  }

  @Override
  public INodeWriteTrx copySubtreeAsLeftSibling(@Nonnull INodeReadTrx pRtx)
    throws AbsTTException {
    return delegate().copySubtreeAsLeftSibling(pRtx);
  }

  @Override
  public INodeWriteTrx copySubtreeAsRightSibling(@Nonnull INodeReadTrx pRtx)
    throws AbsTTException {
    return delegate().copySubtreeAsRightSibling(pRtx);
  }

  @Override
  public INodeWriteTrx insertAttribute(@Nonnull QName pName,
    @Nonnull String pValue) throws AbsTTException {
    return delegate().insertAttribute(pName, pValue);
  }

  @Override
  public INodeWriteTrx insertAttribute(@Nonnull QName pName,
    @Nonnull String pValue, @Nonnull EMove pMove) throws AbsTTException {
    return delegate().insertAttribute(pName, pValue, pMove);
  }

  @Override
  public INodeWriteTrx insertElementAsFirstChild(@Nonnull QName pName)
    throws AbsTTException {
    return delegate().insertElementAsFirstChild(pName);
  }

  @Override
  public INodeWriteTrx insertElementAsLeftSibling(@Nonnull QName pQName)
    throws AbsTTException {
    return delegate().insertElementAsLeftSibling(pQName);
  }

  @Override
  public INodeWriteTrx insertElementAsRightSibling(@Nonnull QName pQName)
    throws AbsTTException {
    return delegate().insertElementAsRightSibling(pQName);
  }

  @Override
  public INodeWriteTrx insertNamespace(@Nonnull QName pName)
    throws AbsTTException {
    return delegate().insertNamespace(pName);
  }

  @Override
  public INodeWriteTrx insertNamespace(@Nonnull QName pQName,
    @Nonnull EMove pMove) throws AbsTTException {
    return delegate().insertNamespace(pQName, pMove);
  }

  @Override
  public INodeWriteTrx insertSubtree(@Nonnull XMLEventReader pReader,
    @Nonnull EInsert pInsert) throws AbsTTException {
    return delegate().insertSubtree(pReader, pInsert);
  }

  @Override
  public INodeWriteTrx insertTextAsFirstChild(@Nonnull String pValue)
    throws AbsTTException {
    return delegate().insertTextAsFirstChild(pValue);
  }

  @Override
  public INodeWriteTrx insertTextAsLeftSibling(@Nonnull String pValue)
    throws AbsTTException {
    return delegate().insertTextAsLeftSibling(pValue);
  }

  @Override
  public INodeWriteTrx insertTextAsRightSibling(@Nonnull String pValue)
    throws AbsTTException {
    return delegate().insertTextAsRightSibling(pValue);
  }

  @Override
  public void setQName(@Nonnull QName pName) throws AbsTTException {
    delegate().setQName(pName);
  }

  @Override
  public void setValue(@Nonnull String pValue) throws AbsTTException {
    delegate().setValue(pValue);
  }

//  @Override
//  public void setURI(@Nonnull String pUri) throws AbsTTException {
//    delegate().setURI(pUri);
//  }

  @Override
  public void remove() throws AbsTTException {
    delegate().remove();
  }
}
