package org.sirix.access;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;

import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.shredder.Insert;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbsForwardingNodeWriteTrx extends
  AbsForwardingNodeReadTrx implements NodeWriteTrx {

  /** Constructor for use by subclasses. */
  protected AbsForwardingNodeWriteTrx() {
  }

  @Override
  protected abstract NodeWriteTrx delegate();

  @Override
  public void abort() throws SirixException {
    delegate().abort();
  }

  @Override
  public void close() throws SirixException {
    delegate().close();
  }

  @Override
  public void commit() throws SirixException {
    delegate().commit();
  }

  @Override
  public NodeWriteTrx moveSubtreeToLeftSibling(long pFromKey)
    throws SirixException {
    return delegate().moveSubtreeToLeftSibling(pFromKey);
  }

  @Override
  public NodeWriteTrx moveSubtreeToRightSibling(long pFromKey)
    throws SirixException {
    return delegate().moveSubtreeToRightSibling(pFromKey);
  }

  @Override
  public NodeWriteTrx moveSubtreeToFirstChild(long pFromKey)
    throws SirixException {
    return delegate().moveSubtreeToFirstChild(pFromKey);
  }

  @Override
  public NodeWriteTrx copySubtreeAsFirstChild(@Nonnull NodeReadTrx pRtx)
    throws SirixException {
    return delegate().copySubtreeAsFirstChild(pRtx);
  }

  @Override
  public NodeWriteTrx copySubtreeAsLeftSibling(@Nonnull NodeReadTrx pRtx)
    throws SirixException {
    return delegate().copySubtreeAsLeftSibling(pRtx);
  }

  @Override
  public NodeWriteTrx copySubtreeAsRightSibling(@Nonnull NodeReadTrx pRtx)
    throws SirixException {
    return delegate().copySubtreeAsRightSibling(pRtx);
  }

  @Override
  public NodeWriteTrx insertAttribute(@Nonnull QName pName,
    @Nonnull String pValue) throws SirixException {
    return delegate().insertAttribute(pName, pValue);
  }

  @Override
  public NodeWriteTrx insertAttribute(@Nonnull QName pName,
    @Nonnull String pValue, @Nonnull Movement pMove) throws SirixException {
    return delegate().insertAttribute(pName, pValue, pMove);
  }

  @Override
  public NodeWriteTrx insertElementAsFirstChild(@Nonnull QName pName)
    throws SirixException {
    return delegate().insertElementAsFirstChild(pName);
  }

  @Override
  public NodeWriteTrx insertElementAsLeftSibling(@Nonnull QName pQName)
    throws SirixException {
    return delegate().insertElementAsLeftSibling(pQName);
  }

  @Override
  public NodeWriteTrx insertElementAsRightSibling(@Nonnull QName pQName)
    throws SirixException {
    return delegate().insertElementAsRightSibling(pQName);
  }

  @Override
  public NodeWriteTrx insertNamespace(@Nonnull QName pName)
    throws SirixException {
    return delegate().insertNamespace(pName);
  }

  @Override
  public NodeWriteTrx insertNamespace(@Nonnull QName pQName,
    @Nonnull Movement pMove) throws SirixException {
    return delegate().insertNamespace(pQName, pMove);
  }

  @Override
  public NodeWriteTrx insertSubtree(@Nonnull XMLEventReader pReader,
    @Nonnull Insert pInsert) throws SirixException {
    return delegate().insertSubtree(pReader, pInsert);
  }

  @Override
  public NodeWriteTrx insertTextAsFirstChild(@Nonnull String pValue)
    throws SirixException {
    return delegate().insertTextAsFirstChild(pValue);
  }

  @Override
  public NodeWriteTrx insertTextAsLeftSibling(@Nonnull String pValue)
    throws SirixException {
    return delegate().insertTextAsLeftSibling(pValue);
  }

  @Override
  public NodeWriteTrx insertTextAsRightSibling(@Nonnull String pValue)
    throws SirixException {
    return delegate().insertTextAsRightSibling(pValue);
  }

  @Override
  public void setQName(@Nonnull QName pName) throws SirixException {
    delegate().setQName(pName);
  }

  @Override
  public void setValue(@Nonnull String pValue) throws SirixException {
    delegate().setValue(pValue);
  }

//  @Override
//  public void setURI(@Nonnull String pUri) throws AbsTTException {
//    delegate().setURI(pUri);
//  }

  @Override
  public void remove() throws SirixException {
    delegate().remove();
  }
}
