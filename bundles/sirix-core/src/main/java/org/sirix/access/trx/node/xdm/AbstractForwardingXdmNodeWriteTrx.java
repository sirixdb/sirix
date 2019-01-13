package org.sirix.access.trx.node.xdm;

import javax.annotation.Nonnull;
import javax.xml.stream.XMLEventReader;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.Movement;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.api.xdm.XdmNodeWriteTrx;
import org.sirix.exception.SirixException;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractForwardingXdmNodeWriteTrx extends AbstractForwardingXdmNodeReadTrx
    implements XdmNodeWriteTrx {

  /** Constructor for use by subclasses. */
  protected AbstractForwardingXdmNodeWriteTrx() {}

  @Override
  protected abstract XdmNodeWriteTrx delegate();

  @Override
  public XdmNodeWriteTrx rollback() {
    return delegate().rollback();
  }

  @Override
  public void close() {
    delegate().close();
  }

  @Override
  public XdmNodeWriteTrx commit() {
    return delegate().commit();
  }

  @Override
  public XdmNodeWriteTrx moveSubtreeToLeftSibling(long fromKey) throws SirixException {
    return delegate().moveSubtreeToLeftSibling(fromKey);
  }

  @Override
  public XdmNodeWriteTrx moveSubtreeToRightSibling(long fromKey) throws SirixException {
    return delegate().moveSubtreeToRightSibling(fromKey);
  }

  @Override
  public XdmNodeWriteTrx moveSubtreeToFirstChild(long fromKey) throws SirixException {
    return delegate().moveSubtreeToFirstChild(fromKey);
  }

  @Override
  public XdmNodeWriteTrx copySubtreeAsFirstChild(XdmNodeReadTrx rtx) throws SirixException {
    return delegate().copySubtreeAsFirstChild(rtx);
  }

  @Override
  public XdmNodeWriteTrx copySubtreeAsLeftSibling(XdmNodeReadTrx rtx) throws SirixException {
    return delegate().copySubtreeAsLeftSibling(rtx);
  }

  @Override
  public XdmNodeWriteTrx copySubtreeAsRightSibling(XdmNodeReadTrx rtx) throws SirixException {
    return delegate().copySubtreeAsRightSibling(rtx);
  }

  @Override
  public XdmNodeWriteTrx insertAttribute(QNm name, @Nonnull String value) throws SirixException {
    return delegate().insertAttribute(name, value);
  }

  @Override
  public XdmNodeWriteTrx insertAttribute(QNm name, @Nonnull String value, @Nonnull Movement move)
      throws SirixException {
    return delegate().insertAttribute(name, value, move);
  }

  @Override
  public XdmNodeWriteTrx insertElementAsFirstChild(QNm name) throws SirixException {
    return delegate().insertElementAsFirstChild(name);
  }

  @Override
  public XdmNodeWriteTrx insertElementAsLeftSibling(QNm name) throws SirixException {
    return delegate().insertElementAsLeftSibling(name);
  }

  @Override
  public XdmNodeWriteTrx insertElementAsRightSibling(QNm name) throws SirixException {
    return delegate().insertElementAsRightSibling(name);
  }

  @Override
  public XdmNodeWriteTrx insertNamespace(QNm name) throws SirixException {
    return delegate().insertNamespace(name);
  }

  @Override
  public XdmNodeWriteTrx insertNamespace(QNm name, @Nonnull Movement move) throws SirixException {
    return delegate().insertNamespace(name, move);
  }

  @Override
  public XdmNodeWriteTrx insertSubtreeAsFirstChild(XMLEventReader reader) throws SirixException {
    return delegate().insertSubtreeAsFirstChild(reader);
  }

  @Override
  public XdmNodeWriteTrx insertSubtreeAsRightSibling(XMLEventReader reader) throws SirixException {
    return delegate().insertSubtreeAsRightSibling(reader);
  }

  @Override
  public XdmNodeWriteTrx insertSubtreeAsLeftSibling(XMLEventReader reader) throws SirixException {
    return delegate().insertSubtreeAsLeftSibling(reader);
  }

  @Override
  public XdmNodeWriteTrx insertTextAsFirstChild(String value) throws SirixException {
    return delegate().insertTextAsFirstChild(value);
  }

  @Override
  public XdmNodeWriteTrx insertTextAsLeftSibling(String value) throws SirixException {
    return delegate().insertTextAsLeftSibling(value);
  }

  @Override
  public XdmNodeWriteTrx insertTextAsRightSibling(String value) throws SirixException {
    return delegate().insertTextAsRightSibling(value);
  }

  @Override
  public XdmNodeWriteTrx setName(QNm name) throws SirixException {
    return delegate().setName(name);
  }

  @Override
  public XdmNodeWriteTrx setValue(String value) throws SirixException {
    return delegate().setValue(value);
  }

  @Override
  public XdmNodeWriteTrx remove() throws SirixException {
    return delegate().remove();
  }
}
