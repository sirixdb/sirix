package org.sirix.access.trx.node.xdm;

import javax.annotation.Nonnull;
import javax.xml.stream.XMLEventReader;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.Movement;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.exception.SirixException;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractForwardingXdmNodeTrx extends AbstractForwardingXdmNodeReadOnlyTrx
    implements XdmNodeTrx {

  /** Constructor for use by subclasses. */
  protected AbstractForwardingXdmNodeTrx() {}

  @Override
  protected abstract XdmNodeTrx delegate();

  @Override
  public XdmNodeTrx rollback() {
    return delegate().rollback();
  }

  @Override
  public void close() {
    delegate().close();
  }

  @Override
  public XdmNodeTrx commit() {
    return delegate().commit();
  }

  @Override
  public XdmNodeTrx moveSubtreeToLeftSibling(long fromKey) throws SirixException {
    return delegate().moveSubtreeToLeftSibling(fromKey);
  }

  @Override
  public XdmNodeTrx moveSubtreeToRightSibling(long fromKey) throws SirixException {
    return delegate().moveSubtreeToRightSibling(fromKey);
  }

  @Override
  public XdmNodeTrx moveSubtreeToFirstChild(long fromKey) throws SirixException {
    return delegate().moveSubtreeToFirstChild(fromKey);
  }

  @Override
  public XdmNodeTrx copySubtreeAsFirstChild(XdmNodeReadOnlyTrx rtx) throws SirixException {
    return delegate().copySubtreeAsFirstChild(rtx);
  }

  @Override
  public XdmNodeTrx copySubtreeAsLeftSibling(XdmNodeReadOnlyTrx rtx) throws SirixException {
    return delegate().copySubtreeAsLeftSibling(rtx);
  }

  @Override
  public XdmNodeTrx copySubtreeAsRightSibling(XdmNodeReadOnlyTrx rtx) throws SirixException {
    return delegate().copySubtreeAsRightSibling(rtx);
  }

  @Override
  public XdmNodeTrx insertAttribute(QNm name, @Nonnull String value) throws SirixException {
    return delegate().insertAttribute(name, value);
  }

  @Override
  public XdmNodeTrx insertAttribute(QNm name, @Nonnull String value, @Nonnull Movement move)
      throws SirixException {
    return delegate().insertAttribute(name, value, move);
  }

  @Override
  public XdmNodeTrx insertElementAsFirstChild(QNm name) throws SirixException {
    return delegate().insertElementAsFirstChild(name);
  }

  @Override
  public XdmNodeTrx insertElementAsLeftSibling(QNm name) throws SirixException {
    return delegate().insertElementAsLeftSibling(name);
  }

  @Override
  public XdmNodeTrx insertElementAsRightSibling(QNm name) throws SirixException {
    return delegate().insertElementAsRightSibling(name);
  }

  @Override
  public XdmNodeTrx insertNamespace(QNm name) throws SirixException {
    return delegate().insertNamespace(name);
  }

  @Override
  public XdmNodeTrx insertNamespace(QNm name, @Nonnull Movement move) throws SirixException {
    return delegate().insertNamespace(name, move);
  }

  @Override
  public XdmNodeTrx insertSubtreeAsFirstChild(XMLEventReader reader) throws SirixException {
    return delegate().insertSubtreeAsFirstChild(reader);
  }

  @Override
  public XdmNodeTrx insertSubtreeAsRightSibling(XMLEventReader reader) throws SirixException {
    return delegate().insertSubtreeAsRightSibling(reader);
  }

  @Override
  public XdmNodeTrx insertSubtreeAsLeftSibling(XMLEventReader reader) throws SirixException {
    return delegate().insertSubtreeAsLeftSibling(reader);
  }

  @Override
  public XdmNodeTrx insertTextAsFirstChild(String value) throws SirixException {
    return delegate().insertTextAsFirstChild(value);
  }

  @Override
  public XdmNodeTrx insertTextAsLeftSibling(String value) throws SirixException {
    return delegate().insertTextAsLeftSibling(value);
  }

  @Override
  public XdmNodeTrx insertTextAsRightSibling(String value) throws SirixException {
    return delegate().insertTextAsRightSibling(value);
  }

  @Override
  public XdmNodeTrx setName(QNm name) throws SirixException {
    return delegate().setName(name);
  }

  @Override
  public XdmNodeTrx setValue(String value) throws SirixException {
    return delegate().setValue(value);
  }

  @Override
  public XdmNodeTrx remove() throws SirixException {
    return delegate().remove();
  }
}
