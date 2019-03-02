package org.sirix.access.trx.node.xml;

import javax.annotation.Nonnull;
import javax.xml.stream.XMLEventReader;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.Movement;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.exception.SirixException;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractForwardingXmlNodeTrx extends AbstractForwardingXmlNodeReadOnlyTrx
    implements XmlNodeTrx {

  /** Constructor for use by subclasses. */
  protected AbstractForwardingXmlNodeTrx() {}

  @Override
  protected abstract XmlNodeTrx delegate();

  @Override
  public XmlNodeTrx rollback() {
    return delegate().rollback();
  }

  @Override
  public void close() {
    delegate().close();
  }

  @Override
  public XmlNodeTrx commit() {
    return delegate().commit();
  }

  @Override
  public XmlNodeTrx moveSubtreeToLeftSibling(long fromKey) throws SirixException {
    return delegate().moveSubtreeToLeftSibling(fromKey);
  }

  @Override
  public XmlNodeTrx moveSubtreeToRightSibling(long fromKey) throws SirixException {
    return delegate().moveSubtreeToRightSibling(fromKey);
  }

  @Override
  public XmlNodeTrx moveSubtreeToFirstChild(long fromKey) throws SirixException {
    return delegate().moveSubtreeToFirstChild(fromKey);
  }

  @Override
  public XmlNodeTrx copySubtreeAsFirstChild(XmlNodeReadOnlyTrx rtx) throws SirixException {
    return delegate().copySubtreeAsFirstChild(rtx);
  }

  @Override
  public XmlNodeTrx copySubtreeAsLeftSibling(XmlNodeReadOnlyTrx rtx) throws SirixException {
    return delegate().copySubtreeAsLeftSibling(rtx);
  }

  @Override
  public XmlNodeTrx copySubtreeAsRightSibling(XmlNodeReadOnlyTrx rtx) throws SirixException {
    return delegate().copySubtreeAsRightSibling(rtx);
  }

  @Override
  public XmlNodeTrx insertAttribute(QNm name, @Nonnull String value) throws SirixException {
    return delegate().insertAttribute(name, value);
  }

  @Override
  public XmlNodeTrx insertAttribute(QNm name, @Nonnull String value, @Nonnull Movement move)
      throws SirixException {
    return delegate().insertAttribute(name, value, move);
  }

  @Override
  public XmlNodeTrx insertElementAsFirstChild(QNm name) throws SirixException {
    return delegate().insertElementAsFirstChild(name);
  }

  @Override
  public XmlNodeTrx insertElementAsLeftSibling(QNm name) throws SirixException {
    return delegate().insertElementAsLeftSibling(name);
  }

  @Override
  public XmlNodeTrx insertElementAsRightSibling(QNm name) throws SirixException {
    return delegate().insertElementAsRightSibling(name);
  }

  @Override
  public XmlNodeTrx insertNamespace(QNm name) throws SirixException {
    return delegate().insertNamespace(name);
  }

  @Override
  public XmlNodeTrx insertNamespace(QNm name, @Nonnull Movement move) throws SirixException {
    return delegate().insertNamespace(name, move);
  }

  @Override
  public XmlNodeTrx insertSubtreeAsFirstChild(XMLEventReader reader) throws SirixException {
    return delegate().insertSubtreeAsFirstChild(reader);
  }

  @Override
  public XmlNodeTrx insertSubtreeAsRightSibling(XMLEventReader reader) throws SirixException {
    return delegate().insertSubtreeAsRightSibling(reader);
  }

  @Override
  public XmlNodeTrx insertSubtreeAsLeftSibling(XMLEventReader reader) throws SirixException {
    return delegate().insertSubtreeAsLeftSibling(reader);
  }

  @Override
  public XmlNodeTrx insertTextAsFirstChild(String value) throws SirixException {
    return delegate().insertTextAsFirstChild(value);
  }

  @Override
  public XmlNodeTrx insertTextAsLeftSibling(String value) throws SirixException {
    return delegate().insertTextAsLeftSibling(value);
  }

  @Override
  public XmlNodeTrx insertTextAsRightSibling(String value) throws SirixException {
    return delegate().insertTextAsRightSibling(value);
  }

  @Override
  public XmlNodeTrx setName(QNm name) throws SirixException {
    return delegate().setName(name);
  }

  @Override
  public XmlNodeTrx setValue(String value) throws SirixException {
    return delegate().setValue(value);
  }

  @Override
  public XmlNodeTrx remove() throws SirixException {
    return delegate().remove();
  }
}
