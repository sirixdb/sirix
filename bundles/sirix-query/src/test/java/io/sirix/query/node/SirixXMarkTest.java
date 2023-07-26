package io.sirix.query.node;

import io.sirix.query.SirixCompileChain;
import org.brackit.xquery.XMarkTest;
import org.brackit.xquery.XQuery;
import org.brackit.xquery.jdm.DocumentException;
import org.brackit.xquery.jdm.node.NodeCollection;
import org.brackit.xquery.jdm.node.NodeStore;
import org.brackit.xquery.node.parser.DocumentParser;
import org.junit.After;

/**
 * XMark test.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixXMarkTest extends XMarkTest {

  /** Sirix database store. */
  private BasicXmlDBStore xmlStore;

  @Override
  protected NodeStore createStore() {
    xmlStore = BasicXmlDBStore.newBuilder().build();
    return xmlStore;
  }

  @Override
  protected XQuery xquery(final String query) {
    return new XQuery(SirixCompileChain.createWithNodeStore(xmlStore), query);
  }

  @Override
  protected NodeCollection<?> createDoc(final DocumentParser parser) {
    return xmlStore.create("testCollection", parser);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void commit() throws DocumentException {
    xmlStore.close();
  }
}
