package io.sirix.query.node;

import io.sirix.query.SirixCompileChain;
import io.brackit.query.XMarkTest;
import io.brackit.query.Query;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.node.NodeCollection;
import io.brackit.query.jdm.node.NodeStore;
import io.brackit.query.node.parser.DocumentParser;
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
  protected Query xquery(final String query) {
    return new Query(SirixCompileChain.createWithNodeStore(xmlStore), query);
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
