package org.sirix.xquery.node;

import java.io.FileNotFoundException;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.XMarkTest;
import org.brackit.xquery.XQuery;
import org.brackit.xquery.node.parser.DocumentParser;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.node.NodeCollection;
import org.brackit.xquery.xdm.node.NodeStore;
import org.junit.After;
import org.sirix.xquery.SirixCompileChain;

/**
 * XMark test.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixXMarkTest extends XMarkTest {

  /** Sirix database store. */
  private BasicXmlDBStore mStore;

  @Override
  protected NodeStore createStore() throws Exception {
    mStore = BasicXmlDBStore.newBuilder().build();
    return mStore;
  }

  @Override
  protected XQuery xquery(final String query) throws QueryException {
    return new XQuery(SirixCompileChain.createWithNodeStore(mStore), query);
  }

  @Override
  protected NodeCollection<?> createDoc(final DocumentParser parser) throws DocumentException {
    return mStore.create("testCollection", parser);
  }

  @Override
  public void setUp() throws Exception, FileNotFoundException {
    super.setUp();
    // mTransaction = ((DBCollection) coll).beginTransaction();
  }

  @After
  public void commit() throws DocumentException {
    // mTransaction.commit();
    mStore.close();
  }
}
