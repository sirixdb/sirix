package io.sirix.query.node;

import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.brackit.query.XMarkTest;
import io.brackit.query.Query;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.node.NodeCollection;
import io.brackit.query.jdm.node.NodeStore;
import io.brackit.query.node.parser.DocumentParser;
import org.junit.After;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * XMark test.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixXMarkTest extends XMarkTest {

  /** Sirix database store. */
  private BasicXmlDBStore xmlStore;

  /** Test directory for XML databases. */
  private Path xmlTestDir;

  @Override
  protected NodeStore createStore() {
    try {
      xmlTestDir = Files.createTempDirectory("sirix-xml-xmark-test");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    xmlStore = BasicXmlDBStore.newBuilder().location(xmlTestDir).build();
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
    if (xmlTestDir != null) {
      Databases.removeDatabase(xmlTestDir);
    }
  }
}
