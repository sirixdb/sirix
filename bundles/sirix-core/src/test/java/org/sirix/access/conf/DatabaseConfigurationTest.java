package org.sirix.access.conf;

import org.sirix.XmlTestHelper;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test case for de-/serialization of {@link DatabaseConfiguration}s.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class DatabaseConfigurationTest {

  @BeforeMethod
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
  }

  @AfterMethod
  public void tearDown() throws SirixException {
    XmlTestHelper.deleteEverything();
  }

  /**
   * Test method for
   * {@link org.sirix.access.treetank.access.conf.DatabaseConfiguration#serialize(org.sirix.access.treetank.access.conf.DatabaseConfiguration)}
   * and {@link org.sirix.access.treetank.access.conf.DatabaseConfiguration#deserialize(java.io.File)} .
   * 
   * @throws SirixIOException if an I/O exception occurs
   */
  @Test
  public void testDeSerialize() throws SirixIOException {
    DatabaseConfiguration conf = new DatabaseConfiguration(XmlTestHelper.PATHS.PATH1.getFile());
    assertTrue(Databases.createXmlDatabase(conf));
    DatabaseConfiguration serializedConf =
        DatabaseConfiguration.deserialize(XmlTestHelper.PATHS.PATH1.getFile());
    assertEquals(conf.toString(), serializedConf.toString());
  }
}
