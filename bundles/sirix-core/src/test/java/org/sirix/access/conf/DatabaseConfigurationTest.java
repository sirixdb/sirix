package org.sirix.access.conf;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import org.sirix.XdmTestHelper;
import org.sirix.access.Databases;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test case for de-/serialization of {@link DatabaseConfiguration}s.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class DatabaseConfigurationTest {

  @BeforeMethod
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
  }

  @AfterMethod
  public void tearDown() throws SirixException {
    XdmTestHelper.deleteEverything();
  }

  /**
   * Test method for
   * {@link org.treetank.access.conf.DatabaseConfiguration#serialize(org.treetank.access.conf.DatabaseConfiguration)}
   * and {@link org.treetank.access.conf.DatabaseConfiguration#deserialize(java.io.File)} .
   * 
   * @throws SirixIOException if an I/O exception occurs
   */
  @Test
  public void testDeSerialize() throws SirixIOException {
    DatabaseConfiguration conf = new DatabaseConfiguration(XdmTestHelper.PATHS.PATH1.getFile());
    assertTrue(Databases.createXdmDatabase(conf));
    DatabaseConfiguration serializedConf =
        DatabaseConfiguration.deserialize(XdmTestHelper.PATHS.PATH1.getFile());
    assertEquals(conf.toString(), serializedConf.toString());
  }
}
