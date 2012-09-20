package org.sirix.access.conf;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.sirix.TestHelper;
import org.sirix.access.Database;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;

/**
 * Test case for de-/serialization of {@link DatabaseConfiguration}s.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class DatabaseConfigurationTest {

	@BeforeMethod
	public void setUp() throws AbsTTException {
		TestHelper.deleteEverything();
	}

	@AfterMethod
	public void tearDown() throws AbsTTException {
		TestHelper.deleteEverything();
	}

	/**
	 * Test method for
	 * {@link org.treetank.access.conf.DatabaseConfiguration#serialize(org.treetank.access.conf.DatabaseConfiguration)}
	 * and
	 * {@link org.treetank.access.conf.DatabaseConfiguration#deserialize(java.io.File)}
	 * .
	 * 
	 * @throws TTIOException
	 *           if an I/O exception occurs
	 */
	@Test
	public void testDeSerialize() throws TTIOException {
		DatabaseConfiguration conf = new DatabaseConfiguration(
				TestHelper.PATHS.PATH1.getFile());
		assertTrue(Database.createDatabase(conf));
		DatabaseConfiguration serializedConf = DatabaseConfiguration
				.deserialize(TestHelper.PATHS.PATH1.getFile());
		assertEquals(conf.toString(), serializedConf.toString());
	}
}
