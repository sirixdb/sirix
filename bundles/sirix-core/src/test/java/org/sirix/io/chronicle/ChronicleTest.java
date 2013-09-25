package org.sirix.io.chronicle;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixException;
import org.sirix.io.IOTestHelper;
import org.sirix.io.StorageType;

public class ChronicleTest {

	private ResourceConfiguration resourceConf;

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
		Holder.generateSession().close();
		resourceConf = IOTestHelper.registerIO(StorageType.CHRONICLE);
	}

	@Test
	public void testFirstRef() throws SirixException {
//		IOTestHelper.testReadWriteFirstRef(resourceConf);
	}

	@After
	public void tearDown() throws SirixException {
		IOTestHelper.clean();
	}

}
