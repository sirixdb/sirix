package org.sirix.xquery.node;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;

import org.brackit.xquery.node.parser.DocumentParser;
import org.brackit.xquery.xdm.DocumentException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.xml.sax.SAXException;

public class SubtreeBuilderTest {

	/** Path to resources folder. */
	public static final String RESOURCES = new StringBuilder("src")
			.append(File.separator).append("test").append(File.separator)
			.append("resources").toString();
	
	/** XMark directory. */
	private static final String XMARK_AUCTION = new StringBuilder(RESOURCES)
			.append(File.separator).append("xmark").append(File.separator)
			.append("auction.xml").toString();
	
	private DBStore mStore;

	@Before
	public void setUp() {
		mStore = new DBStore(true);
	}
	
	@Test
	public void testSubtreeBuilder() throws DocumentException, IOException, SirixException, SAXException {
		final DocumentParser parser = new DocumentParser(new File(XMARK_AUCTION));
		parser.setRetainWhitespace(true);
		mStore.create("testCollection", parser);
		final DBCollection coll = (DBCollection) mStore.lookup("testCollection");
		final Database database = coll.getDatabase();
		final Session session = database.getSession(SessionConfiguration.builder("shredded").build());
		final OutputStream out = new ByteArrayOutputStream();
		XMLSerializer.builder(session, out).build().call();
		final FileWriter writer = new FileWriter(new File("/home/johannes/Desktop/auction.xml"));
		writer.append(out.toString());
		writer.close();
		final StringBuilder sBuilder = TestHelper.readFile(
				new File(XMARK_AUCTION), false);
//		final Diff diff = new Diff(sBuilder.toString(), out.toString());
//		final DetailedDiff detDiff = new DetailedDiff(diff);
//		@SuppressWarnings("unchecked")
//		final List<Difference> differences = detDiff.getAllDifferences();
//		for (final Difference difference : differences) {
//			System.out.println("***********************");
//			System.out.println(difference);
//			System.out.println("***********************");
//		}
	}
	
	
	
	
	@After
	public void tearDown() throws DocumentException {
		mStore.close();
	}
}
