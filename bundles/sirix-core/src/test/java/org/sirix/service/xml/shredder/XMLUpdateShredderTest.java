/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.service.xml.shredder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.Difference;
import org.custommonkey.xmlunit.XMLTestCase;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.service.xml.serialize.XMLSerializer.XMLSerializerBuilder;

/**
 * Test XMLUpdateShredder.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class XMLUpdateShredderTest extends XMLTestCase {
	private static final String RESOURCES = "src" + File.separator + "test"
			+ File.separator + "resources";

	private static final String XMLINSERTFIRST = RESOURCES + File.separator
			+ "revXMLsInsert";

	private static final String XMLINSERTSECOND = RESOURCES + File.separator
			+ "revXMLsInsert1";

	private static final String XMLINSERTTHIRD = RESOURCES + File.separator
			+ "revXMLsInsert2";

	private static final String XMLDELETEFIRST = RESOURCES + File.separator
			+ "revXMLsDelete";

	private static final String XMLDELETESECOND = RESOURCES + File.separator
			+ "revXMLsDelete1";

	private static final String XMLDELETETHIRD = RESOURCES + File.separator
			+ "revXMLsDelete2";

	private static final String XMLDELETEFOURTH = RESOURCES + File.separator
			+ "revXMLsDelete3";

	private static final String XMLSAME = RESOURCES + File.separator
			+ "revXMLsSame";

	private static final String XMLALLSECOND = RESOURCES + File.separator
			+ "revXMLsAll1";

	private static final String XMLALLFOURTH = RESOURCES + File.separator
			+ "revXMLsAll3";

	private static final String XMLALLFIFTH = RESOURCES + File.separator
			+ "revXMLsAll4";

	private static final String XMLALLSEVENTH = RESOURCES + File.separator
			+ "revXMLsAll6";

	// private static final String XMLLINGUISTICS = RESOURCES + File.separator +
	// "linguistics";

	static {
		XMLUnit.setIgnoreComments(true);
		XMLUnit.setIgnoreWhitespace(true);
	}

	@Override
	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
	}

	@Override
	@After
	public void tearDown() throws SirixException {
		TestHelper.closeEverything();
	}

	@Test
	public void testSame() throws Exception {
		test(XMLSAME);
	}

	@Test
	public void testInsertsFirst() throws Exception {
		test(XMLINSERTFIRST);
	}

	@Test
	public void testInsertsSecond() throws Exception {
		test(XMLINSERTSECOND);
	}

	@Test
	public void testInsertsThird() throws Exception {
		test(XMLINSERTTHIRD);
	}

	@Test
	public void testDeletesFirst() throws Exception {
		test(XMLDELETEFIRST);
	}

	@Test
	public void testDeletesSecond() throws Exception {
		test(XMLDELETESECOND);
	}

	@Test
	public void testDeletesThird() throws Exception {
		test(XMLDELETETHIRD);
	}

	@Test
	public void testDeletesFourth() throws Exception {
		test(XMLDELETEFOURTH);
	}

	// @Test
	// public void testAllFirst() throws Exception {
	// test(XMLALLFIRST);
	// }

	@Test
	public void testAllSecond() throws Exception {
		test(XMLALLSECOND);
	}

	// /** Not working anymore due to text merging on deletes. */
	// @Ignore
	// @Test
	// public void testAllThird() throws Exception {
	// test(XMLALLTHIRD);
	// }

	@Test
	public void testAllFourth() throws Exception {
		test(XMLALLFOURTH);
	}

	@Test
	public void testAllFifth() throws Exception {
		test(XMLALLFIFTH);
	}

	// /** Not working anymore due to text merging on deletes. */
	// @Ignore
	// @Test
	// public void testAllSixth() throws Exception {
	// test(XMLALLSIXTH);
	// }

	@Test
	public void testAllSeventh() throws Exception {
		test(XMLALLSEVENTH);
	}

	// /** Not working anymore due to text merging on deletes. */
	// @Ignore
	// @Test
	// public void testAllEighth() throws Exception {
	// test(XMLALLEIGHTH);
	// }
	//
	// /** Not working anymore due to text merging on deletes. */
	// @Ignore
	// @Test
	// public void testAllNineth() throws Exception {
	// test(XMLALLNINETH);
	// }

	// @Test
	// public void testLinguistics() throws Exception {
	// test(XMLLINGUISTICS);
	// }

	private void test(final String FOLDER) throws Exception {
		final Database database = TestHelper.getDatabase(PATHS.PATH1.getFile());
		database.createResource(new ResourceConfiguration.Builder(
				TestHelper.RESOURCE, PATHS.PATH1.getConfig()).build());
		final Session session = database
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		final File folder = new File(FOLDER);
		int i = 2;
		final File[] filesList = folder.listFiles();
		final List<File> list = new ArrayList<File>();
		for (final File file : filesList) {
			if (file.getName().endsWith(".xml")) {
				list.add(file);
			}
		}

		// Sort files array according to file names.
		Collections.sort(list, new Comparator<Object>() {
			@Override
			public int compare(final Object paramFirst, final Object paramSecond) {
				final String firstName = ((File) paramFirst)
						.getName()
						.toString()
						.substring(0, ((File) paramFirst).getName().toString().indexOf('.'));
				final String secondName = ((File) paramSecond)
						.getName()
						.toString()
						.substring(0,
								((File) paramSecond).getName().toString().indexOf('.'));
				if (Integer.parseInt(firstName) < Integer.parseInt(secondName)) {
					return -1;
				} else if (Integer.parseInt(firstName) > Integer.parseInt(secondName)) {
					return +1;
				} else {
					return 0;
				}
			}
		});

		boolean first = true;

		// Shredder files.
		for (final File file : list) {
			if (file.getName().endsWith(".xml")) {
				final NodeWriteTrx wtx = session.beginNodeWriteTrx();
				if (first) {
					final XMLShredder shredder = new XMLShredder.Builder(wtx,
							XMLShredder.createFileReader(file), Insert.ASFIRSTCHILD)
							.commitAfterwards().build();
					shredder.call();
					first = false;
				} else {
					@SuppressWarnings("deprecation")
					final XMLUpdateShredder shredder = new XMLUpdateShredder(wtx,
							XMLShredder.createFileReader(file), Insert.ASFIRSTCHILD, file,
							ShredderCommit.COMMIT);
					shredder.call();
				}
				assertEquals(i, wtx.getRevisionNumber());

				i++;

				final OutputStream out = new ByteArrayOutputStream();
				final XMLSerializer serializer = new XMLSerializerBuilder(session, out)
						.prettyPrint().build();
				serializer.call();
				final StringBuilder sBuilder = TestHelper.readFile(
						file.getAbsoluteFile(), false);

				final Diff diff = new Diff(sBuilder.toString(), out.toString());
				final DetailedDiff detDiff = new DetailedDiff(diff);
				@SuppressWarnings("unchecked")
				final List<Difference> differences = detDiff.getAllDifferences();
				for (final Difference difference : differences) {
					System.out.println("***********************");
					System.out.println(difference);
					System.out.println("***********************");
				}

				assertTrue("pieces of XML are similar " + diff, diff.similar());
				assertTrue("but are they identical? " + diff, diff.identical());
				wtx.close();
			}
		}
	}
}
