/**
 * 
 */
package org.sirix.page;

import static org.testng.AssertJUnit.assertTrue;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.util.Arrays;

import org.sirix.TestHelper;
import org.sirix.exception.SirixIOException;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.node.Kind;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test class for all classes implementing the {@link Page} interface.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @auhtor Johannes Lichtenberger, University of Konstanz
 * 
 */
public class IPageTest {

	/**
	 * Test method for {@link org.Page.page.IPage#IPage(long)} and
	 * {@link org.Page.page.IPage#getByteRepresentation()}.
	 * 
	 * @param pClass
	 *          IPage as class
	 * @param pHandlers
	 *          different pages
	 */
	@Test(dataProvider = "instantiatePages")
	public void testByteRepresentation(final Class<Page> pClass,
			final Page[] pHandlers) {
		for (final Page handler : pHandlers) {
			final ByteArrayDataOutput output = ByteStreams.newDataOutput();
			handler.serialize(output);
			final byte[] pageBytes = output.toByteArray();
			final ByteArrayDataInput input = ByteStreams.newDataInput(pageBytes);

			final ByteArrayDataOutput serializedOutput = ByteStreams.newDataOutput();
			final Page serializedPage = PageKind.getKind(handler.getClass())
					.deserializePage(input);
			serializedPage.serialize(serializedOutput);
			assertTrue(new StringBuilder("Check for ").append(handler.getClass())
					.append(" failed.").toString(),
					Arrays.equals(pageBytes, serializedOutput.toByteArray()));
		}
	}

	/**
	 * Providing different implementations of the {@link Page} as Dataprovider to
	 * the test class.
	 * 
	 * @return different classes of the {@link ByteHandler}
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	@DataProvider(name = "instantiatePages")
	public Object[][] instantiatePages() throws SirixIOException {
		// IndirectPage setup.
		final IndirectPage indirectPage = new IndirectPage(0);
		// RevisionRootPage setup.
		// final RevisionRootPage revRootPage = new RevisionRootPage();

		// NodePage setup.
		final NodePage nodePage = new NodePage(
				TestHelper.random.nextInt(Integer.MAX_VALUE),
				TestHelper.random.nextInt(Integer.MAX_VALUE));
		for (int i = 0; i < Constants.NDP_NODE_COUNT - 1; i++) {
			nodePage.setNode(TestHelper.generateOne());
		}
		// NamePage setup.
		final NamePage namePage = new NamePage(0);
		namePage.setName(TestHelper.random.nextInt(),
				new String(TestHelper.generateRandomBytes(256)), Kind.ELEMENT);

		// ValuePage setup.
		final ValuePage valuePage = new ValuePage(0);

		// PathSummaryPage setup.
		final PathSummaryPage pathSummaryPage = new PathSummaryPage(0);

		Object[][] returnVal = { { Page.class,
				new Page[] { indirectPage, namePage, valuePage, pathSummaryPage } } };
		return returnVal;
	}
}
