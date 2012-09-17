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
import org.sirix.exception.TTIOException;
import org.sirix.io.bytepipe.IByteHandler;
import org.sirix.node.EKind;
import org.sirix.page.interfaces.IPage;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test class for all classes implementing the {@link IPage} interface.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @auhtor Johannes Lichtenberger, University of Konstanz
 * 
 */
public class IPageTest {

  /**
   * Test method for {@link org.treetank.page.IPage#IPage(long)} and
   * {@link org.treetank.page.IPage#getByteRepresentation()}.
   * 
   * @param pClass
   *          IPage as class
   * @param pHandlers
   *          different pages
   */
  @Test(dataProvider = "instantiatePages")
  public void testByteRepresentation(final Class<IPage> pClass, final IPage[] pHandlers) {
    for (final IPage handler : pHandlers) {
      ByteArrayDataOutput output = ByteStreams.newDataOutput();
      handler.serialize(output);
      final byte[] pageBytes = output.toByteArray();
      final ByteArrayDataInput input = ByteStreams.newDataInput(pageBytes);

      output = ByteStreams.newDataOutput();
      final IPage serializedPage =
        EPage.getKind(handler.getClass()).deserializePage(input);
      serializedPage.serialize(output);
      assertTrue(new StringBuilder("Check for ").append(handler.getClass())
        .append(" failed.").toString(), Arrays.equals(pageBytes, output
        .toByteArray()));
    }
  }

  /**
   * Providing different implementations of the {@link IPage} as Dataprovider to the test class.
   * 
   * @return different classes of the {@link IByteHandler}
   * @throws TTIOException
   *           if an I/O error occurs
   */
  @DataProvider(name = "instantiatePages")
  public Object[][] instantiatePages() throws TTIOException {
    // IndirectPage setup.
    IndirectPage indirectPage = new IndirectPage(0);
    // RevisionRootPage setup.
    RevisionRootPage revRootPage = new RevisionRootPage();
    // // NodePage setup.
    // NodePage nodePage =
    // new NodePage(TestHelper.random.nextLong(), TestHelper.random.nextLong());
    // for (int i = 0; i < IConstants.NDP_NODE_COUNT - 1; i++) {
    // nodePage.setNode(i, DumbNodeFactory.generateOne());
    // }
    // NamePage setup.
    NamePage namePage = new NamePage(0);
    namePage.setName(TestHelper.random.nextInt(), new String(TestHelper
      .generateRandomBytes(256)), EKind.ELEMENT);

    // ValuePage setup.
    ValuePage valuePage = new ValuePage(0);

    // PathSummaryPage setup.
    PathSummaryPage pathSummaryPage = new PathSummaryPage(0);

    Object[][] returnVal = {
      {
        IPage.class, new IPage[] {
          indirectPage, revRootPage, namePage, valuePage, pathSummaryPage
        }
      }
    };
    return returnVal;
  }
}
