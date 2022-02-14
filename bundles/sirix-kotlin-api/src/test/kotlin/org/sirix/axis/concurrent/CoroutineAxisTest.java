package org.sirix.axis.concurrent;

import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.api.Axis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.NestedAxis;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.xml.XmlNameFilter;
import org.sirix.service.xml.shredder.XmlShredder;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test {@link CoroutineAxis}. */
public class CoroutineAxisTest {

    /** XML file name to test. */
    private static final String XMLFILE = "10mb.xml";

    /** Path to XML file. */
    private static final Path XML = Paths.get("src", "test", "resources", XMLFILE);

    private Holder holder;

    /**
     * Method is called once before each test. It deletes all states, shreds XML file to database and
     * initializes the required variables.
     */
    @Before
    public void setUp() {
        try {
            XmlTestHelper.deleteEverything();
            XmlShredder.main(XML.toAbsolutePath().toString(), XmlTestHelper.PATHS.PATH1.getFile().toAbsolutePath().toString());
            holder = Holder.generateRtx();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Test coroutine.
     */
    @Test
    public void testCoroutine() {
        /* query: //regions/africa//location */
        final int resultNumber = 55;
        final var firstConcurrRtx = holder.getResourceManager().beginNodeReadOnlyTrx();
        final var secondConcurrRtx = holder.getResourceManager().beginNodeReadOnlyTrx();
        final var thirdConcurrRtx = holder.getResourceManager().beginNodeReadOnlyTrx();
        final var firstRtx = holder.getResourceManager().beginNodeReadOnlyTrx();
        final var secondRtx = holder.getResourceManager().beginNodeReadOnlyTrx();
        final var thirdRtx = holder.getResourceManager().beginNodeReadOnlyTrx();
        final Axis axis =
                new NestedAxis(
                        new NestedAxis(
                                new CoroutineAxis<>(firstConcurrRtx,
                                        new FilterAxis<>(new DescendantAxis(firstRtx, IncludeSelf.YES),
                                                new XmlNameFilter(firstRtx, "regions"))),
                                new CoroutineAxis<>(secondConcurrRtx,
                                        new FilterAxis<>(new ChildAxis(secondRtx), new XmlNameFilter(secondRtx, "africa")))),
                        new CoroutineAxis<>(thirdConcurrRtx,
                                new FilterAxis<>(new DescendantAxis(thirdRtx, IncludeSelf.YES), new XmlNameFilter(thirdRtx, "location"))));

        for (int i = 0; i < resultNumber; i++) {
            assertTrue(axis.hasNext());
            axis.next();
        }
        assertFalse(axis.hasNext());
    }

    /**
     * Test coroutine.
     */
    @Test
    public void testPartCoroutineDescAxis1() {
        /* query: //regions/africa//location */
        final int resultNumber = 55;
        final var firstConcurrRtx = holder.getResourceManager().beginNodeReadOnlyTrx();
        final var axis = new NestedAxis(
                new NestedAxis(
                        new CoroutineAxis<>(firstConcurrRtx,
                                new FilterAxis<>(new DescendantAxis(holder.getXmlNodeReadTrx(), IncludeSelf.YES),
                                        new XmlNameFilter(holder.getXmlNodeReadTrx(), "regions"))),
                        new FilterAxis<>(new ChildAxis(firstConcurrRtx), new XmlNameFilter(firstConcurrRtx, "africa"))),
                new FilterAxis<>(new DescendantAxis(firstConcurrRtx, IncludeSelf.YES),
                        new XmlNameFilter(firstConcurrRtx, "location")));

        for (int i = 0; i < resultNumber; i++) {
            assertTrue(axis.hasNext());
            axis.next();
        }
        assertFalse(axis.hasNext());
    }

    /**
     * Test coroutine.
     */
    @Test
    public void testPartConcurrentDescAxis2() {
        /* query: //regions/africa//location */
        final int resultNumber = 55;
        final var firstConcurrRtx = holder.getResourceManager().beginNodeReadOnlyTrx();
        final var axis = new NestedAxis(
                new NestedAxis(
                        new FilterAxis<>(new DescendantAxis(firstConcurrRtx, IncludeSelf.YES),
                                new XmlNameFilter(firstConcurrRtx, "regions")),
                        new FilterAxis<>(new ChildAxis(firstConcurrRtx), new XmlNameFilter(firstConcurrRtx, "africa"))),
                new CoroutineAxis<>(firstConcurrRtx,
                        new FilterAxis<>(new DescendantAxis(holder.getXmlNodeReadTrx(), IncludeSelf.YES),
                                new XmlNameFilter(holder.getXmlNodeReadTrx(), "location"))));

        for (int i = 0; i < resultNumber; i++) {
            assertTrue(axis.hasNext());
            axis.next();
        }
        assertFalse(axis.hasNext());
    }
}
