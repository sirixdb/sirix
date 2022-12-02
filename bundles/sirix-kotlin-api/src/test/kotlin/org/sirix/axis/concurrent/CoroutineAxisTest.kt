package org.sirix.axis.concurrent

import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.sirix.Holder
import org.sirix.XmlTestHelper
import org.sirix.api.Axis
import org.sirix.axis.ChildAxis
import org.sirix.axis.DescendantAxis
import org.sirix.axis.IncludeSelf
import org.sirix.axis.NestedAxis
import org.sirix.axis.filter.FilterAxis
import org.sirix.axis.filter.xml.XmlNameFilter
import org.sirix.service.xml.shredder.XmlShredder
import java.nio.file.Paths

/** Test [CoroutineAxis].  */
class CoroutineAxisTest {
    private var holder: Holder? = null

    /**
     * Method is called once before each test. It deletes all states, shreds XML file to database and
     * initializes the required variables.
     */
    @Before
    fun setUp() {
        try {
            XmlTestHelper.deleteEverything()
            XmlShredder.main(
                XML.toAbsolutePath().toString(),
                XmlTestHelper.PATHS.PATH1.file.toAbsolutePath().toString()
            )
            holder = Holder.generateRtx()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    /**
     * Test coroutine.
     */
    @Test
    fun testCoroutine() {
        /* query: //regions/africa//location */
        val resultNumber = 55
        val firstConcurrRtx = holder!!.resourceManager.beginNodeReadOnlyTrx()
        val secondConcurrRtx = holder!!.resourceManager.beginNodeReadOnlyTrx()
        val thirdConcurrRtx = holder!!.resourceManager.beginNodeReadOnlyTrx()
        val firstRtx = holder!!.resourceManager.beginNodeReadOnlyTrx()
        val secondRtx = holder!!.resourceManager.beginNodeReadOnlyTrx()
        val thirdRtx = holder!!.resourceManager.beginNodeReadOnlyTrx()
        val axis: Axis = NestedAxis(
            NestedAxis(
                CoroutineAxis(
                    firstConcurrRtx,
                    FilterAxis(
                        DescendantAxis(firstRtx, IncludeSelf.YES),
                        XmlNameFilter(firstRtx, "regions")
                    )
                ),
                CoroutineAxis(
                    secondConcurrRtx,
                    FilterAxis(ChildAxis(secondRtx), XmlNameFilter(secondRtx, "africa"))
                )
            ),
            CoroutineAxis(
                thirdConcurrRtx,
                FilterAxis(DescendantAxis(thirdRtx, IncludeSelf.YES), XmlNameFilter(thirdRtx, "location"))
            )
        )
        for (i in 0 until resultNumber) {
            Assert.assertTrue(axis.hasNext())
            axis.nextLong()
        }
        Assert.assertFalse(axis.hasNext())
    }

    /**
     * Test coroutine.
     */
    @Test
    fun testPartCoroutineDescAxis1() {
        /* query: //regions/africa//location */
        val resultNumber = 55
        val firstConcurrRtx = holder!!.resourceManager.beginNodeReadOnlyTrx()
        val axis = NestedAxis(
            NestedAxis(
                CoroutineAxis(
                    firstConcurrRtx,
                    FilterAxis(
                        DescendantAxis(holder!!.xmlNodeReadTrx, IncludeSelf.YES),
                        XmlNameFilter(holder!!.xmlNodeReadTrx, "regions")
                    )
                ),
                FilterAxis(ChildAxis(firstConcurrRtx), XmlNameFilter(firstConcurrRtx, "africa"))
            ),
            FilterAxis(
                DescendantAxis(firstConcurrRtx, IncludeSelf.YES),
                XmlNameFilter(firstConcurrRtx, "location")
            )
        )
        for (i in 0 until resultNumber) {
            Assert.assertTrue(axis.hasNext())
            axis.nextLong()
        }
        Assert.assertFalse(axis.hasNext())
    }

    /**
     * Test coroutine.
     */
    @Test
    fun testPartConcurrentDescAxis2() {
        /* query: //regions/africa//location */
        val resultNumber = 55
        val firstConcurrRtx = holder!!.resourceManager.beginNodeReadOnlyTrx()
        val axis = NestedAxis(
            NestedAxis(
                FilterAxis(
                    DescendantAxis(firstConcurrRtx, IncludeSelf.YES),
                    XmlNameFilter(firstConcurrRtx, "regions")
                ),
                FilterAxis(ChildAxis(firstConcurrRtx), XmlNameFilter(firstConcurrRtx, "africa"))
            ),
            CoroutineAxis(
                firstConcurrRtx,
                FilterAxis(
                    DescendantAxis(holder!!.xmlNodeReadTrx, IncludeSelf.YES),
                    XmlNameFilter(holder!!.xmlNodeReadTrx, "location")
                )
            )
        )
        for (i in 0 until resultNumber) {
            Assert.assertTrue(axis.hasNext())
            axis.nextLong()
        }
        Assert.assertFalse(axis.hasNext())
    }

    companion object {
        /** XML file name to test.  */
        private const val XMLFILE = "10mb.xml"

        /** Path to XML file.  */
        private val XML = Paths.get("src", "test", "resources", XMLFILE)
    }
}