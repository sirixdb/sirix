package io.sirix.axis.concurrent

import org.junit.Assert
import org.junit.Before
import org.junit.Test
import io.sirix.Holder
import io.sirix.XmlTestHelper
import io.sirix.api.Axis
import io.sirix.axis.ChildAxis
import io.sirix.axis.DescendantAxis
import io.sirix.axis.IncludeSelf
import io.sirix.axis.NestedAxis
import io.sirix.axis.filter.FilterAxis
import io.sirix.axis.filter.xml.XmlNameFilter
import io.sirix.service.xml.shredder.XmlShredder
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
        val firstConcurrRtx = holder!!.resourceSession.beginNodeReadOnlyTrx()
        val secondConcurrRtx = holder!!.resourceSession.beginNodeReadOnlyTrx()
        val thirdConcurrRtx = holder!!.resourceSession.beginNodeReadOnlyTrx()
        val firstRtx = holder!!.resourceSession.beginNodeReadOnlyTrx()
        val secondRtx = holder!!.resourceSession.beginNodeReadOnlyTrx()
        val thirdRtx = holder!!.resourceSession.beginNodeReadOnlyTrx()
        val axis: Axis = NestedAxis(
            NestedAxis(
                CoroutineAxis(
                    firstConcurrRtx,
                    FilterAxis(
                        DescendantAxis(
                            firstRtx,
                            IncludeSelf.YES
                        ),
                        XmlNameFilter(firstRtx, "regions")
                    )
                ),
                CoroutineAxis(
                    secondConcurrRtx,
                    FilterAxis(
                        ChildAxis(secondRtx),
                        XmlNameFilter(secondRtx, "africa")
                    )
                )
            ),
            CoroutineAxis(
                thirdConcurrRtx,
                FilterAxis(
                    DescendantAxis(
                        thirdRtx,
                        IncludeSelf.YES
                    ), XmlNameFilter(thirdRtx, "location")
                )
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
        val firstConcurrRtx = holder!!.resourceSession.beginNodeReadOnlyTrx()
        val axis = NestedAxis(
            NestedAxis(
                CoroutineAxis(
                    firstConcurrRtx,
                    FilterAxis(
                        DescendantAxis(
                            holder!!.xmlNodeReadTrx,
                            IncludeSelf.YES
                        ),
                        XmlNameFilter(
                            holder!!.xmlNodeReadTrx,
                            "regions"
                        )
                    )
                ),
                FilterAxis(
                    ChildAxis(firstConcurrRtx),
                    XmlNameFilter(firstConcurrRtx, "africa")
                )
            ),
            FilterAxis(
                DescendantAxis(
                    firstConcurrRtx,
                    IncludeSelf.YES
                ),
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
        val firstConcurrRtx = holder!!.resourceSession.beginNodeReadOnlyTrx()
        val axis = NestedAxis(
            NestedAxis(
                FilterAxis(
                    DescendantAxis(
                        firstConcurrRtx,
                        IncludeSelf.YES
                    ),
                    XmlNameFilter(firstConcurrRtx, "regions")
                ),
                FilterAxis(
                    ChildAxis(firstConcurrRtx),
                    XmlNameFilter(firstConcurrRtx, "africa")
                )
            ),
            CoroutineAxis(
                firstConcurrRtx,
                FilterAxis(
                    DescendantAxis(
                        holder!!.xmlNodeReadTrx,
                        IncludeSelf.YES
                    ),
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