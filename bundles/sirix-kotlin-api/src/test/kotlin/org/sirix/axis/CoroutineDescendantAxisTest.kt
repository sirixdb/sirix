/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.axis

import com.google.common.collect.ImmutableList
import com.google.common.collect.testing.IteratorFeature
import com.google.common.collect.testing.IteratorTester
import org.checkerframework.org.apache.commons.lang3.time.StopWatch
import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.sirix.Holder
import org.sirix.JsonTestHelper
import org.sirix.XmlTestHelper
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.NodeCursor
import org.sirix.api.json.JsonResourceSession
import org.sirix.io.StorageType
import org.sirix.service.json.shredder.JsonShredder
import org.sirix.settings.Fixed
import org.sirix.settings.VersioningType
import org.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

class CoroutineDescendantAxisTest {
    private var holder: Holder? = null
    @Before
    fun setUp() {
        XmlTestHelper.deleteEverything()
        XmlTestHelper.createTestDocument()
        holder = Holder.generateRtx()
    }

    @After
    fun tearDown() {
        holder!!.close()
        XmlTestHelper.closeEverything()
    }

    @Test
    fun testIterate() {
        val rm = holder!!.resourceManager
        val rtx = holder!!.xmlNodeReadTrx
        rtx.moveToDocumentRoot()
        AbsAxisTest.testAxisConventions(
            CoroutineDescendantAxis(rm),
            longArrayOf(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L)
        )
        object : IteratorTester<Long?>(
            ITERATIONS,
            IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L),
            null
        ) {
            override fun newTargetIterator(): Iterator<Long> {
                return CoroutineDescendantAxis(rm)
            }
        }.test()
        rtx.moveTo(1L)
        AbsAxisTest.testAxisConventions(
            CoroutineDescendantAxis(rm, IncludeSelf.NO, rtx),
            longArrayOf(4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L)
        )
        object : IteratorTester<Long?>(
            ITERATIONS,
            IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L),
            null
        ) {
            override fun newTargetIterator(): Iterator<Long> {
                val rtx = holder!!.xmlNodeReadTrx
                rtx.moveTo(1L)
                return CoroutineDescendantAxis(rm, IncludeSelf.NO, rtx)
            }
        }.test()
        rtx.moveTo(9L)
        AbsAxisTest.testAxisConventions(CoroutineDescendantAxis(rm, IncludeSelf.NO, rtx), longArrayOf(11L, 12L))
        object : IteratorTester<Long?>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(11L, 12L), null) {
            override fun newTargetIterator(): Iterator<Long> {
                val rtx = holder!!.xmlNodeReadTrx
                rtx.moveTo(9L)
                return CoroutineDescendantAxis(rm, IncludeSelf.NO, rtx)
            }
        }.test()
        rtx.moveTo(13L)
        AbsAxisTest.testAxisConventions(CoroutineDescendantAxis(rm, IncludeSelf.NO, rtx), longArrayOf())
        object : IteratorTester<Long?>(ITERATIONS, IteratorFeature.UNMODIFIABLE, emptyList<Long>(), null) {
            override fun newTargetIterator(): Iterator<Long> {
                val rtx = holder!!.xmlNodeReadTrx
                rtx.moveTo(13L)
                return CoroutineDescendantAxis(rm, IncludeSelf.NO, rtx)
            }
        }.test()
    }

    @Test
    fun testIterateIncludingSelf() {
        val rm = holder!!.resourceManager
        val rtx: NodeCursor = rm.beginNodeReadOnlyTrx()
        AbsAxisTest.testAxisConventions(
            CoroutineDescendantAxis(rm, IncludeSelf.YES), longArrayOf(
                Fixed.DOCUMENT_NODE_KEY.standardProperty, 1L, 4L, 5L, 6L, 7L, 8L,
                9L, 11L, 12L, 13L
            )
        )
        object : IteratorTester<Long?>(
            ITERATIONS,
            IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(
                Fixed.DOCUMENT_NODE_KEY.standardProperty,
                1L,
                4L,
                5L,
                6L,
                7L,
                8L,
                9L,
                11L,
                12L,
                13L
            ),
            null
        ) {
            override fun newTargetIterator(): Iterator<Long> {
                return CoroutineDescendantAxis(rm, IncludeSelf.YES)
            }
        }.test()
        rtx.moveTo(1L)
        AbsAxisTest.testAxisConventions(
            CoroutineDescendantAxis(rm, IncludeSelf.YES, rtx),
            longArrayOf(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L)
        )
        object : IteratorTester<Long?>(
            ITERATIONS,
            IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L),
            null
        ) {
            override fun newTargetIterator(): Iterator<Long> {
                val rtx: NodeCursor = rm.beginNodeReadOnlyTrx()
                rtx.moveTo(1L)
                return CoroutineDescendantAxis(rm, IncludeSelf.YES, rtx)
            }
        }.test()
        rtx.moveTo(9L)
        AbsAxisTest.testAxisConventions(CoroutineDescendantAxis(rm, IncludeSelf.YES, rtx), longArrayOf(9L, 11L, 12L))
        object : IteratorTester<Long?>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(9L, 11L, 12L), null) {
            override fun newTargetIterator(): Iterator<Long> {
                val rtx: NodeCursor = rm.beginNodeReadOnlyTrx()
                rtx.moveTo(9L)
                return CoroutineDescendantAxis(rm, IncludeSelf.YES, rtx)
            }
        }.test()
        rtx.moveTo(13L)
        AbsAxisTest.testAxisConventions(CoroutineDescendantAxis(rm, IncludeSelf.YES, rtx), longArrayOf(13L))
        object : IteratorTester<Long?>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(13L), null) {
            override fun newTargetIterator(): Iterator<Long> {
                val rtx: NodeCursor = rm.beginNodeReadOnlyTrx()
                rtx.moveTo(13L)
                return CoroutineDescendantAxis(rm, IncludeSelf.YES, rtx)
            }
        }.test()
    }

    @Test
    fun testIterationTime() {
        val rtx = holder!!.xmlNodeReadTrx
        val rm = holder!!.resourceManager
        rtx.moveToDocumentRoot()
        val time1 = LocalDateTime.now()
        AbsAxisTest.testAxisConventions(
            CoroutineDescendantAxis(rm),
            longArrayOf(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L)
        )
        object : IteratorTester<Long?>(
            ITERATIONS,
            IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L),
            null
        ) {
            override fun newTargetIterator(): Iterator<Long> {
                return CoroutineDescendantAxis(rm)
            }
        }.test()
        val time2 = LocalDateTime.now()
        AbsAxisTest.testAxisConventions(DescendantAxis(rtx), longArrayOf(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L))
        object : IteratorTester<Long?>(
            ITERATIONS,
            IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L),
            null
        ) {
            override fun newTargetIterator(): Iterator<Long> {
                val rtx = holder!!.xmlNodeReadTrx
                rtx.moveToDocumentRoot()
                return DescendantAxis(rtx)
            }
        }.test()
        val time3 = LocalDateTime.now()
        println("CoroutineDescendantAxis -> " + Duration.between(time1, time2).toMillis())
        println("DescendantAxis -> " + Duration.between(time2, time3).toMillis())
    }

    @Ignore
    @Test
    fun testChicago() {
        //JsonTestHelper.deleteEverything()
        try {
            val jsonPath = JSON.resolve("cityofchicago.json")
            Databases.createJsonDatabase(DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.file))
            Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.file).use { database ->
               // createResource(jsonPath, database)
                database.beginResourceSession(JsonTestHelper.RESOURCE).use { session ->
                    session.beginNodeReadOnlyTrx().use { rtx ->
                        val axis = CoroutineDescendantAxis(session)
                        val stopWatch = StopWatch()
                        logger.info("start")
                        stopWatch.start()
                        logger.info("Max node key: " + rtx.maxNodeKey)
                        var count = 0
                        while (axis.hasNext()) {
                            val nodeKey = axis.nextLong()
                            if (count % 50000000L == 0L) {
                                logger.info("nodeKey: $nodeKey")
                            }
                            count++
                        }
                        logger.info(" done [" + stopWatch.getTime(TimeUnit.SECONDS) + " s].")
                    }
                }
            }
        } finally {
            JsonTestHelper.closeEverything()
        }
    }

    private fun createResource(jsonPath: Path, database: Database<JsonResourceSession>) {
        logger.info(" start shredding ")
        val stopWatch = StopWatch()
        stopWatch.start()
        database.createResource(
            ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                .buildPathSummary(true)
                .storeDiffs(true)
                .storeNodeHistory(false)
                .storeChildCount(true)
                .hashKind(HashType.ROLLING)
                .useTextCompression(false)
                .storageType(StorageType.FILE_CHANNEL) //.byteHandlerPipeline(new ByteHandlerPipeline())
                .useDeweyIDs(false) //   .byteHandlerPipeline(new ByteHandlePipeline(new LZ4Compressor()))
                .build()
        )
        database.beginResourceSession(JsonTestHelper.RESOURCE).use { session ->
            session.beginNodeTrx(262144 shl 3)
                .use { trx -> trx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(jsonPath)) }
        }
        logger.info(" done [" + stopWatch.getTime(TimeUnit.SECONDS) + " s].")
    }

    companion object {
        private val logger = LogWrapper(LoggerFactory.getLogger(CoroutineDescendantAxisTest::class.java))
        private val JSON = Paths.get("src", "test", "resources", "json")
        private const val ITERATIONS = 5
    }
}