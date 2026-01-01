/*
 * Copyright (c) 2024, SirixDB
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 * - Redistributions of source code must retain the above copyright notice
 * - Redistributions in binary form must reproduce the above copyright notice
 *
 * THIS SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND.
 */
package io.sirix.axis

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.Assert.assertEquals
import io.sirix.JsonTestHelper
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.api.Axis
import io.sirix.access.trx.node.HashType
import io.sirix.api.json.JsonResourceSession
import io.sirix.io.StorageType
import io.sirix.io.bytepipe.ByteHandlerPipeline
import io.sirix.io.bytepipe.FFILz4Compressor
import io.sirix.service.json.shredder.JsonShredder
import io.sirix.settings.VersioningType
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.nio.file.Paths
import java.io.IOException

/**
 * Tests for [PrefetchingDescendantAxis].
 * 
 * Compares correctness and performance against regular [DescendantAxis].
 */
class PrefetchingDescendantAxisTest {
    
    private val logger = LoggerFactory.getLogger(PrefetchingDescendantAxisTest::class.java)
    
    companion object {
        private val JSON_RESOURCES: Path = Paths.get("src", "test", "resources", "json")
    }
    
    @Before
    fun setUp() {
        // Close any cached database instances first.
        JsonTestHelper.closeEverything()

        // Be defensive: the fixed /tmp test directories can be left behind (or be partially written)
        // if a previous JVM crashed. That can lead to corrupted revision root pages (BufferUnderflow).
        forceRemoveDatabase(JsonTestHelper.PATHS.PATH1.file)
        forceRemoveDatabase(JsonTestHelper.PATHS.PATH2.file)
    }
    
    @After
    fun tearDown() {
        JsonTestHelper.closeEverything()
    }

    private fun forceRemoveDatabase(path: Path) {
        try {
            Databases.removeDatabase(path)
        } catch (_: Exception) {
            // ignore
        }

        // If the Sirix removal couldn't delete everything (e.g., previous crash left broken files),
        // ensure the directory is gone to avoid reusing corrupted files.
        try {
            val file = path.toFile()
            if (file.exists()) {
                file.deleteRecursively()
            }
        } catch (_: SecurityException) {
            // ignore
        } catch (_: IOException) {
            // ignore
        }
    }
    
    /**
     * Test that PrefetchingDescendantAxis produces the same results as DescendantAxis.
     */
    @Test
    fun testCorrectnessAgainstDescendantAxis() {
        // Create a simple JSON document
        createSimpleTestDatabase()
        
        val database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.file)
        database.use { db ->
            val session = db.beginResourceSession(JsonTestHelper.RESOURCE)
            session.use { rs ->
                val rtx = rs.beginNodeReadOnlyTrx()
                rtx.use {
                    rtx.moveToDocumentRoot()
                    
                    // Collect results from regular DescendantAxis
                    val regularResults = mutableListOf<Long>()
                    val regularAxis = DescendantAxis(rtx)
                    while (regularAxis.hasNext()) {
                        regularResults.add(regularAxis.nextLong())
                    }
                    
                    logger.info("Regular axis collected ${regularResults.size} nodes")
                    
                    // Reset and collect results from PrefetchingDescendantAxis
                    rtx.moveToDocumentRoot()
                    val prefetchResults = mutableListOf<Long>()
                    val prefetchAxis = PrefetchingDescendantAxis.create(rtx)
                    while (prefetchAxis.hasNext()) {
                        prefetchResults.add(prefetchAxis.nextLong())
                    }
                    
                    logger.info("Prefetch axis collected ${prefetchResults.size} nodes")
                    
                    // Results should be identical
                    assertEquals("Result counts should match",
                        regularResults.size, prefetchResults.size)
                    assertEquals("Results should be identical in order",
                        regularResults, prefetchResults)
                    
                    logger.info("Correctness test passed: ${regularResults.size} nodes traversed")
                }
            }
        }
    }
    
    /**
     * Test with IncludeSelf option.
     */
    @Test
    fun testIncludeSelf() {
        createSimpleTestDatabase()
        
        val database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.file)
        database.use { db ->
            val session = db.beginResourceSession(JsonTestHelper.RESOURCE)
            session.use { rs ->
                val rtx = rs.beginNodeReadOnlyTrx()
                rtx.use {
                    rtx.moveToDocumentRoot()
                    
                    // With IncludeSelf.YES
                    val withSelf = mutableListOf<Long>()
                    val axis1 = PrefetchingDescendantAxis.create(rtx, IncludeSelf.YES)
                    while (axis1.hasNext()) {
                        withSelf.add(axis1.nextLong())
                    }
                    
                    // With IncludeSelf.NO
                    rtx.moveToDocumentRoot()
                    val withoutSelf = mutableListOf<Long>()
                    val axis2 = PrefetchingDescendantAxis.create(rtx, IncludeSelf.NO)
                    while (axis2.hasNext()) {
                        withoutSelf.add(axis2.nextLong())
                    }
                    
                    // IncludeSelf.YES should have one more element (the root)
                    assertEquals("IncludeSelf.YES should include the starting node",
                        withoutSelf.size + 1, withSelf.size)
                    
                    logger.info("IncludeSelf test passed: with=${withSelf.size}, without=${withoutSelf.size}")
                }
            }
        }
    }
    
    /**
     * Performance comparison test.
     * This test is informational - it doesn't assert anything.
     */
    @Test
    fun testPerformanceComparison() {
        // Create a larger document for meaningful comparison
        createLargerTestDatabase()
        
        val database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.file)
        database.use { db ->
            val session = db.beginResourceSession(JsonTestHelper.RESOURCE)
            session.use { rs ->
                // Warm-up run
                warmUp(rs)
                
                // Time regular DescendantAxis
                val regularTime = timeDescendantAxis(rs, usePrefetching = false)
                
                // Time PrefetchingDescendantAxis
                val prefetchTime = timeDescendantAxis(rs, usePrefetching = true)
                
                println("Performance comparison:")
                println("  Regular DescendantAxis: ${regularTime}ms")
                println("  PrefetchingDescendantAxis: ${prefetchTime}ms")
                println("  Speedup: ${String.format("%.2f", regularTime.toDouble() / prefetchTime)}x")
            }
        }
    }
    
    /**
     * Performance comparison using the larger Chicago dataset.
     * Uses the same database configuration as JsonShredderTest for realistic versioning.
     * Note: This test requires significant memory and may be skipped on low-memory systems.
     */
    @Test
    fun testPerformanceWithChicago() {
        // Skip if running in CI or low memory environment
        val maxMem = Runtime.getRuntime().maxMemory() / (1024 * 1024)
        if (maxMem < 2000) {
            println("Insufficient heap (${maxMem}MB), skipping Chicago test. Need at least 2GB.")
            return
        }
        
        val chicagoPath = Paths.get("src/test/resources/json/cityofchicago.json")
        
        // Check in sirix-core's resources
        val coreChicagoPath = Paths.get("../sirix-core/src/test/resources/json/cityofchicago.json")
        val pathToUse = if (coreChicagoPath.toFile().exists()) coreChicagoPath else chicagoPath
        
        if (!pathToUse.toFile().exists()) {
            println("Chicago JSON file not found, skipping test")
            return
        }
        
        try {
            createChicagoDatabase(pathToUse)
        } catch (e: OutOfMemoryError) {
            println("OutOfMemoryError during shredding, skipping test")
            return
        }
        
        val database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.file)
        database.use { db ->
            val session = db.beginResourceSession(JsonTestHelper.RESOURCE)
            session.use { rs ->
                // Use single transaction like sirix-core test (avoid transaction creation overhead)
                val rtx = rs.beginNodeReadOnlyTrx()
                rtx.use {
                    logger.info("Max node key: ${rtx.maxNodeKey}")
                    
                    // Single comparison run - COLD CACHE for each
                    // Run PrefetchingDescendantAxis FIRST (coldest OS cache)
                    
                    // 1. PrefetchingDescendantAxis with prefetch=4 - COLD CACHE (first run = coldest)
                    Databases.getGlobalBufferManager().clearAllCaches()
                    val prefetchTime = timePrefetchingAxis(rtx, prefetchAhead = 8)
                    logger.info("Prefetching(4) (cold): in ${prefetchTime}ms")
                    
                    // 2. Regular DescendantAxis - COLD CACHE
                    Databases.getGlobalBufferManager().clearAllCaches()
                    val regularTime = timeRegularDescendantAxis(rtx)
                    logger.info("Regular DescendantAxis (cold): in ${regularTime}ms")
                    
                    // 3. PrefetchingDescendantAxis with prefetch=0 - COLD CACHE
                    Databases.getGlobalBufferManager().clearAllCaches()
                    val noPrefetchTime = timePrefetchingAxisNoPrefetch(rtx)
                    logger.info("Prefetching(0) (cold): in ${noPrefetchTime}ms")
                    
                    println("=== CHICAGO PERFORMANCE (COLD CACHE) ===")
                    println("PrefetchingAxis (prefetch=4):   ${prefetchTime}ms (first run = coldest OS cache)")
                    println("Regular DescendantAxis:         ${regularTime}ms")
                    println("PrefetchingAxis (prefetch=0):   ${noPrefetchTime}ms")
                    println("Speedup (prefetch=4): ${String.format("%.2f", regularTime.toDouble() / prefetchTime)}x")
                    println("Speedup (prefetch=0): ${String.format("%.2f", regularTime.toDouble() / noPrefetchTime)}x")
                }
            }
        }
    }
    
    /**
     * Time PrefetchingDescendantAxis with prefetching enabled.
     * Separate function for clear flamegraph stacktraces.
     */
    private fun timePrefetchingAxis(
        rtx: io.sirix.api.json.JsonNodeReadOnlyTrx,
        prefetchAhead: Int,
        prefetchOffsetPages: Int = 0
    ): Long {
        rtx.moveToDocumentRoot()
        // Reset cache counters before run
        io.sirix.cache.ShardedPageCache.resetCacheCounters()
        val startTime = System.currentTimeMillis()
        val axis = PrefetchingDescendantAxis.create(rtx, IncludeSelf.NO, prefetchAhead, prefetchOffsetPages)
        var count = 0
        while (axis.hasNext()) {
            axis.nextLong()
            count++
        }
        val elapsed = System.currentTimeMillis() - startTime
        val cacheHits = io.sirix.cache.ShardedPageCache.getCacheHits()
        val cacheMisses = io.sirix.cache.ShardedPageCache.getCacheMisses()
        logger.info("Prefetching($prefetchAhead): cache hits=$cacheHits, misses=$cacheMisses, hit%=${if (cacheHits + cacheMisses > 0) cacheHits * 100 / (cacheHits + cacheMisses) else 0}")
        logger.info("${axis.getPrefetchStats()}")
        return elapsed
    }
    
    /**
     * Time regular DescendantAxis.
     * Separate function for clear flamegraph stacktraces.
     */
    private fun timeRegularDescendantAxis(rtx: io.sirix.api.json.JsonNodeReadOnlyTrx): Long {
        rtx.moveToDocumentRoot()
        // Reset cache counters before run
        io.sirix.cache.ShardedPageCache.resetCacheCounters()
        val startTime = System.currentTimeMillis()
        val axis = DescendantAxis(rtx)
        var count = 0
        while (axis.hasNext()) {
            axis.nextLong()
            count++
        }
        val elapsed = System.currentTimeMillis() - startTime
        val cacheHits = io.sirix.cache.ShardedPageCache.getCacheHits()
        val cacheMisses = io.sirix.cache.ShardedPageCache.getCacheMisses()
        logger.info("Regular: cache hits=$cacheHits, misses=$cacheMisses, hit%=${if (cacheHits + cacheMisses > 0) cacheHits * 100 / (cacheHits + cacheMisses) else 0}")
        return elapsed
    }
    
    /**
     * Time PrefetchingDescendantAxis with prefetching disabled (prefetchAhead=0).
     * Separate function for clear flamegraph stacktraces.
     */
    private fun timePrefetchingAxisNoPrefetch(rtx: io.sirix.api.json.JsonNodeReadOnlyTrx): Long {
        rtx.moveToDocumentRoot()
        // Reset cache counters before run
        io.sirix.cache.ShardedPageCache.resetCacheCounters()
        val startTime = System.currentTimeMillis()
        val axis = PrefetchingDescendantAxis.create(rtx, IncludeSelf.NO, 0, 0)
        var count = 0
        while (axis.hasNext()) {
            axis.nextLong()
            count++
        }
        val elapsed = System.currentTimeMillis() - startTime
        val cacheHits = io.sirix.cache.ShardedPageCache.getCacheHits()
        val cacheMisses = io.sirix.cache.ShardedPageCache.getCacheMisses()
        logger.info("NoPrefetch: cache hits=$cacheHits, misses=$cacheMisses, hit%=${if (cacheHits + cacheMisses > 0) cacheHits * 100 / (cacheHits + cacheMisses) else 0}")
        return elapsed
    }
    
    private fun warmUp(session: JsonResourceSession) {
        val rtx = session.beginNodeReadOnlyTrx()
        rtx.use {
            rtx.moveToDocumentRoot()
            val axis = DescendantAxis(rtx)
            var count = 0
            while (axis.hasNext()) {
                axis.nextLong()
                count++
            }
            logger.info("Warm-up: traversed $count nodes")
        }
    }
    
    private fun timeDescendantAxis(session: JsonResourceSession, usePrefetching: Boolean, prefetchAhead: Int = 4): Long {
        val rtx = session.beginNodeReadOnlyTrx()
        return rtx.use {
            rtx.moveToDocumentRoot()
            
            val startTime = System.currentTimeMillis()
            
            var count = 0L
            if (usePrefetching) {
                val axis = PrefetchingDescendantAxis.create(rtx, IncludeSelf.NO, prefetchAhead, 0)
                while (axis.hasNext()) {
                    axis.nextLong()
                    count++
                }
            } else {
                val axis = DescendantAxis(rtx)
                while (axis.hasNext()) {
                    axis.nextLong()
                    count++
                }
            }
            
            val elapsed = System.currentTimeMillis() - startTime
            logger.info("${if (usePrefetching) "Prefetching($prefetchAhead)" else "Regular"}: $count nodes in ${elapsed}ms")
            elapsed
        }
    }
    
    private fun createSimpleTestDatabase() {
        val dbConfig = DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.file)
        Databases.createJsonDatabase(dbConfig)
        val database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.file)
        database.use { db ->
            db.createResource(ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build())
            val session = db.beginResourceSession(JsonTestHelper.RESOURCE)
            session.use { rs ->
                val wtx = rs.beginNodeTrx()
                wtx.use {
                    // Create a simple JSON structure
                    val json = """
                        {
                            "name": "test",
                            "items": [1, 2, 3, 4, 5],
                            "nested": {
                                "a": "value1",
                                "b": "value2"
                            }
                        }
                    """.trimIndent()
                    wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json))
                    wtx.commit()
                }
            }
        }
    }
    
    private fun createDatabaseFromFile(jsonPath: Path) {
        val dbConfig = DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.file)
        Databases.createJsonDatabase(dbConfig)
        val database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.file)
        database.use { db ->
            db.createResource(ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build())
            val session = db.beginResourceSession(JsonTestHelper.RESOURCE)
            session.use { rs ->
                val wtx = rs.beginNodeTrx()
                wtx.use {
                    wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(jsonPath))
                    wtx.commit()
                }
            }
        }
    }
    
    private fun createLargerTestDatabase() {
        val dbConfig = DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.file)
        Databases.createJsonDatabase(dbConfig)
        val database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.file)
        database.use { db ->
            db.createResource(ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build())
            val session = db.beginResourceSession(JsonTestHelper.RESOURCE)
            session.use { rs ->
                val wtx = rs.beginNodeTrx()
                wtx.use {
                    // Create a larger JSON structure with many siblings
                    // This should span multiple pages and benefit from prefetching
                    val items = (1..5000).joinToString(",") { 
                        """{"id": $it, "name": "item$it", "value": ${it * 100}}"""
                    }
                    val json = """{"items": [$items]}"""
                    wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json))
                    wtx.commit()
                    logger.info("Created database with ${wtx.getDescendantCount()} descendants")
                }
            }
        }
    }
    
    /**
     * Create a database from the Chicago JSON file using the same configuration as JsonShredderTest.
     * This includes SLIDING_SNAPSHOT versioning with auto-commit every N nodes to create multiple revisions.
     */
    private fun createChicagoDatabase(jsonPath: Path) {
        val dbConfig = DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.file)
        Databases.createJsonDatabase(dbConfig)
        val database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.file)
        database.use { db ->
            // Match JsonShredderTest configuration exactly
            db.createResource(
                ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                    .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                    .buildPathSummary(true)
                    .storeDiffs(true)
                    .storeNodeHistory(false)
                    .storeChildCount(true)
                    .hashKind(HashType.ROLLING)
                    .useTextCompression(false)
                    .storageType(StorageType.FILE_CHANNEL)
                    .useDeweyIDs(false)
                    .byteHandlerPipeline(ByteHandlerPipeline(FFILz4Compressor()))
                    .build()
            )
            val session = db.beginResourceSession(JsonTestHelper.RESOURCE)
            session.use { rs ->
                // Auto-commit every 262_144 << 3 = 2_097_152 nodes (same as JsonShredderTest)
                val wtx = rs.beginNodeTrx(262_144 shl 3)
                wtx.use {
                    logger.info("Starting Chicago shredding...")
                    wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(jsonPath))
                    // Final commit for any remaining nodes
                    wtx.commit()
                    logger.info("Chicago database created with ${wtx.maxNodeKey} max node key")
                }
            }
        }
    }
}

