package io.sirix.rest

import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.parsetools.JsonParser
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.extension.ExtendWith
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.service.json.serialize.JsonSerializer
import io.sirix.service.json.serialize.JsonRecordSerializer
import org.skyscreamer.jsonassert.JSONAssert
import java.io.StringWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import java.util.concurrent.TimeUnit
import io.vertx.junit5.Timeout

private val databaseDirectory: Path = Paths.get(System.getProperty("java.io.tmpdir"), "sirix", "json-path1")

private val json = Paths.get("src", "test", "resources", "json")

/**
 * Test the JSON streaming shredder (indirectly also the JSON serializer).
 */
@DisplayName("KotlinJsonStreamingShredder Tests")
class JsonStreamingShredderTest {

    @BeforeEach
    fun setup() {
        Databases.removeDatabase(databaseDirectory)
    }

    @AfterEach
    fun tearDown() {
        Databases.removeDatabase(databaseDirectory)
    }

    @Nested
    @DisplayName("Simple JSON structures")
    inner class SimpleStructures {
        
        @Test
        @DisplayName("Simple object with string value")
        fun testSimpleObject() {
            testString("""{"foo":"bar"}""")
        }

        @Test
        @DisplayName("Empty object")
        fun testEmptyObject() {
            testString("""{}""")
        }

        @Test
        @DisplayName("Empty array")
        fun testEmptyArray() {
            testString("""[]""")
        }

        @Test
        @DisplayName("Simple array with strings")
        fun testSimpleArray() {
            testString("""["a","b","c"]""")
        }

        @Test
        @DisplayName("Simple array with numbers")
        fun testArrayWithNumbers() {
            testString("""[1,2,3,4,5]""")
        }

        @Test
        @DisplayName("Simple array with mixed types")
        fun testArrayWithMixedTypes() {
            testString("""[1,"two",true,null,3.14]""")
        }

        @Test
        @DisplayName("Object with all primitive types")
        fun testObjectWithAllTypes() {
            testString("""{"string":"hello","number":42,"float":3.14,"bool":true,"nil":null}""")
        }
    }

    @Nested
    @DisplayName("Nested structures")
    inner class NestedStructures {
        
        @Test
        @DisplayName("Nested objects")
        fun testNestedObjects() {
            testString("""{"outer":{"inner":{"deep":"value"}}}""")
        }

        @Test
        @DisplayName("Nested arrays")
        fun testNestedArrays() {
            testString("""[[["deep"]]]""")
        }

        @Test
        @DisplayName("Array of objects")
        fun testArrayOfObjects() {
            testString("""[{"a":1},{"b":2},{"c":3}]""")
        }

        @Test
        @DisplayName("Object with array values")
        fun testObjectWithArrays() {
            testString("""{"arr1":[1,2,3],"arr2":["a","b","c"]}""")
        }

        @Test
        @DisplayName("Complex mixed nesting")
        fun testComplexMixedNesting() {
            testString("""{"a":[{"b":[{"c":"deep"}]}]}""")
        }

        @Test
        @DisplayName("Complex1 - deeply nested with empty arrays")
        fun testComplex1() {
            testString(
                """{"generic":1,"location":{"state":"NY","ddd":{"sssss":[]},"city":"New York","foobar":[[],{"bar":true},[],{}]},"foo":null}"""
            )
        }

        @Test
        @DisplayName("Complex2 - triple nested arrays")
        fun testComplex2() {
            testString(
                """{"generic":1,"location":{"state":"NY","ddd":{"sssss":[[],[],{"tada":true}]},"city":"New York","foobar":[[],{"bar":true},[],{}]},"foo":null}"""
            )
        }

        @Test
        @DisplayName("Complex3 - mixed arrays and objects")
        fun testComplex3() {
            testString(
                """{"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello","tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}"""
            )
        }
    }

    @Nested
    @DisplayName("Edge cases")
    inner class EdgeCases {
        
        @Test
        @DisplayName("Deeply nested empty structures")
        fun testDeeplyNestedEmpty() {
            testString("""{"a":{"b":{"c":{"d":{}}}}}""")
        }

        @Test
        @DisplayName("Multiple empty arrays in sequence")
        fun testMultipleEmptyArrays() {
            testString("""{"a":[],"b":[],"c":[]}""")
        }

        @Test
        @DisplayName("Multiple empty objects in array")
        fun testMultipleEmptyObjects() {
            testString("""[{},{},{}]""")
        }

        @Test
        @DisplayName("Alternating empty arrays and objects")
        fun testAlternatingEmptyContainers() {
            testString("""[{},[],{},[]]""")
        }

        @Test
        @DisplayName("Object with numeric keys")
        fun testNumericKeys() {
            testString("""{"1":"one","2":"two","3":"three"}""")
        }

        @Test
        @DisplayName("Object with special characters in keys")
        fun testSpecialCharKeys() {
            testString("""{"a-b":"hyphen","a_b":"underscore","a.b":"dot"}""")
        }

        @Test
        @DisplayName("Unicode strings")
        fun testUnicodeStrings() {
            testString("""{"emoji":"😀","chinese":"中文","arabic":"العربية"}""")
        }

        @Test
        @DisplayName("Long strings")
        fun testLongStrings() {
            val longString = "x".repeat(10000)
            testString("""{"long":"$longString"}""")
        }

        @Test
        @DisplayName("Many siblings")
        fun testManySiblings() {
            val fields = (1..100).joinToString(",") { "\"f$it\":$it" }
            testString("{$fields}")
        }

        @Test
        @DisplayName("Large array")
        fun testLargeArray() {
            val elements = (1..1000).joinToString(",")
            testString("[$elements]")
        }

        @Test
        @DisplayName("Boolean values")
        fun testBooleanValues() {
            testString("""{"t":true,"f":false}""")
        }

        @Test
        @DisplayName("Null values")
        fun testNullValues() {
            testString("""{"n":null,"arr":[null,null]}""")
        }

        @Test
        @DisplayName("Floating point numbers")
        fun testFloatingPointNumbers() {
            testString("""{"pi":3.14159,"neg":-2.5,"exp":1.23e10}""")
        }

        @Test
        @DisplayName("Integer numbers")
        fun testIntegerNumbers() {
            testString("""{"pos":42,"neg":-17,"zero":0}""")
        }

        @Test
        @DisplayName("Array with trailing object")
        fun testArrayWithTrailingObject() {
            testString("""[1,2,3,{"last":"item"}]""")
        }

        @Test
        @DisplayName("Object ending with array")
        fun testObjectEndingWithArray() {
            testString("""{"first":"value","last":[1,2,3]}""")
        }
    }

    @Nested
    @DisplayName("Real-world JSON files")
    inner class RealWorldFiles {

        @Test
        @DisplayName("Copperfield book JSON")
        fun testCopperfieldBook() {
            testString(Files.readString(json.resolve("copperfield-book.json")))
        }
    }

    @Nested
    @DisplayName("Parent node key verification")
    inner class ParentNodeKeyTests {

        @Test
        @DisplayName("Array children should have correct parent key")
        fun testArrayChildrenParentKey() {
            // Structure similar to chicago dataset: root object with "data" array
            val json = """{"meta":"info","data":[{"id":1},{"id":2},{"id":3}]}"""

            Databases.createJsonDatabase(DatabaseConfiguration(databaseDirectory))
            val database = Databases.openJsonDatabase(databaseDirectory)
            database.use {
                database.createResource(ResourceConfiguration.Builder("parent-key-test").build())
                val manager = database.beginResourceSession("parent-key-test")

                manager.use {
                    val wtx = manager.beginNodeTrx()

                    wtx.use {
                        val parser = JsonParser.newParser()
                        val shredder = KotlinJsonStreamingShredder(wtx, parser)
                        shredder.call().result()
                        parser.handle(Buffer.buffer(json))
                        parser.end()
                        wtx.commit()
                    }

                    // Now verify parent keys using read-only transaction
                    val rtx = manager.beginNodeReadOnlyTrx()
                    rtx.use {
                        // Move to document root (nodeKey 0)
                        rtx.moveToDocumentRoot()

                        // Move to first child - the root object (nodeKey 1)
                        rtx.moveToFirstChild()
                        val rootObjectKey = rtx.nodeKey
                        assertEquals(1L, rootObjectKey, "Root object should have nodeKey 1")

                        // In SirixDB JSON: ObjectKey has its value as CHILD, not sibling
                        // Structure: RootObject -> ObjectKey "meta" -> StringValue "info"
                        //                       -> ObjectKey "data" (sibling of "meta") -> Array (child of "data")
                        rtx.moveToFirstChild() // "meta" object key
                        rtx.moveToRightSibling() // "data" object key (sibling of "meta" key)
                        rtx.moveToFirstChild() // Array (child of "data" object key)

                        val dataArrayKey = rtx.nodeKey
                        println("Data array nodeKey: $dataArrayKey, kind: ${rtx.kind}")

                        // Now check children of the array
                        rtx.moveToFirstChild() // First object in array: {"id":1}
                        val firstChildKey = rtx.nodeKey
                        val firstChildParentKey = rtx.parentKey

                        println("First array child nodeKey: $firstChildKey, parentKey: $firstChildParentKey, kind: ${rtx.kind}")
                        assertEquals(dataArrayKey, firstChildParentKey,
                            "First array child's parent key should be the data array, not document root")

                        // Check second child
                        val movedToSecond = rtx.moveToRightSibling()
                        println("moveToRightSibling returned: $movedToSecond")
                        val secondChildKey = rtx.nodeKey
                        val secondChildParentKey = rtx.parentKey

                        println("Second array child nodeKey: $secondChildKey, parentKey: $secondChildParentKey, kind: ${rtx.kind}")
                        assertEquals(dataArrayKey, secondChildParentKey,
                            "Second array child's parent key should be the data array")

                        // Check third child
                        val movedToThird = rtx.moveToRightSibling()
                        println("moveToRightSibling returned: $movedToThird")
                        val thirdChildKey = rtx.nodeKey
                        val thirdChildParentKey = rtx.parentKey

                        println("Third array child nodeKey: $thirdChildKey, parentKey: $thirdChildParentKey, kind: ${rtx.kind}")
                        assertEquals(dataArrayKey, thirdChildParentKey,
                            "Third array child's parent key should be the data array")

                        // Verify moveToParent works correctly
                        rtx.moveToParent()
                        assertEquals(dataArrayKey, rtx.nodeKey,
                            "moveToParent from array child should return to data array")
                    }
                }
            }
        }

        @Test
        @DisplayName("Deeply nested array children should have correct parent keys")
        fun testDeeplyNestedArrayParentKeys() {
            val json = """{"level1":{"level2":{"items":[1,2,3]}}}"""

            Databases.createJsonDatabase(DatabaseConfiguration(databaseDirectory))
            val database = Databases.openJsonDatabase(databaseDirectory)
            database.use {
                database.createResource(ResourceConfiguration.Builder("deep-parent-test").build())
                val manager = database.beginResourceSession("deep-parent-test")

                manager.use {
                    val wtx = manager.beginNodeTrx()

                    wtx.use {
                        val parser = JsonParser.newParser()
                        val shredder = KotlinJsonStreamingShredder(wtx, parser)
                        shredder.call().result()
                        parser.handle(Buffer.buffer(json))
                        parser.end()
                        wtx.commit()
                    }

                    val rtx = manager.beginNodeReadOnlyTrx()
                    rtx.use {
                        // Navigate to the "items" array
                        // Structure: RootObject -> ObjectKey "level1" -> Object
                        //                                             -> ObjectKey "level2" -> Object
                        //                                                                   -> ObjectKey "items" -> Array
                        rtx.moveToDocumentRoot()
                        rtx.moveToFirstChild() // root object
                        rtx.moveToFirstChild() // "level1" key
                        rtx.moveToFirstChild() // level1 object value (child of key)
                        rtx.moveToFirstChild() // "level2" key
                        rtx.moveToFirstChild() // level2 object value (child of key)
                        rtx.moveToFirstChild() // "items" key
                        rtx.moveToFirstChild() // items array (child of key)

                        val itemsArrayKey = rtx.nodeKey
                        println("Items array nodeKey: $itemsArrayKey, kind: ${rtx.kind}")

                        // Check array children
                        rtx.moveToFirstChild() // value 1
                        println("Array element 1: nodeKey=${rtx.nodeKey}, parentKey=${rtx.parentKey}, kind=${rtx.kind}")
                        assertEquals(itemsArrayKey, rtx.parentKey,
                            "Array element 1's parent should be the items array")

                        val moved2 = rtx.moveToRightSibling() // value 2
                        println("moveToRightSibling returned: $moved2, nodeKey=${rtx.nodeKey}, parentKey=${rtx.parentKey}")
                        assertEquals(itemsArrayKey, rtx.parentKey,
                            "Array element 2's parent should be the items array")

                        val moved3 = rtx.moveToRightSibling() // value 3
                        println("moveToRightSibling returned: $moved3, nodeKey=${rtx.nodeKey}, parentKey=${rtx.parentKey}")
                        assertEquals(itemsArrayKey, rtx.parentKey,
                            "Array element 3's parent should be the items array")

                        // Verify moveToParent chain
                        rtx.moveToParent() // items array
                        assertEquals(itemsArrayKey, rtx.nodeKey)
                    }
                }
            }
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @ExtendWith(VertxExtension::class)
        @DisplayName("Array children should have correct parent key (async path)")
        fun testArrayChildrenParentKeyAsync(vertx: Vertx, testContext: VertxTestContext) {
            // Test the async path used by REST API
            val json = """{"data":[{"id":1},{"id":2},{"id":3}]}"""
            val asyncDbDir = Paths.get(System.getProperty("java.io.tmpdir"), "sirix", "json-async-parent-test-${System.currentTimeMillis()}")

            GlobalScope.launch(vertx.dispatcher()) {
                try {
                    Databases.removeDatabase(asyncDbDir)
                    Databases.createJsonDatabase(DatabaseConfiguration(asyncDbDir))
                    val database = Databases.openJsonDatabase(asyncDbDir)
                    database.use {
                        database.createResource(ResourceConfiguration.Builder("async-parent-test").build())
                        val manager = database.beginResourceSession("async-parent-test")

                        manager.use {
                            val wtx = manager.beginNodeTrx()

                            wtx.use {
                                val parser = JsonParser.newParser()
                                // Use async path with real vertx
                                val shredder = KotlinJsonStreamingShredder(wtx, parser, vertx)

                                launch {
                                    delay(10)
                                    parser.handle(Buffer.buffer(json))
                                    parser.end()
                                }

                                shredder.call().await()
                                wtx.commit()
                            }

                            // Verify parent keys
                            val rtx = manager.beginNodeReadOnlyTrx()
                            rtx.use {
                                rtx.moveToDocumentRoot()
                                rtx.moveToFirstChild() // root object
                                rtx.moveToFirstChild() // "data" key
                                rtx.moveToFirstChild() // data array

                                val dataArrayKey = rtx.nodeKey
                                println("Async test - Data array nodeKey: $dataArrayKey, kind: ${rtx.kind}")

                                rtx.moveToFirstChild() // first element
                                println("Async test - First child: nodeKey=${rtx.nodeKey}, parentKey=${rtx.parentKey}")
                                assertEquals(dataArrayKey, rtx.parentKey, "First child's parent should be the array")

                                rtx.moveToRightSibling()
                                println("Async test - Second child: nodeKey=${rtx.nodeKey}, parentKey=${rtx.parentKey}")
                                assertEquals(dataArrayKey, rtx.parentKey, "Second child's parent should be the array")

                                rtx.moveToRightSibling()
                                println("Async test - Third child: nodeKey=${rtx.nodeKey}, parentKey=${rtx.parentKey}")
                                assertEquals(dataArrayKey, rtx.parentKey, "Third child's parent should be the array")
                            }
                        }
                    }
                    Databases.removeDatabase(asyncDbDir)
                    testContext.completeNow()
                } catch (e: Throwable) {
                    try { Databases.removeDatabase(asyncDbDir) } catch (_: Exception) {}
                    testContext.failNow(e)
                }
            }
        }

        @Test
        @DisplayName("Large array children should all have correct parent key")
        fun testLargeArrayParentKeys() {
            // Create array with 100 children
            val elements = (1..100).joinToString(",") { """{"idx":$it}""" }
            val json = """{"data":[$elements]}"""

            Databases.createJsonDatabase(DatabaseConfiguration(databaseDirectory))
            val database = Databases.openJsonDatabase(databaseDirectory)
            database.use {
                database.createResource(ResourceConfiguration.Builder("large-array-test").build())
                val manager = database.beginResourceSession("large-array-test")

                manager.use {
                    val wtx = manager.beginNodeTrx()

                    wtx.use {
                        val parser = JsonParser.newParser()
                        val shredder = KotlinJsonStreamingShredder(wtx, parser)
                        shredder.call().result()
                        parser.handle(Buffer.buffer(json))
                        parser.end()
                        wtx.commit()
                    }

                    val rtx = manager.beginNodeReadOnlyTrx()
                    rtx.use {
                        // Navigate to the "data" array
                        // Structure: RootObject -> ObjectKey "data" -> Array
                        rtx.moveToDocumentRoot()
                        rtx.moveToFirstChild() // root object
                        rtx.moveToFirstChild() // "data" key
                        rtx.moveToFirstChild() // data array (child of key)

                        val dataArrayKey = rtx.nodeKey
                        println("Data array nodeKey: $dataArrayKey, kind: ${rtx.kind}")

                        // Check first child
                        rtx.moveToFirstChild()
                        var childCount = 1
                        println("First child: nodeKey=${rtx.nodeKey}, parentKey=${rtx.parentKey}, kind=${rtx.kind}")
                        assertEquals(dataArrayKey, rtx.parentKey,
                            "Child $childCount's parent should be the data array")

                        // Check all remaining children via right sibling traversal
                        while (rtx.hasRightSibling()) {
                            rtx.moveToRightSibling()
                            childCount++
                            if (childCount <= 3 || childCount == 100) {
                                println("Child $childCount: nodeKey=${rtx.nodeKey}, parentKey=${rtx.parentKey}")
                            }
                            assertEquals(dataArrayKey, rtx.parentKey,
                                "Child $childCount's parent should be the data array, not ${rtx.parentKey}")
                        }

                        assertEquals(100, childCount, "Should have exactly 100 children")
                        println("Verified parent keys for $childCount array children")
                    }
                }
            }
        }
    }

    @Nested
    @DisplayName("Robustness tests")
    inner class RobustnessTests {
        
        @Test
        @DisplayName("Empty input should not throw")
        fun testEmptyInputDoesNotThrow() {
            // Empty input will result in no nodes, but shouldn't crash
            assertDoesNotThrow {
                Databases.createJsonDatabase(DatabaseConfiguration(databaseDirectory))
                val database = Databases.openJsonDatabase(databaseDirectory)
                database.use {
                    database.createResource(ResourceConfiguration.Builder("empty").build())
                    val manager = database.beginResourceSession("empty")
                    manager.use {
                        val wtx = manager.beginNodeTrx()
                        wtx.use {
                            val parser = JsonParser.newParser()
                            val shredder = KotlinJsonStreamingShredder(wtx, parser)
                            shredder.call().result() // Sync mode returns already-completed Future
                            // Don't send any data, just end
                            parser.end()
                        }
                    }
                }
            }
        }

        @Test
        @DisplayName("Single primitive values")
        fun testSinglePrimitives() {
            // Test that single primitive values work
            testString(""""just a string"""")
        }

        @Test
        @DisplayName("Single number value")
        fun testSingleNumber() {
            testString("42")
        }

        @Test
        @DisplayName("Single boolean value")
        fun testSingleBoolean() {
            testString("true")
        }

        @Test
        @DisplayName("Single null value")
        fun testSingleNull() {
            testString("null")
        }
    }

    /**
     * Async tests that use a real Vertx instance to test the Channel-based async path.
     */
    @Nested
    @ExtendWith(VertxExtension::class)
    @DisplayName("Async path tests (with Vertx)")
    inner class AsyncPathTests {

        // Use unique directory per test to avoid transaction collisions
        private var testId = 0

        private fun nextAsyncDbDir(): Path {
            testId++
            return Paths.get(System.getProperty("java.io.tmpdir"), "sirix", "json-async-test-$testId-${System.currentTimeMillis()}")
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Simple object via async path")
        fun testSimpleObjectAsync(vertx: Vertx, testContext: VertxTestContext) {
            testStringAsync(vertx, testContext, """{"foo":"bar"}""")
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Nested structure via async path")
        fun testNestedAsync(vertx: Vertx, testContext: VertxTestContext) {
            testStringAsync(vertx, testContext, """{"a":{"b":{"c":"deep"}}}""")
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Array via async path")
        fun testArrayAsync(vertx: Vertx, testContext: VertxTestContext) {
            testStringAsync(vertx, testContext, """[1,2,3,"four",true,null]""")
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Large array via async path (tests back-pressure)")
        fun testLargeArrayAsync(vertx: Vertx, testContext: VertxTestContext) {
            val elements = (1..10000).joinToString(",")
            testStringAsync(vertx, testContext, "[$elements]")
        }

        @Test
        @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
        @DisplayName("Object with all types via async path")
        fun testAllTypesAsync(vertx: Vertx, testContext: VertxTestContext) {
            testStringAsync(vertx, testContext, """{"str":"hello","num":42,"bool":true,"nil":null,"arr":[1,2],"obj":{"nested":true}}""")
        }

        private fun testStringAsync(vertx: Vertx, testContext: VertxTestContext, json: String) {
            val asyncDbDir = nextAsyncDbDir()
            GlobalScope.launch(vertx.dispatcher()) {
                try {
                    Databases.removeDatabase(asyncDbDir)
                    Databases.createJsonDatabase(DatabaseConfiguration(asyncDbDir))
                    val database = Databases.openJsonDatabase(asyncDbDir)
                    database.use {
                        database.createResource(ResourceConfiguration.Builder("async-shredded").build())
                        val manager = database.beginResourceSession("async-shredded")

                        manager.use {
                            val wtx = manager.beginNodeTrx()

                            wtx.use {
                                val parser = JsonParser.newParser()
                                // Pass the real vertx instance - this will use the async Channel path
                                val shredder = KotlinJsonStreamingShredder(wtx, parser, vertx)
                                
                                // Start sending data in a separate coroutine, then wait for shredder
                                // This simulates how the REST API works - data streams in while processing
                                // Start sending data in a separate coroutine
                                launch {
                                    // Small delay to ensure handlers are set up
                                    delay(10)
                                    parser.handle(Buffer.buffer(json))
                                    parser.end()
                                }
                                
                                // call() sets up handlers synchronously, returns Future
                                shredder.call().await()
                                wtx.commit()
                            }

                            val writer = StringWriter()
                            writer.use {
                                val serializer = JsonSerializer.Builder(manager, writer).build()
                                serializer.call()
                            }
                            val actual = writer.toString()

                            if (json.trim().let { it.startsWith("{") || it.startsWith("[") }) {
                                JSONAssert.assertEquals(json, actual, true)
                            } else {
                                assertEquals(json.trim(), actual.trim(), "Primitive JSON value mismatch")
                            }
                        }
                    }
                    // Clean up
                    Databases.removeDatabase(asyncDbDir)
                    testContext.completeNow()
                } catch (e: Throwable) {
                    // Try to clean up even on failure
                    try { Databases.removeDatabase(asyncDbDir) } catch (_: Exception) {}
                    testContext.failNow(e)
                }
            }
        }
    }

    /**
     * Tests for JsonRecordSerializer pagination functionality.
     * These tests verify that pagination returns correct siblings and parent metadata.
     */
    @Nested
    @DisplayName("Pagination tests (JsonRecordSerializer)")
    inner class PaginationTests {

        @Test
        @DisplayName("Pagination should return correct siblings after startNodeKey")
        fun testPaginationReturnsSiblings() {
            // Create array with 10 children
            val elements = (1..10).joinToString(",") { """{"idx":$it}""" }
            val json = """{"data":[$elements]}"""

            Databases.createJsonDatabase(DatabaseConfiguration(databaseDirectory))
            val database = Databases.openJsonDatabase(databaseDirectory)
            database.use {
                database.createResource(ResourceConfiguration.Builder("pagination-test").build())
                val manager = database.beginResourceSession("pagination-test")

                manager.use {
                    val wtx = manager.beginNodeTrx()
                    wtx.use {
                        val parser = JsonParser.newParser()
                        val shredder = KotlinJsonStreamingShredder(wtx, parser)
                        shredder.call().result()
                        parser.handle(Buffer.buffer(json))
                        parser.end()
                        wtx.commit()
                    }

                    // Find the data array and its children
                    val rtx = manager.beginNodeReadOnlyTrx()
                    rtx.use {
                        rtx.moveToDocumentRoot()
                        rtx.moveToFirstChild() // root object
                        rtx.moveToFirstChild() // "data" key
                        rtx.moveToFirstChild() // data array
                        val dataArrayKey = rtx.nodeKey
                        println("Data array nodeKey: $dataArrayKey, childCount: ${rtx.childCount}")

                        // Get the 5th child's nodeKey (we'll paginate from here)
                        rtx.moveToFirstChild()
                        repeat(4) { rtx.moveToRightSibling() } // Move to 5th child (index 4)
                        val fifthChildKey = rtx.nodeKey
                        println("5th child nodeKey: $fifthChildKey")

                        // Now use JsonRecordSerializer to get next 3 siblings after the 5th child
                        val writer = StringWriter()
                        val serializer = JsonRecordSerializer.newBuilder(manager, 3, writer)
                            .revisions(intArrayOf(rtx.revisionNumber))
                            .startNodeKey(fifthChildKey)
                            .withNodeKeyAndChildCountMetaData(true)
                            .build()
                        serializer.call()

                        val result = writer.toString()
                        println("Pagination result: $result")

                        // Parse and verify the result
                        // Should contain parent metadata with dataArrayKey and 3 siblings (6th, 7th, 8th children)
                        assert(result.contains("\"nodeKey\":$dataArrayKey")) {
                            "Result should contain parent nodeKey $dataArrayKey but got: $result"
                        }
                        assert(result.contains("\"value\":[")) { "Result should contain value array" }

                        // Count the number of children in the result (should be 3)
                        // The structure is nested: {"key":"idx",...,"value":{"metadata":...,"value":6}}
                        val valueArrayMatch = Regex(""""value":(\d+)\}""").findAll(result)
                        val indices = valueArrayMatch.map { it.groupValues[1].toInt() }.toList()
                        println("Found indices in result: $indices")
                        assertEquals(listOf(6, 7, 8), indices, "Should return children 6, 7, 8 (indices after 5th)")
                    }
                }
            }
        }

        @Test
        @DisplayName("Pagination parent metadata should point to correct parent (array)")
        fun testPaginationParentMetadata() {
            // This test specifically checks the bug where moveToParent() returns wrong parent
            val elements = (1..20).joinToString(",") { """{"idx":$it}""" }
            val json = """{"data":[$elements]}"""

            Databases.createJsonDatabase(DatabaseConfiguration(databaseDirectory))
            val database = Databases.openJsonDatabase(databaseDirectory)
            database.use {
                database.createResource(ResourceConfiguration.Builder("parent-metadata-test").build())
                val manager = database.beginResourceSession("parent-metadata-test")

                manager.use {
                    val wtx = manager.beginNodeTrx()
                    wtx.use {
                        val parser = JsonParser.newParser()
                        val shredder = KotlinJsonStreamingShredder(wtx, parser)
                        shredder.call().result()
                        parser.handle(Buffer.buffer(json))
                        parser.end()
                        wtx.commit()
                    }

                    val rtx = manager.beginNodeReadOnlyTrx()
                    rtx.use {
                        // Navigate to data array
                        rtx.moveToDocumentRoot()
                        rtx.moveToFirstChild() // root object
                        val rootObjectKey = rtx.nodeKey
                        rtx.moveToFirstChild() // "data" key
                        rtx.moveToFirstChild() // data array
                        val dataArrayKey = rtx.nodeKey
                        val dataArrayChildCount = rtx.childCount

                        println("Root object nodeKey: $rootObjectKey")
                        println("Data array nodeKey: $dataArrayKey, childCount: $dataArrayChildCount")

                        // Get 10th child
                        rtx.moveToFirstChild()
                        repeat(9) { rtx.moveToRightSibling() }
                        val tenthChildKey = rtx.nodeKey
                        println("10th child nodeKey: $tenthChildKey, parentKey: ${rtx.parentKey}")

                        // Verify the stored parentKey is correct
                        assertEquals(dataArrayKey, rtx.parentKey,
                            "10th child's stored parentKey should be the data array")

                        // Test pagination - the parent metadata should show the array, NOT the root object
                        val writer = StringWriter()
                        val serializer = JsonRecordSerializer.newBuilder(manager, 5, writer)
                            .revisions(intArrayOf(rtx.revisionNumber))
                            .startNodeKey(tenthChildKey)
                            .withNodeKeyAndChildCountMetaData(true)
                            .build()
                        serializer.call()

                        val result = writer.toString()
                        println("Pagination result: $result")

                        // The metadata should contain the data array's nodeKey, NOT the root object's
                        assert(result.contains("\"nodeKey\":$dataArrayKey")) {
                            "Parent metadata should show data array nodeKey ($dataArrayKey), not root object ($rootObjectKey). Result: $result"
                        }
                        assert(result.contains("\"childCount\":$dataArrayChildCount")) {
                            "Parent metadata should show data array childCount ($dataArrayChildCount). Result: $result"
                        }
                        // Verify it does NOT contain root object as parent
                        assert(!result.startsWith("{\"metadata\":{\"nodeKey\":$rootObjectKey")) {
                            "Parent should NOT be root object ($rootObjectKey). Result: $result"
                        }
                    }
                }
            }
        }

        @Test
        @DisplayName("Pagination on historical revision should work correctly")
        fun testPaginationOnHistoricalRevision() {
            // Create initial data, commit, modify, commit again - then paginate on first revision
            val elements = (1..10).joinToString(",") { """{"idx":$it}""" }
            val json = """{"data":[$elements]}"""

            Databases.createJsonDatabase(DatabaseConfiguration(databaseDirectory))
            val database = Databases.openJsonDatabase(databaseDirectory)
            database.use {
                database.createResource(ResourceConfiguration.Builder("historical-pagination-test").build())
                val manager = database.beginResourceSession("historical-pagination-test")

                manager.use {
                    // Create revision 1
                    val wtx = manager.beginNodeTrx()
                    wtx.use {
                        val parser = JsonParser.newParser()
                        val shredder = KotlinJsonStreamingShredder(wtx, parser)
                        shredder.call().result()
                        parser.handle(Buffer.buffer(json))
                        parser.end()
                        wtx.commit()
                    }

                    val revision1 = manager.mostRecentRevisionNumber
                    println("Created revision $revision1")

                    // Create revision 2 by modifying something
                    val wtx2 = manager.beginNodeTrx()
                    wtx2.use {
                        wtx2.moveToDocumentRoot()
                        wtx2.moveToFirstChild()
                        // Just commit to create a new revision
                        wtx2.commit()
                    }

                    val revision2 = manager.mostRecentRevisionNumber
                    println("Created revision $revision2")

                    // Now test pagination on revision 1 (historical)
                    val rtx = manager.beginNodeReadOnlyTrx(revision1)
                    rtx.use {
                        // Navigate to data array
                        rtx.moveToDocumentRoot()
                        rtx.moveToFirstChild() // root object
                        rtx.moveToFirstChild() // "data" key
                        rtx.moveToFirstChild() // data array
                        val dataArrayKey = rtx.nodeKey
                        println("Historical rev $revision1 - Data array nodeKey: $dataArrayKey")

                        // Get 3rd child
                        rtx.moveToFirstChild()
                        repeat(2) { rtx.moveToRightSibling() }
                        val thirdChildKey = rtx.nodeKey
                        println("3rd child nodeKey: $thirdChildKey, parentKey: ${rtx.parentKey}")

                        // Test pagination on historical revision
                        val writer = StringWriter()
                        val serializer = JsonRecordSerializer.newBuilder(manager, 3, writer)
                            .revisions(intArrayOf(revision1))
                            .startNodeKey(thirdChildKey)
                            .withNodeKeyAndChildCountMetaData(true)
                            .build()
                        serializer.call()

                        val result = writer.toString()
                        println("Historical pagination result: $result")

                        // Parent should be the data array
                        assert(result.contains("\"nodeKey\":$dataArrayKey")) {
                            "Historical pagination parent should be data array ($dataArrayKey). Result: $result"
                        }

                        // Should contain children 4, 5, 6
                        // The structure is nested: {"key":"idx",...,"value":{"metadata":...,"value":4}}
                        val valueArrayMatch = Regex(""""value":(\d+)\}""").findAll(result)
                        val indices = valueArrayMatch.map { it.groupValues[1].toInt() }.toList()
                        println("Found indices in historical result: $indices")
                        assertEquals(listOf(4, 5, 6), indices, "Should return children 4, 5, 6")
                    }
                }
            }
        }
    }

    private fun testString(json: String) {
        Databases.createJsonDatabase(
            DatabaseConfiguration(
                databaseDirectory
            )
        )
        val database = Databases.openJsonDatabase(databaseDirectory)
        database.use {
            database.createResource(ResourceConfiguration.Builder("shredded").build())
            val manager = database.beginResourceSession("shredded")

            manager.use {
                val wtx = manager.beginNodeTrx()

                wtx.use {
                            val parser = JsonParser.newParser()
                    val shredder =
                        KotlinJsonStreamingShredder(wtx, parser)
                    shredder.call().result() // Sync mode returns already-completed Future
                    parser.handle(
                        Buffer.buffer(
                            json
                        )
                    )
                    parser.end()
                    wtx.commit()
                }

                val writer = StringWriter()
                writer.use {
                    val serializer = JsonSerializer.Builder(manager, writer).build()
                    serializer.call()
                }
                val actual = writer.toString()

                // JSONAssert doesn't support standalone primitives (true, false, null, numbers, strings)
                // For these cases, do a simple string comparison
                if (json.trim().let { it.startsWith("{") || it.startsWith("[") }) {
                    JSONAssert.assertEquals(json, actual, true)
                } else {
                    assertEquals(json.trim(), actual.trim(), "Primitive JSON value mismatch")
                }
            }
        }
    }
}
