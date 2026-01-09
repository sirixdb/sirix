package io.sirix.rest

import io.vertx.core.buffer.Buffer
import io.vertx.core.parsetools.JsonParser
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.service.json.serialize.JsonSerializer
import org.skyscreamer.jsonassert.JSONAssert
import java.io.StringWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals

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
                            shredder.call()
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
                    shredder.call()
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
                
                // JSONAssert can't handle standalone primitives (true, false, null, numbers, strings)
                // For those cases, use simple string comparison
                if (json.trim().let { 
                        it.startsWith("{") || it.startsWith("[") 
                    }) {
                    JSONAssert.assertEquals(json, actual, true)
                } else {
                    assertEquals(json.trim(), actual.trim(), "Standalone primitive should match")
                }
            }
        }
    }
}
