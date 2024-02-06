package io.sirix.cli.commands

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import io.sirix.cli.MetaDataEnum
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.io.PrintStream

internal class QueryTest : CliCommandTest() {

    val xmlSimple = """
        Execute Query. Result is:
        <rest:sequence xmlns:rest="https://sirix.io/rest">
          <rest:item>
            <xml rest:id="1">
              <bar rest:id="2">
                <hello rest:id="3">world</hello>
                <helloo rest:id="5">true</helloo>
              </bar>
              <baz rest:id="7">hello</baz>
              <foo rest:id="9">
                <element rest:id="10">bar</element>
                <element rest:id="12" null="true"/>
                <element rest:id="14">2.33</element>
              </foo>
              <tada rest:id="16">
                <element rest:id="17">
                  <foo rest:id="18">bar</foo>
                </element>
                <element rest:id="20">
                  <baz rest:id="21">false</baz>
                </element>
                <element rest:id="23">boo</element>
                <element rest:id="25"/>
                <element rest:id="26"/>
              </tada>
            </xml>
          </rest:item>
        </rest:sequence>
        Query executed (123)
    """.trimIndent();

    val jsonSimple = """
        Execute Query. Result is:
        {
          "foo": [
            "bar",
            null,
            2.33
          ],
          "bar": {
            "hello": "world",
            "helloo": true
          },
          "baz": "hello",
          "tada": [
            {
              "foo": "bar"
            },
            {
              "baz": false
            },
            "boo",
            {},
            []
          ]
        }
        Query executed (123)
    """.trimIndent();



    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(QueryTest::class.java)

        @JvmField
        val sirixQueryTestFileJson = createSirixTestFileName()

        @JvmField
        val sirixQueryTestFileXml = createSirixTestFileName()


        @BeforeAll
        @JvmStatic
        internal fun setup() {
            createJsonDatabase(sirixQueryTestFileJson)
            createXmlDatabase(sirixQueryTestFileXml)
            setupTestDbJsonFromFile(sirixQueryTestFileJson)
            setupTestDbXmlFromFile(sirixQueryTestFileXml)
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            removeTestDatabase(sirixQueryTestFileJson, LOGGER)
            removeTestDatabase(sirixQueryTestFileXml, LOGGER)
        }
    }

    @Test
    fun testJsonSimple() {
        val byteArrayOutputStream = ByteArrayOutputStream()
        System.setOut(PrintStream(byteArrayOutputStream))

        Query(io.sirix.cli.CliOptions(sirixQueryTestFileJson, true), giveASimpleQueryOption()).execute()

        val queryResult = prepareQueryResult(byteArrayOutputStream)

        assertEquals(jsonSimple, queryResult)
    }

    @Test
    fun testXmlSimple() {
        val byteArrayOutputStream = ByteArrayOutputStream()
        System.setOut(PrintStream(byteArrayOutputStream))

        Query(io.sirix.cli.CliOptions(sirixQueryTestFileXml, true), giveASimpleQueryOption()).execute()

        val queryResult = prepareQueryResult(byteArrayOutputStream)

        assertEquals(xmlSimple, queryResult)
    }

    @Test
    fun testJsonXQuery() {
        val byteArrayOutputStream = ByteArrayOutputStream()
        System.setOut(PrintStream(byteArrayOutputStream))

        Query(io.sirix.cli.CliOptions(sirixQueryTestFileJson, true), giveAJSONiqQueryOption()).execute()

        val queryResult = prepareQueryResult(byteArrayOutputStream)

        assertEquals(
            "Execute Query. Result is:\n{\"rest\":[{\"nodeKey\":6}]}\nQuery executed (123)",
            queryResult
        )
    }

    @Test
    fun testXmlXQuery() {
        val byteArrayOutputStream = ByteArrayOutputStream()
        System.setOut(PrintStream(byteArrayOutputStream))

        Query(io.sirix.cli.CliOptions(sirixQueryTestFileXml, true), giveAXQueryOption()).execute()

        val queryResult = prepareQueryResult(byteArrayOutputStream)

        assertEquals(
            "Execute Query. Result is:\n<result nodeKey=\"14\"/>\nQuery executed (123)",
            queryResult
        )
    }

    private fun giveASimpleQueryOption() =
        QueryOptions(
            queryStr = null,
            resource = CliCommandTestConstants.TEST_RESOURCE,
            revision = null,
            revisionTimestamp = null,
            startRevision = null,
            endRevision = null,
            startRevisionTimestamp = null,
            endRevisionTimestamp = null,
            nodeId = null,
            nextTopLevelNodes = null,
            lastTopLevelNodeKey = null,
            startResultSeqIndex = null,
            endResultSeqIndex = null,
            maxLevel = null,
            metaData = MetaDataEnum.NONE,
            prettyPrint = true,
            user = null
        )

    private fun giveAXQueryOption() =
        QueryOptions(
            queryStr = "xquery version \"1.0\";let \$nodeKey := sdb:nodekey(/xml/foo/element[3]) return <result nodeKey=\"{ \$nodeKey }\"/>",
            resource = CliCommandTestConstants.TEST_RESOURCE,
            revision = null,
            revisionTimestamp = null,
            startRevision = null,
            endRevision = null,
            startRevisionTimestamp = null,
            endRevisionTimestamp = null,
            nodeId = null,
            nextTopLevelNodes = null,
            lastTopLevelNodeKey = null,
            startResultSeqIndex = null,
            endResultSeqIndex = null,
            maxLevel = null,
            metaData = MetaDataEnum.NONE,
            prettyPrint = true,
            user = null
        )

    private fun giveAJSONiqQueryOption() =
        QueryOptions(
            queryStr = "let \$nodeKey := sdb:nodekey($$.foo[2]) return {\"nodeKey\": \$nodeKey}",
            resource = CliCommandTestConstants.TEST_RESOURCE,
            revision = null,
            revisionTimestamp = null,
            startRevision = null,
            endRevision = null,
            startRevisionTimestamp = null,
            endRevisionTimestamp = null,
            nodeId = null,
            nextTopLevelNodes = null,
            lastTopLevelNodeKey = null,
            startResultSeqIndex = null,
            endResultSeqIndex = null,
            maxLevel = null,
            metaData = MetaDataEnum.NONE,
            prettyPrint = true,
            user = null
        )
}

private fun prepareQueryResult(outputStream: OutputStream): String {
    return outputStream.toString().trim().replace("\\d+ms".toRegex(), "123")
}


