package org.sirix.cli.commands

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.cli.CliOptions
import org.sirix.cli.MetaDataEnum
import org.sirix.service.json.shredder.JsonShredder
import org.sirix.service.xml.shredder.XmlShredder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.FileInputStream
import java.io.OutputStream
import java.io.PrintStream
import java.nio.file.Paths

internal class QueryTest : CliCommandTest() {

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
            setupTestDbJson()
            setupTestDbXml()
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            removeTestDatabase(sirixQueryTestFileJson, LOGGER)
            removeTestDatabase(sirixQueryTestFileXml, LOGGER)
        }

    }

    private fun queryOptionList() = listOf<QueryOptions>(
        QueryOptions(
            null,
            CliCommandTestConstants.TEST_RESOURCE,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            MetaDataEnum.NONE,
            true,
            null
        )
    )


    @ParameterizedTest
    @MethodSource("queryOptionList")
    fun testJson(queryOptions: QueryOptions) {

        val byteArrayOutputStream = ByteArrayOutputStream()
        System.setOut(PrintStream(byteArrayOutputStream))

        Query(CliOptions(sirixQueryTestFileJson, true), queryOptions).execute()

        val queryResult = prepareQueryResult(byteArrayOutputStream)

        assertEquals(
            "ExecuteQuery.Resultis:{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}Queryexecuted(123)",
            queryResult
        )
    }

    @ParameterizedTest
    @MethodSource("queryOptionList")
    fun testXml(queryOptions: QueryOptions) {
        val byteArrayOutputStream = ByteArrayOutputStream()
        System.setOut(PrintStream(byteArrayOutputStream))

        Query(CliOptions(sirixQueryTestFileXml, true), queryOptions).execute()

        val queryResult = prepareQueryResult(byteArrayOutputStream)

        assertEquals(
            "ExecuteQuery.Resultis:<rest:sequencexmlns:rest=\"https://sirix.io/rest\"><rest:item><xmlrest:id=\"1\"><barrest:id=\"2\"><hellorest:id=\"3\">world</hello><helloorest:id=\"5\">true</helloo></bar><bazrest:id=\"7\">hello</baz><foorest:id=\"9\"><elementrest:id=\"10\">bar</element><elementrest:id=\"12\"null=\"true\"/><elementrest:id=\"14\">2.33</element></foo><tadarest:id=\"16\"><elementrest:id=\"17\"><foorest:id=\"18\">bar</foo></element><elementrest:id=\"20\"><bazrest:id=\"21\">false</baz></element><elementrest:id=\"23\">boo</element><elementrest:id=\"25\"/><elementrest:id=\"26\"/></tada></xml></rest:item></rest:sequence>Queryexecuted(123)",
            queryResult
        )


    }

}


private fun prepareQueryResult(outputStream: OutputStream): String {
    return outputStream.toString().replace("\\s".toRegex(), "").replace("\\d+ms".toRegex(), "123")
}


private fun setupTestDbJson() {
    val database =
        Databases.openJsonDatabase(Paths.get(QueryTest.sirixQueryTestFileJson), CliCommandTestConstants.TEST_USER)
    database.use {
        val resConfig = ResourceConfiguration.Builder(CliCommandTestConstants.TEST_RESOURCE).build()
        if (!database.createResource(resConfig)) {
            throw IllegalStateException("Failed to create resource '${CliCommandTestConstants.TEST_RESOURCE}'!")
        }
        val manager = database.openResourceManager(CliCommandTestConstants.TEST_RESOURCE)
        manager.use {
            val wtx = manager.beginNodeTrx()
            wtx.use {
                wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(Paths.get(CliCommandTestConstants.TEST_JSON_DATA_PATH)))
                wtx.commit(CliCommandTestConstants.TEST_COMMIT_MESSAGE)
            }
        }
    }
}

private fun setupTestDbXml() {
    val database =
        Databases.openXmlDatabase(Paths.get(QueryTest.sirixQueryTestFileXml), CliCommandTestConstants.TEST_USER)
    database.use {
        val resConfig = ResourceConfiguration.Builder(CliCommandTestConstants.TEST_RESOURCE).build()
        if (!database.createResource(resConfig)) {
            throw IllegalStateException("Failed to create resource '${CliCommandTestConstants.TEST_RESOURCE}'!")
        }
        val manager = database.openResourceManager(CliCommandTestConstants.TEST_RESOURCE)
        manager.use {
            val wtx = manager.beginNodeTrx()
            wtx.use {
                wtx.insertSubtreeAsFirstChild(
                    XmlShredder.createFileReader(FileInputStream(CliCommandTestConstants.TEST_XML_DATA_PATH))
                )
                wtx.commit(CliCommandTestConstants.TEST_COMMIT_MESSAGE)
            }
        }
    }
}

