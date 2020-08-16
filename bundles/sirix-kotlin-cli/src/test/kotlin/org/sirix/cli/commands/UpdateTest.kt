package org.sirix.cli.commands

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.sirix.cli.CliOptions
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class UpdateTest : CliCommandTest() {

    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(UpdateTest::class.java)

        @JvmField
        val sirixQueryTestFileJson = createSirixTestFileName()

        @JvmField
        val sirixQueryTestFileXml = createSirixTestFileName()


        @BeforeAll
        @JvmStatic
        internal fun setup() {
            createJsonDatabase(sirixQueryTestFileJson)
            createXmlDatabase(sirixQueryTestFileXml)
            setupTestDbJson(sirixQueryTestFileJson)
            setupTestDbXml(sirixQueryTestFileXml)
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            removeTestDatabase(QueryTest.sirixQueryTestFileJson, QueryTest.LOGGER)
            removeTestDatabase(QueryTest.sirixQueryTestFileXml, QueryTest.LOGGER)
        }

    }


    @Test
    fun testJsonSimple() {
        val jsonUpdateStr = """
                 {
                   "foo": ["bar", null, 2.33],
                   "bar": { "hello": "world", "helloo": true },
                   "baz": "hello",
                   "tada": [{"foo":"bar"},{"baz":false},"boo",{},[]]
                 }
                """.trimIndent()


        giveASimpleUpdateObject(
            sirixQueryTestFileJson,
            jsonUpdateStr,
            JsonInsertionMode.AS_RIGHT_SIBLING,
            null
        ).execute()

    }

    @Test
    fun testXmlSimple() {

    }


    private fun giveASimpleUpdateObject(
        updateTestFile: String,
        updateStr: String,
        jsonInsertionMode: JsonInsertionMode?,
        xmlInsertionMode: XmlInsertionMode?
    ): Update {

        var insertionMode: String? = null

        if (jsonInsertionMode == null) {
            insertionMode = xmlInsertionMode!!.name
        } else {
            insertionMode = jsonInsertionMode!!.name
        }

        return Update(
            CliOptions(updateTestFile, true),
            updateStr,
            CliCommandTestConstants.TEST_RESOURCE, insertionMode.replace('_', '-').toLowerCase(),
            null,
            null,
            CliCommandTestConstants.TEST_USER
        )
    }
}
