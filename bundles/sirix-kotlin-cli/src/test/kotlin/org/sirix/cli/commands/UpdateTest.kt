package org.sirix.cli.commands

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.sirix.cli.CliOptions
import org.sirix.cli.MetaDataEnum
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
            setupTestDbJsonFromString(sirixQueryTestFileJson, CliCommandTestConstants.TEST_JSON_DATA)
            setupTestDbXmlFromString(sirixQueryTestFileXml, CliCommandTestConstants.TEST_XML_DATA)
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            removeTestDatabase(QueryTest.sirixQueryTestFileJson, QueryTest.LOGGER)
            removeTestDatabase(QueryTest.sirixQueryTestFileXml, QueryTest.LOGGER)
        }

    }


    @Test
    @Disabled
    fun testJsonHappyPath() {
        Query(CliOptions(sirixQueryTestFileJson, true), giveASimpleQueryOption()).execute()

        giveASimpleUpdateObject(
            sirixQueryTestFileJson,
            CliCommandTestConstants.TEST_JSON_DATA_MODIFIED,
            JsonInsertionMode.AS_FIRST_CHILD,
            null,
            1
        ).execute()



        Query(CliOptions(sirixQueryTestFileJson, false), giveASimpleQueryOption()).execute()
    }


    @Test
    @Disabled
    fun testXmlHappyPath() {
        Query(CliOptions(sirixQueryTestFileXml, false), giveASimpleQueryOption()).execute()

        giveASimpleUpdateObject(
            sirixQueryTestFileXml,
            CliCommandTestConstants.TEST_XML_DATA_MODIFIED,
            JsonInsertionMode.AS_FIRST_CHILD,
            null,
            1
        ).execute()

        Query(CliOptions(sirixQueryTestFileXml, false), giveASimpleQueryOption()).execute()
    }


    fun giveASimpleUpdateObject(
        updateTestFile: String,
        updateStr: String,
        jsonInsertionMode: JsonInsertionMode?,
        xmlInsertionMode: XmlInsertionMode?,
        nodeId: Long?

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
            nodeId,
            null,
            CliCommandTestConstants.TEST_USER
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


}
