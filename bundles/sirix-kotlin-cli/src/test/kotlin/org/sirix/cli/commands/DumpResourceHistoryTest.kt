package org.sirix.cli.commands

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.cli.commands.CliCommandTestConstants.Companion.TEST_MESSAGE
import org.sirix.cli.commands.CliCommandTestConstants.Companion.TEST_RESOURCE
import org.sirix.cli.commands.CliCommandTestConstants.Companion.TEST_USER
import org.sirix.cli.commands.CliCommandTestConstants.Companion.TEST_XML_DATA
import org.sirix.service.xml.shredder.XmlShredder
import org.skyscreamer.jsonassert.JSONAssert
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.PrintStream

internal class DumpResourceHistoryTest: CliCommandTest() {

    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(DumpResourceHistoryTest::class.java)
    }

    @BeforeEach
    fun setUp() {
        super.createSirixTestFileName()
    }

    @AfterEach
    fun tearDown() {
        removeTestDatabase(LOGGER)
    }

    @Test
    fun happyPath() {
        // GIVEN
        createXmlDatabase()
        var database = Databases.openXmlDatabase(path(), CliCommandTestConstants.TEST_USER)
        database.use {
            database.createResource(ResourceConfiguration.Builder(TEST_RESOURCE).build())
            val manager = database.openResourceManager(TEST_RESOURCE)
            manager.use {
                val wtx = manager.beginNodeTrx()
                wtx.use {

                    wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(TEST_XML_DATA))
                        wtx.commit(TEST_MESSAGE)
                }
            }
        }
        val dumpResourceHistoryCommand = DumpResourceHistory(giveACliOptions(), TEST_RESOURCE, TEST_USER)

        val byteArrayOutputStream = ByteArrayOutputStream()
        System.setOut(PrintStream(byteArrayOutputStream))

        // WHEN
        dumpResourceHistoryCommand.execute()

        // THEN
        val expectedHistoryDump = """
            {"history":[{"revision":2,"revisionTimestamp":"2020-06-13T15:44:43.922Z","author":"testuser","commitMessage":"This is a test commit Message."},{"revision":1,"revisionTimestamp":"2020-06-13T15:44:43.922Z","author":"testuser","commitMessage":""}]}
        """.trimIndent()

        val historyDump =  byteArrayOutputStream.toString().replace("\"revisionTimestamp\":\"(?!\").+?\"".toRegex(), "\"revisionTimestamp\":\"2020-06-13T15:44:43.922Z\"")
        JSONAssert.assertEquals(expectedHistoryDump, historyDump, false)
    }
}