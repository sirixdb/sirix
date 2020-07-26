package org.sirix.cli.commands

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.fail
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.exception.SirixIOException
import org.sirix.service.xml.serialize.XmlSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream

internal class XmlCreateTest : CliCommandTest() {

    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(XmlCreateTest::class.java)
    }

    private fun dataCommandOptions() = listOf(
        DataCommandOptions(
            CliCommandTestConstants.TEST_RESOURCE,
            CliCommandTestConstants.TEST_XML_DATA,
            "",
            CliCommandTestConstants.TEST_COMMIT_MESSAGE,
            CliCommandTestConstants.TEST_USER
        ),
        DataCommandOptions(
            CliCommandTestConstants.TEST_RESOURCE,
            "",
            CliCommandTestConstants.TEST_XML_DATA_PATH,
            CliCommandTestConstants.TEST_COMMIT_MESSAGE,
            CliCommandTestConstants.TEST_USER
        )
    )

    @BeforeEach
    fun setUp() {
        super.sirixTestFile = createSirixTestFileName()
    }

    @AfterEach
    fun tearDown() {
        super.removeTestDatabase(LOGGER)
    }

    @ParameterizedTest
    @MethodSource("dataCommandOptions")
    fun executeData(dataCommandOptions: DataCommandOptions) {

        // GIVEN
        val create = XmlCreate(giveACliOptions(), dataCommandOptions)

        // WHEN
        create.execute()

        // THEN

        try {
            assertEquals(DatabaseType.XML, Databases.getDatabaseType(path()))
        } catch (ex: SirixIOException) {
            fail(ex)
        }

        val database = Databases.openXmlDatabase(path())
        database.use {
            assertTrue(database.existsResource(CliCommandTestConstants.TEST_RESOURCE))

            val manager = database.openResourceManager(CliCommandTestConstants.TEST_RESOURCE)
            manager.use {
                val out = ByteArrayOutputStream()
                XmlSerializer.XmlSerializerBuilder(manager, out).build().call()

                val s = String(out.toByteArray())
                assertEquals(CliCommandTestConstants.TEST_XML_DATA, s)
            }
        }
    }
}
