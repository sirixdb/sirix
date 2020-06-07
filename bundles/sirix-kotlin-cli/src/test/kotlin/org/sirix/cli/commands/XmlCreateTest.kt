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
import org.sirix.access.User
import org.sirix.cli.CliOptions
import org.sirix.exception.SirixIOException
import org.sirix.service.xml.serialize.XmlSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.file.Paths
import java.util.*

internal class XmlCreateTest {

    val LOGGER: Logger = LoggerFactory.getLogger(XmlCreateTest::class.java)

    val TEST_XML_DATA: String = "<xml><foo>Test</foo></xml>"

    val JSON_DATA: String = "{\"json\": \"Test\"}"

    val TEST_USER: User = User("testuser", UUID.fromString("091a12f2-e0dc-4795-abde-4b6b9c2932d1"))

    val TEST_RESOURCE: String = "resource1"

    val TEST_MESSAGE: String = "This is a test commit Message."

    var sirixTestFile = ""




    private fun dataCommandOptions() = listOf(DataCommandOptions(TEST_RESOURCE, TEST_XML_DATA, "", TEST_MESSAGE, TEST_USER),
            DataCommandOptions(TEST_RESOURCE, "", testFilePath(), TEST_MESSAGE, TEST_USER)
    )


    private fun testFilePath() :String {
        return {}::class.java.getResource("/org/sirix/cli/commands/test_data.xml").path
    }



    @BeforeEach
    fun setUp() {
        sirixTestFile = getTestFileCompletePath("create_test_sirix-" + UUID.randomUUID() + ".db")
    }

    @AfterEach
    fun tearDown() {
        LOGGER.info("Removing test Database\n$sirixTestFile")
        if(!File(sirixTestFile).deleteRecursively()) {
            LOGGER.error("Can not delete test Database!\n$sirixTestFile")
        }
    }

    @ParameterizedTest
    @MethodSource("dataCommandOptions")
    fun executeData(dataCommandOptions: DataCommandOptions) {

        // GIVEN
        val create = XmlCreate(CliOptions(sirixTestFile, true), dataCommandOptions)

        // WHEN
        create.execute()

        // THEN

        try {
            assertEquals(DatabaseType.XML, Databases.getDatabaseType(Paths.get(sirixTestFile)))
        } catch (ex: SirixIOException) {
            fail(ex)
        }

        val database = Databases.openXmlDatabase(Paths.get(sirixTestFile))
        assertTrue(database.existsResource(TEST_RESOURCE))
        database.use{
            val manager = database.openResourceManager(TEST_RESOURCE)
            manager.use {
                val out = ByteArrayOutputStream()
                XmlSerializer.XmlSerializerBuilder(manager, out).build().call()

                val s = String (out.toByteArray())
                assertEquals(TEST_XML_DATA, s)
            }
        }
    }
}