package io.sirix.cli.commands

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.fail
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import io.sirix.access.DatabaseType
import io.sirix.access.Databases
import io.sirix.exception.SirixIOException
import io.sirix.service.json.serialize.JsonSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.StringWriter

internal class JsonCreateTest : CliCommandTest() {

    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(JsonCreateTest::class.java)
    }

    private fun dataCommandOptions() = listOf(
        DataCommandOptions(
            CliCommandTestConstants.TEST_RESOURCE,
            CliCommandTestConstants.TEST_JSON_DATA,
            "",
            CliCommandTestConstants.TEST_COMMIT_MESSAGE,
            CliCommandTestConstants.TEST_USER
        ),
        DataCommandOptions(
            CliCommandTestConstants.TEST_RESOURCE,
            "",
            CliCommandTestConstants.TEST_JSON_DATA_PATH,
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
        val create = JsonCreate(giveACliOptions(), dataCommandOptions)

        // WHEN
        create.execute()

        // THEN
        try {
            assertEquals(DatabaseType.JSON, Databases.getDatabaseType(path()))
        } catch (ex: SirixIOException) {
            fail(ex)
        }

        val database = Databases.openJsonDatabase(path())
        database.use {
            assertTrue(database.existsResource(CliCommandTestConstants.TEST_RESOURCE))

            val manager = database.beginResourceSession(CliCommandTestConstants.TEST_RESOURCE)
            manager.use {
                val out = StringWriter()
                JsonSerializer.newBuilder(manager, out).build().call()

                val s = out.toString()
                assertEquals(CliCommandTestConstants.TEST_JSON_DATA, s)
            }
        }
    }
}
