package org.sirix.cli.commands.json

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.cli.commands.*
import org.sirix.service.json.shredder.JsonShredder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Paths
import java.util.*

internal class JsonQueryCommandTest : CliCommandTest() {

    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(JsonQueryCommandTest::class.java)

        @JvmField
        val sirixTestFile = getTestFileCompletePath("test_sirix-" + UUID.randomUUID() + ".db")


        @BeforeAll
        @JvmStatic
        internal fun setup() {
            createJsonDatabase(sirixTestFile)
            setupTestDb()
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            removeTestDatabase(sirixTestFile, LOGGER)
        }

    }

    private fun queryCommandOptions() = listOf<QueryCommandOptions>(
        QueryCommandOptions(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )
    )


    @Test
    fun happyPath() {
        // GIVEN
        

        // WHEN

        // THEN
    }
}


private fun setupTestDb() {
    val database =
        Databases.openJsonDatabase(Paths.get(JsonQueryCommandTest.sirixTestFile), CliCommandTestConstants.TEST_USER)
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

