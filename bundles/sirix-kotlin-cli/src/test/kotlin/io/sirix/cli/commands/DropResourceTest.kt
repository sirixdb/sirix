package io.sirix.cli.commands

import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.cli.commands.CliCommandTestConstants.Companion.RESOURCE_LIST
import io.sirix.cli.commands.CliCommandTestConstants.Companion.TEST_USER
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class DropResourceTest : CliCommandTest() {

    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(DropResourceTest::class.java)
    }

    @BeforeEach
    fun setUp() {
        createXmlDatabase()
    }

    @AfterEach
    fun tearDown() {
        removeTestDatabase(CreateResourceTest.LOGGER)
    }

    @Test
    fun happyPath() {
        // GIVEN
        var database = Databases.openXmlDatabase(path())
        database.use {
            RESOURCE_LIST.forEach {
                database.createResource(ResourceConfiguration.Builder(it).build())
            }
        }

        val dropResourceList = RESOURCE_LIST.filterIndexed { index, _ -> (index % 2) == 0 }
        val dropResourceCommand = DropResource(giveACliOptions(), dropResourceList, TEST_USER)

        // WHEN
        dropResourceCommand.execute()

        // THEN
        database = Databases.openXmlDatabase(path())
        database.use {
            for ((index, value) in RESOURCE_LIST.withIndex()) {
                if (index % 2 == 0) {
                    Assertions.assertFalse(database.existsResource(value))
                } else {
                    Assertions.assertTrue(database.existsResource(value))
                }
            }
        }
    }
}
