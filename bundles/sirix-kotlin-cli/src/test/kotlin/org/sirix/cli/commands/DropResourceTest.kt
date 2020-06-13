package org.sirix.cli.commands

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.cli.commands.CliCommandTestConstants.Companion.RESOURCE_LIST
import org.sirix.cli.commands.CliCommandTestConstants.Companion.TEST_USER
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class DropResourceTest: CliCommandTest() {

    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(DropResourceTest::class.java)
    }


    @BeforeEach
    fun setUp() {
        super.createSirixTestFileName()
    }

    @AfterEach
    fun tearDown() {
        super.removeTestDatabase(CreateResourceTest.LOGGER)
    }


    @Test
    fun happyPath() {
        // GIVEN
        createXmlDatabase()
        var database = Databases.openXmlDatabase(path())
        database.use {
            RESOURCE_LIST.forEach {
                database.createResource(ResourceConfiguration.Builder(it).build())
            }
        }

        val dropResourceList = RESOURCE_LIST.filterIndexed { index, s -> (index % 2) == 0}
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