package org.sirix.cli.commands

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.sirix.access.Databases
import org.sirix.cli.commands.CliCommandTestConstants.Companion.RESOURCE_LIST
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class CreateResourceTest : CliCommandTest() {

    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(JsonCreateTest::class.java)
    }

    @AfterEach
    fun tearDown() {
        super.removeTestDatabase(LOGGER)
    }

    @Test
    fun happyPathXML() {
        // GIVEN
        createXmlDatabase()
        val createResourceCommand = CreateResource(giveACliOptions(), RESOURCE_LIST, CliCommandTestConstants.TEST_USER)

        // WHEN
        createResourceCommand.execute()

        // THEN
        val database = Databases.openXmlDatabase(path())
        database.use {
            RESOURCE_LIST.forEach {
                Assertions.assertTrue(database.existsResource(it))
            }
        }
    }

    @Test
    fun happyPathJson() {
        // GIVEN
        createJsonDatabase()
        val createResourceCommand = CreateResource(giveACliOptions(), RESOURCE_LIST, CliCommandTestConstants.TEST_USER)

        // WHEN
        createResourceCommand.execute()

        // THEN
        val database = Databases.openJsonDatabase((path()))
        database.use {
            RESOURCE_LIST.forEach {
                Assertions.assertTrue(database.existsResource(it))
            }
        }
    }
}
