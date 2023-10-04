package io.sirix.cli.commands

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import io.sirix.access.Databases
import io.sirix.exception.SirixUsageException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class DropTest : CliCommandTest() {

    companion object {
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(DropTest::class.java)
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
        val dropCommand = Drop(giveACliOptions())

        // WHEN
        dropCommand.execute()

        // THEN
        Assertions.assertThrows(SirixUsageException::class.java) {
            Databases.openXmlDatabase(path())
        }
    }
}
