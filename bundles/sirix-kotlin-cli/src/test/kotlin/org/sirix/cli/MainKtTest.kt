package org.sirix.cli

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class MainKtTest {

    val DB_FILE = "/tmp/sirix.db"

    var args: Array<String> = emptyArray()

    @BeforeEach
    fun setUp() {

        args = arrayOf("-f", DB_FILE, "-v")

    }

    @AfterEach
    fun tearDown() {
    }


    @Test
    fun testCreateCommand() {
        // GIVEN
        val args: Array<String> = arrayOf("-f", "/tmp/sirix.db", "create", "xml")

        // WHEN
        val cliCommand = parseArgs(args)

        // THEN
        assertNotNull(cliCommand)
    }


}