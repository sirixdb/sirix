package org.sirix.cli.commands

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.cli.CliOptions
import org.sirix.exception.SirixIOException
import java.io.File
import java.nio.file.Paths
import java.util.*

internal class CreateTest {

    var sirixTestFile = ""

    @BeforeEach
    fun setUp() {
        sirixTestFile = getTestFileCompletePath("create_test_sirix-" + UUID.randomUUID() + ".db")
    }

    @AfterEach
    fun tearDown() {
        File(sirixTestFile).deleteRecursively()
    }



    @Test
    fun executeXml() {
        // GIVEN
        val create = Create(CliOptions(sirixTestFile, true), DatabaseType.XML)

        // WHEN
        create.execute()

        // THEN
        try {
            assertEquals(DatabaseType.XML, Databases.getDatabaseType(Paths.get(sirixTestFile)))
        } catch (ex: SirixIOException) {
            fail(ex)
        }
    }


    @Test
    fun executeJSON() {
        // GIVEN
        val create = Create(CliOptions(sirixTestFile, true), DatabaseType.JSON)

        // WHEN
        create.execute()

        // THEN
        try {
            assertEquals(DatabaseType.JSON, Databases.getDatabaseType(Paths.get(sirixTestFile)))
        } catch (ex: SirixIOException) {
            fail(ex)
        }
    }

}