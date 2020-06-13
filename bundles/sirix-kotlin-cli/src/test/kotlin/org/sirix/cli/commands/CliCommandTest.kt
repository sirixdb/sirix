package org.sirix.cli.commands

import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.sirix.cli.CliOptions
import org.slf4j.Logger
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

abstract class CliCommandTest {


    protected var sirixTestFile = ""

    fun path(): Path {
        return Paths.get(sirixTestFile)
    }

    fun createSirixTestFileName() {
        sirixTestFile = getTestFileCompletePath("create_test_sirix-" + UUID.randomUUID() + ".db")
    }

    fun removeTestDatabase(LOGGER: Logger) {
        LOGGER.info("Removing test Database\n$sirixTestFile")
        if(!File(sirixTestFile).deleteRecursively()) {
            LOGGER.error("Can not delete test Database!\n$sirixTestFile")
        }
    }


    fun createXmlDatabase() {
        if (!Databases.createXmlDatabase(DatabaseConfiguration(path()))) {
            throw IllegalStateException("Can not create xml Database!\n$sirixTestFile")
        }
    }

    fun createJsonDatabase() {
        if (!Databases.createXmlDatabase(DatabaseConfiguration(path()))) {
            throw IllegalStateException("Can not create xml Database!\n$sirixTestFile")
        }
    }

    fun giveACliOptions() : CliOptions {
        return CliOptions(sirixTestFile, true)
    }


}