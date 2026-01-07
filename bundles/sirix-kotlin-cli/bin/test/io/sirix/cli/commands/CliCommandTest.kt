package io.sirix.cli.commands

import org.slf4j.Logger
import java.nio.file.Path
import java.nio.file.Paths

abstract class CliCommandTest {
    protected var sirixTestFile = ""

    fun path(): Path {
        return Paths.get(sirixTestFile)
    }

    fun removeTestDatabase(LOGGER: Logger) {
        removeTestDatabase(sirixTestFile, LOGGER)
    }

    fun createXmlDatabase() {
        if (sirixTestFile.isEmpty()) {
            sirixTestFile = createSirixTestFileName()
        }
        createXmlDatabase(sirixTestFile)
    }

    fun createJsonDatabase() {
        if (sirixTestFile.isEmpty()) {
            sirixTestFile = createSirixTestFileName()
        }
        createJsonDatabase(sirixTestFile)
    }

    fun giveACliOptions(): io.sirix.cli.CliOptions {
        return io.sirix.cli.CliOptions(sirixTestFile, true)
    }

}
